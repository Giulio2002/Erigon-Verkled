package verkledb

import (
	"context"
	"encoding/binary"
	"time"

	"github.com/anacrolix/sync"
	"github.com/gballet/go-verkle"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
	"github.com/ledgerwatch/log/v3"
)

func Int256ToVerkleFormat(x *uint256.Int, buffer []byte) {
	bbytes := x.ToBig().Bytes()
	if len(bbytes) > 0 {
		for i, b := range bbytes {
			buffer[len(bbytes)-i-1] = b
		}
	}
}

func FlushVerkleNode(db kv.RwTx, node verkle.VerkleNode, logInterval *time.Ticker, key []byte) error {
	var err error
	totalInserted := 0
	node.(*verkle.InternalNode).Flush(func(node verkle.VerkleNode) {
		if err != nil {
			return
		}
		var encodedNode []byte

		rootHash := node.ComputeCommitment().Bytes()
		encodedNode, err = node.Serialize()
		if err != nil {
			return
		}
		err = db.Put(VerkleTrie, rootHash[:], encodedNode)
		totalInserted++
		select {
		case <-logInterval.C:
			log.Info("Flushing Verkle nodes", "inserted", totalInserted, "key", common.Bytes2Hex(key))
		default:
		}
	})
	return err
}

type VerkleTreeWriter struct {
	db        kv.RwTx
	collector *etl.Collector
	mu        sync.Mutex
	tmpdir    string
}

func NewVerkleTreeWriter(db kv.RwTx, tmpdir string) *VerkleTreeWriter {
	return &VerkleTreeWriter{
		db:        db,
		collector: etl.NewCollector("verkleTreeWriterLogPrefix", tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize*8)),
		tmpdir:    tmpdir,
	}
}

func (v *VerkleTreeWriter) UpdateAccount(versionKey []byte, codeSize uint64, acc accounts.Account) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	var codeHashKey, nonceKey, balanceKey, codeSizeKey, nonce, balance, cs [32]byte
	copy(codeHashKey[:], versionKey[:31])
	copy(nonceKey[:], versionKey[:31])
	copy(balanceKey[:], versionKey[:31])
	copy(codeSizeKey[:], versionKey[:31])
	codeHashKey[31] = vtree.CodeKeccakLeafKey
	nonceKey[31] = vtree.NonceLeafKey
	balanceKey[31] = vtree.BalanceLeafKey
	codeSizeKey[31] = vtree.CodeSizeLeafKey
	// Process values
	Int256ToVerkleFormat(&acc.Balance, balance[:])
	binary.LittleEndian.PutUint64(nonce[:], acc.Nonce)
	binary.LittleEndian.PutUint64(cs[:], codeSize)

	// Insert in the tree
	if err := v.collector.Collect(versionKey, []byte{0}); err != nil {
		return err
	}

	if err := v.collector.Collect(nonceKey[:], nonce[:]); err != nil {
		return err
	}
	if err := v.collector.Collect(codeHashKey[:], acc.CodeHash[:]); err != nil {
		return err
	}
	if err := v.collector.Collect(balanceKey[:], balance[:]); err != nil {
		return err
	}
	if err := v.collector.Collect(codeSizeKey[:], cs[:]); err != nil {
		return err
	}
	return nil
}

func (v *VerkleTreeWriter) Insert(key, value []byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.collector.Collect(key, value)
}

func (v *VerkleTreeWriter) Delete(key []byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	return v.collector.Collect(key, nil)
}

func (v *VerkleTreeWriter) WriteContractCodeChunks(codeKeys [][]byte, chunks [][]byte) error {
	v.mu.Lock()
	defer v.mu.Unlock()

	for i, codeKey := range codeKeys {
		if err := v.collector.Collect(codeKey, chunks[i]); err != nil {
			return err
		}
	}
	return nil
}

func (v *VerkleTreeWriter) CommitVerkleTreeFromScratch() (common.Hash, error) {
	v.mu.Lock()
	defer v.mu.Unlock()

	if err := v.db.ClearBucket(VerkleTrie); err != nil {
		return common.Hash{}, err
	}

	verkleCollector := etl.NewCollector(VerkleTrie, v.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer verkleCollector.Close()

	root := verkle.New()

	logInterval := time.NewTicker(30 * time.Second)
	if err := v.collector.Load(v.db, VerkleTrie, func(k []byte, v []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if err := root.InsertOrdered(common.CopyBytes(k), common.CopyBytes(v), func(node verkle.VerkleNode) {
			rootHash := node.ComputeCommitment().Bytes()
			encodedNode, err := node.Serialize()
			if err != nil {
				panic(err)
			}
			if err := verkleCollector.Collect(rootHash[:], encodedNode); err != nil {
				panic(err)
			}
			select {
			case <-logInterval.C:
				log.Info("[Verkle] Assembling Verkle Tree", "key", common.Bytes2Hex(k))
			default:
			}
		}); err != nil {
			return err
		}
		return next(k, nil, nil)
	}, etl.TransformArgs{Quit: context.Background().Done()}); err != nil {
		return common.Hash{}, err
	}

	log.Info("Started Verkle Tree Flushing")
	return root.ComputeCommitment().Bytes(), verkleCollector.Load(v.db, VerkleTrie, etl.IdentityLoadFunc, etl.TransformArgs{Quit: context.Background().Done(),
		LogDetailsLoad: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"key", common.Bytes2Hex(k)}
		}})
}

func (v *VerkleTreeWriter) CommitVerkleTree(root common.Hash) (common.Hash, error) {
	resolverFunc := func(root []byte) ([]byte, error) {
		return v.db.GetOne(VerkleTrie, root)
	}

	v.mu.Lock()
	defer v.mu.Unlock()

	verkleCollector := etl.NewCollector(VerkleTrie, v.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer verkleCollector.Close()

	var rootNode verkle.VerkleNode
	if root != (common.Hash{}) {
		nodeEncoded, err := v.db.GetOne(VerkleTrie, root[:])
		if err != nil {
			return common.Hash{}, err
		}

		rootNode, err = verkle.ParseNode(nodeEncoded, 0, root[:])
		if err != nil {
			return common.Hash{}, err
		}
	} else {
		rootNode = verkle.New()
	}

	insertionBeforeFlushing := 2_000_000 // 2M node to flush at a time
	insertions := 0
	logInterval := time.NewTicker(30 * time.Second)
	if err := v.collector.Load(v.db, VerkleTrie, func(key []byte, value []byte, _ etl.CurrentTableReader, next etl.LoadNextFunc) error {
		if len(value) == 0 {
			if err := rootNode.Delete(common.CopyBytes(key), resolverFunc); err != nil {
				return err
			}
		} else {
			if err := rootNode.Insert(common.CopyBytes(key), common.CopyBytes(value), resolverFunc); err != nil {
				return err
			}
		}
		insertions++
		if insertions > insertionBeforeFlushing {
			if err := FlushVerkleNode(v.db, rootNode, logInterval, key); err != nil {
				return err
			}
			insertions = 0
		}
		return next(key, nil, nil)
	}, etl.TransformArgs{Quit: context.Background().Done()}); err != nil {
		return common.Hash{}, err
	}
	commitment := rootNode.ComputeCommitment().Bytes()
	return common.BytesToHash(commitment[:]), FlushVerkleNode(v.db, rootNode, logInterval, nil)
}
