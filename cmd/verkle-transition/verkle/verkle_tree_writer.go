package verkle

import (
	"encoding/binary"

	verkledb "github.com/ledgerwatch/erigon/cmd/verkle/verkle-db"

	"github.com/gballet/go-verkle"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
)

const maxInsert = 200_000

func Int256ToVerkleFormat(x *uint256.Int, buffer []byte) {
	bbytes := x.ToBig().Bytes()
	if len(bbytes) > 0 {
		for i, b := range bbytes {
			buffer[len(bbytes)-i-1] = b
		}
	}
}

func flushVerkleNode(db kv.RwTx, node verkle.VerkleNode) error {
	var err error
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
		err = db.Put(verkledb.VerkleTrie, rootHash[:], encodedNode)
	})
	return err
}

type VerkleTree struct {
	db       kv.RwTx
	node     verkle.VerkleNode
	inserted uint64
}

func NewVerkleTree(db kv.RwTx, tmpdir string, root common.Hash) *VerkleTree {
	var rootNode verkle.VerkleNode
	if root != (common.Hash{}) {
		nodeEncoded, err := db.GetOne(verkledb.VerkleTrie, root[:])
		if err != nil {
			panic(err)
		}

		rootNode, err = verkle.ParseNode(nodeEncoded, 0, root[:])
		if err != nil {
			panic(err)
		}
	} else {
		rootNode = verkle.New()
	}
	return &VerkleTree{
		db:   db,
		node: rootNode,
	}
}

func (v *VerkleTree) UpdateAccount(versionKey []byte, codeSize uint64, acc accounts.Account) error {
	resolver := func(key []byte) ([]byte, error) {
		return v.db.GetOne(verkledb.VerkleTrie, key)
	}
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
	if err := v.node.Insert(versionKey, []byte{0}, resolver); err != nil {
		return err
	}

	if err := v.node.Insert(nonceKey[:], nonce[:], resolver); err != nil {
		return err
	}
	if err := v.node.Insert(codeHashKey[:], acc.CodeHash[:], resolver); err != nil {
		return err
	}
	if err := v.node.Insert(balanceKey[:], balance[:], resolver); err != nil {
		return err
	}
	if err := v.node.Insert(codeSizeKey[:], cs[:], resolver); err != nil {
		return err
	}
	v.inserted += 4
	if v.inserted > maxInsert {
		flushVerkleNode(v.db, v.node)
		v.inserted = 0
	}
	return nil
}

func (v *VerkleTree) Insert(key, value []byte) error {
	resolver := func(key []byte) ([]byte, error) {
		return v.db.GetOne(verkledb.VerkleTrie, key)
	}
	v.inserted++
	if v.inserted > maxInsert {
		flushVerkleNode(v.db, v.node)
		v.inserted = 0
	}

	return v.node.Insert(key, value, resolver)
}

func (v *VerkleTree) Delete(key []byte) error {
	resolver := func(key []byte) ([]byte, error) {
		return v.db.GetOne(verkledb.VerkleTrie, key)
	}
	v.inserted++
	if v.inserted > maxInsert {
		flushVerkleNode(v.db, v.node)
		v.inserted = 0
	}

	return v.node.Delete(key, resolver)
}

func (v *VerkleTree) WriteContractCodeChunks(codeKeys [][]byte, chunks [][]byte) error {

	for i, codeKey := range codeKeys {
		if err := v.Insert(codeKey, chunks[i]); err != nil {
			return err
		}
		v.inserted++
	}
	if v.inserted > maxInsert {
		flushVerkleNode(v.db, v.node)
		v.inserted = 0
	}
	return nil
}

func (v *VerkleTree) CommitVerkleTree(root common.Hash) (common.Hash, error) {
	return v.node.ComputeCommitment().Bytes(), flushVerkleNode(v.db, v.node)
}
