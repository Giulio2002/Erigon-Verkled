package verkle

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	verkledb "github.com/ledgerwatch/erigon/cmd/verkle/verkle-db"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
)

func processStorage(coreTx kv.Tx, tx kv.RwTx, writer *verkledb.VerkleTreeWriter, from uint64, cfg OptionsCfg, prevRoot common.Hash) (common.Hash, error) {
	//logInterval := time.NewTicker(30 * time.Second)
	//logPrefix := "processing verkle accounts"

	// Collectd PedersenAccounts
	collectorLookup := etl.NewCollector(verkledb.PedersenHashedCodeLookup, cfg.Tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorLookup.Close()

	storageCursor, err := coreTx.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return common.Hash{}, err
	}
	for k, v, err := storageCursor.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, v, err = storageCursor.Next() {
		if err != nil {
			return common.Hash{}, err
		}
		_, chKey, _, err := changeset.DecodeStorage(k, v)
		if err != nil {
			return common.Hash{}, err
		}
		storageValue, err := coreTx.GetOne(kv.PlainState, chKey)
		if err != nil {
			return common.Hash{}, err
		}
		storageSlot := new(uint256.Int).SetBytes(chKey[28:])
		// Begin
		if len(storageValue) == 0 {
			if err := writer.Delete(vtree.GetTreeKeyStorageSlot(chKey[:20], storageSlot)); err != nil {
				return common.Hash{}, err
			}
		} else {
			var val [32]byte
			verkledb.Int256ToVerkleFormat(storageSlot, val[:])
			if err := writer.Insert(chKey, val[:]); err != nil {
				return common.Hash{}, err
			}
		}
	}

	return writer.CommitVerkleTree(prevRoot)
}
