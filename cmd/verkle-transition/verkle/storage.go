package verkle

import (
	"encoding/binary"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	verkledb "github.com/ledgerwatch/erigon/cmd/verkle/verkle-db"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
)

func processStorage(coreTx kv.Tx, tx kv.RwTx, writer *VerkleTree, from uint64, cfg OptionsCfg, prevRoot common.Hash) (common.Hash, error) {
	//logInterval := time.NewTicker(30 * time.Second)
	//logPrefix := "processing verkle accounts"
	storageCursor, err := coreTx.CursorDupSort(kv.StorageChangeSet)
	if err != nil {
		return common.Hash{}, err
	}

	executionProgress, err := stages.GetStageProgress(coreTx, stages.Execution)
	if err != nil {
		return common.Hash{}, err
	}

	marker := verkledb.NewVerkleMarker(executionProgress != from+1)
	defer marker.Rollback()

	for k, v, err := storageCursor.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, v, err = storageCursor.Next() {
		if err != nil {
			return common.Hash{}, err
		}
		_, chKey, _, err := changeset.DecodeStorage(k, v)
		if err != nil {
			return common.Hash{}, err
		}

		marked, err := marker.IsMarked(chKey)
		if err != nil {
			return common.Hash{}, err
		}
		if marked {
			continue
		}

		storageValue, err := coreTx.GetOne(kv.PlainState, chKey)
		if err != nil {
			return common.Hash{}, err
		}
		storageSlot := new(uint256.Int).SetBytes(chKey[28:])
		var acc accounts.Account

		has, err := rawdb.ReadAccount(coreTx, common.BytesToAddress(chKey[:20]), &acc)
		if err != nil {
			return common.Hash{}, err
		}
		if !has {
			if err := marker.MarkAsDone(chKey); err != nil {
				return common.Hash{}, err
			}
			continue
		}

		if acc.Incarnation != binary.BigEndian.Uint64(chKey[20:28]) {
			continue
		}

		// Begin
		if len(storageValue) == 0 {
			if err := writer.Delete(vtree.GetTreeKeyStorageSlot(chKey[:20], storageSlot)); err != nil {
				return common.Hash{}, err
			}
		} else {
			var val [32]byte
			verkledb.Int256ToVerkleFormat(storageSlot, val[:])
			key := vtree.GetTreeKeyStorageSlot(chKey[:20], storageSlot)
			if err := writer.Insert(key, val[:]); err != nil {
				return common.Hash{}, err
			}
			if err := verkledb.WritePedersenStorageLookup(tx, chKey[:20], storageSlot, key); err != nil {
				return common.Hash{}, err
			}
		}
		if err := marker.MarkAsDone(chKey); err != nil {
			return common.Hash{}, err
		}
	}
	root, err := writer.CommitVerkleTree(prevRoot)
	if err != nil {
		return common.Hash{}, err
	}
	return root, verkledb.WriteVerkleRoot(tx, executionProgress, root)
}
