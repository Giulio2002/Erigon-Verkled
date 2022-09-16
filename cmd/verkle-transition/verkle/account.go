package verkle

import (
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	verkledb "github.com/ledgerwatch/erigon/cmd/verkle/verkle-db"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
)

func processAccounts(coreTx kv.Tx, tx kv.RwTx, writer *VerkleTree, from uint64, cfg OptionsCfg) (common.Hash, error) {
	// TODO: later logging
	//logInterval := time.NewTicker(30 * time.Second)
	//logPrefix := "processing verkle accounts"

	// Collectd PedersenAccounts
	collectorLookup := etl.NewCollector(verkledb.PedersenHashedCodeLookup, cfg.Tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorLookup.Close()

	accountCursor, err := coreTx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return common.Hash{}, err
	}
	var lastBlockNumber uint64
	for k, v, err := accountCursor.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, v, err = accountCursor.Next() {
		if err != nil {
			return common.Hash{}, err
		}
		blockNumber, addressBytes, _, err := changeset.DecodeAccounts(k, v)
		if err != nil {
			return common.Hash{}, err
		}
		encodedAccount, err := coreTx.GetOne(kv.PlainState, addressBytes)
		if err != nil {
			return common.Hash{}, err
		}
		// Begin
		if len(encodedAccount) == 0 {
			err = writer.Insert(vtree.GetTreeKeyVersion(addressBytes), nil)
			return common.Hash{}, err
		}
		var acc accounts.Account
		if err := acc.DecodeForStorage(encodedAccount); err != nil {
			return common.Hash{}, nil
		}
		code, err := coreTx.GetOne(kv.Code, addressBytes)
		if err != nil {
			return common.Hash{}, err
		}
		if err = writer.UpdateAccount(vtree.GetTreeKeyVersion(addressBytes), uint64(len(code)), acc); err != nil {
			return common.Hash{}, err
		}
		lastBlockNumber = blockNumber
	}
	lastRoot, err := verkledb.ReadVerkleRoot(coreTx, lastBlockNumber)
	if err != nil {
		return common.Hash{}, err
	}
	return writer.CommitVerkleTree(lastRoot)
}
