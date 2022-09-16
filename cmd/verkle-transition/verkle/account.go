package verkle

import (
	"time"

	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	verkledb "github.com/ledgerwatch/erigon/cmd/verkle/verkle-db"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
)

func processAccounts(coreTx kv.Tx, tx kv.RwTx, writer *verkledb.VerkleTreeWriter, from uint64, cfg OptionsCfg) error {
	logInterval := time.NewTicker(30 * time.Second)
	logPrefix := "processing verkle accounts"

	// Collectd PedersenAccounts
	collectorLookup := etl.NewCollector(verkledb.PedersenHashedCodeLookup, cfg.tmpdir, etl.NewSortableBuffer(etl.BufferOptimalSize))
	defer collectorLookup.Close()

	accountCursor, err := coreTx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return err
	}
	for k, v, err := accountCursor.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, v, err = accountCursor.Next() {
		if err != nil {
			return err
		}
		blockNumber, addressBytes, _, err := changeset.DecodeAccounts(k, v)
		if err != nil {
			return err
		}
		encodedAccount, err := coreTx.GetOne(kv.PlainState, addressBytes)
		if err != nil {
			return err
		}
		// Begin

	}
	return nil
}
