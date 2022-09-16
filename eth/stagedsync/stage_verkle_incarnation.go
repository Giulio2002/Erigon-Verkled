package stagedsync

import (
	"context"
	"encoding/binary"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common/cmp"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	verkledb "github.com/ledgerwatch/erigon/cmd/verkle/verkle-db"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types/accounts"
)

type VerkleIncarnationCfg struct {
	db     kv.RwDB
	tmpdir string
}

func StageVerkleIncarnationCfg(
	db kv.RwDB,
	tmpdir string,
) TxLookupCfg {
	return TxLookupCfg{
		db:     db,
		tmpdir: tmpdir,
	}
}

func SpawnVerkleIncarnation(s *StageState, tx kv.RwTx, toBlock uint64, cfg TxLookupCfg, ctx context.Context) (err error) {
	quitCh := ctx.Done()
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	logPrefix := s.LogPrefix()
	endBlock, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}
	if toBlock > 0 {
		endBlock = libcommon.Min(endBlock, toBlock)
	}

	startBlock := s.BlockNumber

	if startBlock > 0 {
		startBlock++
	}
	// etl.Transform uses ExtractEndKey as exclusive bound, therefore endBlock + 1
	if err = verkleIncarnation(logPrefix, tx, startBlock, endBlock+1, quitCh, cfg); err != nil {
		return fmt.Errorf("txnLookupTransform: %w", err)
	}

	if cfg.isBor {
		if err = borTxnLookupTransform(logPrefix, tx, startBlock, endBlock+1, quitCh, cfg); err != nil {
			return fmt.Errorf("borTxnLookupTransform: %w", err)
		}
	}

	if err = s.Update(tx, endBlock); err != nil {
		return err
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

// txnLookupTransform - [startKey, endKey)
func verkleIncarnation(logPrefix string, tx kv.RwTx, blockFrom, blockTo uint64, quitCh <-chan struct{}, cfg TxLookupCfg) error {
	return etl.Transform(logPrefix, tx, kv.AccountChangeSet, verkledb.VerkleIncarnation, cfg.tmpdir, func(k, v []byte, next etl.ExtractNextFunc) error {
		_, addressBytes, _, err := changeset.DecodeAccounts(k, v)
		if err != nil {
			return err
		}
		var acc accounts.Account
		var has bool
		if has, err = rawdb.ReadAccount(tx, common.BytesToAddress(addressBytes), &acc); err != nil {
			return err
		}
		if !has {
			return nil
		}
		val := make([]byte, 8)
		binary.BigEndian.PutUint64(val, acc.Incarnation)

		if err != nil {
			return err
		}
		return next(k, addressBytes, val)
	}, etl.IdentityLoadFunc, etl.TransformArgs{
		Quit:            quitCh,
		ExtractStartKey: dbutils.EncodeBlockNumber(blockFrom),
		ExtractEndKey:   dbutils.EncodeBlockNumber(blockTo),
		LogDetailsExtract: func(k, v []byte) (additionalLogArguments []interface{}) {
			return []interface{}{"block", binary.BigEndian.Uint64(k)}
		},
	})
}
