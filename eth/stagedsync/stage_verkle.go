package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/verkle-transition/verkle"
	verkledb "github.com/ledgerwatch/erigon/cmd/verkle/verkle-db"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/log/v3"
)

type VerkleCfg struct {
	db     kv.RwDB
	coreDb kv.RwDB
	cfg    *params.ChainConfig
	tmpdir string
}

func StageVerkleCfg(
	db kv.RwDB,
	coreDb kv.RwDB,
	cfg *params.ChainConfig,
	tmpdir string,
) VerkleCfg {
	return VerkleCfg{
		db:     db,
		coreDb: db,
		tmpdir: tmpdir,
		cfg:    cfg,
	}
}

func SpawnVerkle(s *StageState, tx kv.RwTx, toBlock uint64, cfg VerkleCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.coreDb.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	endBlock, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}
	fmt.Println("aaa")

	vTx, err := cfg.db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer vTx.Commit()

	progress, err := stages.GetStageProgress(vTx, verkledb.VerkleTrie)
	if endBlock < cfg.cfg.MartinBlock.Uint64() {
		return s.Update(tx, progress)
	}

	root, err := verkledb.ReadVerkleRoot(vTx, progress)
	if err != nil {
		panic(err)
	}

	verkleTree := verkle.NewVerkleTree(vTx, root)
	var accRoot common.Hash
	var storageRoot common.Hash

	if err = s.Update(tx, endBlock); err != nil {
		return err
	}

	if accRoot, err = verkle.ProcessAccounts(tx, vTx, verkleTree, progress); err != nil {
		panic(err)
	}

	if storageRoot, err = verkle.ProcessStorage(tx, vTx, verkleTree, progress, accRoot); err != nil {
		panic(err)
	}
	// TODO: end here

	log.Info("Verkle tree is synced up", "root", storageRoot, "from", progress)

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}
