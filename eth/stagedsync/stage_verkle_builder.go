package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/cmd/verkle-transition/verkle"
	verkledb "github.com/ledgerwatch/erigon/cmd/verkle/verkle-db"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/params"
)

// SpawnMiningExecStage
// TODO:
// - resubmitAdjustCh - variable is not implemented
func SpawnMiningExecVerkleStage(s *StageState, tx kv.RwTx, cfg MiningExecCfg, ctx context.Context) error {
	if cfg.miningState.MiningBlock.Header.Number.Uint64() < params.AllCliqueProtocolChanges.MartinBlock.Uint64() {
		return nil
	}
	select {
	case cfg.verkleCh <- cfg.chainConfig.MartinBlock.Uint64():
		fmt.Println("lol")
	default:
	}
	verkeDb, err := mdbx.Open("verkledb", log.Root(), false)
	if err != nil {
		return err
	}
	defer verkeDb.Close()
	vTx, err := verkeDb.BeginRw(ctx)
	if err != nil {
		return err
	}

	verkledb.InitDB(vTx)

	defer vTx.Rollback()
	progress, err := stages.GetStageProgress(vTx, verkledb.VerkleTrie)
	if err != nil {
		return err
	}

	root, err := verkledb.ReadVerkleRoot(vTx, progress)
	if err != nil {
		panic(err)
	}
	from := progress
	verkleTree := verkle.NewVerkleTree(vTx, root)

	var accRoot common.Hash
	var storageRoot common.Hash
	if accRoot, err = verkle.ProcessAccounts(tx, vTx, verkleTree, from); err != nil {
		panic(err)
	}

	if storageRoot, err = verkle.ProcessStorage(tx, vTx, verkleTree, from, accRoot); err != nil {
		panic(err)
	}
	cfg.miningState.MiningBlock.Header.Root = storageRoot
	return nil
}
