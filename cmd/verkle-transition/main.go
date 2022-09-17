package main

import (
	"context"
	"flag"

	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon/cmd/verkle-transition/verkle"
	verkledb "github.com/ledgerwatch/erigon/cmd/verkle/verkle-db"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

func main() {
	ctx := context.Background()
	mainDb := flag.String("state-chaindata", "chaindata", "path to the chaindata database file")
	verkleDb := flag.String("verkle-chaindata", "out", "path to the output chaindata database file")
	workersCount := flag.Uint("workers", 5, "amount of goroutines")
	tmpdir := flag.String("tmpdir", "/tmp/etl-temp", "amount of goroutines")
	disableLookups := flag.Bool("disable-lookups", false, "disable lookups generation (more compact database)")

	flag.Parse()
	log.Root().SetHandler(log.LvlFilterHandler(log.Lvl(3), log.StderrHandler))

	cfg := verkle.OptionsCfg{
		Ctx:             ctx,
		StateDb:         *mainDb,
		VerkleDb:        *verkleDb,
		WorkersCount:    *workersCount,
		Tmpdir:          *tmpdir,
		DisabledLookups: *disableLookups,
	}
	db, err := mdbx.Open(cfg.StateDb, log.Root(), true)
	if err != nil {
		log.Error("Error while opening database", "err", err.Error())
		return
	}
	defer db.Close()

	vDb, err := mdbx.Open(cfg.VerkleDb, log.Root(), false)
	if err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return
	}
	defer vDb.Close()

	vTx, err := vDb.BeginRw(cfg.Ctx)
	if err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return
	}
	defer vTx.Rollback()

	tx, err := db.BeginRo(cfg.Ctx)
	if err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return
	}
	defer tx.Rollback()

	if err := verkledb.InitDB(vTx); err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return
	}

	from, err := stages.GetStageProgress(vTx, stages.VerkleTrie)
	if err != nil {
		return
	}

	root, _ := verkledb.ReadVerkleRoot(vTx, from)
	verkleTree := verkle.NewVerkleTree(vTx, root)
	var accRoot common.Hash
	var storageRoot common.Hash

	if accRoot, err = verkle.ProcessAccounts(tx, vTx, verkleTree, from); err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return
	}
	if storageRoot, err = verkle.ProcessStorage(tx, vTx, verkleTree, from, accRoot); err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return
	}
	log.Info("Generated", "root", storageRoot)

	if err := vTx.Commit(); err != nil {
		log.Error("Error while opening db transaction", "err", err.Error())
		return
	}
}
