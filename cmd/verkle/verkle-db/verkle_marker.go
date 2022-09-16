package verkledb

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/mdbx"
)

type VerkleMarker struct {
	db kv.RwDB
	tx kv.RwTx
}

//nolint:gocritic
func NewVerkleMarker(enabled bool) *VerkleMarker {
	if !enabled {
		return &VerkleMarker{}
	}
	markedSlotsDb, err := mdbx.NewTemporaryMdbx()
	if err != nil {
		panic(err)
	}

	tx, err := markedSlotsDb.BeginRw(context.TODO())
	if err != nil {
		panic(err)
	}

	return &VerkleMarker{
		db: markedSlotsDb,
		tx: tx,
	}
}

func (v *VerkleMarker) MarkAsDone(key []byte) error {
	if v.tx == nil {
		return nil
	}
	return v.tx.Put(kv.Headers, key, []byte{0})
}

func (v *VerkleMarker) IsMarked(key []byte) (bool, error) {
	if v.tx == nil {
		return false, nil
	}
	return v.tx.Has(kv.Headers, key)
}

func (v *VerkleMarker) Rollback() {
	if v.db == nil {
		return
	}
	v.tx.Rollback()
	v.db.Close()
}
