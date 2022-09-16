package verkledb

import (
	"encoding/binary"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
)

func ReadVerkleIncarnation(tx kv.Tx, address common.Address) (uint64, error) {
	inc, err := tx.GetOne(VerkleIncarnation, address[:])
	if err != nil {
		return 0, err
	}
	if len(inc) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(inc), nil
}

func WriteVerkleRootLookup(tx kv.Tx, address common.Address) (uint64, error) {
	inc, err := tx.GetOne(VerkleIncarnation, address[:])
	if err != nil {
		return 0, err
	}
	if len(inc) == 0 {
		return 0, nil
	}
	return binary.BigEndian.Uint64(inc), nil
}

func ReadVerkleRoot(tx kv.Tx, blockNum uint64) (common.Hash, error) {
	root, err := tx.GetOne(VerkleIncarnation, dbutils.EncodeBlockNumber(blockNum))
	if err != nil {
		return common.Hash{}, err
	}

	return common.BytesToHash(root), nil
}

func WriteVerkleRoot(tx kv.RwTx, blockNum uint64, root common.Hash) error {
	return tx.Put(VerkleRoots, dbutils.EncodeBlockNumber(blockNum), root[:])
}

func WritePedersenStorageLookup(tx kv.RwTx, addr []byte, storageKey *uint256.Int, treeKey []byte) error {
	return tx.Put(PedersenHashedStorageLookup, append(addr, storageKey.ToBig().Bytes()...), treeKey)
}

func WritePedersenCodeLookup(tx kv.RwTx, addr []byte, i uint32, treeKey []byte) error {
	indexBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(indexBytes, i)
	return tx.Put(PedersenHashedCodeLookup, append(addr, indexBytes...), treeKey)
}
