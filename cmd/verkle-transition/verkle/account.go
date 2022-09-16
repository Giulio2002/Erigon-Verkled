package verkle

import (
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	verkledb "github.com/ledgerwatch/erigon/cmd/verkle/verkle-db"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/changeset"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/types/accounts"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/trie/vtree"
)

func getVerkleCodeChunks(address, code []byte) ([][]byte, [][]byte) {
	var chunks [][]byte
	var chunkKeys [][]byte

	// Chunkify contract code and build keys for each chunks and insert them in the tree
	chunkedCode := vtree.ChunkifyCode(code)
	offset := byte(0)
	offsetOverflow := false
	currentKey := vtree.GetTreeKeyCodeChunk(address, uint256.NewInt(0))
	// Write code chunks
	for i := 0; i < len(chunkedCode); i += 32 {
		chunks = append(chunks, common.CopyBytes(chunkedCode[i:i+32]))
		codeKey := common.CopyBytes(currentKey)
		if currentKey[31]+offset < currentKey[31] || offsetOverflow {
			currentKey = vtree.GetTreeKeyCodeChunk(address, uint256.NewInt(uint64(i)/32))
			chunkKeys = append(chunkKeys, codeKey)
			offset = 1
			offsetOverflow = false
		} else {
			codeKey[31] += offset
			chunkKeys = append(chunkKeys, codeKey)
			offset += 1
			// If offset overflows, handle it.
			offsetOverflow = offset == 0
		}
	}
	return chunks, chunkKeys
}

func processAccounts(coreTx kv.Tx, tx kv.RwTx, writer *VerkleTree, from uint64, cfg OptionsCfg) (common.Hash, error) {
	// TODO: later logging
	//logInterval := time.NewTicker(30 * time.Second)
	//logPrefix := "processing verkle accounts"

	accountCursor, err := coreTx.CursorDupSort(kv.AccountChangeSet)
	if err != nil {
		return common.Hash{}, err
	}

	executionProgress, err := stages.GetStageProgress(coreTx, stages.Execution)
	if err != nil {
		return common.Hash{}, err
	}

	marker := verkledb.NewVerkleMarker(executionProgress != from+1)
	defer marker.Rollback()

	for k, v, err := accountCursor.Seek(dbutils.EncodeBlockNumber(from + 1)); k != nil; k, v, err = accountCursor.Next() {
		if err != nil {
			return common.Hash{}, err
		}
		_, addressBytes, _, err := changeset.DecodeAccounts(k, v)
		if err != nil {
			return common.Hash{}, err
		}

		marked, err := marker.IsMarked(addressBytes)
		if err != nil {
			return common.Hash{}, err
		}
		if marked {
			continue
		}
		encodedAccount, err := coreTx.GetOne(kv.PlainState, addressBytes)
		if err != nil {
			return common.Hash{}, err
		}

		if len(encodedAccount) == 0 {
			if err := writer.DeleteAccount(vtree.GetTreeKeyVersion(addressBytes)); err != nil {
				return common.Hash{}, err
			}
			if err := marker.MarkAsDone(addressBytes); err != nil {
				return common.Hash{}, err
			}
			continue
		}
		var acc accounts.Account
		if err := acc.DecodeForStorage(encodedAccount); err != nil {
			return common.Hash{}, nil
		}
		code, err := coreTx.GetOne(kv.Code, acc.CodeHash[:])
		if err != nil {
			return common.Hash{}, err
		}
		if !acc.IsEmptyCodeHash() {
			prevIncarnation, err := verkledb.ReadVerkleIncarnation(tx, common.BytesToAddress(addressBytes))
			if err != nil {
				return common.Hash{}, err
			}
			if prevIncarnation != acc.Incarnation {
				if err := writer.DeleteCode(tx, addressBytes); err != nil {
					return common.Hash{}, err
				}
			}
			chunks, chunkKeys := getVerkleCodeChunks(addressBytes, code)
			for i := range chunks {
				if err := verkledb.WritePedersenCodeLookup(tx, addressBytes, uint32(i), chunkKeys[i]); err != nil {
					return common.Hash{}, err
				}
				if err := writer.Insert(chunkKeys[i], chunks[i]); err != nil {
					return common.Hash{}, err
				}
			}
		}

		if err = writer.UpdateAccount(vtree.GetTreeKeyVersion(addressBytes), uint64(len(code)), acc); err != nil {
			return common.Hash{}, err
		}
		if err := marker.MarkAsDone(addressBytes); err != nil {
			return common.Hash{}, err
		}
	}
	lastRoot, err := verkledb.ReadVerkleRoot(coreTx, executionProgress)
	if err != nil {
		return common.Hash{}, err
	}
	return writer.CommitVerkleTree(lastRoot)
}
