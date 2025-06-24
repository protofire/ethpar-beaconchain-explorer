package eth1indexer

import (
	"bytes"
	"context"
	"fmt"
	"math/big"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc"
)

// HandleChainReorgs checks the latest blocks in the chain against what's stored
// in the database to detect chain reorganizations (reorgs).
// If a reorg is detected, it rolls back and deletes inconsistent blocks.
//
// Parameters:
//   - bt:    Bigtable interface used to access and modify block storage.
//   - client: JSON-RPC client for communicating with the execution client.
//   - depth: Number of blocks from the tip to check for potential reorgs.
//   - log:   Logger for emitting debug and warning messages.
//
// Returns:
//   - error: if any unexpected issue occurs during validation or rollback.
func HandleChainReorgs(bt *db.Bigtable, client *rpc.ErigonClient, depth int, log *logger.Logger) error {
	ctx := context.Background()

	latestNodeBlock, err := client.GetNativeClient().BlockByNumber(ctx, nil)
	if err != nil {
		log.Errorf("failed to get latest block from node: %v", err)
		return err
	}
	latest := latestNodeBlock.NumberU64()

	if depth > int(latest) {
		depth = int(latest)
	}

	start := latest - uint64(depth)

	for height := start; height <= latest; height++ {
		reorgDetected, err := fetchAndCompareBlock(ctx, bt, client, height, log)
		if err != nil {
			return err
		}
		if reorgDetected {
			log.Warnf("reorg confirmed at height %d, initiating rollback", height)
			return rollbackFork(bt, height, latest, log)
		}
	}

	return nil
}

// fetchAndCompareBlock loads a block header from the execution client and
// a stored block from the database at the given height, and compares their hashes.
//
// Parameters:
//   - ctx:    Context for cancellation and timeout propagation.
//   - bt:     Bigtable instance to retrieve the block from storage.
//   - client: Execution-layer RPC client to fetch the block header.
//   - height: Block height to check.
//   - log:    Logger for debug and warning messages.
//
// Returns:
//   - bool:   true if a reorg is detected (hash mismatch), false otherwise.
//   - error:  if any I/O or RPC error occurs.
func fetchAndCompareBlock(ctx context.Context, bt *db.Bigtable, client *rpc.ErigonClient, height uint64, log *logger.Logger) (bool, error) {
	nodeHeader, err := client.GetNativeClient().HeaderByNumber(ctx, big.NewInt(int64(height)))
	if err != nil {
		log.Errorf("failed to fetch node block at height %d: %v", height, err)
		return false, err
	}

	dbBlock, err := bt.GetBlockFromBlocksTable(height)
	if err != nil {
		if err == db.ErrBlockNotFound {
			log.Infof("block %d not found in DB, skipping reorg check", height)
			return false, nil
		}
		log.Errorf("failed to fetch DB block at height %d: %v", height, err)
		return false, err
	}

	if !bytes.Equal(nodeHeader.Hash().Bytes(), dbBlock.Hash) {
		log.Warnf("mismatch at height %d — node hash: %x, db hash: %x", height, nodeHeader.Hash().Bytes(), dbBlock.Hash)
		return true, nil
	}

	log.Debugf("block %d OK — hash: %x", height, dbBlock.Hash)
	return false, nil
}

// rollbackFork resets internal metadata and deletes all blocks from the fork point
// up to the latest known block in the node. This ensures the DB reflects the canonical chain.
//
// Parameters:
//   - bt:         Bigtable instance to update metadata and delete blocks.
//   - forkHeight: Height at which the reorg was detected.
//   - latest:     Latest known block height from the node.
//   - log:        Logger for progress and error reporting.
//
// Returns:
//   - error: if any operation (metadata update or block deletion) fails.
func rollbackFork(bt *db.Bigtable, forkHeight, latest uint64, log *logger.Logger) error {
	if forkHeight > 0 {
		previous := int64(forkHeight - 1)
		log.Infof("resetting metadata to block %d", previous)

		if err := bt.SetLastBlockInBlocksTable(previous); err != nil {
			return fmt.Errorf("failed to update last block in blocks table: %w", err)
		}
		if err := bt.SetLastBlockInDataTable(previous); err != nil {
			return fmt.Errorf("failed to update last block in data table: %w", err)
		}
	}

	for height := forkHeight; height <= latest; height++ {
		dbBlock, err := bt.GetBlockFromBlocksTable(height)
		if err != nil {
			if err == db.ErrBlockNotFound {
				log.Infof("stopped deletion at height %d — block not found in DB", height)
				return nil
			}
			return fmt.Errorf("error fetching block at height %d: %w", height, err)
		}

		log.Infof("deleting block %d with hash %x", dbBlock.Number, dbBlock.Hash)
		if err := bt.DeleteBlock(dbBlock.Number, dbBlock.Hash); err != nil {
			return fmt.Errorf("failed to delete block at height %d: %w", dbBlock.Number, err)
		}
	}

	return nil
}