package eth1indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"

	"golang.org/x/sync/errgroup"
)

// IndexFromNode retrieves Ethereum 1.0 blocks from an execution-layer node
// via JSON-RPC and stores them in Bigtable. Blocks are processed concurrently,
// and execution metrics are exported via Prometheus.
//
// Parameters:
//   - bt: Bigtable client used to persist block data.
//   - client: JSON-RPC client to fetch block headers, receipts, and traces.
//   - start: Starting block number (inclusive).
//   - end: Ending block number (inclusive).
//   - concurrency: Maximum number of concurrent workers.
//   - traceMode: Trace mode used when querying block traces ("parity", "geth", etc.).
//   - logger: Logger for progress and debug information.
//
// Returns:
//   - error: if any block retrieval or save operation fails.
func IndexFromNode(bt *db.Bigtable, client execution.ExecutionClient, start, end, concurrency int64, traceMode string, logger *logger.Logger) error {
	ctx := context.Background()
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(int(concurrency))

	progress := newProgressTracker(start, end, logger)

	// Iterate over block numbers and schedule processing
	for i := start; i <= end; i++ {

		blockNumber := i
		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err() // Handle early cancellation
			default:
			}

			startTime := time.Now()

			// Fetch full block from Execution layer JSON-RPC
			bc, timings, err := client.GetBlock(blockNumber, traceMode)
			if err != nil {
				return fmt.Errorf("error getting block: %d from ethereum node err: %w", blockNumber, err)
			}

			// Record RPC and processing durations
			observeBlockMetrics(timings, time.Since(startTime))

			// Save block to Bigtable
			if err := bt.SaveBlock(bc); err != nil {
				return fmt.Errorf("error saving block: %d to bigtable: %w", blockNumber, err)

			}

			// Log progress every 5 seconds
			progress.Tick(blockNumber)
			return nil
		})

	}

	// Wait for all goroutines to complete
	if err := g.Wait(); err != nil {
		return err
	}

	// Update latest processed block number in metadata
	lastBlockInCache, err := bt.GetLastBlockInBlocksTable()
	if err != nil {
		return err
	}

	if end > int64(lastBlockInCache) {
		err := bt.SetLastBlockInBlocksTable(end)

		if err != nil {
			return err
		}
	}
	return nil
}
