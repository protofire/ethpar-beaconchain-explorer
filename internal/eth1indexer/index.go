package eth1indexer

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc"

	"golang.org/x/sync/errgroup"
)

// IndexFromNode retrieves Ethereum 1.0 blocks via JSON-RPC and stores them into Bigtable.
//
// Parameters:
//   - bt:        Bigtable instance for persistence.
//   - client:    Eth1 JSON-RPC client for fetching full block data.
//   - start:     Starting block number to index (inclusive).
//   - end:       Ending block number to index (inclusive).
//   - concurrency: Number of concurrent block retrieval workers.
//   - traceMode: Trace mode to use when fetching execution traces (e.g., "parity", "geth").
//   - logger:    Custom logger instance for structured logging.
//
// This function spawns a bounded goroutine pool and processes blocks in parallel,
// measuring fetch and write timings via Prometheus metrics.
// Once complete, it updates the metadata with the latest indexed block number.
func IndexFromNode(bt *db.Bigtable, client *rpc.ErigonClient, start, end, concurrency int64, traceMode string, logger *logger.Logger) error {
	ctx := context.Background()
	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(int(concurrency))

	startTs := time.Now()
	lastTickTs := time.Now()

	processedBlocks := int64(0)

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
			defer func() {
				metrics.TaskDuration.WithLabelValues("bt_index_from_node").Observe(time.Since(startTime).Seconds())
			}()

			// Fetch full block
			blockStartTs := time.Now()
			bc, timings, err := client.GetBlock(blockNumber, traceMode)
			if err != nil {
				return fmt.Errorf("error getting block: %v from ethereum node err: %w", blockNumber, err)
			}

			// Record fetch time metrics
			metrics.TaskDuration.WithLabelValues("rpc_el_get_block_headers").Observe(timings.Headers.Seconds())
			metrics.TaskDuration.WithLabelValues("rpc_el_get_block_receipts").Observe(timings.Receipts.Seconds())
			metrics.TaskDuration.WithLabelValues("rpc_el_get_block_traces").Observe(timings.Traces.Seconds())

			// Persist block to Bigtable
			dbStart := time.Now()
			err = bt.SaveBlock(bc)
			if err != nil {
				return fmt.Errorf("error saving block: %v to bigtable: %w", blockNumber, err)

			}

			// Log progress every 100 blocks
			current := atomic.AddInt64(&processedBlocks, 1)
			if current%100 == 0 {
				r := end - start
				if r == 0 {
					r = 1
				}
				perc := float64(blockNumber-start) * 100 / float64(r)

				logger.Infof("retrieved & saved block %v (0x%x) in %v (header: %v, receipts: %v, traces: %v, db: %v)", bc.Number, bc.Hash, time.Since(blockStartTs), timings.Headers, timings.Receipts, timings.Traces, time.Since(dbStart))
				logger.Infof("processed %v blocks in %v (%.1f blocks / sec); sync is %.1f%% complete", current, time.Since(startTs), float64((current))/time.Since(lastTickTs).Seconds(), perc)

				lastTickTs = time.Now()
				atomic.StoreInt64(&processedBlocks, 0)
			}
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
