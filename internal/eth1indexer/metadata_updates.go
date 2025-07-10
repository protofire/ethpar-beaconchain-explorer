package eth1indexer

import (
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
)

// ProcessMetadataUpdates processes metadata entries discovered in Bigtable,
// retrieves current ETH1 balances from the execution node, and persists
// the results back to Bigtable.
//
// It scans metadata keys with the given prefix starting from the provided key,
// paginates through entries in batches, and for each entry:
//   - fetches the latest balance using the Erigon RPC client
//   - stores the result in Bigtable
//
// Parameters:
//   - bt: Bigtable client instance used for reading and writing metadata.
//   - client: Erigon RPC client used to fetch current balances.
//   - prefix: Metadata key prefix to scan.
//   - batchSize: Number of entries to process per batch.
//   - iterations: Max number of batches to process; -1 means no limit.
//   - log: Logger used for diagnostic and error reporting.
func ProcessMetadataUpdates(bt *db.Bigtable, client execution.ExecutionClient, prefix string, batchSize, iterations int, log *logger.Logger) {
	lastKey := prefix
	iterationCount := 0

	for {
		if iterations != -1 && iterationCount >= iterations {
			return
		}

		start := time.Now()

		keys, pairs, err := bt.GetMetadataUpdates(prefix, lastKey, batchSize)
		if err != nil {
			log.Errorf("error retrieving metadata updates from bigtable: %v", err)
			return
		}
		if len(keys) == 0 {
			return
		}

		balances, err := fetchBalancesInChunks(client, pairs, batchSize, log)
		if err != nil {
			log.Errorf("error retrieving balances from node: %v", err)
			return
		}

		err = bt.SaveBalances(balances, keys)
		if err != nil {
			log.Errorf("error saving balances to bigtable: %v", err)
			return
		}

		lastKey = keys[len(keys)-1]
		log.Infof("retrieved %d balances in %v, currently at %v", len(balances), time.Since(start), lastKey)

		iterationCount++
	}
}

// fetchBalancesInChunks retrieves balances for the given address pairs,
// splitting them into smaller chunks to avoid RPC overload.
//
// Parameters:
//   - client: Execution layer JSON-RPC client used to fetch balances.
//   - pairs: Slice of address pairs to query balances for.
//   - chunkSize: Number of addresses per individual RPC call.
//
// Returns:
//   - Slice of fetched balances (Eth1AddressBalance).
//   - Error if any RPC call or decoding step fails.
func fetchBalancesInChunks(client execution.ExecutionClient, pairs []*types.Eth1AddressBalance, chunkSize int, log *logger.Logger) ([]*types.Eth1AddressBalance, error) {
	var result []*types.Eth1AddressBalance

	for i := 0; i < len(pairs); i += chunkSize {
		end := i + chunkSize
		if end > len(pairs) {
			end = len(pairs)
		}

		log.Infof("processing balance chunk: %d to %d", i, end)

		balances, err := client.GetBalances(pairs[i:end], 2, 4)
		if err != nil {
			return nil, err
		}
		result = append(result, balances...)
	}

	return result, nil
}
