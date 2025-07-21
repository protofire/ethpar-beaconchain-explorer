package db

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	gcp_bigtable "cloud.google.com/go/bigtable"
	"golang.org/x/sync/errgroup"

	"github.com/coocood/freecache"
	"github.com/ethereum/go-ethereum/common"
	"github.com/go-redis/redis/v8"

	"google.golang.org/protobuf/proto"
)

const (
	MAX_BLOCK_RANK uint32 = 4
)

// SaveBlock stores the given Eth1Block in the Bigtable instance.
//
// It uses a composite row key based on the chain ID, reversed and padded block number,
// and the block's rank within the same slot. This allows multiple blocks with the same
// block number (e.g., from different forks or proposer candidates) to coexist in the table.
//
// The block is serialized using protobuf and written into the "data" column of the
// DEFAULT_FAMILY_BLOCKS column family. A timeout of 30 seconds is enforced for the write.
//
// Returns an error if marshaling fails or the Bigtable write operation fails.
func (bigtable *Bigtable) SaveBlock(block *types.Eth1Block) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_save_block").Observe(time.Since(startTime).Seconds())
	}()

	encodedBc, err := proto.Marshal(block)

	if err != nil {
		return err
	}
	ts := gcp_bigtable.Timestamp(0)

	mut := gcp_bigtable.NewMutation()
	mut.Set(DEFAULT_FAMILY_BLOCKS, "data", ts, encodedBc)

	key := makeBlockKey(bigtable.chainId, block.Number, uint8(block.Rank))
	err = bigtable.tableBlocks.Apply(ctx, key, mut)

	if err != nil {
		return err
	}
	return nil
}

// DeleteBlock removes every piece of Bigtable state that belongs to the
// given Ethereum (or EVM-compatible) block height.
//
// The cleanup spans three logical areas:
//
//  1. Account metadata – All versions of the `ACCOUNT_METADATA_FAMILY :
//     ACCOUNT_IS_CONTRACT` column that were written in the target block
//     are deleted.  The function scans every account row whose key starts
//     with "<chainID>:S:" and issues a DeleteTimestampRange mutation bounded
//     by the block’s timestamp window.
//
//  2. Per-block data rows – Using the metadata_updates table it discovers
//     the block hash, derives every data-row key that encodes
//     (blockNumber, blockHash) via GetBlockKeys, and deletes those rows from
//     the data table.
//
//  3. Block headers – For the canonical block (rank 00) and every possible
//     parallel rank up to MAX_BLOCK_RANK it deletes the row
//     "<chainID>:<reversedPaddedBlockNumber>:<rank>" from the blocks table.
//
// All deletions are issued in batches (DEFAULT_BATCH_INSERTS) to minimise
// round-trips.  A 30-second context bounds the entire operation.
//
// If any intermediate read or mutation fails, the method aborts and returns
// the encountered error, leaving any successful sub-mutations in place (no
// rollback).
func (bigtable *Bigtable) DeleteBlock(blockNumber uint64) error {

	ctx, cleanup := withTimeoutAndWarning(fmt.Sprintf("delete block %d", blockNumber), 30*time.Second)
	defer cleanup()

	// handle contract state updates: delete is_contract by timestamp
	starttime, err := encodeIsContractUpdateTs(blockNumber, 0, 0)
	if err != nil {
		return err
	}
	endtime, err := encodeIsContractUpdateTs(blockNumber+1, 0, 0)
	if err != nil {
		return err
	}

	filter := gcp_bigtable.ChainFilters(
		gcp_bigtable.FamilyFilter(ACCOUNT_METADATA_FAMILY),
		gcp_bigtable.ColumnFilter(ACCOUNT_IS_CONTRACT),
		gcp_bigtable.TimestampRangeFilterMicros(starttime, endtime-1),
	)

	mutsMeta := &types.BulkMutations{}
	err = bigtable.tableMetadata.ReadRows(ctx, gcp_bigtable.PrefixRange(fmt.Sprintf("%s:S:", bigtable.chainId)), func(row gcp_bigtable.Row) bool {
		mut := gcp_bigtable.NewMutation()
		mut.DeleteTimestampRange(ACCOUNT_METADATA_FAMILY, ACCOUNT_IS_CONTRACT, starttime, endtime)
		mutsMeta.Keys = append(mutsMeta.Keys, row.Key())
		mutsMeta.Muts = append(mutsMeta.Muts, mut)
		return true
	}, gcp_bigtable.RowFilter(filter))
	if err != nil {
		return err
	}
	if len(mutsMeta.Keys) > 0 {
		if err := bigtable.WriteBulk(mutsMeta, bigtable.tableMetadata, DEFAULT_BATCH_INSERTS); err != nil {
			return err
		}
	}

	// Delete block and keys from bigtable
	reversedNum := reversedPaddedBlockNumber(blockNumber)
	prefix := fmt.Sprintf("%s:BLOCK:%s:", bigtable.chainId, reversedNum)

	mutsData := &types.BulkMutations{}
	mutsBlocks := &types.BulkMutations{}

	// Get block hash from metadata_updates table. Search for chainId:BLOCK:<revNum>:<blockHash>
	err = bigtable.tableMetadataUpdates.ReadRows(ctx, gcp_bigtable.PrefixRange(prefix), func(row gcp_bigtable.Row) bool {
		parts := strings.Split(row.Key(), ":")
		if len(parts) < 4 {
			log.Warnf("unexpected metadata_updates key format: %s", row.Key())
			return true
		}
		blockHashHex := parts[len(parts)-1]
		blockHash, err := hex.DecodeString(blockHashHex)
		if err != nil {
			log.Warnf("invalid hex in metadata_updates key: %s", row.Key())
			return true
		}

		// Get all keys related to this block
		keys, err := bigtable.GetBlockKeys(blockNumber, blockHash)
		if err != nil {
			log.Warnf("no metadata updates found for block %d (hash=%x): %v", blockNumber, blockHash, err)
			return true
		}

		for _, key := range keys {
			mut := gcp_bigtable.NewMutation()
			mut.DeleteRow()
			mutsData.Keys = append(mutsData.Keys, key)
			mutsData.Muts = append(mutsData.Muts, mut)
		}

		// Delete all blocks with the same number and different ranks
		for rank := 0; rank <= int(MAX_BLOCK_RANK); rank++ {
			blockKey := fmt.Sprintf("%s:%s:%02d", bigtable.chainId, reversedNum, rank)
			mut := gcp_bigtable.NewMutation()
			mut.DeleteRow()
			mutsBlocks.Keys = append(mutsBlocks.Keys, blockKey)
			mutsBlocks.Muts = append(mutsBlocks.Muts, mut)
		}

		return true
	})
	if err != nil {
		return err
	}

	if len(mutsData.Keys) > 0 {
		if err := bigtable.WriteBulk(mutsData, bigtable.tableData, DEFAULT_BATCH_INSERTS); err != nil {
			return err
		}
	}
	if len(mutsBlocks.Keys) > 0 {
		if err := bigtable.WriteBulk(mutsBlocks, bigtable.tableBlocks, DEFAULT_BATCH_INSERTS); err != nil {
			return err
		}
	}

	return nil
}

// GetBlockKeys retrieves the list of Bigtable row keys associated with a specific block,
// uniquely identified by its block number and block hash.
//
// It reads the metadata_updates table using a key of the form:
//
//	<chainId>:BLOCK:<reversedPaddedBlockNumber>:<blockHash>
//
// The corresponding row is expected to store a comma-separated list of full row keys
// (e.g., for transactions, receipts, logs, etc.) in the METADATA_UPDATES_FAMILY_BLOCKS column.
//
// Returns an error if the row is not found or the read fails.
func (bigtable *Bigtable) GetBlockKeys(blockNumber uint64, blockHash []byte) ([]string, error) {

	ctx, cleanup := withTimeoutAndWarning(fmt.Sprintf("get block keys %d", blockNumber), 30*time.Second)
	defer cleanup()

	key := fmt.Sprintf("%s:BLOCK:%s:%x", bigtable.chainId, reversedPaddedBlockNumber(blockNumber), blockHash)

	row, err := bigtable.tableMetadataUpdates.ReadRow(ctx, key)

	if err != nil {
		return nil, err
	}

	if row == nil {
		return nil, fmt.Errorf("keys for block %v not found", blockNumber)
	}

	return strings.Split(string(row[METADATA_UPDATES_FAMILY_BLOCKS][0].Value), ","), nil
}

// SaveBlockKeys stores a comma-separated list of Bigtable row keys associated with
// a specific block into the metadata_updates table.
//
// The row key format is:
//
//	<chainId>:BLOCK:<reversedPaddedBlockNumber>:<blockHash>
//
// The stored keys typically point to data rows (transactions, receipts, logs, etc.) that
// belong to this specific block.
//
// Returns an error if the write operation fails.
func (bigtable *Bigtable) SaveBlockKeys(blockNumber uint64, blockHash []byte, keys string) error {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	mut := gcp_bigtable.NewMutation()
	mut.Set(METADATA_UPDATES_FAMILY_BLOCKS, "keys", gcp_bigtable.Now(), []byte(keys))

	key := fmt.Sprintf("%s:BLOCK:%s:%x", bigtable.chainId, reversedPaddedBlockNumber(blockNumber), blockHash)
	err := bigtable.tableMetadataUpdates.Apply(ctx, key, mut)

	return err
}

// SaveBalances writes a batch of token balances to the Bigtable metadata table,
// and optionally deletes specified rows from the metadata_updates table.
//
// For each balance entry, a cell is written to the ACCOUNT_METADATA_FAMILY column family
// with the column name formatted as "B:<tokenAddressHex>", and the value set to the balance.
// All balances are written with a timestamp of 0, effectively overwriting any existing values.
//
// The row key format used for balances is:
//
//	<chainId>:<address>
//
// If deleteKeys is provided, each key is used to delete an entire row from the
// metadata_updates table.
//
// Returns an error if either the write or delete operation fails.
func (bigtable *Bigtable) SaveBalances(balances []*types.Eth1AddressBalance, deleteKeys []string) error {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_save_balances").Observe(time.Since(startTime).Seconds())
	}()

	if len(balances) == 0 {
		return nil
	}

	mutsWrite := &types.BulkMutations{
		Keys: make([]string, 0, len(balances)),
		Muts: make([]*gcp_bigtable.Mutation, 0, len(balances)),
	}

	for _, balance := range balances {
		mutWrite := gcp_bigtable.NewMutation()

		mutWrite.Set(ACCOUNT_METADATA_FAMILY, fmt.Sprintf("B:%x", balance.Token), gcp_bigtable.Timestamp(0), balance.Balance)
		mutsWrite.Keys = append(mutsWrite.Keys, fmt.Sprintf("%s:%x", bigtable.chainId, balance.Address))
		mutsWrite.Muts = append(mutsWrite.Muts, mutWrite)
	}

	err := bigtable.WriteBulk(mutsWrite, bigtable.tableMetadata, DEFAULT_BATCH_INSERTS)

	if err != nil {
		return err
	}

	if len(deleteKeys) == 0 {
		return nil
	}
	mutsDelete := &types.BulkMutations{
		Keys: make([]string, 0, len(balances)),
		Muts: make([]*gcp_bigtable.Mutation, 0, len(balances)),
	}
	for _, key := range deleteKeys {
		mutDelete := gcp_bigtable.NewMutation()
		mutDelete.DeleteRow()
		mutsDelete.Keys = append(mutsDelete.Keys, key)
		mutsDelete.Muts = append(mutsDelete.Muts, mutDelete)
	}

	err = bigtable.WriteBulk(mutsDelete, bigtable.tableMetadataUpdates, DEFAULT_BATCH_INSERTS)

	if err != nil {
		return err
	}

	return nil
}

// SaveERC20TokenPrices writes a batch of ERC-20 token price and total supply entries
// into the Bigtable metadata table.
//
// For each token, the function creates a row with the key:
//
//	<chainId>:<tokenAddress>
//
// It writes two columns in the ERC20_METADATA_FAMILY column family:
//   - ERC20_COLUMN_PRICE:      current token price
//   - ERC20_COLUMN_TOTALSUPPLY: current total supply
//
// All values are written with timestamp 0, overwriting any existing values.
//
// Returns an error if the write operation fails.
func (bigtable *Bigtable) SaveERC20TokenPrices(prices []*types.ERC20TokenPrice) error {
	if len(prices) == 0 {
		return nil
	}

	mutsWrite := &types.BulkMutations{
		Keys: make([]string, 0, len(prices)),
		Muts: make([]*gcp_bigtable.Mutation, 0, len(prices)),
	}

	for _, price := range prices {
		rowKey := fmt.Sprintf("%s:%x", bigtable.chainId, price.Token)
		mut := gcp_bigtable.NewMutation()
		mut.Set(ERC20_METADATA_FAMILY, ERC20_COLUMN_PRICE, gcp_bigtable.Timestamp(0), price.Price)
		mut.Set(ERC20_METADATA_FAMILY, ERC20_COLUMN_TOTALSUPPLY, gcp_bigtable.Timestamp(0), price.TotalSupply)
		mutsWrite.Keys = append(mutsWrite.Keys, rowKey)
		mutsWrite.Muts = append(mutsWrite.Muts, mut)
	}

	err := bigtable.WriteBulk(mutsWrite, bigtable.tableMetadata, DEFAULT_BATCH_INSERTS)

	if err != nil {
		return err
	}

	return nil
}

// GetBlockFromBlocksTable retrieves a specific Eth1Block from Bigtable using its block number and rank.
//
// The function constructs the Bigtable row key as:
//
//	<chainId>:<reversedPaddedBlockNumber>:<rank>
//
// It then reads the "data" column from the DEFAULT_FAMILY_BLOCKS column family,
// and deserializes the value into an *Eth1Block using protobuf.
//
// Returns ErrBlockNotFound if no such block is found, or an error if unmarshalling fails.
func (bigtable *Bigtable) GetBlockFromBlocksTable(number uint64, rank uint32) (*types.Eth1Block, error) {

	ctx, cleanup := withTimeoutAndWarning(fmt.Sprintf("get block from blocks table %d", number), 30*time.Second)
	defer cleanup()

	key := makeBlockKey(bigtable.chainId, number, uint8(rank))
	row, err := bigtable.tableBlocks.ReadRow(ctx, key)

	if err != nil {
		return nil, err
	}

	if len(row[DEFAULT_FAMILY_BLOCKS]) == 0 { // block not found
		log.WithField("block", number).Warnf("block not found in block table")
		return nil, ErrBlockNotFound
	}

	bc := &types.Eth1Block{}
	err = proto.Unmarshal(row[DEFAULT_FAMILY_BLOCKS][0].Value, bc)

	if err != nil {
		return nil, err
	}

	return bc, nil
}

// GetLastBlockInBlocksTable returns the number of the most recent block present in the Bigtable.
//
// It first attempts to retrieve the value from a Redis cache using the key:
//
//	<chainID>:lastBlockInBlocksTable
//
// If the value is not cached, it falls back to querying Bigtable via
// getLastBlockInBlocksTableFromBigtable(), and caches the result back in Redis.
//
// Returns:
//   - int: the highest block number found (typically with rank == 0)
//   - error: if Redis or Bigtable access fails
//
// Metrics:
//   - Records execution duration under "bt_get_last_block_in_blocks_table"
//   - Logs a warning if execution exceeds REPORT_TIMEOUT
func (bigtable *Bigtable) GetLastBlockInBlocksTable() (int, error) {

	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_get_last_block_in_blocks_table").Observe(time.Since(startTime).Seconds())
	}()

	ctx, cleanup := withTimeoutAndWarning("get last block in blocks table", 30*time.Second)
	defer cleanup()

	redisKey := bigtable.chainId + ":lastBlockInBlocksTable"

	res, err := bigtable.redisCache.Get(ctx, redisKey).Result()

	if err != nil {
		// key is not yet set, get data from bigtable and store the key in redis
		if errors.Is(err, redis.Nil) {
			lastBlock, err := bigtable.getLastBlockInBlocksTableFromBigtable()

			if err != nil {
				return 0, err
			}

			return lastBlock, bigtable.SetLastBlockInBlocksTable(int64(lastBlock))

		}
		return 0, err
	}

	lastBlock, err := strconv.Atoi(res)
	if err != nil {
		return 0, err
	}
	return lastBlock, nil
}

// SetLastBlockInBlocksTable stores the most recent canonical block number in Redis
// under the key: <chainID>:lastBlockInBlocksTable.
//
// The value is stored as a string representation of the block number, without an expiration (no TTL).
//
// This cache is used to quickly retrieve the last known block without scanning Bigtable.
//
// Parameters:
//   - lastBlock: the block number to store (typically with rank == 0)
//
// Returns:
//   - error: if the Redis operation fails
//
// Metrics:
//   - Records execution duration under "bt_set_last_block_in_blocks_table"
func (bigtable *Bigtable) SetLastBlockInBlocksTable(lastBlock int64) error {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_set_last_block_in_blocks_table").Observe(time.Since(startTime).Seconds())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	redisKey := bigtable.chainId + ":lastBlockInBlocksTable"

	return bigtable.redisCache.Set(ctx, redisKey, fmt.Sprintf("%d", lastBlock), 0).Err()
}

// getLastBlockInBlocksTableFromBigtable scans the blocks table in Bigtable and returns
// the most recent block number that is considered canonical (i.e., rank == 0).
//
// The block keys are expected to follow the format:
//
//	<chainID>:<reversedPaddedBlockNumber>:<rank>
//
// The scan uses lexicographic order to quickly locate the most recent block (thanks to reversed numbering),
// and skips any blocks with rank > 0 to avoid selecting non-primary versions.
//
// Returns:
//   - int: the highest block number found with rank == 0
//   - error: if the Bigtable scan fails or key parsing fails
func (bigtable *Bigtable) getLastBlockInBlocksTableFromBigtable() (int, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	prefix := bigtable.chainId + ":"
	lastBlock := 0
	err := bigtable.tableBlocks.ReadRows(ctx, gcp_bigtable.PrefixRange(prefix), func(r gcp_bigtable.Row) bool {
		key := strings.TrimPrefix(r.Key(), prefix)
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			log.Errorf("unexpected key format: %v", r.Key())
			return true // keep scanning
		}

		reversedStr, rankStr := parts[0], parts[1]

		rank, err := strconv.ParseUint(rankStr, 10, 8)
		if err != nil {
			log.Errorf("error parsing rank: %v", err)
			return true
		}
		if rank != 0 {
			// skip non-canonical blocks
			return true
		}

		reversedNum, err := strconv.ParseUint(reversedStr, 10, 64)
		if err != nil {
			log.Errorf("error parsing reversed block number: %v", err)
			return true
		}

		blockNum := MAX_EL_BLOCK_NUMBER - int(reversedNum)
		lastBlock = blockNum

		// If blockNum == 0, keep scanning to avoid falsely assuming it's the latest
		return blockNum == 0
	}, gcp_bigtable.LimitRows(2), gcp_bigtable.RowFilter(gcp_bigtable.StripValueFilter()))

	if err != nil {
		return 0, err
	}

	return lastBlock, nil
}

// GetLastBlockInDataTable returns the number of the most recent block found in the data table.
//
// It first attempts to retrieve the block number from Redis using the key:
//
//	<chainID>:lastBlockInDataTable
//
// If the value is not found in Redis, the function scans Bigtable via
// getLastBlockInDataTableFromBigtable(), then caches the result back in Redis.
//
// Returns:
//   - int: the most recent block number found in the data table
//   - error: if Redis or Bigtable access fails
//
// Metrics:
//   - Tracks call duration using "bt_get_last_block_in_data_table"
//   - Logs a warning if execution time exceeds REPORT_TIMEOUT
func (bigtable *Bigtable) GetLastBlockInDataTable() (int, error) {

	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_get_last_block_in_data_table").Observe(time.Since(startTime).Seconds())
	}()

	ctx, cleanup := withTimeoutAndWarning("get last block in data table", 30*time.Second)
	defer cleanup()

	redisKey := bigtable.chainId + ":lastBlockInDataTable"

	res, err := bigtable.redisCache.Get(ctx, redisKey).Result()

	if err != nil {
		// key is not yet set, get data from bigtable and store the key in redis
		if errors.Is(err, redis.Nil) {
			lastBlock, err := bigtable.getLastBlockInDataTableFromBigtable()

			if err != nil {
				return 0, err
			}

			return lastBlock, bigtable.SetLastBlockInDataTable(int64(lastBlock))
		}
		return 0, err
	}

	lastBlock, err := strconv.Atoi(res)
	if err != nil {
		return 0, err
	}
	return lastBlock, nil
}

// SetLastBlockInDataTable stores the most recent block number observed in the data table
// into Redis using the key: <chainID>:lastBlockInDataTable.
//
// The value is stored as a plain string and does not expire (TTL is zero).
// This cached value allows quick access to the last processed block without querying Bigtable.
//
// Parameters:
//   - lastBlock: the block number to store
//
// Returns:
//   - error: if the Redis operation fails
func (bigtable *Bigtable) SetLastBlockInDataTable(lastBlock int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	redisKey := bigtable.chainId + ":lastBlockInDataTable"

	return bigtable.redisCache.Set(ctx, redisKey, fmt.Sprintf("%d", lastBlock), 0).Err()
}

// getLastBlockInDataTableFromBigtable scans the data table in Bigtable and returns
// the highest (most recent) block number associated with a rank 0 block.
//
// Keys are expected to follow the format:
//
//	<chainID>:B:<reversedPaddedBlockNumber>:<rank>
//
// Only blocks with rank == 0 are considered for this lookup, since they are guaranteed
// to exist for every slot and represent the minimal canonical entry.
//
// Returns:
//   - int: the most recent block number found (rank 0 only)
//   - error: if the Bigtable scan or key parsing fails
func (bigtable *Bigtable) getLastBlockInDataTableFromBigtable() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	prefix := bigtable.chainId + ":B:"
	lastBlock := 0

	err := bigtable.tableData.ReadRows(ctx, gcp_bigtable.PrefixRange(prefix), func(r gcp_bigtable.Row) bool {
		key := strings.TrimPrefix(r.Key(), prefix)
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			log.Errorf("unexpected key format: %v", r.Key())
			return true // keep scanning
		}

		reversedStr, rankStr := parts[0], parts[1]

		rank, err := strconv.ParseUint(rankStr, 10, 8)
		if err != nil {
			log.Errorf("error parsing rank from key %v: %v", r.Key(), err)
			return true
		}

		if rank != 0 {
			// We only consider rank 0 blocks
			return true
		}

		reversedNum, err := strconv.ParseUint(reversedStr, 10, 64)
		if err != nil {
			log.Errorf("error parsing reversed block number from key %v: %v", r.Key(), err)
			return true
		}

		blockNum := MAX_EL_BLOCK_NUMBER - int(reversedNum)
		lastBlock = blockNum

		// If blockNum == 0, keep scanning to avoid falsely assuming it's the latest
		return blockNum == 0
	}, gcp_bigtable.LimitRows(2), gcp_bigtable.RowFilter(gcp_bigtable.StripValueFilter()))

	if err != nil {
		return 0, err
	}

	return lastBlock, nil
}

// CheckForGapsInBlocksTable scans the blocks table in Bigtable and checks for gaps
// in the block number sequence within a given lookback range.
//
// This function is designed to work with block keys formatted as:
//
//	<chainID>:<reversedPaddedBlockNumber>:<rank>
//
// Only blocks with rank == 0 are considered part of the canonical sequence;
// parallel blocks with rank > 0 are ignored.
//
// The scan proceeds in descending block number order using the reversed block number
// to leverage Bigtable's lexicographic key ordering.
//
// A gap is detected if two consecutive blocks are not numerically adjacent
// (i.e., if currentBlockNum != previousBlockNum - 1).
//
// Parameters:
//   - lookback: the maximum number of blocks to scan.
//
// Returns:
//   - gapFound: true if a gap is detected;
//   - start: the lower block number where the gap begins;
//   - end: the higher block number before the gap;
//   - err: any error encountered during the Bigtable scan.
func (bigtable *Bigtable) CheckForGapsInBlocksTable(lookback int) (gapFound bool, start int, end int, err error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	prefix := bigtable.chainId + ":"
	previous := 0
	i := 0
	err = bigtable.tableBlocks.ReadRows(ctx, gcp_bigtable.PrefixRange(prefix), func(r gcp_bigtable.Row) bool {

		key := strings.TrimPrefix(r.Key(), prefix)
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			log.Errorf("unexpected key format: %v", r.Key())
			return false
		}
		blockNumStr, rankStr := parts[0], parts[1]

		rank, err := strconv.ParseUint(rankStr, 10, 8)
		if err != nil {
			log.Errorf("error parsing rank from key %v: %v", r.Key(), err)
			return false
		}
		if rank != 0 {
			// Skip blocks with rank > 0
			return true
		}

		reversedBlockNum, err := strconv.ParseUint(blockNumStr, 10, 64)
		if err != nil {
			log.Errorf("error parsing reversed block number from key %v: %v", r.Key(), err)
			return false
		}

		blockNum := MAX_EL_BLOCK_NUMBER - int(reversedBlockNum)

		if blockNum%10000 == 0 {
			log.Infof("scanning, currently at block %v", blockNum)
		}

		if previous != 0 && previous != blockNum+1 {
			gapFound = true
			start = blockNum
			end = previous
			log.Fatalf("found gap between block %v and block %v in blocks table", previous, blockNum)
			return false
		}
		previous = blockNum

		i++

		return i < lookback
	}, gcp_bigtable.RowFilter(gcp_bigtable.StripValueFilter()))

	return gapFound, start, end, err
}

// CheckForGapsInDataTable scans the data table in Bigtable and checks for gaps
// in the block number sequence based on the most recent entries.
//
// This function operates on keys prefixed with:
//
//	<chainID>:B:<reversedPaddedBlockNumber>:<rank>
//
// Only blocks with rank == 0 are considered canonical and used for gap detection.
// Reversed block numbers are converted back to their actual values to ensure descending order scan.
//
// A gap is reported (via log.Fatalf) when the current block number is not exactly one less than the previous.
//
// Parameters:
//   - lookback: the maximum number of blocks to check.
//
// Returns:
//   - error: any error encountered during the Bigtable scan
func (bigtable *Bigtable) CheckForGapsInDataTable(lookback int) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	prefix := bigtable.chainId + ":B:"
	previous := 0
	i := 0

	err := bigtable.tableData.ReadRows(ctx, gcp_bigtable.PrefixRange(prefix), func(r gcp_bigtable.Row) bool {
		key := strings.TrimPrefix(r.Key(), prefix)
		parts := strings.Split(key, ":")
		if len(parts) != 2 {
			log.Errorf("unexpected key format in data table: %v", r.Key())
			return true
		}

		blockStr, rankStr := parts[0], parts[1]

		rank, err := strconv.ParseUint(rankStr, 10, 8)
		if err != nil {
			log.Errorf("error parsing rank from data table key %v: %v", r.Key(), err)
			return true
		}
		if rank != 0 {
			return true // skip other parallel blocks with rank > 0
		}

		revBlockNum, err := strconv.ParseUint(blockStr, 10, 64)
		if err != nil {
			log.Errorf("error parsing reversed block number from key %v: %v", r.Key(), err)
			return true
		}
		blockNum := MAX_EL_BLOCK_NUMBER - int(revBlockNum)

		if blockNum%10000 == 0 {
			log.Infof("scanning, currently at block %v", blockNum)
		}

		if previous != 0 && previous != blockNum+1 {
			log.Fatalf("found gap between block %v and block %v in data table", previous, blockNum)
			return false
		}

		previous = blockNum
		i++
		return i < lookback
	}, gcp_bigtable.RowFilter(gcp_bigtable.StripValueFilter()))

	return err
}

// IndexEventsWithTransformers streams full blocks from Bigtable (including all ranks),
// applies a configurable pipeline of transformation functions to each block,
// and bulk-writes the resulting mutations into the `data` and `metadata_updates` tables.
//
// The function processes blocks in batches (default: 1000 blockNumbers per batch), and supports
// concurrent execution across both batches and blocks via `errgroup.Group` with a configurable concurrency limit.
//
// For each block, all transformation functions are executed in sequence. Each function may produce:
//   - Bulk mutations to be written to the data table (e.g., transactions, logs, receipts)
//   - Optional metadata update mutations (e.g., indexing metadata)
//
// If any data mutations are produced, the function also saves a summary of the row keys
// via `SaveBlockKeys(block.Number, block.Hash, keys...)` into the metadata_updates table,
// allowing downstream reorg/reindex logic to cleanly delete block data by hash.
//
// Parameters:
//   - start, end:        inclusive range of block numbers to index
//   - transforms:        list of transformation functions applied to each block
//   - concurrency:       maximum number of concurrent goroutines for block-level processing
//   - cache:             shared freecache instance passed to each transform (can be used for lookups or deduplication)
//
// Returns an error if any transformation or write operation fails.
// Logging and Prometheus metrics (if integrated) provide additional observability.
func (bigtable *Bigtable) IndexEventsWithTransformers(start, end int64, transforms []func(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error), concurrency int64, cache *freecache.Cache) error {
	g := new(errgroup.Group)
	g.SetLimit(int(concurrency))

	log.Infof("indexing blocks from %d to %d", start, end)
	batchSize := int64(1000)
	for i := start; i <= end; i += batchSize {
		firstBlock := int64(i)
		lastBlock := firstBlock + batchSize - 1
		if lastBlock > end {
			lastBlock = end
		}

		g.Go(func() error {
			blocksChan := make(chan *types.Eth1Block, batchSize)

			go func(stream chan *types.Eth1Block) {
				log.Infof("querying blocks from %v to %v", firstBlock, lastBlock)
				high := lastBlock
				low := lastBlock - batchSize + 1
				if int64(firstBlock) > low {
					low = firstBlock
				}

				err := bigtable.GetFullBlocksDescending(stream, uint64(high), uint64(low))
				if err != nil {
					log.Errorf("error getting blocks descending high: %v low: %v err: %v", high, low, err)
				}
				close(stream)
			}(blocksChan)
			subG := new(errgroup.Group)
			subG.SetLimit(int(concurrency))
			for b := range blocksChan {
				block := b
				subG.Go(func() error {
					bulkMutsData := types.BulkMutations{}
					bulkMutsMetadataUpdate := types.BulkMutations{}
					for _, transform := range transforms {
						mutsData, mutsMetadataUpdate, err := transform(block, cache)
						if err != nil {
							log.WithError(err).Errorf("error transforming block [%v]", block.Number)
						}
						bulkMutsData.Keys = append(bulkMutsData.Keys, mutsData.Keys...)
						bulkMutsData.Muts = append(bulkMutsData.Muts, mutsData.Muts...)

						if mutsMetadataUpdate != nil {
							bulkMutsMetadataUpdate.Keys = append(bulkMutsMetadataUpdate.Keys, mutsMetadataUpdate.Keys...)
							bulkMutsMetadataUpdate.Muts = append(bulkMutsMetadataUpdate.Muts, mutsMetadataUpdate.Muts...)
						}
					}

					if len(bulkMutsData.Keys) > 0 {
						metaKeys := strings.Join(bulkMutsData.Keys, ",") // save block keys in order to be able to handle chain reorgs
						err := bigtable.SaveBlockKeys(block.Number, block.Hash, metaKeys)
						if err != nil {
							return fmt.Errorf("error saving block [%v] keys to bigtable metadata updates table: %w", block.Number, err)
						}

						err = bigtable.WriteBulk(&bulkMutsData, bigtable.tableData, DEFAULT_BATCH_INSERTS)
						if err != nil {
							return fmt.Errorf("error writing block [%v] to bigtable data table: %w", block.Number, err)
						}
					}

					if len(bulkMutsMetadataUpdate.Keys) > 0 {
						err := bigtable.WriteBulk(&bulkMutsMetadataUpdate, bigtable.tableMetadataUpdates, DEFAULT_BATCH_INSERTS)
						if err != nil {
							return fmt.Errorf("error writing block [%v] to bigtable metadata updates table: %w", block.Number, err)
						}
					}

					return nil
				})
			}
			return subG.Wait()
		})

	}

	if err := g.Wait(); err == nil {
		log.Info("data table indexing completed")
	} else {
		bigtable.log.Errorf("wait group error: %v", err)
		return err
	}

	lastBlockInCache, err := bigtable.GetLastBlockInDataTable()
	if err != nil {
		return err
	}

	if end > int64(lastBlockInCache) {
		err := bigtable.SetLastBlockInDataTable(end)

		if err != nil {
			return err
		}
	}
	return nil
}

func (bigtable *Bigtable) GetMetadataUpdates(prefix string, startToken string, limit int) ([]string, []*types.Eth1AddressBalance, error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_get_metadata_updates").Observe(time.Since(startTime).Seconds())
	}()

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"prefix":     prefix,
			"startToken": startToken,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour*2))
	defer cancel()

	keys := make([]string, 0, limit)
	pairs := make([]*types.Eth1AddressBalance, 0, limit)

	err := bigtable.tableMetadataUpdates.ReadRows(ctx, gcp_bigtable.NewRange(startToken, ""), func(row gcp_bigtable.Row) bool {
		if !strings.Contains(row.Key(), prefix) {
			return false
		}
		keys = append(keys, row.Key())

		for _, ri := range row {
			for _, item := range ri {
				pairs = append(pairs, &types.Eth1AddressBalance{Address: common.FromHex(strings.Split(row.Key(), ":")[2]), Token: common.FromHex(strings.Split(item.Column, ":")[1])})
			}
		}
		return true
	}, gcp_bigtable.LimitRows(int64(limit)))

	if err == context.DeadlineExceeded && len(keys) > 0 {
		return keys, pairs, nil
	}
	return keys, pairs, err
}
