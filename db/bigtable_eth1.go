package db

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/cache"
	"github.com/protofire/ethpar-beaconchain-explorer/erc1155"
	"github.com/protofire/ethpar-beaconchain-explorer/erc20"
	"github.com/protofire/ethpar-beaconchain-explorer/erc721"
	"github.com/protofire/ethpar-beaconchain-explorer/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	gcp_bigtable "cloud.google.com/go/bigtable"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coocood/freecache"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	eth_types "github.com/ethereum/go-ethereum/core/types"
	"github.com/go-redis/redis/v8"
	"github.com/sirupsen/logrus"
	"google.golang.org/protobuf/proto"
)

const (
	ECR20TokensPerAddressLimit    = uint64(200) // when changing this, you will have to update the swagger docu for func ApiEth1Address too
	digitLimitInAddressPagesTable = 17
	nameLimitInAddressPagesTable  = 0
)

var ErrBlockNotFound = errors.New("block not found")

type IndexFilter string

const (
	FILTER_TIME           IndexFilter = "TIME"
	FILTER_TO             IndexFilter = "TO"
	FILTER_FROM           IndexFilter = "FROM"
	FILTER_TOKEN_RECEIVED IndexFilter = "TOKEN_RECEIVED"
	FILTER_TOKEN_SENT     IndexFilter = "TOKEN_SENT"
	FILTER_METHOD         IndexFilter = "METHOD"
	FILTER_CONTRACT       IndexFilter = "CONTRACT"
	FILTER_ERROR          IndexFilter = "ERROR"
)

const (
	DATA_COLUMN                    = "d"
	INDEX_COLUMN                   = "i"
	DEFAULT_FAMILY_BLOCKS          = "default"
	METADATA_UPDATES_FAMILY_BLOCKS = "blocks"
	ACCOUNT_METADATA_FAMILY        = "a"
	CONTRACT_METADATA_FAMILY       = "c"
	ERC20_METADATA_FAMILY          = "erc20"
	ERC721_METADATA_FAMILY         = "erc721"
	ERC1155_METADATA_FAMILY        = "erc1155"
	TX_PER_BLOCK_LIMIT             = 10_000
	ITX_PER_TX_LIMIT               = 100_000
	MAX_INT                        = 9223372036854775807
	MIN_INT                        = -9223372036854775808
)

const (
	ACCOUNT_COLUMN_NAME = "NAME"
	ACCOUNT_IS_CONTRACT = "ISCONTRACT"

	CONTRACT_NAME = "CONTRACTNAME"
	CONTRACT_ABI  = "ABI"

	ERC20_COLUMN_DECIMALS    = "DECIMALS"
	ERC20_COLUMN_TOTALSUPPLY = "TOTALSUPPLY"
	ERC20_COLUMN_SYMBOL      = "SYMBOL"

	ERC20_COLUMN_PRICE = "PRICE"

	ERC20_COLUMN_NAME           = "NAME"
	ERC20_COLUMN_DESCRIPTION    = "DESCRIPTION"
	ERC20_COLUMN_LOGO           = "LOGO"
	ERC20_COLUMN_LOGO_FORMAT    = "LOGOFORMAT"
	ERC20_COLUMN_LINK           = "LINK"
	ERC20_COLUMN_OGIMAGE        = "OGIMAGE"
	ERC20_COLUMN_OGIMAGE_FORMAT = "OGIMAGEFORMAT"
)

const (
	// see https://cloud.google.com/bigtable/docs/using-filters#timestamp-range
	TIMESTAMP_GBT_SCALE = 1000
	// tests showed it's possible to have 36900+ subcalls per tx, but very unlikely - save a bit
	TIMESTAMP_TRACE_SCALE = 1 << 15
	// 30m gas / 21.000 gas per transfer = 1428
	TIMESTAMP_TX_SCALE = 1 << 11
	// 64 - (10 bits for TIMESTAMP_GBT_SCALE + TIMESTAMP_TRACE_SCALE + TIMESTAMP_TX_SCALE)
	// = 28 bits left; with a block time of 12s, that's enough for 50+ years
	TIMESTAMP_BLOCK_SCALE = 1 << (64 - (10 + 15 + 11))
)

var ZERO_ADDRESS []byte = []byte{0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00}

var (
	ERC20TOPIC   []byte
	ERC721TOPIC  []byte
	ERC1155Topic []byte
)

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

	err = bigtable.tableBlocks.Apply(ctx, fmt.Sprintf("%s:%s", bigtable.chainId, reversedPaddedBlockNumber(block.Number)), mut)

	if err != nil {
		return err
	}
	return nil
}

func (bigtable *Bigtable) GetBlockFromBlocksTable(number uint64) (*types.Eth1Block, error) {
	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"validators": number,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	paddedNumber := reversedPaddedBlockNumber(number)

	row, err := bigtable.tableBlocks.ReadRow(ctx, fmt.Sprintf("%s:%s", bigtable.chainId, paddedNumber))

	if err != nil {
		return nil, err
	}

	if len(row[DEFAULT_FAMILY_BLOCKS]) == 0 { // block not found
		logger.WithFields(logrus.Fields{"block": number}).Warnf("block not found in block table")
		return nil, ErrBlockNotFound
	}

	bc := &types.Eth1Block{}
	err = proto.Unmarshal(row[DEFAULT_FAMILY_BLOCKS][0].Value, bc)

	if err != nil {
		return nil, err
	}

	return bc, nil
}

func (bigtable *Bigtable) CheckForGapsInBlocksTable(lookback int) (gapFound bool, start int, end int, err error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	prefix := bigtable.chainId + ":"
	previous := 0
	i := 0
	err = bigtable.tableBlocks.ReadRows(ctx, gcp_bigtable.PrefixRange(prefix), func(r gcp_bigtable.Row) bool {
		c, err := strconv.Atoi(strings.Replace(r.Key(), prefix, "", 1))

		if err != nil {
			logger.Errorf("error parsing block number from key %v: %v", r.Key(), err)
			return false
		}
		c = MAX_EL_BLOCK_NUMBER - c

		if c%10000 == 0 {
			logger.Infof("scanning, currently at block %v", c)
		}

		if previous != 0 && previous != c+1 {
			gapFound = true
			start = c
			end = previous
			logger.Fatalf("found gap between block %v and block %v in blocks table", previous, c)
			return false
		}
		previous = c

		i++

		return i < lookback
	}, gcp_bigtable.RowFilter(gcp_bigtable.StripValueFilter()))

	return gapFound, start, end, err
}

func (bigtable *Bigtable) GetLastBlockInBlocksTable() (int, error) {
	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_get_last_block_in_blocks_table").Observe(time.Since(startTime).Seconds())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
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

func (bigtable *Bigtable) CheckForGapsInDataTable(lookback int) error {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	prefix := bigtable.chainId + ":B:"
	previous := 0
	i := 0
	err := bigtable.tableData.ReadRows(ctx, gcp_bigtable.PrefixRange(prefix), func(r gcp_bigtable.Row) bool {
		c, err := strconv.Atoi(strings.Replace(r.Key(), prefix, "", 1))

		if err != nil {
			logger.Errorf("error parsing block number from key %v: %v", r.Key(), err)
			return false
		}
		c = MAX_EL_BLOCK_NUMBER - c

		if c%10000 == 0 {
			logger.Infof("scanning, currently at block %v", c)
		}

		if previous != 0 && previous != c+1 {
			logger.Fatalf("found gap between block %v and block %v in data table", previous, c)
		}
		previous = c

		i++

		return i < lookback
	}, gcp_bigtable.RowFilter(gcp_bigtable.StripValueFilter()))

	if err != nil {
		return err
	}

	return nil
}

func (bigtable *Bigtable) GetLastBlockInDataTable() (int, error) {
	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_get_last_block_in_data_table").Observe(time.Since(startTime).Seconds())
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
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

func (bigtable *Bigtable) getLastBlockInDataTableFromBigtable() (int, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	prefix := bigtable.chainId + ":B:"
	lastBlock := 0
	err := bigtable.tableData.ReadRows(ctx, gcp_bigtable.PrefixRange(prefix), func(r gcp_bigtable.Row) bool {
		c, err := strconv.Atoi(strings.Replace(r.Key(), prefix, "", 1))

		if err != nil {
			logger.Errorf("error parsing block number from key %v: %v", r.Key(), err)
			return false
		}
		c = MAX_EL_BLOCK_NUMBER - c

		lastBlock = c
		return c == 0 // required as the block with number 0 will be returned as first block before the most recent one
	}, gcp_bigtable.LimitRows(2), gcp_bigtable.RowFilter(gcp_bigtable.StripValueFilter()))

	if err != nil {
		return 0, err
	}

	return lastBlock, nil
}

func (bigtable *Bigtable) getLastBlockInBlocksTableFromBigtable() (int, error) {

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	prefix := bigtable.chainId + ":"
	lastBlock := 0
	err := bigtable.tableBlocks.ReadRows(ctx, gcp_bigtable.PrefixRange(prefix), func(r gcp_bigtable.Row) bool {
		c, err := strconv.Atoi(strings.Replace(r.Key(), prefix, "", 1))

		if err != nil {
			logger.Errorf("error parsing block number from key %v: %v", r.Key(), err)
			return false
		}
		c = MAX_EL_BLOCK_NUMBER - c

		lastBlock = c
		return c == 0 // required as the block with number 0 will be returned as first block before the most recent one
	}, gcp_bigtable.LimitRows(2), gcp_bigtable.RowFilter(gcp_bigtable.StripValueFilter()))

	if err != nil {
		return 0, err
	}

	return lastBlock, nil
}

func (bigtable *Bigtable) SetLastBlockInDataTable(lastBlock int64) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()
	redisKey := bigtable.chainId + ":lastBlockInDataTable"

	return bigtable.redisCache.Set(ctx, redisKey, fmt.Sprintf("%d", lastBlock), 0).Err()
}

func (bigtable *Bigtable) GetMostRecentBlockFromDataTable() (*types.Eth1BlockIndexed, error) {
	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	prefix := fmt.Sprintf("%s:B:", bigtable.chainId)

	rowRange := gcp_bigtable.PrefixRange(prefix)
	block := types.Eth1BlockIndexed{}

	rowHandler := func(row gcp_bigtable.Row) bool {
		c, err := strconv.Atoi(strings.Replace(row.Key(), prefix, "", 1))
		if err != nil {
			logger.Errorf("error parsing block number from key %v: %v", row.Key(), err)
			return false
		}

		c = MAX_EL_BLOCK_NUMBER - c

		err = proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, &block)
		if err != nil {
			logger.Errorf("error could not unmarschal proto object, err: %v", err)
		}

		return c == 0
	}

	err := bigtable.tableData.ReadRows(ctx, rowRange, rowHandler, gcp_bigtable.LimitRows(2), gcp_bigtable.RowFilter(gcp_bigtable.ColumnFilter("d")))
	if err != nil {
		return nil, err
	}

	return &block, nil
}

func getBlockHandler(blocks *[]*types.Eth1BlockIndexed) func(gcp_bigtable.Row) bool {
	return func(row gcp_bigtable.Row) bool {
		if row == nil {
			return false
		}

		if !strings.Contains(row.Key(), ":B:") {
			return false
		}

		block := types.Eth1BlockIndexed{}
		err := proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, &block)
		if err != nil {
			logger.Errorf("error could not unmarschal proto object, err: %v", err)
			return false
		}

		*blocks = append(*blocks, &block)
		return true
	}
}

// GetFullBlocksDescending streams blocks ranging from high to low (both borders are inclusive) in the correct descending order via a channel.
//
// Special handling for block 0 is implemented.
//
//   - stream: channel the function will use for streaming
//   - high: highest (max) block number
//   - low: lowest (min) block number
func (bigtable *Bigtable) GetFullBlocksDescending(stream chan<- *types.Eth1Block, high, low uint64) error {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"high": high,
			"low":  low,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute*3))
	defer cancel()

	if high < low {
		return fmt.Errorf("invalid block range provided (high: %v, low: %v)", high, low)
	}

	if high > 0 {
		limitedLow := low
		if limitedLow == 0 {
			// block 0 cannot be included in the range as it is padded incorrectly (will be fetched last, see below)
			limitedLow = 1
		}

		highKey := fmt.Sprintf("%s:%s", bigtable.chainId, reversedPaddedBlockNumber(high))
		lowKey := fmt.Sprintf("%s:%s\x00", bigtable.chainId, reversedPaddedBlockNumber(limitedLow)) // add \x00 to make the range inclusive

		limit := high - limitedLow + 1

		rowRange := gcp_bigtable.NewRange(highKey, lowKey)
		rowFilter := gcp_bigtable.RowFilter(gcp_bigtable.ColumnFilter("data"))
		rowHandler := func(row gcp_bigtable.Row) bool {
			block := types.Eth1Block{}
			err := proto.Unmarshal(row[DEFAULT_FAMILY_BLOCKS][0].Value, &block)
			if err != nil {
				logger.Errorf("error could not unmarschal proto object, err: %v", err)
				return false
			}
			stream <- &block
			return true
		}

		err := bigtable.tableBlocks.ReadRows(ctx, rowRange, rowHandler, rowFilter, gcp_bigtable.LimitRows(int64(limit)))
		if err != nil {
			return err
		}
	}

	if low == 0 {
		// special handling for block 0 which is padded incorrectly
		b, err := BigtableClient.GetBlockFromBlocksTable(0)
		if err != nil {
			return fmt.Errorf("could not retreive block 0:  %v", err)
		}
		stream <- b
	}

	return nil
}

func (bigtable *Bigtable) GetBlocksIndexedMultiple(blockNumbers []uint64, limit uint64) ([]*types.Eth1BlockIndexed, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"blockNumbers": blockNumbers,
			"limit":        limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	rowList := gcp_bigtable.RowList{}
	for _, block := range blockNumbers {
		rowList = append(rowList, fmt.Sprintf("%s:B:%s", bigtable.chainId, reversedPaddedBlockNumber(block)))
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	rowFilter := gcp_bigtable.RowFilter(gcp_bigtable.ColumnFilter("d"))

	blocks := make([]*types.Eth1BlockIndexed, 0, 100)

	rowHandler := getBlockHandler(&blocks)

	err := bigtable.tableData.ReadRows(ctx, rowList, rowHandler, rowFilter, gcp_bigtable.LimitRows(int64(limit)))
	if err != nil {
		return nil, err
	}

	return blocks, nil
}

// GetBlocksDescending gets a given amount of Eth1BlockIndexed starting at block start from tableData
//
//	start: highest block number to be returned
//	limit: amount of blocks to be returned
//		- if limit > start + 1, limit will be set to start + 1
//		- if limit = start + 1, block 0 will be included as last block (special handling for broken padding is implemented)
//		- if limit = start, block 0 will of course not be included
func (bigtable *Bigtable) GetBlocksDescending(start, limit uint64) ([]*types.Eth1BlockIndexed, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"start": start,
			"limit": limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	if limit == 0 {
		return nil, fmt.Errorf("error limit is set to 0 which would not fetch any blocks")
	}

	// clamp limit
	if limit > start+1 {
		limit = start + 1
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	blocks := make([]*types.Eth1BlockIndexed, 0, limit)

	rowHandler := getBlockHandler(&blocks)
	rowFilter := gcp_bigtable.RowFilter(gcp_bigtable.ColumnFilter("d"))

	if start > 0 {
		// block 0 cannot be included in the range as it is padded incorrectly (will be fetched last, see below)
		limitedEnd := uint64(1)
		if start > limit {
			limitedEnd = start - limit
		}

		startKey := fmt.Sprintf("%s:B:%s", bigtable.chainId, reversedPaddedBlockNumber(start))
		endKey := fmt.Sprintf("%s:B:%s\x00", bigtable.chainId, reversedPaddedBlockNumber(limitedEnd)) // add \x00 to make the range inclusive

		rowRange := gcp_bigtable.NewRange(startKey, endKey)

		err := bigtable.tableData.ReadRows(ctx, rowRange, rowHandler, rowFilter, gcp_bigtable.LimitRows(int64(limit)))
		if err != nil {
			return nil, err
		}
	}

	if start < limit {
		// special handling for block 0 with broken padding (see reversedPaddedBlockNumber)
		row, err := bigtable.tableData.ReadRow(ctx, fmt.Sprintf("%s:B:%s", bigtable.chainId, reversedPaddedBlockNumber(0)), rowFilter)
		if err != nil {
			return nil, err
		}

		// rowHandler will add block 0 to blocks if it is found
		if !rowHandler(row) {
			return nil, fmt.Errorf("error could not read block 0")
		}
	}

	return blocks, nil
}

func reversedPaddedBlockNumber(blockNumber uint64) string {
	return fmt.Sprintf("%09d", MAX_EL_BLOCK_NUMBER-blockNumber)
}

func reversePaddedBigtableTimestamp(timestamp *timestamppb.Timestamp) string {
	if timestamp == nil {
		log.Fatalf("unknown timestamp: %v", timestamp)
	}
	return fmt.Sprintf("%019d", MAX_INT-timestamp.Seconds)
}

func reversePaddedIndex(i int, maxValue int) string {
	if i > maxValue {
		logrus.Fatalf("padded index %v is greater than the max index of %v", i, maxValue)
	}
	length := fmt.Sprintf("%d", len(fmt.Sprintf("%d", maxValue))-1)
	fmtStr := "%0" + length + "d"
	return fmt.Sprintf(fmtStr, maxValue-i)
}

func TimestampToBigtableTimeDesc(ts time.Time) string {
	return fmt.Sprintf("%04d%02d%02d%02d%02d%02d", 9999-ts.Year(), 12-ts.Month(), 31-ts.Day(), 23-ts.Hour(), 59-ts.Minute(), 59-ts.Second())
}

func (bigtable *Bigtable) IndexEventsWithTransformers(start, end int64, transforms []TransformFunc, concurrency int64, cache *freecache.Cache) error {
	g := new(errgroup.Group)
	g.SetLimit(int(concurrency))

	logrus.Infof("indexing blocks from %d to %d", start, end)
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
				logger.Infof("querying blocks from %v to %v", firstBlock, lastBlock)
				high := lastBlock
				low := lastBlock - batchSize + 1
				if int64(firstBlock) > low {
					low = firstBlock
				}

				err := BigtableClient.GetFullBlocksDescending(stream, uint64(high), uint64(low))
				if err != nil {
					logger.Errorf("error getting blocks descending high: %v low: %v err: %v", high, low, err)
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
							logrus.WithError(err).Errorf("error transforming block [%v]", block.Number)
						}
						if mutsData != nil {
							bulkMutsData.Keys = append(bulkMutsData.Keys, mutsData.Keys...)
							bulkMutsData.Muts = append(bulkMutsData.Muts, mutsData.Muts...)
						}
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
		logrus.Info("data table indexing completed")
	} else {
		utils.LogError(err, "wait group error", 0)
		return err
	}

	err := g.Wait()

	if err != nil {
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

type TransformFunc func(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error)

func (bigtable *Bigtable) blockKeysMutation(blockNumber uint64, blockHash []byte, keys string) (string, *gcp_bigtable.Mutation) {
	mut := gcp_bigtable.NewMutation()
	mut.Set(METADATA_UPDATES_FAMILY_BLOCKS, "keys", gcp_bigtable.Now(), []byte(keys))

	key := fmt.Sprintf("%s:BLOCK:%s:%x", bigtable.chainId, reversedPaddedBlockNumber(blockNumber), blockHash)
	return key, mut
}

func (bigtable *Bigtable) IndexBlocksWithTransformers(blocks []*types.Eth1Block, transforms []TransformFunc, cache *freecache.Cache) error {
	bulkMutsData := types.BulkMutations{}
	bulkMutsMetadataUpdate := types.BulkMutations{}
	for _, block := range blocks {
		for _, transform := range transforms {
			mutsData, mutsMetadataUpdate, err := transform(block, cache)
			if err != nil {
				logrus.WithError(err).Errorf("error transforming block [%v]", block.Number)
			}
			if mutsData != nil {
				bulkMutsData.Keys = append(bulkMutsData.Keys, mutsData.Keys...)
				bulkMutsData.Muts = append(bulkMutsData.Muts, mutsData.Muts...)
			}

			if mutsMetadataUpdate != nil {
				bulkMutsMetadataUpdate.Keys = append(bulkMutsMetadataUpdate.Keys, mutsMetadataUpdate.Keys...)
				bulkMutsMetadataUpdate.Muts = append(bulkMutsMetadataUpdate.Muts, mutsMetadataUpdate.Muts...)
			}

			if mutsData != nil && len(mutsData.Keys) > 0 {
				metaKeys := strings.Join(bulkMutsData.Keys, ",") // save block keys in order to be able to handle chain reorgs
				key, mut := bigtable.blockKeysMutation(block.Number, block.Hash, metaKeys)
				bulkMutsMetadataUpdate.Keys = append(bulkMutsMetadataUpdate.Keys, key)
				bulkMutsMetadataUpdate.Muts = append(bulkMutsMetadataUpdate.Muts, mut)
			}
		}
	}

	if len(bulkMutsData.Keys) > 0 {
		err := bigtable.WriteBulk(&bulkMutsData, bigtable.tableData, DEFAULT_BATCH_INSERTS)
		if err != nil {
			return fmt.Errorf("error writing blocks [%v-%v] to bigtable data table: %w", blocks[0].Number, blocks[len(blocks)-1].Number, err)
		}
	}

	if len(bulkMutsMetadataUpdate.Keys) > 0 {
		err := bigtable.WriteBulk(&bulkMutsMetadataUpdate, bigtable.tableMetadataUpdates, DEFAULT_BATCH_INSERTS)
		if err != nil {
			return fmt.Errorf("error writing blocks [%v-%v] to bigtable metadata updates table: %w", blocks[0].Number, blocks[len(blocks)-1].Number, err)
		}
	}

	return nil
}

// TransformBlock extracts blocks from bigtable more specifically from the table blocks.
// It transforms the block and strips any information that is not necessary for a blocks view
// It writes blocks to table data:
// Row:    <chainID>:B:<reversePaddedBlockNumber>
// Family: f
// Column: data
// Cell:   Proto<Eth1BlockIndexed>
//
// It indexes blocks by:
// Row:    <chainID>:I:B:<Miner>:<reversePaddedBlockNumber>
// Family: f
// Column: <chainID>:B:<reversePaddedBlockNumber>
// Cell:   nil
func (bigtable *Bigtable) TransformBlock(block *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_block").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	idx := types.Eth1BlockIndexed{
		Hash:       block.GetHash(),
		ParentHash: block.GetParentHash(),
		UncleHash:  block.GetUncleHash(),
		Coinbase:   block.GetCoinbase(),
		Difficulty: block.GetDifficulty(),
		Number:     block.GetNumber(),
		GasLimit:   block.GetGasLimit(),
		GasUsed:    block.GetGasUsed(),
		Time:       block.GetTime(),
		BaseFee:    block.GetBaseFee(),
		// Duration:               uint64(block.GetTime().AsTime().Unix() - previous.GetTime().AsTime().Unix()),
		UncleCount:       uint64(len(block.GetUncles())),
		TransactionCount: uint64(len(block.GetTransactions())),
		// BaseFeeChange:          new(big.Int).Sub(new(big.Int).SetBytes(block.GetBaseFee()), new(big.Int).SetBytes(previous.GetBaseFee())).Bytes(),
		// BlockUtilizationChange: new(big.Int).Sub(new(big.Int).Div(big.NewInt(int64(block.GetGasUsed())), big.NewInt(int64(block.GetGasLimit()))), new(big.Int).Div(big.NewInt(int64(previous.GetGasUsed())), big.NewInt(int64(previous.GetGasLimit())))).Bytes(),
		BlobGasUsed:   block.GetBlobGasUsed(),
		ExcessBlobGas: block.GetExcessBlobGas(),
	}

	uncleReward := big.NewInt(0)
	r := new(big.Int)

	for _, uncle := range block.Uncles {

		if len(block.Difficulty) == 0 { // no uncle rewards in PoS
			continue
		}

		r.Add(big.NewInt(int64(uncle.GetNumber())), big.NewInt(8))
		r.Sub(r, big.NewInt(int64(block.GetNumber())))
		r.Mul(r, utils.Eth1BlockReward(block.GetNumber(), block.Difficulty))
		r.Div(r, big.NewInt(8))

		r.Div(utils.Eth1BlockReward(block.GetNumber(), block.Difficulty), big.NewInt(32))
		uncleReward.Add(uncleReward, r)
	}

	idx.UncleReward = uncleReward.Bytes()

	var maxGasPrice *big.Int
	var minGasPrice *big.Int
	txReward := big.NewInt(0)

	for _, t := range block.GetTransactions() {
		price := new(big.Int).SetBytes(t.GasPrice)

		if minGasPrice == nil {
			minGasPrice = price
		}
		if maxGasPrice == nil {
			maxGasPrice = price
		}

		if price.Cmp(maxGasPrice) > 0 {
			maxGasPrice = price
		}

		if price.Cmp(minGasPrice) < 0 {
			minGasPrice = price
		}

		txFee := new(big.Int).Mul(new(big.Int).SetBytes(t.GasPrice), big.NewInt(int64(t.GasUsed)))

		if len(block.BaseFee) > 0 {
			effectiveGasPrice := bigMin(new(big.Int).Add(new(big.Int).SetBytes(t.MaxPriorityFeePerGas), new(big.Int).SetBytes(block.BaseFee)), new(big.Int).SetBytes(t.MaxFeePerGas))
			proposerGasPricePart := new(big.Int).Sub(effectiveGasPrice, new(big.Int).SetBytes(block.BaseFee))

			if proposerGasPricePart.Cmp(big.NewInt(0)) >= 0 {
				txFee = new(big.Int).Mul(proposerGasPricePart, big.NewInt(int64(t.GasUsed)))
			} else {
				logger.Errorf("error minerGasPricePart is below 0 for tx %v: %v", t.Hash, proposerGasPricePart)
				txFee = big.NewInt(0)
			}

		}

		txReward.Add(txReward, txFee)

		for _, itx := range t.Itx {
			if itx.Path == "[]" || bytes.Equal(itx.Value, []byte{0x0}) { // skip top level call & empty calls
				continue
			}
			idx.InternalTransactionCount++
		}

		if t.GetType() == 3 {
			idx.BlobTransactionCount++
		}

	}

	idx.TxReward = txReward.Bytes()

	// logger.Infof("tx reward for block %v is %v", block.Number, txReward.String())

	if maxGasPrice != nil {
		idx.LowestGasPrice = minGasPrice.Bytes()

	}
	if minGasPrice != nil {
		idx.HighestGasPrice = maxGasPrice.Bytes()
	}

	idx.Mev = CalculateMevFromBlock(block).Bytes() // deprecated but we still write the value to keep all blocks consistent

	// Mark Coinbase for balance update
	bigtable.markBalanceUpdate(idx.Coinbase, []byte{0x0}, bulkMetadataUpdates, cache)

	// <chainID>:b:<reverse number>
	key := fmt.Sprintf("%s:B:%s", bigtable.chainId, reversedPaddedBlockNumber(block.GetNumber()))
	mut := gcp_bigtable.NewMutation()

	b, err := proto.Marshal(&idx)
	if err != nil {
		return nil, nil, fmt.Errorf("error marshalling proto object err: %w", err)
	}

	mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

	bulkData.Keys = append(bulkData.Keys, key)
	bulkData.Muts = append(bulkData.Muts, mut)

	indexes := []string{
		// Index blocks by the miners address
		fmt.Sprintf("%s:I:B:%x:TIME:%s", bigtable.chainId, block.GetCoinbase(), reversePaddedBigtableTimestamp(block.Time)),
	}

	for _, idx := range indexes {
		mut := gcp_bigtable.NewMutation()
		mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

		bulkData.Keys = append(bulkData.Keys, idx)
		bulkData.Muts = append(bulkData.Muts, mut)
	}

	return bulkData, bulkMetadataUpdates, nil
}

func CalculateMevFromBlock(block *types.Eth1Block) *big.Int {
	mevReward := big.NewInt(0)

	for _, tx := range block.GetTransactions() {
		for _, itx := range tx.GetItx() {
			if common.BytesToAddress(itx.To) == common.BytesToAddress(block.GetCoinbase()) {
				mevReward = new(big.Int).Add(mevReward, new(big.Int).SetBytes(itx.GetValue()))
			}
		}

	}
	return mevReward
}

func CalculateTxFeesFromBlock(block *types.Eth1Block) *big.Int {
	txFees := new(big.Int)
	for _, tx := range block.Transactions {
		txFees.Add(txFees, CalculateTxFeeFromTransaction(tx, new(big.Int).SetBytes(block.BaseFee)))
	}
	return txFees
}

func bigMin(x, y *big.Int) *big.Int {
	if x.Cmp(y) > 0 {
		return y
	}
	return x
}

func CalculateTxFeeFromTransaction(tx *types.Eth1Transaction, blockBaseFee *big.Int) *big.Int {
	// calculate tx fee depending on tx type
	txFee := new(big.Int).SetUint64(tx.GasUsed)
	switch tx.Type {
	case 0, 1:
		txFee.Mul(txFee, new(big.Int).SetBytes(tx.GasPrice))
	case 2, 3:
		// multiply gasused with min(baseFee + maxpriorityfee, maxfee)
		if normalGasPrice, maxGasPrice := new(big.Int).Add(blockBaseFee, new(big.Int).SetBytes(tx.MaxPriorityFeePerGas)), new(big.Int).SetBytes(tx.MaxFeePerGas); normalGasPrice.Cmp(maxGasPrice) <= 0 {
			txFee.Mul(txFee, normalGasPrice)
		} else {
			txFee.Mul(txFee, maxGasPrice)
		}
	default:
		logger.Errorf("unknown tx type %v", tx.Type)
	}
	return txFee
}

// TransformTx extracts transactions from bigtable more specifically from the table blocks.
func (bigtable *Bigtable) TransformTx(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_tx").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	for i, tx := range blk.Transactions {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		iReverse := reversePaddedIndex(i, TX_PER_BLOCK_LIMIT)
		// logger.Infof("address to: %x address: contract: %x, len(to): %v, len(contract): %v, contranct zero: %v", tx.GetTo(), tx.GetContractAddress(), len(tx.GetTo()), len(tx.GetContractAddress()), bytes.Equal(tx.GetContractAddress(), ZERO_ADDRESS))
		to := tx.GetTo()
		isContract := false
		if !bytes.Equal(tx.GetContractAddress(), ZERO_ADDRESS) {
			to = tx.GetContractAddress()
			isContract = true
		}
		// logger.Infof("sending to: %x", to)
		method := make([]byte, 0)
		if len(tx.GetData()) > 3 {
			method = tx.GetData()[:4]
		}

		key := fmt.Sprintf("%s:TX:%x", bigtable.chainId, tx.GetHash())
		fee := new(big.Int).Mul(new(big.Int).SetBytes(tx.GetGasPrice()), big.NewInt(int64(tx.GetGasUsed()))).Bytes()
		blobFee := new(big.Int).Mul(new(big.Int).SetBytes(tx.GetBlobGasPrice()), big.NewInt(int64(tx.GetBlobGasUsed()))).Bytes()
		indexedTx := &types.Eth1TransactionIndexed{
			Hash:               tx.GetHash(),
			BlockNumber:        blk.GetNumber(),
			Time:               blk.GetTime(),
			MethodId:           method,
			From:               tx.GetFrom(),
			To:                 to,
			Value:              tx.GetValue(),
			TxFee:              fee,
			GasPrice:           tx.GetGasPrice(),
			IsContractCreation: isContract,
			ErrorMsg:           "",
			BlobTxFee:          blobFee,
			BlobGasPrice:       tx.GetBlobGasPrice(),
			Status:             types.StatusType(tx.Status),
		}
		for _, itx := range tx.Itx {
			if itx.ErrorMsg != "" {
				indexedTx.ErrorMsg = itx.ErrorMsg
				if indexedTx.Status == types.StatusType_SUCCESS {
					indexedTx.Status = types.StatusType_PARTIAL
				}
				break
			}
		}

		// Mark Sender and Recipient for balance update
		bigtable.markBalanceUpdate(indexedTx.From, []byte{0x0}, bulkMetadataUpdates, cache)
		bigtable.markBalanceUpdate(indexedTx.To, []byte{0x0}, bulkMetadataUpdates, cache)

		if len(indexedTx.Hash) != 32 {
			logger.Fatalf("retrieved hash of length %v for a tx in block %v", len(indexedTx.Hash), blk.GetNumber())
		}

		b, err := proto.Marshal(indexedTx)
		if err != nil {
			return nil, nil, err
		}

		mut := gcp_bigtable.NewMutation()
		mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

		bulkData.Keys = append(bulkData.Keys, key)
		bulkData.Muts = append(bulkData.Muts, mut)

		indexes := []string{
			fmt.Sprintf("%s:I:TX:%x:TO:%x:%s:%s", bigtable.chainId, tx.GetFrom(), to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:TIME:%s:%s", bigtable.chainId, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:BLOCK:%s:%s", bigtable.chainId, tx.GetFrom(), reversedPaddedBlockNumber(blk.GetNumber()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:METHOD:%x:%s:%s", bigtable.chainId, tx.GetFrom(), method, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:FROM:%x:%s:%s", bigtable.chainId, to, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:TIME:%s:%s", bigtable.chainId, to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:BLOCK:%s:%s", bigtable.chainId, to, reversedPaddedBlockNumber(blk.GetNumber()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:METHOD:%x:%s:%s", bigtable.chainId, to, method, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
		}

		if indexedTx.ErrorMsg != "" {
			indexes = append(indexes, fmt.Sprintf("%s:I:TX:%x:ERROR:%s:%s", bigtable.chainId, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse))
			indexes = append(indexes, fmt.Sprintf("%s:I:TX:%x:ERROR:%s:%s", bigtable.chainId, to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse))
		}

		if indexedTx.IsContractCreation {
			indexes = append(indexes, fmt.Sprintf("%s:I:TX:%x:CONTRACT:%s:%s", bigtable.chainId, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse))
			indexes = append(indexes, fmt.Sprintf("%s:I:TX:%x:CONTRACT:%s:%s", bigtable.chainId, to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse))
		}

		for _, idx := range indexes {
			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

			bulkData.Keys = append(bulkData.Keys, idx)
			bulkData.Muts = append(bulkData.Muts, mut)
		}

	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformBlobTx extracts transactions from bigtable more specifically from the table blocks.
func (bigtable *Bigtable) TransformBlobTx(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_blob_tx").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	for i, tx := range blk.Transactions {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		if tx.Type != 3 {
			// skip non blob-txs
			continue
		}
		iReverse := reversePaddedIndex(i, TX_PER_BLOCK_LIMIT)
		// logger.Infof("address to: %x address: contract: %x, len(to): %v, len(contract): %v, contranct zero: %v", tx.GetTo(), tx.GetContractAddress(), len(tx.GetTo()), len(tx.GetContractAddress()), bytes.Equal(tx.GetContractAddress(), ZERO_ADDRESS))
		to := tx.GetTo()

		// logger.Infof("sending to: %x", to)

		key := fmt.Sprintf("%s:BTX:%x", bigtable.chainId, tx.GetHash())
		fee := new(big.Int).Mul(new(big.Int).SetBytes(tx.GetGasPrice()), big.NewInt(int64(tx.GetGasUsed()))).Bytes()
		blobFee := new(big.Int).Mul(new(big.Int).SetBytes(tx.GetBlobGasPrice()), big.NewInt(int64(tx.GetBlobGasUsed()))).Bytes()
		indexedTx := &types.Eth1BlobTransactionIndexed{
			Hash:                tx.GetHash(),
			BlockNumber:         blk.GetNumber(),
			Time:                blk.GetTime(),
			From:                tx.GetFrom(),
			To:                  to,
			Value:               tx.GetValue(),
			TxFee:               fee,
			GasPrice:            tx.GetGasPrice(),
			BlobTxFee:           blobFee,
			BlobGasPrice:        tx.GetBlobGasPrice(),
			ErrorMsg:            "",
			BlobVersionedHashes: tx.GetBlobVersionedHashes(),
		}
		for _, itx := range tx.Itx {
			if itx.ErrorMsg != "" {
				indexedTx.ErrorMsg = itx.ErrorMsg
				break
			}
		}

		// Mark Sender and Recipient for balance update
		bigtable.markBalanceUpdate(indexedTx.From, []byte{0x0}, bulkMetadataUpdates, cache)
		bigtable.markBalanceUpdate(indexedTx.To, []byte{0x0}, bulkMetadataUpdates, cache)

		if len(indexedTx.Hash) != 32 {
			logger.Fatalf("retrieved hash of length %v for a tx in block %v", len(indexedTx.Hash), blk.GetNumber())
		}

		b, err := proto.Marshal(indexedTx)
		if err != nil {
			return nil, nil, err
		}

		mut := gcp_bigtable.NewMutation()
		mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

		bulkData.Keys = append(bulkData.Keys, key)
		bulkData.Muts = append(bulkData.Muts, mut)

		indexes := []string{
			fmt.Sprintf("%s:I:BTX:%x:TO:%x:%s:%s", bigtable.chainId, tx.GetFrom(), to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:BTX:%x:TIME:%s:%s", bigtable.chainId, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:BTX:%x:BLOCK:%s:%s", bigtable.chainId, tx.GetFrom(), reversedPaddedBlockNumber(blk.GetNumber()), iReverse),
			fmt.Sprintf("%s:I:BTX:%x:FROM:%x:%s:%s", bigtable.chainId, to, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:BTX:%x:TIME:%s:%s", bigtable.chainId, to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:BTX:%x:BLOCK:%s:%s", bigtable.chainId, to, reversedPaddedBlockNumber(blk.GetNumber()), iReverse),
		}

		if indexedTx.ErrorMsg != "" {
			indexes = append(indexes, fmt.Sprintf("%s:I:BTX:%x:ERROR:%s:%s", bigtable.chainId, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse))
			indexes = append(indexes, fmt.Sprintf("%s:I:BTX:%x:ERROR:%s:%s", bigtable.chainId, to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse))
		}

		for _, idx := range indexes {
			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

			bulkData.Keys = append(bulkData.Keys, idx)
			bulkData.Muts = append(bulkData.Muts, mut)
		}

	}

	return bulkData, bulkMetadataUpdates, nil
}

// custom timestamp
func encodeIsContractUpdateTs(block_number, tx_idx, trace_idx uint64) (gcp_bigtable.Timestamp, error) {
	var res uint64

	if block_number >= TIMESTAMP_BLOCK_SCALE {
		return 0, fmt.Errorf("error encoding IsContractTimestamp: block idx is >= %d (block %d, tx %d, trace %d)", TIMESTAMP_BLOCK_SCALE, block_number, tx_idx, trace_idx)
	}
	res += block_number

	if tx_idx >= TIMESTAMP_TX_SCALE {
		return 0, fmt.Errorf("error encoding IsContractTimestamp: tx idx is >= %d (block %d, tx %d, trace %d)", TIMESTAMP_TX_SCALE, block_number, tx_idx, trace_idx)
	}
	res *= TIMESTAMP_TX_SCALE
	res += tx_idx

	if trace_idx >= TIMESTAMP_TRACE_SCALE {
		return 0, fmt.Errorf("error encoding IsContractTimestamp: trace idx is >= %d (block %d, tx %d, trace %d)", TIMESTAMP_TRACE_SCALE, block_number, tx_idx, trace_idx)
	}
	res *= TIMESTAMP_TRACE_SCALE
	res += trace_idx

	return gcp_bigtable.Timestamp(res * TIMESTAMP_GBT_SCALE), nil
}

func decodeIsContractUpdateTs(ts gcp_bigtable.Timestamp) (block_number, tx_idx, trace_idx uint64) {
	n := uint64(ts)
	n /= TIMESTAMP_GBT_SCALE

	trace_idx = n % TIMESTAMP_TRACE_SCALE
	n /= TIMESTAMP_TRACE_SCALE

	tx_idx = n % TIMESTAMP_TX_SCALE

	block_number = n / TIMESTAMP_TX_SCALE

	return block_number, tx_idx, trace_idx
}

func (bigtable *Bigtable) TransformContract(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_contract").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}
	contractUpdateWrites := &types.BulkMutations{}

	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}

		for j, itx := range tx.GetItx() {
			if j >= ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of internal transactions in block expected at most %d but got: %v, tx: %x", ITX_PER_TX_LIMIT, j, tx.GetHash())
			}

			if itx.GetType() == "create" || itx.GetType() == "suicide" {
				contractUpdate := &types.IsContractUpdate{
					IsContract: itx.GetType() == "create",
					// also use success status of enclosing transaction, as even successful sub-calls can still be reverted later in the tx
					Success: itx.GetErrorMsg() == "" && tx.GetStatus() == 1,
				}
				b, err := proto.Marshal(contractUpdate)
				if err != nil {
					return nil, nil, err
				}
				address := itx.GetTo()
				if itx.GetType() == "suicide" {
					address = itx.GetFrom()
				}

				mutWrite := gcp_bigtable.NewMutation()
				ts, err := encodeIsContractUpdateTs(blk.GetNumber(), uint64(i), uint64(j))
				if err != nil {
					utils.LogError(err, "error generating bigtable isContract timestamp", 0)
				} else {
					mutWrite.Set(ACCOUNT_METADATA_FAMILY, ACCOUNT_IS_CONTRACT, ts, b)
					contractUpdateWrites.Keys = append(contractUpdateWrites.Keys, fmt.Sprintf("%s:S:%x", bigtable.chainId, address))
					contractUpdateWrites.Muts = append(contractUpdateWrites.Muts, mutWrite)
				}
			}
		}
	}

	err = bigtable.WriteBulk(contractUpdateWrites, bigtable.tableMetadata, DEFAULT_BATCH_INSERTS)
	return bulkData, bulkMetadataUpdates, err
}

// TransformItx extracts internal transactions from bigtable more specifically from the table blocks.
// It transforms the internal transactions contained within a block and strips any information that is not necessary for our frontend views
// It writes internal transactions to table data:
// Row:    <chainID>:ITX:<TX_HASH>:<paddedITXIndex>
// Family: f
// Column: data
// Cell:   Proto<Eth1InternalTransactionIndexed>
//
// It indexes internal transactions by:
// Row:    <chainID>:I:ITX:<FROM_ADDRESS>:TIME:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<paddedITXIndex>
// Family: f
// Column: <chainID>:ITX:<HASH>:<paddedITXIndex>
// Cell:   nil
// Row:    <chainID>:I:ITX:<TO_ADDRESS>:TIME:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<paddedITXIndex>
// Family: f
// Column: <chainID>:ITX:<HASH>:<paddedITXIndex>
// Cell:   nil
// Row:    <chainID>:I:ITX:<FROM_ADDRESS>:TO:<TO_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<paddedITXIndex>
// Family: f
// Column: <chainID>:ITX:<HASH>:<paddedITXIndex>
// Cell:   nil
// Row:    <chainID>:I:ITX:<TO_ADDRESS>:FROM:<FROM_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<paddedITXIndex>
// Family: f
// Column: <chainID>:ITX:<HASH>:<paddedITXIndex>
// Cell:   nil
func (bigtable *Bigtable) TransformItx(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_itx").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		iReversed := reversePaddedIndex(i, TX_PER_BLOCK_LIMIT)

		var revertSource string
		for j, itx := range tx.GetItx() {
			if j > ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of internal transactions in block expected at most %d but got: %v, tx: %x", ITX_PER_TX_LIMIT, j, tx.GetHash())
			}
			jReversed := reversePaddedIndex(j, ITX_PER_TX_LIMIT)

			// check for error before skipping, otherwise we loose track of cascading reverts
			var reverted bool
			if itx.ErrorMsg != "" {
				reverted = true
				// only save the highest root revert
				if revertSource == "" || !strings.HasPrefix(itx.Path, revertSource) {
					revertSource = strings.TrimSuffix(itx.Path, "]")
				}
			}
			if revertSource != "" && strings.HasPrefix(itx.Path, revertSource) {
				reverted = true
			}

			if itx.Path == "[]" || bytes.Equal(itx.Value, []byte{0x0}) { // skip top level and empty calls
				continue
			}

			key := fmt.Sprintf("%s:ITX:%x:%s", bigtable.chainId, tx.GetHash(), jReversed)
			indexedItx := &types.Eth1InternalTransactionIndexed{
				ParentHash:  tx.GetHash(),
				BlockNumber: blk.GetNumber(),
				Time:        blk.GetTime(),
				Type:        itx.GetType(),
				From:        itx.GetFrom(),
				To:          itx.GetTo(),
				Value:       itx.GetValue(),
				Reverted:    reverted,
			}

			bigtable.markBalanceUpdate(indexedItx.To, []byte{0x0}, bulkMetadataUpdates, cache)
			bigtable.markBalanceUpdate(indexedItx.From, []byte{0x0}, bulkMetadataUpdates, cache)

			indexes := []string{
				// fmt.Sprintf("%s:i:ITX::%s:%s:%s", bigtable.chainId, reversePaddedBigtableTimestamp(blk.GetTime()), fmt.Sprintf("%04d", i), fmt.Sprintf("%05d", j)),
				fmt.Sprintf("%s:I:ITX:%x:TO:%x:%s:%s:%s", bigtable.chainId, itx.GetFrom(), itx.GetTo(), reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ITX:%x:FROM:%x:%s:%s:%s", bigtable.chainId, itx.GetTo(), itx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ITX:%x:TIME:%s:%s:%s", bigtable.chainId, itx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ITX:%x:TIME:%s:%s:%s", bigtable.chainId, itx.GetTo(), reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
			}

			// Delete existing delegatecall data or add/update other data
			if itx.GetType() == "delegatecall" {
				mut := gcp_bigtable.NewMutation()
				mut.DeleteCellsInColumn(DEFAULT_FAMILY, DATA_COLUMN)

				bulkData.Keys = append(bulkData.Keys, key)
				bulkData.Muts = append(bulkData.Muts, mut)

				for _, idx := range indexes {
					mut := gcp_bigtable.NewMutation()
					mut.DeleteCellsInColumn(DEFAULT_FAMILY, key)

					bulkData.Keys = append(bulkData.Keys, idx)
					bulkData.Muts = append(bulkData.Muts, mut)
				}
			} else {
				b, err := proto.Marshal(indexedItx)
				if err != nil {
					return nil, nil, err
				}

				mut := gcp_bigtable.NewMutation()
				mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

				bulkData.Keys = append(bulkData.Keys, key)
				bulkData.Muts = append(bulkData.Muts, mut)

				for _, idx := range indexes {
					mut := gcp_bigtable.NewMutation()
					mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

					bulkData.Keys = append(bulkData.Keys, idx)
					bulkData.Muts = append(bulkData.Muts, mut)
				}
			}
		}
	}

	return bulkData, bulkMetadataUpdates, nil
}

// https://etherscan.io/tx/0xb10588bde42cb8eb14e72d24088bd71ad3903857d23d50b3ba4187c0cb7d3646#eventlog
// TransformERC20 accepts an eth1 block and creates bigtable mutations for ERC20 transfer events.
// It transforms the logs contained within a block and writes the transformed logs to bigtable
// It writes ERC20 events to the table data:
// Row:    <chainID>:ERC20:<txHash>:<paddedLogIndex>
// Family: f
// Column: data
// Cell:   Proto<Eth1ERC20Indexed>
// Example scan: "1:ERC20:b10588bde42cb8eb14e72d24088bd71ad3903857d23d50b3ba4187c0cb7d3646" returns mainnet ERC20 event(s) for transaction 0xb10588bde42cb8eb14e72d24088bd71ad3903857d23d50b3ba4187c0cb7d3646
//
// It indexes ERC20 events by:
// Row:    <chainID>:I:ERC20:<TOKEN_ADDRESS>:TIME:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC20:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC20:<FROM_ADDRESS>:TIME:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC20:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC20:<TO_ADDRESS>:TIME:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC20:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC20:<FROM_ADDRESS>:TO:<TO_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC20:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC20:<TO_ADDRESS>:FROM:<FROM_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC20:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC20:<FROM_ADDRESS>:TOKEN_SENT:<TOKEN_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC20:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC20:<TO_ADDRESS>:TOKEN_RECEIVED:<TOKEN_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC20:<txHash>:<paddedLogIndex>
// Cell:   nil
func (bigtable *Bigtable) TransformERC20(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_erc20").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	filterer, err := erc20.NewErc20Filterer(common.Address{}, nil)
	if err != nil {
		log.Printf("error creating filterer: %v", err)
	}

	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		iReversed := reversePaddedIndex(i, TX_PER_BLOCK_LIMIT)
		for j, log := range tx.GetLogs() {
			if j >= ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of logs in block expected at most %d but got: %v tx: %x", ITX_PER_TX_LIMIT-1, j, tx.GetHash())
			}
			jReversed := reversePaddedIndex(j, ITX_PER_TX_LIMIT)
			if len(log.GetTopics()) != 3 || !bytes.Equal(log.GetTopics()[0], erc20.TransferTopic) {
				continue
			}

			topics := make([]common.Hash, 0, len(log.GetTopics()))

			for _, lTopic := range log.GetTopics() {
				topics = append(topics, common.BytesToHash(lTopic))
			}

			ethLog := eth_types.Log{
				Address:     common.BytesToAddress(log.GetAddress()),
				Data:        log.Data,
				Topics:      topics,
				BlockNumber: blk.GetNumber(),
				TxHash:      common.BytesToHash(tx.GetHash()),
				TxIndex:     uint(i),
				BlockHash:   common.BytesToHash(blk.GetHash()),
				Index:       uint(j),
				Removed:     log.GetRemoved(),
			}

			transfer, _ := filterer.ParseTransfer(ethLog)
			if transfer == nil {
				continue
			}

			value := []byte{}
			if transfer != nil && transfer.Value != nil {
				value = transfer.Value.Bytes()
			}

			key := fmt.Sprintf("%s:ERC20:%x:%s", bigtable.chainId, tx.GetHash(), jReversed)
			indexedLog := &types.Eth1ERC20Indexed{
				ParentHash:   tx.GetHash(),
				BlockNumber:  blk.GetNumber(),
				Time:         blk.GetTime(),
				TokenAddress: log.Address,
				From:         transfer.From.Bytes(),
				To:           transfer.To.Bytes(),
				Value:        value,
			}
			bigtable.markBalanceUpdate(indexedLog.From, indexedLog.TokenAddress, bulkMetadataUpdates, cache)
			bigtable.markBalanceUpdate(indexedLog.To, indexedLog.TokenAddress, bulkMetadataUpdates, cache)

			b, err := proto.Marshal(indexedLog)
			if err != nil {
				return nil, nil, err
			}

			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

			bulkData.Keys = append(bulkData.Keys, key)
			bulkData.Muts = append(bulkData.Muts, mut)

			indexes := []string{
				fmt.Sprintf("%s:I:ERC20:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC20:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),

				fmt.Sprintf("%s:I:ERC20:%x:ALL:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC20:%x:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC20:%x:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),

				fmt.Sprintf("%s:I:ERC20:%x:TO:%x:%s:%s:%s", bigtable.chainId, indexedLog.From, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC20:%x:FROM:%x:%s:%s:%s", bigtable.chainId, indexedLog.To, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC20:%x:TOKEN_SENT:%x:%s:%s:%s", bigtable.chainId, indexedLog.From, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC20:%x:TOKEN_RECEIVED:%x:%s:%s:%s", bigtable.chainId, indexedLog.To, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
			}

			for _, idx := range indexes {
				mut := gcp_bigtable.NewMutation()
				mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

				// if i == 3 || i == 4 {
				// 	mut.DeleteRow()
				// }

				bulkData.Keys = append(bulkData.Keys, idx)
				bulkData.Muts = append(bulkData.Muts, mut)
			}
		}
	}

	return bulkData, bulkMetadataUpdates, nil
}

// example: https://etherscan.io/tx/0x4d3a6c56cecb40637c070601c275df9cc7b599b5dc1d5ac2473c92c7a9e62c64#eventlog
// TransformERC721 accepts an eth1 block and creates bigtable mutations for erc721 transfer events.
// It transforms the logs contained within a block and writes the transformed logs to bigtable
// It writes erc721 events to the table data:
// Row:    <chainID>:ERC721:<txHash>:<paddedLogIndex>
// Family: f
// Column: data
// Cell:   Proto<Eth1ERC721Indexed>
// Example scan: "1:ERC721:4d3a6c56cecb40637c070601c275df9cc7b599b5dc1d5ac2473c92c7a9e62c64" returns mainnet ERC721 event(s) for transaction 0x4d3a6c56cecb40637c070601c275df9cc7b599b5dc1d5ac2473c92c7a9e62c64
//
// It indexes ERC721 events by:
// Row:    <chainID>:I:ERC721:<FROM_ADDRESS>:TIME:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC721:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC721:<TO_ADDRESS>:TIME:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC721:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC721:<TOKEN_ADDRESS>:TIME:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC721:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC721:<FROM_ADDRESS>:TO:<TO_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC721:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC721:<TO_ADDRESS>:FROM:<FROM_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC721:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC721:<FROM_ADDRESS>:TOKEN_SENT:<TOKEN_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC721:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC721:<TO_ADDRESS>:TOKEN_RECEIVED:<TOKEN_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC721:<txHash>:<paddedLogIndex>
// Cell:   nil
func (bigtable *Bigtable) TransformERC721(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_erc721").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	filterer, err := erc721.NewErc721Filterer(common.Address{}, nil)
	if err != nil {
		log.Printf("error creating filterer: %v", err)
	}

	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		iReversed := reversePaddedIndex(i, TX_PER_BLOCK_LIMIT)
		for j, log := range tx.GetLogs() {
			if j >= ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of logs in block expected at most %d but got: %v tx: %x", ITX_PER_TX_LIMIT-1, j, tx.GetHash())
			}
			if len(log.GetTopics()) != 4 || !bytes.Equal(log.GetTopics()[0], erc721.TransferTopic) {
				continue
			}
			jReversed := reversePaddedIndex(j, ITX_PER_TX_LIMIT)

			topics := make([]common.Hash, 0, len(log.GetTopics()))

			for _, lTopic := range log.GetTopics() {
				topics = append(topics, common.BytesToHash(lTopic))
			}

			ethLog := eth_types.Log{
				Address:     common.BytesToAddress(log.GetAddress()),
				Data:        log.Data,
				Topics:      topics,
				BlockNumber: blk.GetNumber(),
				TxHash:      common.BytesToHash(tx.GetHash()),
				TxIndex:     uint(i),
				BlockHash:   common.BytesToHash(blk.GetHash()),
				Index:       uint(j),
				Removed:     log.GetRemoved(),
			}

			transfer, _ := filterer.ParseTransfer(ethLog)
			if transfer == nil {
				continue
			}

			tokenId := new(big.Int)
			if transfer != nil && transfer.TokenId != nil {
				tokenId = transfer.TokenId
			}

			key := fmt.Sprintf("%s:ERC721:%x:%s", bigtable.chainId, tx.GetHash(), jReversed)
			indexedLog := &types.Eth1ERC721Indexed{
				ParentHash:   tx.GetHash(),
				BlockNumber:  blk.GetNumber(),
				Time:         blk.GetTime(),
				TokenAddress: log.Address,
				From:         transfer.From.Bytes(),
				To:           transfer.To.Bytes(),
				TokenId:      tokenId.Bytes(),
			}

			b, err := proto.Marshal(indexedLog)
			if err != nil {
				return nil, nil, err
			}

			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

			bulkData.Keys = append(bulkData.Keys, key)
			bulkData.Muts = append(bulkData.Muts, mut)

			indexes := []string{
				// fmt.Sprintf("%s:I:ERC721:%s:%s:%s", bigtable.chainId, reversePaddedBigtableTimestamp(blk.GetTime()), fmt.Sprintf("%04d", i), fmt.Sprintf("%05d", j)),
				fmt.Sprintf("%s:I:ERC721:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC721:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),

				fmt.Sprintf("%s:I:ERC721:%x:ALL:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC721:%x:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC721:%x:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),

				fmt.Sprintf("%s:I:ERC721:%x:TO:%x:%s:%s:%s", bigtable.chainId, indexedLog.From, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC721:%x:FROM:%x:%s:%s:%s", bigtable.chainId, indexedLog.To, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC721:%x:TOKEN_SENT:%x:%s:%s:%s", bigtable.chainId, indexedLog.From, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC721:%x:TOKEN_RECEIVED:%x:%s:%s:%s", bigtable.chainId, indexedLog.To, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
			}

			for _, idx := range indexes {
				mut := gcp_bigtable.NewMutation()
				mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

				// if i == 3 || i == 4 {
				// 	mut.DeleteRow()
				// }

				bulkData.Keys = append(bulkData.Keys, idx)
				bulkData.Muts = append(bulkData.Muts, mut)
			}
		}
	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformERC1155 accepts an eth1 block and creates bigtable mutations for erc1155 transfer events.
// Example: https://etherscan.io/tx/0xcffdd4b44ba9361a769a559c360293333d09efffeab79c36125bb4b20bd04270#eventlog
// It transforms the logs contained within a block and writes the transformed logs to bigtable
// It writes erc1155 events to the table data:
// Row:    <chainID>:ERC1155:<txHash>:<paddedLogIndex>
// Family: f
// Column: data
// Cell:   Proto<Eth1ERC1155Indexed>
// Example scan: "1:ERC1155:cffdd4b44ba9361a769a559c360293333d09efffeab79c36125bb4b20bd04270" returns mainnet erc1155 event(s) for transaction 0xcffdd4b44ba9361a769a559c360293333d09efffeab79c36125bb4b20bd04270
//
// It indexes erc1155 events by:
// Row:    <chainID>:I:ERC1155:<FROM_ADDRESS>:TIME:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC1155:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC1155:<TO_ADDRESS>:TIME:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC1155:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC1155:<TOKEN_ADDRESS>:TIME:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC1155:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC1155:<TO_ADDRESS>:TO:<FROM_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC1155:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC1155:<FROM_ADDRESS>:FROM:<TO_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC1155:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC1155:<FROM_ADDRESS>:TOKEN_SENT:<TOKEN_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC1155:<txHash>:<paddedLogIndex>
// Cell:   nil
//
// Row:    <chainID>:I:ERC1155:<TO_ADDRESS>:TOKEN_RECEIVED:<TOKEN_ADDRESS>:<reversePaddedBigtableTimestamp>:<paddedTxIndex>:<PaddedLogIndex>
// Family: f
// Column: <chainID>:ERC1155:<txHash>:<paddedLogIndex>
// Cell:   nil
func (bigtable *Bigtable) TransformERC1155(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_erc1155").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	filterer, err := erc1155.NewErc1155Filterer(common.Address{}, nil)
	if err != nil {
		log.Printf("error creating filterer: %v", err)
	}

	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		iReversed := reversePaddedIndex(i, TX_PER_BLOCK_LIMIT)
		for j, log := range tx.GetLogs() {
			if j >= ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of logs in block expected at most %d but got: %v tx: %x", ITX_PER_TX_LIMIT-1, j, tx.GetHash())
			}
			jReversed := reversePaddedIndex(j, ITX_PER_TX_LIMIT)

			key := fmt.Sprintf("%s:ERC1155:%x:%s", bigtable.chainId, tx.GetHash(), jReversed)

			// no events emitted continue
			if len(log.GetTopics()) != 4 || (!bytes.Equal(log.GetTopics()[0], erc1155.TransferBulkTopic) && !bytes.Equal(log.GetTopics()[0], erc1155.TransferSingleTopic)) {
				continue
			}

			topics := make([]common.Hash, 0, len(log.GetTopics()))

			for _, lTopic := range log.GetTopics() {
				topics = append(topics, common.BytesToHash(lTopic))
			}

			ethLog := eth_types.Log{
				Address:     common.BytesToAddress(log.GetAddress()),
				Data:        log.Data,
				Topics:      topics,
				BlockNumber: blk.GetNumber(),
				TxHash:      common.BytesToHash(tx.GetHash()),
				TxIndex:     uint(i),
				BlockHash:   common.BytesToHash(blk.GetHash()),
				Index:       uint(j),
				Removed:     log.GetRemoved(),
			}

			indexedLog := &types.ETh1ERC1155Indexed{}
			transferBatch, _ := filterer.ParseTransferBatch(ethLog)
			transferSingle, _ := filterer.ParseTransferSingle(ethLog)
			if transferBatch == nil && transferSingle == nil {
				continue
			}

			// && len(transferBatch.Operator) == 20 && len(transferBatch.From) == 20 && len(transferBatch.To) == 20 && len(transferBatch.Ids) > 0 && len(transferBatch.Values) > 0
			if transferBatch != nil {
				ids := make([][]byte, 0, len(transferBatch.Ids))
				for _, id := range transferBatch.Ids {
					ids = append(ids, id.Bytes())
				}

				values := make([][]byte, 0, len(transferBatch.Values))
				for _, val := range transferBatch.Values {
					values = append(values, val.Bytes())
				}

				if len(ids) != len(values) {
					logrus.Errorf("error parsing erc1155 batch transfer logs. Expected len(ids): %v len(values): %v to be the same", len(ids), len(values))
					continue
				}
				for ti := range ids {
					indexedLog.BlockNumber = blk.GetNumber()
					indexedLog.Time = blk.GetTime()
					indexedLog.ParentHash = tx.GetHash()
					indexedLog.From = transferBatch.From.Bytes()
					indexedLog.To = transferBatch.To.Bytes()
					indexedLog.Operator = transferBatch.Operator.Bytes()
					indexedLog.TokenId = ids[ti]
					indexedLog.Value = values[ti]
					indexedLog.TokenAddress = log.GetAddress()
				}
			} else if transferSingle != nil {
				indexedLog.BlockNumber = blk.GetNumber()
				indexedLog.Time = blk.GetTime()
				indexedLog.ParentHash = tx.GetHash()
				indexedLog.From = transferSingle.From.Bytes()
				indexedLog.To = transferSingle.To.Bytes()
				indexedLog.Operator = transferSingle.Operator.Bytes()
				indexedLog.TokenId = transferSingle.Id.Bytes()
				indexedLog.Value = transferSingle.Value.Bytes()
				indexedLog.TokenAddress = log.GetAddress()
			}

			b, err := proto.Marshal(indexedLog)
			if err != nil {
				return nil, nil, err
			}

			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

			bulkData.Keys = append(bulkData.Keys, key)
			bulkData.Muts = append(bulkData.Muts, mut)

			indexes := []string{
				// fmt.Sprintf("%s:I:ERC1155:%s:%s:%s", bigtable.chainId, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),

				fmt.Sprintf("%s:I:ERC1155:%x:ALL:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),

				fmt.Sprintf("%s:I:ERC1155:%x:TO:%x:%s:%s:%s", bigtable.chainId, indexedLog.From, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:FROM:%x:%s:%s:%s", bigtable.chainId, indexedLog.To, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:TOKEN_SENT:%x:%s:%s:%s", bigtable.chainId, indexedLog.From, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:TOKEN_RECEIVED:%x:%s:%s:%s", bigtable.chainId, indexedLog.To, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
			}

			for _, idx := range indexes {
				mut := gcp_bigtable.NewMutation()
				mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

				// if i == 3 || i == 4 {
				// 	mut.DeleteRow()
				// }

				bulkData.Keys = append(bulkData.Keys, idx)
				bulkData.Muts = append(bulkData.Muts, mut)
			}
		}
	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformUncle accepts an eth1 block and creates bigtable mutations.
// It transforms the uncles contained within a block, extracts the necessary information to create a view and writes that information to bigtable
// It writes uncles to table data:
// Row:    <chainID>:U:<reversePaddedNumber>
// Family: f
// Column: data
// Cell:   Proto<Eth1UncleIndexed>
// Example scan: "1:U:" returns mainnet uncles mined in desc order
// Example scan: "1:U:984886725" returns mainnet uncles mined after block 15113275 (1000000000 - 984886725)
//
// It indexes uncles by:
// Row:    <chainID>:I:U:<Miner>:TIME:<reversePaddedBigtableTimestamp>
// Family: f
// Column: <chainID>:U:<reversePaddedNumber>
// Cell:   nil
// Example lookup: "1:I:U:ea674fdde714fd979de3edf0f56aa9716b898ec8:TIME:" returns mainnet uncles mined by ethermine in desc order
func (bigtable *Bigtable) TransformUncle(block *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_uncle").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	for i, uncle := range block.Uncles {
		if i > 99 {
			return nil, nil, fmt.Errorf("unexpected number of uncles in block expected at most 99 but got: %v", i)
		}
		iReversed := reversePaddedIndex(i, 10)
		r := new(big.Int)

		if len(block.Difficulty) > 0 {
			r.Add(big.NewInt(int64(uncle.GetNumber())), big.NewInt(8))
			r.Sub(r, big.NewInt(int64(block.GetNumber())))
			r.Mul(r, utils.Eth1BlockReward(block.GetNumber(), block.Difficulty))
			r.Div(r, big.NewInt(8))

			r.Div(utils.Eth1BlockReward(block.GetNumber(), block.Difficulty), big.NewInt(32))
		}

		uncleIndexed := types.Eth1UncleIndexed{
			Number:      uncle.GetNumber(),
			BlockNumber: block.GetNumber(),
			GasLimit:    uncle.GetGasLimit(),
			GasUsed:     uncle.GetGasUsed(),
			BaseFee:     uncle.GetBaseFee(),
			Difficulty:  uncle.GetDifficulty(),
			Time:        uncle.GetTime(),
			Reward:      r.Bytes(),
		}

		bigtable.markBalanceUpdate(uncle.Coinbase, []byte{0x0}, bulkMetadataUpdates, cache)

		// store uncles in with the key <chainid>:U:<reversePaddedBlockNumber>:<reversePaddedUncleIndex>
		key := fmt.Sprintf("%s:U:%s:%s", bigtable.chainId, reversedPaddedBlockNumber(block.GetNumber()), iReversed)
		mut := gcp_bigtable.NewMutation()

		b, err := proto.Marshal(&uncleIndexed)
		if err != nil {
			return nil, nil, fmt.Errorf("error marshalling proto object err: %w", err)
		}

		mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

		bulkData.Keys = append(bulkData.Keys, key)
		bulkData.Muts = append(bulkData.Muts, mut)

		indexes := []string{
			// Index uncle by the miners address
			fmt.Sprintf("%s:I:U:%x:TIME:%s:%s", bigtable.chainId, uncle.GetCoinbase(), reversePaddedBigtableTimestamp(block.Time), iReversed),
		}

		for _, idx := range indexes {
			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

			bulkData.Keys = append(bulkData.Keys, idx)
			bulkData.Muts = append(bulkData.Muts, mut)
		}
	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformWithdrawals accepts an eth1 block and creates bigtable mutations.
// It transforms the withdrawals contained within a block, extracts the necessary information to create a view and writes that information to bigtable
// It writes uncles to table data:
// Row:    <chainID>:W:<reversePaddedNumber>:<reversedWithdrawalIndex>
// Family: f
// Column: data
// Cell:   Proto<Eth1WithdrawalIndexed>
// Example scan: "1:W:" returns withdrawals in desc order
// Example scan: "1:W:984886725" returns mainnet withdrawals included after block 15113275 (1000000000 - 984886725)
//
// It indexes withdrawals by:
// Row:    <chainID>:I:W:<Address>:TIME:<reversePaddedBigtableTimestamp>
// Family: f
// Column: <chainID>:W:<reversePaddedNumber>
// Cell:   nil
// Example lookup: "1:I:W:ea674fdde714fd979de3edf0f56aa9716b898ec8:TIME:" returns withdrawals received by ethermine in desc order
func (bigtable *Bigtable) TransformWithdrawals(block *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_withdrawals").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	if len(block.Withdrawals) > int(utils.Config.Chain.ClConfig.MaxWithdrawalsPerPayload) {
		return nil, nil, fmt.Errorf("unexpected number of withdrawals in block expected at most %v but got: %v", utils.Config.Chain.ClConfig.MaxWithdrawalsPerPayload, len(block.Withdrawals))
	}

	for _, withdrawal := range block.Withdrawals {
		iReversed := reversePaddedIndex(int(withdrawal.Index), 9999999999999)

		withdrawalIndexed := types.Eth1WithdrawalIndexed{
			BlockNumber:    block.Number,
			Index:          withdrawal.Index,
			ValidatorIndex: withdrawal.ValidatorIndex,
			Address:        withdrawal.Address,
			Amount:         withdrawal.Amount,
			Time:           block.Time,
		}

		bigtable.markBalanceUpdate(withdrawal.Address, []byte{0x0}, bulkMetadataUpdates, cache)

		// store withdrawals with the key <chainid>:W:<reversePaddedBlockNumber>:<reversePaddedWithdrawalIndex>
		key := fmt.Sprintf("%s:W:%s:%s", bigtable.chainId, reversedPaddedBlockNumber(block.GetNumber()), iReversed)
		mut := gcp_bigtable.NewMutation()

		b, err := proto.Marshal(&withdrawalIndexed)
		if err != nil {
			return nil, nil, fmt.Errorf("error marshalling proto object err: %w", err)
		}

		mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

		bulkData.Keys = append(bulkData.Keys, key)
		bulkData.Muts = append(bulkData.Muts, mut)

		indexes := []string{
			// Index withdrawal by address
			fmt.Sprintf("%s:I:W:%x:TIME:%s:%s", bigtable.chainId, withdrawal.Address, reversePaddedBigtableTimestamp(block.Time), iReversed),
		}

		for _, idx := range indexes {
			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

			bulkData.Keys = append(bulkData.Keys, idx)
			bulkData.Muts = append(bulkData.Muts, mut)
		}
	}

	return bulkData, bulkMetadataUpdates, nil
}

type BridgeQueueRequest struct {
	TxHash         []byte
	TxIndex        int
	ItxIndex       int
	BlockNumber    uint64
	BlockTimestamp time.Time
	From           []byte
	Fee            uint64
}

func (bigtable *Bigtable) TransformConsolidationRequests(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	consolidationContractAddress := hexutil.MustDecode(utils.Config.Chain.PectraConsolidationRequestContractAddress)
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_consolidation_requests").Observe(time.Since(startTime).Seconds())
	}()

	var queueRequests []BridgeQueueRequest
	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}

		var revertSource string
		for j, itx := range tx.GetItx() {
			if j > ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of internal transactions in block expected at most %d but got: %v, tx: %x", ITX_PER_TX_LIMIT, j, tx.GetHash())
			}
			// check for error before skipping, otherwise we loose track of cascading reverts
			if itx.ErrorMsg != "" {
				if revertSource == "" || !strings.HasPrefix(itx.Path, revertSource) {
					revertSource = strings.TrimSuffix(itx.Path, "]")
				}
				continue
			}
			if revertSource != "" && strings.HasPrefix(itx.Path, revertSource) {
				continue
			}

			if !bytes.Equal(itx.To, consolidationContractAddress) {
				continue
			}

			if itx.Type == "staticcall" {
				continue
			}

			if bytes.Equal(itx.Value, []byte{0x0}) {
				continue
			}

			queueRequests = append(queueRequests, BridgeQueueRequest{
				Fee:            new(big.Int).SetBytes(itx.Value).Uint64(),
				TxHash:         tx.Hash,
				TxIndex:        i,
				ItxIndex:       j,
				BlockNumber:    blk.Number,
				BlockTimestamp: blk.Time.AsTime(),
				From:           tx.From,
			})
		}
	}

	var requestIndex int
	for _, tx := range blk.GetTransactions() {
		for _, log := range tx.GetLogs() {
			if bytes.Equal(log.Address, consolidationContractAddress) {
				// we have found a consolidation event
				// now slice out the data
				// source_address: Bytes20
				// source_pubkey: Bytes48
				// target_pubkey: Bytes48

				elData := types.ElConsolidationRequestData(log.Data)
				sourceAddress, _ := elData.GetSourceAddressBytes()
				sourcePubkey, _ := elData.GetSourceValidatorPubkey()
				targetPubkey, _ := elData.GetTargetValidatorPubkey()

				if sourceAddress == nil || sourcePubkey == nil || targetPubkey == nil {
					logger.Warnf("error parsing consolidation event: %x %x %d", sourceAddress, sourcePubkey, targetPubkey)
					continue
				}

				logger.Infof("consolidation event: %x %x %x", sourceAddress, sourcePubkey, targetPubkey)

				request := queueRequests[requestIndex]
				_, err := WriterDb.Exec(`
				INSERT INTO eth1_consolidation_requests (tx_hash, tx_index, itx_index, block_number, block_ts, from_address, fee, source_address, source_pubkey, target_pubkey) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				ON CONFLICT (tx_hash, tx_index, itx_index) DO UPDATE
				SET block_number = EXCLUDED.block_number, block_ts = EXCLUDED.block_ts, from_address = EXCLUDED.from_address, fee = EXCLUDED.fee, source_address = EXCLUDED.source_address, source_pubkey = EXCLUDED.source_pubkey, target_pubkey = EXCLUDED.target_pubkey
				`, request.TxHash, request.TxIndex, request.ItxIndex, request.BlockNumber, request.BlockTimestamp, request.From, request.Fee, sourceAddress, sourcePubkey, targetPubkey)

				if err != nil {
					return nil, nil, err
				}
				requestIndex++
			}
		}
	}

	if requestIndex != len(queueRequests) {
		logger.Errorf("unexpected number of consolidation requests for block %d", blk.Number)
	}

	return bulkData, bulkMetadataUpdates, nil
}

func (bigtable *Bigtable) TransformWithdrawalRequests(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	withdrawalContractAddress := hexutil.MustDecode(utils.Config.Chain.PectraWithdrawalRequestContractAddress)
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_withdrawal_requests").Observe(time.Since(startTime).Seconds())
	}()

	var queueRequests []BridgeQueueRequest
	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}

		var revertSource string
		for j, itx := range tx.GetItx() {
			if j > ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of internal transactions in block expected at most %d but got: %v, tx: %x", ITX_PER_TX_LIMIT, j, tx.GetHash())
			}
			// check for error before skipping, otherwise we loose track of cascading reverts
			if itx.ErrorMsg != "" {
				if revertSource == "" || !strings.HasPrefix(itx.Path, revertSource) {
					revertSource = strings.TrimSuffix(itx.Path, "]")
				}
				continue
			}
			if revertSource != "" && strings.HasPrefix(itx.Path, revertSource) {
				continue
			}

			if !bytes.Equal(itx.To, withdrawalContractAddress) {
				continue
			}

			if itx.Type == "staticcall" {
				continue
			}

			if bytes.Equal(itx.Value, []byte{0x0}) {
				continue
			}

			queueRequests = append(queueRequests, BridgeQueueRequest{
				Fee:            new(big.Int).SetBytes(itx.Value).Uint64(),
				TxHash:         tx.Hash,
				TxIndex:        i,
				ItxIndex:       j,
				BlockNumber:    blk.Number,
				BlockTimestamp: blk.Time.AsTime(),
				From:           tx.From,
			})
		}
	}

	var requestIndex int
	for _, tx := range blk.GetTransactions() {
		for _, log := range tx.GetLogs() {
			if bytes.Equal(log.Address, withdrawalContractAddress) {
				// we have found a withdrawal event
				// now slice out the data
				// source_address: Bytes20
				// validator_pubkey: Bytes48
				// amount: uint64

				elData := types.ElWithdrawalRequestData(log.Data)
				sourceAddress, _ := elData.GetSourceAddressBytes()
				validatorPubkey, _ := elData.GetValidatorPubkey()
				amount, _ := elData.GetAmountUint64()

				if sourceAddress == nil || validatorPubkey == nil {
					logger.Warnf("error parsing withdrawal event: %x %x %d", sourceAddress, validatorPubkey, amount)
					continue
				}

				logger.Infof("withdrawal event: %x %x %d", sourceAddress, validatorPubkey, amount)

				request := queueRequests[requestIndex]
				_, err := WriterDb.Exec(`
				INSERT INTO eth1_withdrawal_requests (tx_hash, tx_index, itx_index, block_number, block_ts, from_address, fee, source_address, validator_pubkey, amount) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
				ON CONFLICT (tx_hash, tx_index, itx_index) DO UPDATE
				SET block_number = EXCLUDED.block_number, block_ts = EXCLUDED.block_ts, from_address = EXCLUDED.from_address, fee = EXCLUDED.fee, source_address = EXCLUDED.source_address, validator_pubkey = EXCLUDED.validator_pubkey, amount = EXCLUDED.amount
				`, request.TxHash, request.TxIndex, request.ItxIndex, request.BlockNumber, request.BlockTimestamp, request.From, request.Fee, sourceAddress, validatorPubkey, amount)

				if err != nil {
					return nil, nil, err
				}
				requestIndex++
			}
		}
	}

	if requestIndex != len(queueRequests) {
		logger.Errorf("unexpected number of withdrawal requests for block %d", blk.Number)
	}

	return bulkData, bulkMetadataUpdates, nil
}

type IndexKeys struct {
	indexes []string
	keys    []string
}

type SortByIndexes IndexKeys

func (sbi SortByIndexes) Len() int {
	return len(sbi.indexes)
}

func (sbi SortByIndexes) Swap(i, j int) {
	sbi.indexes[i], sbi.indexes[j] = sbi.indexes[j], sbi.indexes[i]
	sbi.keys[i], sbi.keys[j] = sbi.keys[j], sbi.keys[i]
}

func (sbi SortByIndexes) Less(i, j int) bool {
	i_splits := strings.Split(sbi.indexes[i], ":")
	j_splits := strings.Split(sbi.indexes[j], ":")
	if len(i_splits) != len(j_splits) || len(i_splits) < 7 {
		utils.LogError(nil, "unexpected bigtable transaction indices", 0, map[string]interface{}{"index_i": sbi.indexes[i], "index_j": sbi.indexes[j]})
		return false
	}

	// block
	if i_splits[5] != j_splits[5] {
		return i_splits[5] < j_splits[5]
	}
	// tx idx
	if i_splits[6] != j_splits[6] {
		if i_splits[6] == strconv.Itoa(TX_PER_BLOCK_LIMIT) || j_splits[6] == strconv.Itoa(TX_PER_BLOCK_LIMIT) {
			return j_splits[6] == strconv.Itoa(TX_PER_BLOCK_LIMIT)
		}
		return i_splits[6] < j_splits[6]
	}
	// itx idx
	if len(i_splits) > 7 && i_splits[7] != j_splits[7] {
		if i_splits[7] == strconv.Itoa(ITX_PER_TX_LIMIT) || j_splits[7] == strconv.Itoa(ITX_PER_TX_LIMIT) {
			return j_splits[7] == strconv.Itoa(ITX_PER_TX_LIMIT)
		}
		return i_splits[7] < j_splits[7]
	}
	// shouldn't happen, this means we've the same key twice
	utils.LogError(nil, "unexpected bigtable transaction indices", 0, map[string]interface{}{"index_i": sbi.indexes[i], "index_j": sbi.indexes[j]})
	return false
}

func (bigtable *Bigtable) rearrangeReversePaddedIndexZero(ctx context.Context, indexes, keys []string) ([]string, []string) {
	if len(indexes) < 2 {
		return indexes, keys
	}

	// first find out if we've a (sub)transaction with index 0 whose block/transaction has maybe not been completed by the request
	// if we find one, make sure we do complete the request by querying the remainder from bigtable. So we won't miss that (i)tx next time (=> ignoring the query limit)
	for i := 0; i < len(indexes); i++ {
		splits := strings.Split(indexes[i], ":")
		if len(splits) < 7 {
			utils.LogError(nil, "unexpected bigtable transaction index", 0, map[string]interface{}{"index": indexes[i]})
			continue
		}

		if splits[6] != strconv.Itoa(TX_PER_BLOCK_LIMIT) && len(splits) > 7 && splits[7] != strconv.Itoa(ITX_PER_TX_LIMIT) {
			continue
		}
		// check if results list all following (i)txs already
		for i++; i < len(indexes); i++ {
			next_splits := strings.Split(indexes[i], ":")
			if len(next_splits) < 7 {
				utils.LogError(nil, "unexpected bigtable transaction index", 0, map[string]interface{}{"index": indexes[i]})
				continue
			}
			if next_splits[5] != splits[5] || (len(splits) > 7 && next_splits[6] != splits[6]) {
				// next block/tx
				break
			}
		}
		if i == len(indexes) {
			// block/tx maybe isn't fully included in results, request all missing entries (ignoring the query limit)
			i, err := strconv.Atoi(splits[5])
			if err != nil {
				utils.LogError(err, "error converting bigtable transaction index timestamp", 0, map[string]interface{}{"index": splits[5]})
				continue
			}
			splits[5] = fmt.Sprintf("%d", i+1)
			rowRange := gcp_bigtable.NewRange(indexes[len(indexes)-1]+"\x00", strings.Join(splits[:6], ":"))
			err = bigtable.tableData.ReadRows(ctx, rowRange, func(row gcp_bigtable.Row) bool {
				keys = append(keys, strings.TrimPrefix(row[DEFAULT_FAMILY][0].Column, "f:"))
				indexes = append(indexes, row.Key())
				return true
			})
			if err != nil {
				utils.LogError(err, "error reading from bigtable", 0)
			}
			break
		}
		i--
	}

	// sort
	ik := IndexKeys{indexes, keys}
	sort.Sort(SortByIndexes(ik))

	return ik.indexes, ik.keys
}

func skipBlockIfLastTxIndex(key string) string {
	splits := strings.Split(key, ":")
	if len(splits) < 7 {
		utils.LogError(nil, "unexpected bigtable transaction index", 0, map[string]interface{}{"index": key})
		return key
	}
	if len(splits) == 8 && splits[7] == strconv.Itoa(ITX_PER_TX_LIMIT) && splits[6] != strconv.Itoa(TX_PER_BLOCK_LIMIT) {
		i, err := strconv.Atoi(splits[6])
		if err != nil {
			utils.LogError(err, "error converting bigtable transaction index", 0, map[string]interface{}{"index": splits[6]})
		} else {
			splits[6] = fmt.Sprintf("%d", i+1)
		}
		splits = splits[:7]
	}
	if splits[6] == strconv.Itoa(TX_PER_BLOCK_LIMIT) {
		i, err := strconv.Atoi(splits[5])
		if err != nil {
			utils.LogError(err, "error converting bigtable transaction index timestamp", 0, map[string]interface{}{"index": splits[5]})
		} else {
			splits[5] = fmt.Sprintf("%d", i+1)
			return strings.Join(splits[:6], ":") + ":"
		}
	}
	return key
}

func (bigtable *Bigtable) GetEth1TxsForAddress(prefix string, limit int64) ([]*types.Eth1TransactionIndexed, []string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"prefix": prefix,
			"limit":  limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	// add \x00 to the row range such that we skip the previous value
	rowRange := gcp_bigtable.NewRange(prefix+"\x00", prefixSuccessor(prefix, 5))
	data := make([]*types.Eth1TransactionIndexed, 0, limit)
	keys := make([]string, 0, limit)
	indexes := make([]string, 0, limit)
	keysMap := make(map[string]*types.Eth1TransactionIndexed, limit)

	err := bigtable.tableData.ReadRows(ctx, rowRange, func(row gcp_bigtable.Row) bool {
		keys = append(keys, strings.TrimPrefix(row[DEFAULT_FAMILY][0].Column, "f:"))
		indexes = append(indexes, row.Key())
		return true
	}, gcp_bigtable.LimitRows(limit))
	if err != nil {
		return nil, nil, err
	}

	if len(keys) == 0 {
		return data, nil, nil
	}

	indexes, keys = bigtable.rearrangeReversePaddedIndexZero(ctx, indexes, keys)

	err = bigtable.tableData.ReadRows(ctx, gcp_bigtable.RowList(keys), func(row gcp_bigtable.Row) bool {
		b := &types.Eth1TransactionIndexed{}
		err := proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, b)

		if err != nil {
			logrus.Fatalf("error parsing Eth1TransactionIndexed data: %v", err)
		}
		keysMap[row.Key()] = b

		return true
	})
	if err != nil {
		logger.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1TxsForAddress")
		return nil, nil, err
	}

	for _, key := range keys {
		if d := keysMap[key]; d != nil {
			data = append(data, d)
		}
	}

	return data, indexes, nil
}

func (bigtable *Bigtable) GetAddressesNamesArMetadata(names *map[string]string, inputMetadata *map[string]*types.ERC20Metadata) (map[string]string, map[string]*types.ERC20Metadata, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"names":         names,
			"inputMetadata": inputMetadata,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	outputMetadata := make(map[string]*types.ERC20Metadata)

	g := new(errgroup.Group)
	g.SetLimit(25)
	mux := sync.Mutex{}

	if names != nil {
		g.Go(func() error {
			err := bigtable.GetAddressNames(*names)
			if err != nil {
				return err
			}
			return nil
		})
	}

	if inputMetadata != nil {
		for address := range *inputMetadata {
			address := address
			g.Go(func() error {
				metadata, err := bigtable.GetERC20MetadataForAddress([]byte(address))
				if err != nil {
					return err
				}
				mux.Lock()
				outputMetadata[address] = metadata
				mux.Unlock()
				return nil
			})
		}
	}

	err := g.Wait()
	if err != nil {
		return nil, nil, err
	}

	return *names, outputMetadata, nil
}

func (bigtable *Bigtable) GetIndexedEth1Transaction(txHash []byte) (*types.Eth1TransactionIndexed, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"txHash": txHash,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()
	key := fmt.Sprintf("%s:TX:%x", bigtable.chainId, txHash)
	row, err := bigtable.tableData.ReadRow(ctx, key)

	if err != nil {
		return nil, err
	}
	if row == nil {
		return nil, nil
	}

	indexedTx := &types.Eth1TransactionIndexed{}
	err = proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, indexedTx)
	if err != nil {
		return nil, err
	} else {
		return indexedTx, nil
	}
}

func (bigtable *Bigtable) GetAddressTransactionsTableData(address []byte, pageToken string) (*types.DataTableResponse, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address":   address,
			"pageToken": pageToken,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	defaultPageToken := fmt.Sprintf("%s:I:TX:%x:%s:", bigtable.chainId, address, FILTER_TIME)
	if pageToken == "" {
		pageToken = defaultPageToken
	} else if !strings.HasPrefix(pageToken, defaultPageToken) {
		return nil, fmt.Errorf("invalid pageToken for function GetAddressTransactionsTableData: %s", pageToken)
	}

	transactions, keys, err := BigtableClient.GetEth1TxsForAddress(pageToken, DefaultInfScrollRows)
	if err != nil {
		return nil, err
	}

	idxs := make([]int64, len(keys))
	for i, k := range keys {
		tx_idx, err := strconv.Atoi(strings.Split(k, ":")[6])
		if err != nil {
			return nil, fmt.Errorf("error parsing Eth1InternalTransactionIndexed tx index: %v", err)
		}
		tx_idx = TX_PER_BLOCK_LIMIT - tx_idx
		if tx_idx < 0 {
			return nil, fmt.Errorf("invalid Eth1InternalTransactionIndexed tx index: %d", tx_idx)
		}

		idxs[i] = int64(tx_idx)
	}

	contractInteractionTypes, err := BigtableClient.GetAddressContractInteractionsAtTransactions(transactions, idxs)
	if err != nil {
		utils.LogError(err, "error getting contract states", 0)
	}

	// retrieve metadata
	names := make(map[string]string)
	for _, t := range transactions {
		names[string(t.From)] = ""
		names[string(t.To)] = ""
	}
	names, _, err = BigtableClient.GetAddressesNamesArMetadata(&names, nil)
	if err != nil {
		return nil, err
	}

	tableData := make([][]interface{}, len(transactions))
	for i, t := range transactions {
		fromName := names[string(t.From)]
		var contractInteraction types.ContractInteractionType
		if len(contractInteractionTypes) > i {
			contractInteraction = contractInteractionTypes[i]
		}

		tableData[i] = []interface{}{
			utils.FormatTransactionHashFromStatus(t.Hash, t.Status),
			utils.FormatMethod(bigtable.GetMethodLabel(t.MethodId, contractInteraction)),
			utils.FormatBlockNumber(t.BlockNumber),
			utils.FormatTimestamp(t.Time.AsTime().Unix()),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.From, fromName, false, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatInOutSelf(address, t.From, t.To),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.To, BigtableClient.GetAddressLabel(names[string(t.To)], contractInteraction), contractInteraction != types.CONTRACT_NONE, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatAmount(new(big.Int).SetBytes(t.Value), utils.Config.Frontend.ElCurrency, 6),
		}
	}

	token := ""
	if len(keys) > 0 {
		token = skipBlockIfLastTxIndex(keys[len(keys)-1])
	}

	data := &types.DataTableResponse{
		Data:        tableData,
		PagingToken: token,
	}

	return data, nil
}

func (bigtable *Bigtable) GetEth1BlocksForAddress(prefix string, limit int64) ([]*types.Eth1BlockIndexed, string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"prefix": prefix,
			"limit":  limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	// add \x00 to the row range such that we skip the previous value
	rowRange := gcp_bigtable.NewRange(prefix+"\x00", prefixSuccessor(prefix, 4))
	data := make([]*types.Eth1BlockIndexed, 0, limit)
	keys := make([]string, 0, limit)
	indexes := make([]string, 0, limit)
	keysMap := make(map[string]*types.Eth1BlockIndexed, limit)

	err := bigtable.tableData.ReadRows(ctx, rowRange, func(row gcp_bigtable.Row) bool {
		keys = append(keys, strings.TrimPrefix(row[DEFAULT_FAMILY][0].Column, "f:"))
		indexes = append(indexes, row.Key())
		return true
	}, gcp_bigtable.LimitRows(limit))
	if err != nil {
		return nil, "", err
	}

	if len(keys) == 0 {
		return data, "", nil
	}

	err = bigtable.tableData.ReadRows(ctx, gcp_bigtable.RowList(keys), func(row gcp_bigtable.Row) bool {
		b := &types.Eth1BlockIndexed{}
		err := proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, b)

		if err != nil {
			logrus.Fatalf("error parsing Eth1BlockIndexed data: %v", err)
		}
		keysMap[row.Key()] = b

		return true
	})
	if err != nil {
		logger.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1BlocksForAddress")
		return nil, "", err
	}

	for _, key := range keys {
		if d := keysMap[key]; d != nil {
			data = append(data, d)
		}
	}

	return data, indexes[len(indexes)-1], nil
}

func (bigtable *Bigtable) GetAddressBlocksMinedTableData(address string, pageToken string) (*types.DataTableResponse, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address":   address,
			"pageToken": pageToken,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	defaultPageToken := fmt.Sprintf("%s:I:B:%s:", bigtable.chainId, address)
	if pageToken == "" {
		pageToken = defaultPageToken
	} else if !strings.HasPrefix(pageToken, defaultPageToken) {
		return nil, fmt.Errorf("invalid pageToken for function GetAddressBlocksMinedTableData: %s", pageToken)
	}

	blocks, lastKey, err := BigtableClient.GetEth1BlocksForAddress(pageToken, DefaultInfScrollRows)
	if err != nil {
		return nil, err
	}

	tableData := make([][]interface{}, len(blocks))
	for i, b := range blocks {
		reward := new(big.Int).Add(utils.Eth1BlockReward(b.Number, b.Difficulty), new(big.Int).SetBytes(b.TxReward))

		tableData[i] = []interface{}{
			utils.FormatBlockNumber(b.Number),
			utils.FormatTimestamp(b.Time.AsTime().Unix()),
			utils.FormatBlockUsage(b.GasUsed, b.GasLimit),
			utils.FormatAmount(reward, utils.Config.Frontend.ElCurrency, 6),
		}
	}

	data := &types.DataTableResponse{
		Data:        tableData,
		PagingToken: lastKey,
	}

	return data, nil
}

func (bigtable *Bigtable) GetEth1UnclesForAddress(prefix string, limit int64) ([]*types.Eth1UncleIndexed, string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"prefix": prefix,
			"limit":  limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	// add \x00 to the row range such that we skip the previous value
	rowRange := gcp_bigtable.NewRange(prefix+"\x00", prefixSuccessor(prefix, 4))
	data := make([]*types.Eth1UncleIndexed, 0, limit)
	keys := make([]string, 0, limit)
	indexes := make([]string, 0, limit)
	keysMap := make(map[string]*types.Eth1UncleIndexed, limit)

	err := bigtable.tableData.ReadRows(ctx, rowRange, func(row gcp_bigtable.Row) bool {
		keys = append(keys, strings.TrimPrefix(row[DEFAULT_FAMILY][0].Column, "f:"))
		indexes = append(indexes, row.Key())
		return true
	}, gcp_bigtable.LimitRows(limit))
	if err != nil {
		return nil, "", err
	}

	if len(keys) == 0 {
		return data, "", nil
	}

	err = bigtable.tableData.ReadRows(ctx, gcp_bigtable.RowList(keys), func(row gcp_bigtable.Row) bool {
		b := &types.Eth1UncleIndexed{}
		err := proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, b)

		if err != nil {
			logrus.Fatalf("error parsing Eth1UncleIndexed data: %v", err)
		}
		keysMap[row.Key()] = b

		return true
	})
	if err != nil {
		logger.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1UnclesForAddress")
		return nil, "", err
	}

	for _, key := range keys {
		if d := keysMap[key]; d != nil {
			data = append(data, d)
		}
	}

	return data, indexes[len(indexes)-1], nil
}

func (bigtable *Bigtable) GetAddressUnclesMinedTableData(address string, pageToken string) (*types.DataTableResponse, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address":   address,
			"pageToken": pageToken,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	defaultPageToken := fmt.Sprintf("%s:I:U:%s:", bigtable.chainId, address)
	if pageToken == "" {
		pageToken = defaultPageToken
	} else if !strings.HasPrefix(pageToken, defaultPageToken) {
		return nil, fmt.Errorf("invalid pageToken for function GetAddressUnclesMinedTableData: %s", pageToken)
	}

	uncles, lastKey, err := BigtableClient.GetEth1UnclesForAddress(pageToken, DefaultInfScrollRows)
	if err != nil {
		return nil, err
	}

	tableData := make([][]interface{}, len(uncles))
	for i, u := range uncles {
		tableData[i] = []interface{}{
			utils.FormatBlockNumber(u.Number),
			utils.FormatTimestamp(u.Time.AsTime().Unix()),
			utils.FormatDifficulty(new(big.Int).SetBytes(u.Difficulty)),
			utils.FormatAmount(new(big.Int).SetBytes(u.Reward), utils.Config.Frontend.ElCurrency, 6),
		}
	}

	data := &types.DataTableResponse{
		Data:        tableData,
		PagingToken: lastKey,
	}

	return data, nil
}

func (bigtable *Bigtable) GetEth1BtxForAddress(prefix string, limit int64) ([]*types.Eth1BlobTransactionIndexed, string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"prefix": prefix,
			"limit":  limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	// add \x00 to the row range such that we skip the previous value
	rowRange := gcp_bigtable.NewRange(prefix+"\x00", prefixSuccessor(prefix, 5))
	data := make([]*types.Eth1BlobTransactionIndexed, 0, limit)
	keys := make([]string, 0, limit)
	indexes := make([]string, 0, limit)
	keysMap := make(map[string]*types.Eth1BlobTransactionIndexed, limit)

	err := bigtable.tableData.ReadRows(ctx, rowRange, func(row gcp_bigtable.Row) bool {
		keys = append(keys, strings.TrimPrefix(row[DEFAULT_FAMILY][0].Column, "f:"))
		indexes = append(indexes, row.Key())
		return true
	}, gcp_bigtable.LimitRows(limit))
	if err != nil {
		return nil, "", err
	}
	if len(keys) == 0 {
		return data, "", nil
	}

	indexes, keys = bigtable.rearrangeReversePaddedIndexZero(ctx, indexes, keys)

	err = bigtable.tableData.ReadRows(ctx, gcp_bigtable.RowList(keys), func(row gcp_bigtable.Row) bool {
		b := &types.Eth1BlobTransactionIndexed{}
		err := proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, b)
		if err != nil {
			logrus.Fatalf("error parsing Eth1BlobTransactionIndexed data: %v", err)
		}
		keysMap[row.Key()] = b
		return true
	})
	if err != nil {
		logger.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1BtxForAddress")
		return nil, "", err
	}

	for _, key := range keys {
		if d := keysMap[key]; d != nil {
			data = append(data, d)
		}
	}

	return data, skipBlockIfLastTxIndex(indexes[len(indexes)-1]), nil
}

func (bigtable *Bigtable) GetAddressBlobTableData(address []byte, pageToken string) (*types.DataTableResponse, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address":   address,
			"pageToken": pageToken,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	// defaults to most recent
	defaultPageToken := fmt.Sprintf("%s:I:BTX:%x:%s:", bigtable.chainId, address, FILTER_TIME)
	if pageToken == "" {
		pageToken = defaultPageToken
	} else if !strings.HasPrefix(pageToken, defaultPageToken) {
		return nil, fmt.Errorf("invalid pageToken for function GetAddressBlobTableData: %s", pageToken)
	}

	transactions, lastKey, err := bigtable.GetEth1BtxForAddress(pageToken, DefaultInfScrollRows)
	if err != nil {
		return nil, err
	}

	names := make(map[string]string)
	for _, t := range transactions {
		names[string(t.From)] = ""
		names[string(t.To)] = ""
	}
	names, _, err = BigtableClient.GetAddressesNamesArMetadata(&names, nil)
	if err != nil {
		return nil, err
	}

	tableData := make([][]interface{}, len(transactions))
	for i, t := range transactions {

		fromName := names[string(t.From)]
		toName := names[string(t.To)]

		tableData[i] = []interface{}{
			utils.FormatTransactionHash(t.Hash, t.ErrorMsg == ""),
			utils.FormatBlockNumber(t.BlockNumber),
			utils.FormatTimestamp(t.Time.AsTime().Unix()),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.From, fromName, false, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatInOutSelf(address, t.From, t.To),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.To, toName, false, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatBytesAmount(t.BlobGasPrice, "GWei", 6),
			utils.FormatBytesAmount(t.BlobTxFee, "ETH", 6),
			len(t.BlobVersionedHashes),
		}
	}

	data := &types.DataTableResponse{
		Data:        tableData,
		PagingToken: lastKey,
	}

	return data, nil
}

func (bigtable *Bigtable) GetEth1ItxsForAddress(prefix string, limit int64) ([]*types.Eth1InternalTransactionIndexed, []string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"prefix": prefix,
			"limit":  limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	// add \x00 to the row range such that we skip the previous value
	rowRange := gcp_bigtable.NewRange(prefix+"\x00", prefixSuccessor(prefix, 5))
	data := make([]*types.Eth1InternalTransactionIndexed, 0, limit)
	keys := make([]string, 0, limit)
	indexes := make([]string, 0, limit)

	keysMap := make(map[string]*types.Eth1InternalTransactionIndexed, limit)
	err := bigtable.tableData.ReadRows(ctx, rowRange, func(row gcp_bigtable.Row) bool {
		keys = append(keys, strings.TrimPrefix(row[DEFAULT_FAMILY][0].Column, "f:"))
		indexes = append(indexes, row.Key())
		return true
	}, gcp_bigtable.LimitRows(limit))
	if err != nil {
		return nil, nil, err
	}
	if len(keys) == 0 {
		return data, nil, nil
	}

	indexes, keys = bigtable.rearrangeReversePaddedIndexZero(ctx, indexes, keys)

	err = bigtable.tableData.ReadRows(ctx, gcp_bigtable.RowList(keys), func(row gcp_bigtable.Row) bool {
		b := &types.Eth1InternalTransactionIndexed{}
		err := proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, b)

		if err != nil {
			logrus.Fatalf("error parsing Eth1InternalTransactionIndexed data: %v", err)
		}

		// geth traces include zero-value staticalls
		if bytes.Equal(b.Value, []byte{}) {
			return true
		}
		keysMap[row.Key()] = b
		return true
	})
	if err != nil {
		logger.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1ItxForAddress")
		return nil, nil, err
	}

	for _, key := range keys {
		if d := keysMap[key]; d != nil {
			data = append(data, d)
		}
	}

	return data, indexes, nil
}

func (bigtable *Bigtable) GetAddressInternalTableData(address []byte, pageToken string) (*types.DataTableResponse, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address":   address,
			"pageToken": pageToken,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	// defaults to most recent
	defaultPageToken := fmt.Sprintf("%s:I:ITX:%x:%s:", bigtable.chainId, address, FILTER_TIME)
	if pageToken == "" {
		pageToken = defaultPageToken
	} else if !strings.HasPrefix(pageToken, defaultPageToken) {
		return nil, fmt.Errorf("invalid pageToken for function GetAddressInternalTableData: %s", pageToken)
	}

	itransactions, keys, err := bigtable.GetEth1ItxsForAddress(pageToken, DefaultInfScrollRows)
	if err != nil {
		return nil, err
	}

	names := make(map[string]string)
	for _, t := range itransactions {
		names[string(t.From)] = ""
		names[string(t.To)] = ""
	}
	names, _, err = BigtableClient.GetAddressesNamesArMetadata(&names, nil)
	if err != nil {
		return nil, err
	}

	idxs := make([][2]int64, len(keys))
	for i, k := range keys {
		tx_idx, err := strconv.Atoi(strings.Split(k, ":")[6])
		if err != nil {
			return nil, fmt.Errorf("error parsing Eth1InternalTransactionIndexed tx index: %v", err)
		}
		tx_idx = TX_PER_BLOCK_LIMIT - tx_idx
		if tx_idx < 0 {
			return nil, fmt.Errorf("invalid Eth1InternalTransactionIndexed tx index: %d", tx_idx)
		}

		trace_idx, err := strconv.Atoi(strings.Split(k, ":")[7])
		if err != nil {
			return nil, fmt.Errorf("error parsing Eth1InternalTransactionIndexed trace index: %v", err)
		}
		trace_idx = ITX_PER_TX_LIMIT - trace_idx
		if tx_idx < 0 {
			return nil, fmt.Errorf("invalid Eth1InternalTransactionIndexed trace index: %d", trace_idx)
		}
		idxs[i] = [2]int64{int64(tx_idx), int64(trace_idx)}
	}
	contractInteractionTypes, err := BigtableClient.GetAddressContractInteractionsAtITransactions(itransactions, idxs)
	if err != nil {
		utils.LogError(err, "error getting contract states", 0)
	}

	tableData := make([][]interface{}, len(itransactions))
	for i, t := range itransactions {

		fromName := names[string(t.From)]
		toName := names[string(t.To)]

		var from_contractInteraction, to_contractInteraction types.ContractInteractionType
		if len(contractInteractionTypes) > i {
			from_contractInteraction = contractInteractionTypes[i][0]
			to_contractInteraction = contractInteractionTypes[i][1]
		}

		if t.Type == "suicide" {
			// erigon's "suicide" might be misleading for users
			t.Type = "selfdestruct"
		}

		tableData[i] = []interface{}{
			utils.FormatTransactionHash(t.ParentHash, !t.Reverted),
			utils.FormatBlockNumber(t.BlockNumber),
			utils.FormatTimestamp(t.Time.AsTime().Unix()),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.From, BigtableClient.GetAddressLabel(fromName, from_contractInteraction), from_contractInteraction != types.CONTRACT_NONE, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatInOutSelf(address, t.From, t.To),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.To, BigtableClient.GetAddressLabel(toName, to_contractInteraction), to_contractInteraction != types.CONTRACT_NONE, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatAmount(new(big.Int).SetBytes(t.Value), utils.Config.Frontend.ElCurrency, 6),
			t.Type,
		}
	}

	token := ""
	if len(keys) > 0 {
		token = skipBlockIfLastTxIndex(keys[len(keys)-1])
	}

	data := &types.DataTableResponse{
		Data:        tableData,
		PagingToken: token,
	}

	return data, nil
}

func (bigtable *Bigtable) GetInternalTransfersForTransaction(transaction []byte, from []byte, traces []*rpc.GethTraceCallResult, currency string, blockNumber *big.Int) ([]types.ITransaction, error) {
	if len(traces) == 0 {
		return nil, nil
	}

	names := make(map[string]string)
	for _, trace := range traces {
		names[trace.From.String()] = ""
		names[trace.To.String()] = ""
	}

	err := bigtable.GetAddressNames(names)
	if err != nil {
		return nil, err
	}

	contractInteractionTypes, err := BigtableClient.GetAddressContractInteractionsAtTraces(traces, blockNumber)
	if err != nil {
		utils.LogError(err, "error getting contract states", 0)
	}

	paths := make(map[*rpc.GethTraceCallResult]string)
	data := make([]types.ITransaction, 0, len(traces)-1)
	revertedTraces := make(map[*rpc.GethTraceCallResult]struct{})
	for i := 1; i < len(traces); i++ {
		for index, call := range traces[i].Calls {
			paths[call] = fmt.Sprintf("%s %d", paths[traces[i]], index)
		}

		reverted := isReverted(traces[i], revertedTraces)

		from := traces[i].From.Bytes()
		to := traces[i].To.Bytes()
		value := common.FromHex(traces[i].Value)
		tx_type := traces[i].Type
		if tx_type == "suicide" {
			// erigon's "suicide" might be misleading for users
			tx_type = "selfdestruct"
		}
		input := make([]byte, 0)
		if len(traces[i].Input) > 2 {
			input, err = hex.DecodeString(traces[i].Input[2:])
			if err != nil {
				utils.LogError(err, "can't convert hex string", 0)
			}
		}

		var from_contractInteraction, to_contractInteraction types.ContractInteractionType
		if len(contractInteractionTypes) > i {
			from_contractInteraction = contractInteractionTypes[i][0]
			to_contractInteraction = contractInteractionTypes[i][1]
		}

		fromName := BigtableClient.GetAddressLabel(names[string(from)], from_contractInteraction)
		toName := BigtableClient.GetAddressLabel(names[string(to)], to_contractInteraction)

		itx := types.ITransaction{
			From:      utils.FormatAddress(from, nil, fromName, false, from_contractInteraction != types.CONTRACT_NONE, true),
			To:        utils.FormatAddress(to, nil, toName, false, to_contractInteraction != types.CONTRACT_NONE, true),
			Amount:    utils.FormatElCurrency(value, currency, 8, true, false, false, true),
			TracePath: utils.FormatGethTracePath(tx_type, paths[traces[i]], !reverted, bigtable.GetMethodLabel(input, from_contractInteraction)),
			Advanced:  tx_type == "delegatecall" || string(value) == "\x00",
			Reverted:  reverted,
		}

		gaslimit, err := strconv.ParseUint(traces[i].Gas, 0, 0)
		if err == nil {
			itx.Gas.Limit = gaslimit
		}

		data = append(data, itx)
		// gasusage, err := strconv.ParseUint(traces[i].Result.GasUsed, 0, 0)
		// if err == nil {
		// 	itx.Gas.Usage = gasusage
		// }
	}
	return data, nil
}

func isReverted(internal *rpc.GethTraceCallResult, revertedTraces map[*rpc.GethTraceCallResult]struct{}) bool {
	if _, exist := revertedTraces[internal]; exist {
		return true
	}
	var reverted bool
	if internal.Error != "" {
		reverted = true
		calls := internal.Calls
		for len(calls) != 0 {
			revertedTraces[calls[0]] = struct{}{}
			calls = append(calls[1:], calls[0].Calls...)
		}
	}
	return reverted
}

// currently only erc20
func (bigtable *Bigtable) GetArbitraryTokenTransfersForTransaction(transaction []byte) ([]*types.Transfer, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"transaction": transaction,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()
	// uses a more standard transfer in-between type so multiple token types can be handle before the final table response is generated
	transfers := map[int]*types.Eth1ERC20Indexed{}
	mux := sync.Mutex{}

	// get erc20 rows
	prefix := fmt.Sprintf("%s:ERC20:%x:", bigtable.chainId, transaction)
	rowRange := gcp_bigtable.NewRange(prefix+"\x00", prefixSuccessor(prefix, 3))
	err := bigtable.tableData.ReadRows(ctx, rowRange, func(row gcp_bigtable.Row) bool {
		b := &types.Eth1ERC20Indexed{}
		row_ := row[DEFAULT_FAMILY][0]
		err := proto.Unmarshal(row_.Value, b)
		if err != nil {
			logrus.Fatalf("error unmarshalling data for row %v: %v", row.Key(), err)
			return false
		}
		rowN, err := strconv.Atoi(strings.Split(row_.Row, ":")[3])
		if err != nil {
			logrus.Fatalf("error parsing data for row %v: %v", row.Key(), err)
			return false
		}
		rowN = ITX_PER_TX_LIMIT - rowN
		mux.Lock()
		transfers[rowN] = b
		mux.Unlock()
		return true
	}, gcp_bigtable.LimitRows(256))
	if err != nil {
		return nil, fmt.Errorf("error reading rows: %w", err)
	}

	names := make(map[string]string)
	tokens := make(map[string]*types.ERC20Metadata)
	tokensToAdd := make(map[string]*types.ERC20Metadata)
	// init
	for _, t := range transfers {
		names[string(t.From)] = ""
		names[string(t.To)] = ""
		tokens[string(t.TokenAddress)] = nil
	}
	g := new(errgroup.Group)
	g.SetLimit(25)
	g.Go(func() error {
		err := bigtable.GetAddressNames(names)
		if err != nil {
			return fmt.Errorf("error getting address names: %w", err)
		}
		return nil
	})

	for address := range tokens {
		address := address
		g.Go(func() error {
			metadata, err := bigtable.GetERC20MetadataForAddress([]byte(address))
			if err != nil {
				return fmt.Errorf("error getting erc20 metadata for address %v: %w", address, err)
			}
			mux.Lock()
			tokensToAdd[address] = metadata
			mux.Unlock()
			return nil
		})
	}
	err = g.Wait()
	if err != nil {
		return nil, fmt.Errorf("error getting token metadata: %w", err)
	}

	for k, v := range tokensToAdd {
		tokens[k] = v
	}

	data := make([]*types.Transfer, len(transfers))

	// sort by event id
	keys := make([]int, 0, len(transfers))
	for k := range transfers {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	for i, k := range keys {
		t := transfers[k]

		fromName := names[string(t.From)]
		toName := names[string(t.To)]
		from := utils.FormatAddress(t.From, t.TokenAddress, fromName, false, false, true)
		to := utils.FormatAddress(t.To, t.TokenAddress, toName, false, false, true)

		tb := &types.Eth1AddressBalance{
			Balance:  t.Value,
			Token:    t.TokenAddress,
			Metadata: tokens[string(t.TokenAddress)],
		}

		data[i] = &types.Transfer{
			From:   from,
			To:     to,
			Amount: utils.FormatTokenValue(tb, false),
			Token:  utils.FormatTokenName(tb),
		}

	}

	return data, nil
}

func (bigtable *Bigtable) GetEth1ERC20ForAddress(prefix string, limit int64) ([]*types.Eth1ERC20Indexed, string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"prefix": prefix,
			"limit":  limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	// add \x00 to the row range such that we skip the previous value
	rowRange := gcp_bigtable.NewRange(prefix+"\x00", prefixSuccessor(prefix, 5))
	data := make([]*types.Eth1ERC20Indexed, 0, limit)
	keys := make([]string, 0, limit)
	indexes := make([]string, 0, limit)

	keysMap := make(map[string]*types.Eth1ERC20Indexed, limit)
	err := bigtable.tableData.ReadRows(ctx, rowRange, func(row gcp_bigtable.Row) bool {
		keys = append(keys, strings.TrimPrefix(row[DEFAULT_FAMILY][0].Column, "f:"))
		indexes = append(indexes, row.Key())
		return true
	}, gcp_bigtable.LimitRows(limit))
	if err != nil {
		return nil, "", err
	}
	if len(keys) == 0 {
		return data, "", nil
	}

	indexes, keys = bigtable.rearrangeReversePaddedIndexZero(ctx, indexes, keys)

	err = bigtable.tableData.ReadRows(ctx, gcp_bigtable.RowList(keys), func(row gcp_bigtable.Row) bool {
		b := &types.Eth1ERC20Indexed{}
		err := proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, b)

		if err != nil {
			logrus.Fatalf("error parsing Eth1ERC20Indexed data: %v", err)
		}
		keysMap[row.Key()] = b
		return true
	})
	if err != nil {
		logger.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1ERC20ForAddress")
		return nil, "", err
	}

	for _, key := range keys {
		if d := keysMap[key]; d != nil {
			data = append(data, d)
		}
	}

	return data, skipBlockIfLastTxIndex(indexes[len(indexes)-1]), nil
}

func (bigtable *Bigtable) GetAddressErc20TableData(address []byte, pageToken string) (*types.DataTableResponse, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address":   address,
			"pageToken": pageToken,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	defaultPageToken := fmt.Sprintf("%s:I:ERC20:%x:%s:", bigtable.chainId, address, FILTER_TIME)
	if pageToken == "" {
		pageToken = defaultPageToken
	} else if !strings.HasPrefix(pageToken, defaultPageToken) {
		return nil, fmt.Errorf("invalid pageToken for function GetAddressErc20TableData: %s", pageToken)
	}

	transactions, lastKey, err := bigtable.GetEth1ERC20ForAddress(pageToken, DefaultInfScrollRows)
	if err != nil {
		return nil, err
	}

	names := make(map[string]string)
	tokens := make(map[string]*types.ERC20Metadata)
	for _, t := range transactions {
		names[string(t.From)] = ""
		names[string(t.To)] = ""
		tokens[string(t.TokenAddress)] = nil
	}
	names, tokens, err = BigtableClient.GetAddressesNamesArMetadata(&names, &tokens)
	if err != nil {
		return nil, err
	}

	tableData := make([][]interface{}, len(transactions))

	for i, t := range transactions {

		fromName := names[string(t.From)]
		toName := names[string(t.To)]

		tb := &types.Eth1AddressBalance{
			Address:  address,
			Balance:  t.Value,
			Token:    t.TokenAddress,
			Metadata: tokens[string(t.TokenAddress)],
		}

		tableData[i] = []interface{}{
			utils.FormatTransactionHash(t.ParentHash, true),
			utils.FormatBlockNumber(t.BlockNumber),
			utils.FormatTimestamp(t.Time.AsTime().Unix()),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.From, fromName, false, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatInOutSelf(address, t.From, t.To),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.To, toName, false, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatTokenValue(tb, true),
			utils.FormatTokenName(tb),
		}

	}

	data := &types.DataTableResponse{
		Data:        tableData,
		PagingToken: lastKey,
	}

	return data, nil
}

func (bigtable *Bigtable) GetEth1ERC721ForAddress(prefix string, limit int64) ([]*types.Eth1ERC721Indexed, string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"prefix": prefix,
			"limit":  limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	// add \x00 to the row range such that we don't include the prefix itself in the response. Converts range to open interval (start, end).
	// "1:I:ERC721:81d98c8fda0410ee3e9d7586cb949cd19fa4cf38:TIME;"
	rowRange := gcp_bigtable.NewRange(prefix+"\x00", prefixSuccessor(prefix, 5))

	data := make([]*types.Eth1ERC721Indexed, 0, limit)

	keys := make([]string, 0, limit)
	keysMap := make(map[string]*types.Eth1ERC721Indexed, limit)
	indexes := make([]string, 0, limit)

	//  1:I:ERC721:81d98c8fda0410ee3e9d7586cb949cd19fa4cf38:TIME:9223372035220135322:0052:00000
	err := bigtable.tableData.ReadRows(ctx, rowRange, func(row gcp_bigtable.Row) bool {
		keys = append(keys, strings.TrimPrefix(row[DEFAULT_FAMILY][0].Column, "f:"))
		indexes = append(indexes, row.Key())
		return true
	}, gcp_bigtable.LimitRows(limit))
	if err != nil {
		return nil, "", err
	}

	if len(keys) == 0 {
		return data, "", nil
	}

	indexes, keys = bigtable.rearrangeReversePaddedIndexZero(ctx, indexes, keys)

	err = bigtable.tableData.ReadRows(ctx, gcp_bigtable.RowList(keys), func(row gcp_bigtable.Row) bool {
		b := &types.Eth1ERC721Indexed{}
		err := proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, b)

		if err != nil {
			logrus.Fatalf("error parsing Eth1ERC721Indexed data: %v", err)
		}
		keysMap[row.Key()] = b
		return true
	})
	if err != nil {
		logger.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1ERC721ForAddress")
		return nil, "", err
	}

	for _, key := range keys {
		if d := keysMap[key]; d != nil {
			data = append(data, d)
		}
	}
	return data, skipBlockIfLastTxIndex(indexes[len(indexes)-1]), nil
}

func (bigtable *Bigtable) GetAddressErc721TableData(address []byte, pageToken string) (*types.DataTableResponse, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address":   address,
			"pageToken": pageToken,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	defaultPageToken := fmt.Sprintf("%s:I:ERC721:%x:%s:", bigtable.chainId, address, FILTER_TIME)
	if pageToken == "" {
		pageToken = defaultPageToken
	} else if !strings.HasPrefix(pageToken, defaultPageToken) {
		return nil, fmt.Errorf("invalid pageToken for function GetAddressErc721TableData: %s", pageToken)
	}

	transactions, lastKey, err := bigtable.GetEth1ERC721ForAddress(pageToken, DefaultInfScrollRows)
	if err != nil {
		return nil, err
	}

	names := make(map[string]string)
	for _, t := range transactions {
		names[string(t.From)] = ""
		names[string(t.To)] = ""
	}
	names, _, err = BigtableClient.GetAddressesNamesArMetadata(&names, nil)
	if err != nil {
		return nil, err
	}

	tableData := make([][]interface{}, len(transactions))
	for i, t := range transactions {
		fromName := names[string(t.From)]
		toName := names[string(t.To)]

		tableData[i] = []interface{}{
			utils.FormatTransactionHash(t.ParentHash, true),
			utils.FormatBlockNumber(t.BlockNumber),
			utils.FormatTimestamp(t.Time.AsTime().Unix()),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.From, fromName, false, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.To, toName, false, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatAddressAsLink(t.TokenAddress, "", true),
			new(big.Int).SetBytes(t.TokenId).String(),
		}
	}

	data := &types.DataTableResponse{
		Data:        tableData,
		PagingToken: lastKey,
	}

	return data, nil
}

func (bigtable *Bigtable) GetEth1ERC1155ForAddress(prefix string, limit int64) ([]*types.ETh1ERC1155Indexed, string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"prefix": prefix,
			"limit":  limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	rowRange := gcp_bigtable.NewRange(prefix+"\x00", prefixSuccessor(prefix, 5))

	data := make([]*types.ETh1ERC1155Indexed, 0, limit)

	keys := make([]string, 0, limit)
	keysMap := make(map[string]*types.ETh1ERC1155Indexed, limit)
	indexes := make([]string, 0, limit)

	err := bigtable.tableData.ReadRows(ctx, rowRange, func(row gcp_bigtable.Row) bool {
		keys = append(keys, strings.TrimPrefix(row[DEFAULT_FAMILY][0].Column, "f:"))
		indexes = append(indexes, row.Key())
		return true
	}, gcp_bigtable.LimitRows(limit))
	if err != nil {
		return nil, "", err
	}

	if len(keys) == 0 {
		return data, "", nil
	}

	indexes, keys = bigtable.rearrangeReversePaddedIndexZero(ctx, indexes, keys)

	err = bigtable.tableData.ReadRows(ctx, gcp_bigtable.RowList(keys), func(row gcp_bigtable.Row) bool {
		b := &types.ETh1ERC1155Indexed{}
		err := proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, b)

		if err != nil {
			logrus.Fatalf("error parsing ETh1ERC1155Indexed data: %v", err)
		}
		keysMap[row.Key()] = b
		return true
	})
	if err != nil {
		logger.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1ERC1155ForAddress")
		return nil, "", err
	}

	for _, key := range keys {
		if d := keysMap[key]; d != nil {
			data = append(data, d)
		}
	}
	return data, skipBlockIfLastTxIndex(indexes[len(indexes)-1]), nil
}

func (bigtable *Bigtable) GetAddressErc1155TableData(address []byte, pageToken string) (*types.DataTableResponse, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address":   address,
			"pageToken": pageToken,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	defaultPageToken := fmt.Sprintf("%s:I:ERC1155:%x:%s:", bigtable.chainId, address, FILTER_TIME)
	if pageToken == "" {
		pageToken = defaultPageToken
	} else if !strings.HasPrefix(pageToken, defaultPageToken) {
		return nil, fmt.Errorf("invalid pageToken for function GetAddressErc1155TableData: %s", pageToken)
	}

	transactions, lastKey, err := bigtable.GetEth1ERC1155ForAddress(pageToken, DefaultInfScrollRows)
	if err != nil {
		return nil, err
	}

	tableData := make([][]interface{}, len(transactions))

	names := make(map[string]string)
	for _, t := range transactions {
		names[string(t.From)] = ""
		names[string(t.To)] = ""
	}
	names, _, err = BigtableClient.GetAddressesNamesArMetadata(&names, nil)
	if err != nil {
		return nil, err
	}

	for i, t := range transactions {
		fromName := names[string(t.From)]
		toName := names[string(t.To)]

		tableData[i] = []interface{}{
			utils.FormatTransactionHash(t.ParentHash, true),
			utils.FormatBlockNumber(t.BlockNumber),
			utils.FormatTimestamp(t.Time.AsTime().Unix()),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.From, fromName, false, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.To, toName, false, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatAddressAsLink(t.TokenAddress, "", true),
			new(big.Int).SetBytes(t.TokenId).String(),
			new(big.Int).SetBytes(t.Value).String(),
		}
	}

	data := &types.DataTableResponse{
		Data:        tableData,
		PagingToken: lastKey,
	}

	return data, nil
}

func (bigtable *Bigtable) GetMetadataUpdates(prefix string, startToken string, limit int) ([]string, []*types.Eth1AddressBalance, error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_get_metadata_updates").Observe(time.Since(startTime).Seconds())
	}()

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
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

func (bigtable *Bigtable) GetMetadata(startToken string, limit int) ([]string, []*types.Eth1AddressBalance, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"startToken": startToken,
			"limit":      limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour*2))
	defer cancel()

	keys := make([]string, 0, limit)
	pairs := make([]*types.Eth1AddressBalance, 0, limit)

	err := bigtable.tableMetadata.ReadRows(ctx, gcp_bigtable.NewRange(startToken, ""), func(row gcp_bigtable.Row) bool {
		if !strings.HasPrefix(row.Key(), bigtable.chainId+":") {
			return false
		}
		keys = append(keys, row.Key())

		for _, ri := range row {
			for _, item := range ri {
				if strings.Contains(item.Column, "a:B:") {
					pairs = append(pairs, &types.Eth1AddressBalance{Address: common.FromHex(strings.Split(row.Key(), ":")[1]), Token: common.FromHex(strings.Split(item.Column, ":")[2])})
				}
			}
		}
		return true
	}, gcp_bigtable.LimitRows(int64(limit)))

	if err == context.DeadlineExceeded && len(keys) > 0 {
		return keys, pairs, nil
	}
	return keys, pairs, err
}

func (bigtable *Bigtable) GetMetadataForAddress(address []byte, offset uint64, limit uint64) (*types.Eth1AddressMetadata, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address": address,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	filter := gcp_bigtable.FamilyFilter(ACCOUNT_METADATA_FAMILY)
	row, err := bigtable.tableMetadata.ReadRow(ctx, fmt.Sprintf("%s:%x", bigtable.chainId, address), gcp_bigtable.RowFilter(filter))

	if err != nil {
		return nil, err
	}

	ret := &types.Eth1AddressMetadata{
		Balances: []*types.Eth1AddressBalance{},
		ERC20:    &types.ERC20Metadata{},
		Name:     "",
		EthBalance: &types.Eth1AddressBalance{
			Metadata: &types.ERC20Metadata{},
		},
		ERC20TokenLimit: ECR20TokensPerAddressLimit,
	}

	if limit == 0 || limit > ECR20TokensPerAddressLimit {
		limit = ECR20TokensPerAddressLimit
	}

	tokenCount := uint64(0)

	g := new(errgroup.Group)
	g.SetLimit(10)

	mux := sync.Mutex{}
	for _, ri := range row {
		for _, column := range ri {
			if strings.HasPrefix(column.Column, ACCOUNT_METADATA_FAMILY+":B:") {
				column := column

				if bytes.Equal(address, ZERO_ADDRESS) && column.Column != ACCOUNT_METADATA_FAMILY+":B:00" {
					//do not return token balances for the zero address
					continue
				}

				token := common.FromHex(strings.TrimPrefix(column.Column, "a:B:"))

				isNativeEth := bytes.Equal([]byte{0x00}, token)
				if !isNativeEth {
					// token is not ETH, check if token limit is reached
					if tokenCount >= limit {
						ret.ERC20TokenLimitExceeded = true
						continue
					}

					// skip token without value
					if len(column.Value) == 0 && len(token) > 1 {
						continue
					}

					// handle pagination
					if offset > 0 {
						offset--
						continue
					}

					// at this point, token will be added
					tokenCount++
				}

				g.Go(func() error {
					balance := &types.Eth1AddressBalance{
						Address: address,
						Token:   token,
						Balance: column.Value,
					}

					metadata, err := bigtable.GetERC20MetadataForAddress(token)
					if err != nil {
						return err
					}
					balance.Metadata = metadata

					mux.Lock()
					if isNativeEth {
						ret.EthBalance = balance
					} else {
						ret.Balances = append(ret.Balances, balance)
					}
					mux.Unlock()

					return nil
				})
			} else if column.Column == ACCOUNT_METADATA_FAMILY+":"+ACCOUNT_COLUMN_NAME {
				ret.Name = string(column.Value)
			}
		}
	}

	err = g.Wait()
	if err != nil {
		return nil, err
	}

	// sort balances based on token address (required for proper pagination)
	sort.Slice(ret.Balances, func(i, j int) bool {
		return bytes.Compare(ret.Balances[i].Token, ret.Balances[j].Token) < 0
	})

	return ret, nil
}

func (bigtable *Bigtable) GetBalanceForAddress(address []byte, token []byte) (*types.Eth1AddressBalance, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address": address,
			"token":   token,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	filter := gcp_bigtable.ChainFilters(gcp_bigtable.FamilyFilter(ACCOUNT_METADATA_FAMILY), gcp_bigtable.ColumnFilter(fmt.Sprintf("B:%x", token)))
	row, err := bigtable.tableMetadata.ReadRow(ctx, fmt.Sprintf("%s:%x", bigtable.chainId, address), gcp_bigtable.RowFilter(filter))

	if err != nil {
		return nil, err
	}

	if row == nil {
		return nil, nil
	}
	if val, ok := row[ACCOUNT_METADATA_FAMILY]; ok {
		if len(val) < 1 {
			return nil, fmt.Errorf("ReadItem is empty or nil")
		}

		ret := &types.Eth1AddressBalance{
			Address: address,
			Token:   token,
			Balance: row[ACCOUNT_METADATA_FAMILY][0].Value,
		}

		metadata, err := bigtable.GetERC20MetadataForAddress(token)
		if err != nil {
			return nil, err
		}
		ret.Metadata = metadata

		return ret, nil
	}

	return nil, fmt.Errorf("ACCOUNT_METADATA_FAMILY is not a valid index in row map")
}

func (bigtable *Bigtable) GetERC20MetadataForAddress(address []byte) (*types.ERC20Metadata, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address": address,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	if len(address) == 1 {
		return &types.ERC20Metadata{
			Decimals:    big.NewInt(18).Bytes(),
			Symbol:      utils.Config.Frontend.ElCurrency,
			TotalSupply: []byte{},
		}, nil
	}

	cacheKey := fmt.Sprintf("%s:ERC20:%#x", bigtable.chainId, address)
	if cached, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Hour*1, new(types.ERC20Metadata)); err == nil {
		return cached.(*types.ERC20Metadata), nil
	}

	// this function actually does not use bigtable right now, but it will in the future (see BIDS-1846, BIDS-1234)

	var row gcp_bigtable.Row
	var err error

	// ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	// defer cancel()
	// rowKey := fmt.Sprintf("%s:%x", bigtable.chainId, address)
	// filter := gcp_bigtable.FamilyFilter(ERC20_METADATA_FAMILY)
	// row, err = bigtable.tableMetadata.ReadRow(ctx, rowKey, gcp_bigtable.RowFilter(filter))
	// if err != nil {
	// 	 return nil, err
	// }

	if row == nil { // Retrieve token metadata from Ethplorer and store it for later usage
		logger.Infof("retrieving metadata for token %x via rpc", address)

		metadata, err := rpc.CurrentGethClient.GetERC20TokenMetadata(address)
		if err != nil {
			logger.Warnf("error retrieving metadata for token %x: %v", address, err)
			metadata = &types.ERC20Metadata{
				Decimals:    []byte{0x0},
				Symbol:      "UNKNOWN",
				TotalSupply: []byte{0x0}}

			err = cache.TieredCache.Set(cacheKey, metadata, time.Minute*10)
			if err != nil {
				return nil, err
			}
			return metadata, nil
		}

		// err = bigtable.SaveERC20Metadata(address, metadata)
		// if err != nil {
		// 	return nil, err
		// }

		err = cache.TieredCache.Set(cacheKey, metadata, time.Hour*1)
		if err != nil {
			return nil, err
		}

		return metadata, nil
	}

	// logger.Infof("retrieving metadata for token %x via bigtable", address)
	ret := &types.ERC20Metadata{}
	for _, ri := range row {
		for _, item := range ri {
			if item.Column == ERC20_METADATA_FAMILY+":"+ERC20_COLUMN_DECIMALS {
				ret.Decimals = item.Value
			} else if item.Column == ERC20_METADATA_FAMILY+":"+ERC20_COLUMN_TOTALSUPPLY {
				ret.TotalSupply = item.Value
			} else if item.Column == ERC20_METADATA_FAMILY+":"+ERC20_COLUMN_SYMBOL {
				ret.Symbol = string(item.Value)
			} else if item.Column == ERC20_METADATA_FAMILY+":"+ERC20_COLUMN_DESCRIPTION {
				ret.Description = string(item.Value)
			} else if item.Column == ERC20_METADATA_FAMILY+":"+ERC20_COLUMN_NAME {
				ret.Name = string(item.Value)
			} else if item.Column == ERC20_METADATA_FAMILY+":"+ERC20_COLUMN_LOGO {
				ret.Logo = item.Value
			} else if item.Column == ERC20_METADATA_FAMILY+":"+ERC20_COLUMN_LOGO_FORMAT {
				ret.LogoFormat = string(item.Value)
			} else if item.Column == ERC20_METADATA_FAMILY+":"+ERC20_COLUMN_PRICE {
				ret.Price = item.Value
			}
		}
	}

	err = cache.TieredCache.Set(cacheKey, ret, time.Hour*1)
	if err != nil {
		return nil, err
	}
	return ret, nil
}

func (bigtable *Bigtable) SaveERC20Metadata(address []byte, metadata *types.ERC20Metadata) error {
	rowKey := fmt.Sprintf("%s:%x", bigtable.chainId, address)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	mut := gcp_bigtable.NewMutation()
	if len(metadata.Decimals) > 0 {
		mut.Set(ERC20_METADATA_FAMILY, ERC20_COLUMN_DECIMALS, gcp_bigtable.Timestamp(0), metadata.Decimals)
	}

	if len(metadata.TotalSupply) > 0 {
		mut.Set(ERC20_METADATA_FAMILY, ERC20_COLUMN_TOTALSUPPLY, gcp_bigtable.Timestamp(0), metadata.TotalSupply)
	}

	if len(metadata.Symbol) > 0 {
		mut.Set(ERC20_METADATA_FAMILY, ERC20_COLUMN_SYMBOL, gcp_bigtable.Timestamp(0), []byte(metadata.Symbol))
	}

	if len(metadata.Name) > 0 {
		mut.Set(ERC20_METADATA_FAMILY, ERC20_COLUMN_NAME, gcp_bigtable.Timestamp(0), []byte(metadata.Name))
	}

	if len(metadata.Description) > 0 {
		mut.Set(ERC20_METADATA_FAMILY, ERC20_COLUMN_DESCRIPTION, gcp_bigtable.Timestamp(0), []byte(metadata.Description))
	}

	if len(metadata.Price) > 0 {
		mut.Set(ERC20_METADATA_FAMILY, ERC20_COLUMN_PRICE, gcp_bigtable.Timestamp(0), []byte(metadata.Price))
	}

	if len(metadata.Logo) > 0 && len(metadata.LogoFormat) > 0 {
		mut.Set(ERC20_METADATA_FAMILY, ERC20_COLUMN_LOGO, gcp_bigtable.Timestamp(0), metadata.Logo)
		mut.Set(ERC20_METADATA_FAMILY, ERC20_COLUMN_LOGO_FORMAT, gcp_bigtable.Timestamp(0), []byte(metadata.LogoFormat))
	}

	return bigtable.tableMetadata.Apply(ctx, rowKey, mut)
}

func (bigtable *Bigtable) GetAddressName(address []byte) (string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address": address,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	add := common.Address{}
	add.SetBytes(address)
	name, err := GetEnsNameForAddress(add)
	if err == nil && len(name) > 0 {
		return name, nil
	}
	if err != nil && err != sql.ErrNoRows {
		return "", err
	}

	rowKey := fmt.Sprintf("%s:%x", bigtable.chainId, address)
	cacheKey := bigtable.chainId + ":NAME:" + rowKey

	if wanted, err := cache.TieredCache.GetStringWithLocalTimeout(cacheKey, utils.Day); err == nil {
		// logrus.Infof("retrieved name for address %x from cache", address)
		return wanted, nil
	}

	filter := gcp_bigtable.ChainFilters(gcp_bigtable.FamilyFilter(ACCOUNT_METADATA_FAMILY), gcp_bigtable.ColumnFilter(ACCOUNT_COLUMN_NAME))

	row, err := bigtable.tableMetadata.ReadRow(ctx, rowKey, gcp_bigtable.RowFilter(filter))

	if err != nil || row == nil {
		err = cache.TieredCache.SetString(cacheKey, "", time.Hour)
		return "", err
	}

	wanted := string(row[ACCOUNT_METADATA_FAMILY][0].Value)
	err = cache.TieredCache.SetString(cacheKey, wanted, time.Hour)
	return wanted, err
}

func (bigtable *Bigtable) GetAddressNames(addresses map[string]string) error {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"addresses": addresses,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	if len(addresses) == 0 {
		return nil
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	keys := make([]string, 0, len(addresses))

	if err := GetEnsNamesForAddress(addresses); err != nil {
		return err
	}

	for address, label := range addresses {
		if label == "" {
			keys = append(keys, fmt.Sprintf("%s:%x", bigtable.chainId, address))
		}
	}

	filter := gcp_bigtable.ChainFilters(gcp_bigtable.FamilyFilter(ACCOUNT_METADATA_FAMILY), gcp_bigtable.ColumnFilter(ACCOUNT_COLUMN_NAME))

	keyPrefix := fmt.Sprintf("%s:", bigtable.chainId)
	err := bigtable.tableMetadata.ReadRows(ctx, gcp_bigtable.RowList(keys), func(r gcp_bigtable.Row) bool {
		address := strings.TrimPrefix(r.Key(), keyPrefix)
		addressBytes, _ := hex.DecodeString(address)
		addresses[string(addressBytes)] = string(r[ACCOUNT_METADATA_FAMILY][0].Value)

		return true
	}, gcp_bigtable.RowFilter(filter))

	return err
}

type isContractInfo struct {
	update *types.IsContractUpdate
	ts     gcp_bigtable.Timestamp
}

type contractInteractionAtRequest struct {
	address  string
	block    int64
	txIdx    int64
	traceIdx int64
}

func (bigtable *Bigtable) getAddressIsContractHistories(histories map[string][]isContractInfo) error {
	if len(histories) == 0 {
		return nil
	}

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	keys := make([]string, 0, len(histories))
	for address := range histories {
		keys = append(keys, fmt.Sprintf("%s:S:%s", bigtable.chainId, address))
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	filter := gcp_bigtable.ChainFilters(gcp_bigtable.FamilyFilter(ACCOUNT_METADATA_FAMILY), gcp_bigtable.ColumnFilter(ACCOUNT_IS_CONTRACT))

	keyPrefix := fmt.Sprintf("%s:S:", bigtable.chainId)
	err := bigtable.tableMetadata.ReadRows(ctx, gcp_bigtable.RowList(keys), func(row gcp_bigtable.Row) bool {
		// results are returned in reverse order, so highest ts is first
		address := strings.TrimPrefix(row.Key(), keyPrefix)
		for _, v := range row[ACCOUNT_METADATA_FAMILY] {
			b := &types.IsContractUpdate{}
			err := proto.Unmarshal(v.Value, b)
			if err != nil {
				utils.LogError(err, "error parsing IsContractUpdate data", 0)
			}
			histories[address] = append(histories[address], isContractInfo{update: b, ts: v.Timestamp})
		}

		return true
	}, gcp_bigtable.RowFilter(filter))

	if err != nil {
		return fmt.Errorf("error reading isContract histories from bigtable: %w", err)
	}

	return nil
}

// returns account state after the given execution state
// -1 is latest (e.g. "txIdx" = -1 returns the contract state after execution of "block", "block" = -1 returns the state at chain head)
func (bigtable *Bigtable) GetAddressContractInteractionsAt(requests []contractInteractionAtRequest) ([]types.ContractInteractionType, error) {
	results := make([]types.ContractInteractionType, len(requests))
	if len(requests) == 0 {
		return results, nil
	}

	// get histories
	histories := make(map[string][]isContractInfo, len(requests))
	for _, request := range requests {
		histories[request.address] = nil
	}
	err := bigtable.getAddressIsContractHistories(histories)
	if err != nil {
		return nil, err
	}

	// evaluate requests; CONTRACT_NONE is default
	for i, request := range requests {
		history, ok := histories[request.address]
		if !ok || history == nil || len(history) == 0 {
			continue
		}
		latestUpdateIdxBeforeReq := 0
		if request.block != -1 {
			var block, tx, itx uint64
			if request.txIdx == -1 {
				block = uint64(request.block + 1)
			} else if request.traceIdx == -1 {
				block = uint64(request.block)
				tx = uint64(request.txIdx + 1)
			} else {
				block = uint64(request.block)
				tx = uint64(request.txIdx)
				itx = uint64(request.traceIdx + 1)
			}
			req_ts, err := encodeIsContractUpdateTs(block, tx, itx)
			if err != nil {
				return nil, err
			}
			latestUpdateIdxBeforeReq = sort.Search(len(history), func(j int) bool {
				return history[j].ts < req_ts
			})
			if len(history) == latestUpdateIdxBeforeReq {
				// all updates happened after our request
				continue
			}
		}

		b, tx, trace := decodeIsContractUpdateTs(history[latestUpdateIdxBeforeReq].ts)
		exact_match := request.block == -1 || request.block == int64(b) && (request.txIdx == -1 || request.txIdx == int64(tx) && (request.traceIdx == -1 || request.traceIdx == int64(trace)))

		if exact_match {
			results[i] = types.CONTRACT_DESTRUCTION
			if history[latestUpdateIdxBeforeReq].update.IsContract {
				results[i] = types.CONTRACT_CREATION
			}
		} else {
			// find first successful prev update
			for j := latestUpdateIdxBeforeReq; j < len(history); j++ {
				if history[j].update.Success {
					if history[j].update.IsContract {
						results[i] = types.CONTRACT_PRESENT
					}
					break
				}
			}
		}
	}
	return results, nil
}

// convenience function to get contract interaction status per transaction of a block
func (bigtable *Bigtable) GetAddressContractInteractionsAtBlock(block *types.Eth1Block) ([]types.ContractInteractionType, error) {
	requests := make([]contractInteractionAtRequest, len(block.GetTransactions()))
	for i, tx := range block.GetTransactions() {
		address := tx.GetTo()
		if len(address) == 0 {
			address = tx.GetContractAddress()
		}
		requests[i] = contractInteractionAtRequest{
			address:  fmt.Sprintf("%x", address),
			block:    int64(block.GetNumber()),
			txIdx:    int64(i),
			traceIdx: -1,
		}
	}

	return bigtable.GetAddressContractInteractionsAt(requests)
}

// convenience function to get contract interaction status per subtransaction of a transaction
// 2nd parameter specifies [tx_idx, trace_idx] for each internal tx
func (bigtable *Bigtable) GetAddressContractInteractionsAtITransactions(itransactions []*types.Eth1InternalTransactionIndexed, idxs [][2]int64) ([][2]types.ContractInteractionType, error) {
	requests := make([]contractInteractionAtRequest, 0, len(itransactions)*2)
	for i, tx := range itransactions {
		requests = append(requests, contractInteractionAtRequest{
			address:  fmt.Sprintf("%x", tx.GetFrom()),
			block:    int64(tx.GetBlockNumber()),
			txIdx:    idxs[i][0],
			traceIdx: idxs[i][1],
		})
		requests = append(requests, contractInteractionAtRequest{
			address:  fmt.Sprintf("%x", tx.GetTo()),
			block:    int64(tx.GetBlockNumber()),
			txIdx:    idxs[i][0],
			traceIdx: idxs[i][1],
		})
	}
	results, err := bigtable.GetAddressContractInteractionsAt(requests)
	if err != nil {
		return nil, err
	}

	resultPairs := make([][2]types.ContractInteractionType, len(itransactions))
	for i, v := range results {
		resultPairs[i/2][i%2] = v
	}
	return resultPairs, nil
}

// convenience function to get contract interaction status per trace
func (bigtable *Bigtable) GetAddressContractInteractionsAtTraces(traces []*rpc.GethTraceCallResult, blockNumber *big.Int) ([][2]types.ContractInteractionType, error) {
	requests := make([]contractInteractionAtRequest, 0, len(traces)*2)
	for i, itx := range traces {
		requests = append(requests, contractInteractionAtRequest{
			address:  itx.From.String(),
			block:    blockNumber.Int64(),
			txIdx:    int64(itx.TransactionPosition),
			traceIdx: int64(i),
		})
		requests = append(requests, contractInteractionAtRequest{
			address:  itx.To.String(),
			block:    blockNumber.Int64(),
			txIdx:    int64(itx.TransactionPosition),
			traceIdx: int64(i),
		})
	}
	results, err := bigtable.GetAddressContractInteractionsAt(requests)
	if err != nil {
		return nil, err
	}

	resultPairs := make([][2]types.ContractInteractionType, len(traces))
	for i, v := range results {
		resultPairs[i/2][i%2] = v
	}
	return resultPairs, nil
}

// convenience function to get contract interaction status per transaction
func (bigtable *Bigtable) GetAddressContractInteractionsAtTransactions(transactions []*types.Eth1TransactionIndexed, idxs []int64) ([]types.ContractInteractionType, error) {
	requests := make([]contractInteractionAtRequest, len(transactions))
	for i, tx := range transactions {
		requests[i] = contractInteractionAtRequest{
			address:  fmt.Sprintf("%x", tx.GetTo()),
			block:    int64(tx.GetBlockNumber()),
			txIdx:    idxs[i],
			traceIdx: -1,
		}
	}
	return bigtable.GetAddressContractInteractionsAt(requests)
}

func (bigtable *Bigtable) SaveAddressName(address []byte, name string) error {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	mut := gcp_bigtable.NewMutation()
	mut.Set(ACCOUNT_METADATA_FAMILY, ACCOUNT_COLUMN_NAME, gcp_bigtable.Timestamp(0), []byte(name))

	return bigtable.tableMetadata.Apply(ctx, fmt.Sprintf("%s:%x", bigtable.chainId, address), mut)
}

func (bigtable *Bigtable) GetContractMetadata(address []byte) (*types.ContractMetadata, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"address": address,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	rowKey := fmt.Sprintf("%s:%x", bigtable.chainId, address)
	cacheKey := bigtable.chainId + ":CONTRACT:" + rowKey
	if cached, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, utils.Day, new(types.ContractMetadata)); err == nil {
		ret := cached.(*types.ContractMetadata)
		val, err := abi.JSON(bytes.NewReader(ret.ABIJson))
		ret.ABI = &val
		return ret, err
	}

	row, err := bigtable.tableMetadata.ReadRow(ctx, rowKey, gcp_bigtable.RowFilter(gcp_bigtable.FamilyFilter(CONTRACT_METADATA_FAMILY)))

	ret := &types.ContractMetadata{}

	if err != nil || row == nil {
		ret, err := utils.TryFetchContractMetadata(address)

		if err != nil {
			if err == utils.ErrRateLimit {
				logrus.Warnf("Hit rate limit when fetching contract metadata for address %x", address)
			} else {
				logAdditionalInfo := map[string]interface{}{"address": fmt.Sprintf("%x", address)}
				if strings.Contains(err.Error(), "unsupported arg type") {
					// open issue in the go-ethereum lib: https://github.com/ethereum/go-ethereum/issues/24572
					logrus.Warnf("could not parse ABI for %x: %v", address, err)
				} else {
					utils.LogError(err, "Fetching contract metadata", 0, logAdditionalInfo)
				}
				err := cache.TieredCache.Set(cacheKey, &types.ContractMetadata{}, utils.Day)
				if err != nil {
					utils.LogError(err, "Caching contract metadata", 0, logAdditionalInfo)
				}
			}
			return nil, err
		}

		// No contract found, caching empty
		if ret == nil {
			err = cache.TieredCache.Set(cacheKey, &types.ContractMetadata{}, utils.Day)
			if err != nil {
				utils.LogError(err, "Caching contract metadata", 0, map[string]interface{}{"address": fmt.Sprintf("%x", address)})
			}
			return nil, nil
		}

		err = cache.TieredCache.Set(cacheKey, ret, utils.Day)
		if err != nil {
			utils.LogError(err, "Caching contract metadata", 0, map[string]interface{}{"address": fmt.Sprintf("%x", address)})
		}

		err = bigtable.SaveContractMetadata(address, ret)
		if err != nil {
			logger.Errorf("error saving contract metadata to bigtable: %v", err)
		}
		return ret, nil
	}

	for _, ri := range row {
		for _, item := range ri {
			if item.Column == CONTRACT_METADATA_FAMILY+":"+CONTRACT_NAME {
				ret.Name = string(item.Value)
			} else if item.Column == CONTRACT_METADATA_FAMILY+":"+CONTRACT_ABI {
				ret.ABIJson = item.Value
				val, err := abi.JSON(bytes.NewReader(ret.ABIJson))

				if err != nil {
					logrus.Fatalf("error decoding abi for address 0x%x: %v", address, err)
				}
				ret.ABI = &val
			}
		}
	}

	err = cache.TieredCache.Set(cacheKey, ret, utils.Day)
	return ret, err
}

func (bigtable *Bigtable) SaveContractMetadata(address []byte, metadata *types.ContractMetadata) error {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	mut := gcp_bigtable.NewMutation()
	mut.Set(CONTRACT_METADATA_FAMILY, CONTRACT_NAME, gcp_bigtable.Timestamp(0), []byte(metadata.Name))
	mut.Set(CONTRACT_METADATA_FAMILY, CONTRACT_ABI, gcp_bigtable.Timestamp(0), metadata.ABIJson)

	return bigtable.tableMetadata.Apply(ctx, fmt.Sprintf("%s:%x", bigtable.chainId, address), mut)
}

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

func (bigtable *Bigtable) SaveBlockKeys(blockNumber uint64, blockHash []byte, keys string) error {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	mut := gcp_bigtable.NewMutation()
	mut.Set(METADATA_UPDATES_FAMILY_BLOCKS, "keys", gcp_bigtable.Now(), []byte(keys))

	key := fmt.Sprintf("%s:BLOCK:%s:%x", bigtable.chainId, reversedPaddedBlockNumber(blockNumber), blockHash)
	err := bigtable.tableMetadataUpdates.Apply(ctx, key, mut)

	return err
}

func (bigtable *Bigtable) GetBlockKeys(blockNumber uint64, blockHash []byte) ([]string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"blockNumber": blockNumber,
			"blockHash":   blockHash,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

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

// Deletes all block data from bigtable
func (bigtable *Bigtable) DeleteBlock(blockNumber uint64, blockHash []byte) error {

	// handle contract state updates
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

	mutsDelete := &types.BulkMutations{
		Keys: make([]string, 0),
		Muts: make([]*gcp_bigtable.Mutation, 0),
	}

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"blockNumber": blockNumber,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	err = bigtable.tableMetadata.ReadRows(ctx, gcp_bigtable.PrefixRange(fmt.Sprintf("%s:S:", bigtable.chainId)), func(row gcp_bigtable.Row) bool {
		mutDelete := gcp_bigtable.NewMutation()
		mutDelete.DeleteTimestampRange(ACCOUNT_METADATA_FAMILY, ACCOUNT_IS_CONTRACT, starttime, endtime)

		mutsDelete.Keys = append(mutsDelete.Keys, row.Key())
		mutsDelete.Muts = append(mutsDelete.Muts, mutDelete)
		return true
	}, gcp_bigtable.RowFilter(filter))
	if err != nil {
		return err
	}

	if len(mutsDelete.Keys) > 0 {
		err = bigtable.WriteBulk(mutsDelete, bigtable.tableMetadata, DEFAULT_BATCH_INSERTS)
		if err != nil {
			return err
		}
	}

	// receive all keys that were written by this block (entities & indices)
	keys, err := bigtable.GetBlockKeys(blockNumber, blockHash)
	if err != nil {
		return err
	}

	// Delete all of those keys
	mutsDelete = &types.BulkMutations{
		Keys: make([]string, 0, len(keys)),
		Muts: make([]*gcp_bigtable.Mutation, 0, len(keys)),
	}
	for _, key := range keys {
		mutDelete := gcp_bigtable.NewMutation()
		mutDelete.DeleteRow()
		mutsDelete.Keys = append(mutsDelete.Keys, key)
		mutsDelete.Muts = append(mutsDelete.Muts, mutDelete)
	}

	err = bigtable.WriteBulk(mutsDelete, bigtable.tableData, DEFAULT_BATCH_INSERTS)
	if err != nil {
		return err
	}

	mutsDelete = &types.BulkMutations{
		Keys: make([]string, 0, len(keys)),
		Muts: make([]*gcp_bigtable.Mutation, 0, len(keys)),
	}
	mutDelete := gcp_bigtable.NewMutation()
	mutDelete.DeleteRow()
	mutsDelete.Keys = append(mutsDelete.Keys, fmt.Sprintf("%s:%s", bigtable.chainId, reversedPaddedBlockNumber(blockNumber)))
	mutsDelete.Muts = append(mutsDelete.Muts, mutDelete)
	err = bigtable.WriteBulk(mutsDelete, bigtable.tableBlocks, DEFAULT_BATCH_INSERTS)
	if err != nil {
		return err
	}

	return nil
}

func (bigtable *Bigtable) GetEth1TxForToken(prefix string, limit int64) ([]*types.Eth1ERC20Indexed, string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"prefix": prefix,
			"limit":  limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	// add \x00 to the row range such that we skip the previous value
	rowRange := gcp_bigtable.NewRange(prefix+"\x00", prefixSuccessor(prefix, 5))
	data := make([]*types.Eth1ERC20Indexed, 0, limit)
	keys := make([]string, 0, limit)
	indexes := make([]string, 0, limit)
	keysMap := make(map[string]*types.Eth1ERC20Indexed, limit)

	err := bigtable.tableData.ReadRows(ctx, rowRange, func(row gcp_bigtable.Row) bool {
		keys = append(keys, strings.TrimPrefix(row[DEFAULT_FAMILY][0].Column, "f:"))
		indexes = append(indexes, row.Key())
		return true
	}, gcp_bigtable.LimitRows(limit))
	if err != nil {
		return nil, "", err
	}

	if len(keys) == 0 {
		return data, "", nil
	}

	err = bigtable.tableData.ReadRows(ctx, gcp_bigtable.RowList(keys), func(row gcp_bigtable.Row) bool {
		b := &types.Eth1ERC20Indexed{}
		err := proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, b)

		if err != nil {
			logrus.Fatalf("error parsing Eth1ERC20Indexed data: %v", err)
		}
		keysMap[row.Key()] = b

		return true
	})
	if err != nil {
		logger.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1TxForToken")
		return nil, "", err
	}

	for _, key := range keys {
		if d := keysMap[key]; d != nil {
			data = append(data, d)
		}
	}

	return data, indexes[len(indexes)-1], nil
}

func (bigtable *Bigtable) GetTokenTransactionsTableData(token []byte, address []byte, pageToken string) (*types.DataTableResponse, error) {

	defaultPageToken := ""
	if len(address) == 0 {
		defaultPageToken = fmt.Sprintf("%s:I:ERC20:%x:ALL:%s", bigtable.chainId, token, FILTER_TIME)
	} else {
		defaultPageToken = fmt.Sprintf("%s:I:ERC20:%x:%x:%s", bigtable.chainId, token, address, FILTER_TIME)
	}

	if pageToken == "" {
		pageToken = defaultPageToken
	} else {
		if !strings.HasPrefix(pageToken, defaultPageToken) {
			return nil, fmt.Errorf("invalid pageToken for function GetTokenTransactionsTableData: %s", pageToken)
		}
	}

	transactions, lastKey, err := BigtableClient.GetEth1TxForToken(pageToken, DefaultInfScrollRows)
	if err != nil {
		return nil, err
	}

	names := make(map[string]string)
	tokens := make(map[string]*types.ERC20Metadata)
	for _, t := range transactions {
		names[string(t.From)] = ""
		names[string(t.To)] = ""
		tokens[string(t.TokenAddress)] = nil
	}
	names, tokens, err = BigtableClient.GetAddressesNamesArMetadata(&names, &tokens)
	if err != nil {
		return nil, err
	}

	tableData := make([][]interface{}, len(transactions))

	for i, t := range transactions {

		fromName := names[string(t.From)]
		toName := names[string(t.To)]
		from := utils.FormatAddress(t.From, t.TokenAddress, fromName, false, false, !bytes.Equal(t.From, address))
		to := utils.FormatAddress(t.To, t.TokenAddress, toName, false, false, !bytes.Equal(t.To, address))

		tb := &types.Eth1AddressBalance{
			Address:  address,
			Balance:  t.Value,
			Token:    t.TokenAddress,
			Metadata: tokens[string(t.TokenAddress)],
		}

		tableData[i] = []interface{}{
			utils.FormatTransactionHash(t.ParentHash, true),
			utils.FormatTimestamp(t.Time.AsTime().Unix()),
			from,
			utils.FormatInOutSelf(address, t.From, t.To),
			to,
			utils.FormatTokenValue(tb, false),
		}

	}

	data := &types.DataTableResponse{
		Data:        tableData,
		PagingToken: lastKey,
	}

	return data, nil
}

func (bigtable *Bigtable) SearchForAddress(addressPrefix []byte, limit int) ([]*types.Eth1AddressSearchItem, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"addressPrefix": addressPrefix,
			"limit":         limit,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	data := make([]*types.Eth1AddressSearchItem, 0, limit)

	prefix := fmt.Sprintf("%s:%x", bigtable.chainId, addressPrefix)

	err := bigtable.tableMetadata.ReadRows(ctx, gcp_bigtable.PrefixRange(prefix), func(row gcp_bigtable.Row) bool {
		si := &types.Eth1AddressSearchItem{
			Address: strings.TrimPrefix(row.Key(), bigtable.chainId+":"),
			Name:    "",
			Token:   "",
		}
		for _, ri := range row {
			for _, item := range ri {
				if item.Column == ACCOUNT_METADATA_FAMILY+":"+ACCOUNT_COLUMN_NAME {
					si.Name = string(item.Value)
				}

				if item.Column == ERC20_METADATA_FAMILY+":"+ERC20_COLUMN_SYMBOL {
					si.Token = "ERC20"
				}
			}
		}
		data = append(data, si)
		return true
	}, gcp_bigtable.LimitRows(int64(limit)))

	if err != nil {
		return nil, err
	}

	return data, nil
}

func getSignaturePrefix(st types.SignatureType) string {
	if st == types.EventSignature {
		return "e"
	}
	return "m"
}

// Get the status of the last signature import run
func (bigtable *Bigtable) GetSignatureImportStatus(st types.SignatureType) (*types.SignatureImportStatus, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"st": st,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()
	key := fmt.Sprintf("1:%v_SIGNATURE_IMPORT_STATUS", getSignaturePrefix(st))
	row, err := bigtable.tableData.ReadRow(ctx, key)
	if err != nil {
		logrus.Errorf("error reading signature imoprt status row %v: %v", row.Key(), err)
		return nil, err
	}
	s := &types.SignatureImportStatus{}
	if row == nil {
		return s, nil
	}
	row_ := row[DEFAULT_FAMILY][0]
	err = json.Unmarshal(row_.Value, s)
	if err != nil {
		logrus.Errorf("error unmarshalling signature import status for row %v: %v", row.Key(), err)
		return nil, err
	}

	return s, nil
}

// Save the status of the last signature import run
func (bigtable *Bigtable) SaveSignatureImportStatus(status types.SignatureImportStatus, st types.SignatureType) error {

	mutsWrite := &types.BulkMutations{
		Keys: make([]string, 0, 1),
		Muts: make([]*gcp_bigtable.Mutation, 0, 1),
	}

	s, err := json.Marshal(status)
	if err != nil {
		return err
	}

	mut := gcp_bigtable.NewMutation()
	mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), s)

	key := fmt.Sprintf("1:%v_SIGNATURE_IMPORT_STATUS", getSignaturePrefix(st))

	mutsWrite.Keys = append(mutsWrite.Keys, key)
	mutsWrite.Muts = append(mutsWrite.Muts, mut)

	err = bigtable.WriteBulk(mutsWrite, bigtable.tableData, DEFAULT_BATCH_INSERTS)

	if err != nil {
		return err
	}

	return nil
}

// Save a list of signatures
func (bigtable *Bigtable) SaveSignatures(signatures []types.Signature, st types.SignatureType) error {

	mutsWrite := &types.BulkMutations{
		Keys: make([]string, 0, 1),
		Muts: make([]*gcp_bigtable.Mutation, 0, 1),
	}

	for _, sig := range signatures {
		mut := gcp_bigtable.NewMutation()
		mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), []byte(sig.Text))

		key := fmt.Sprintf("1:%v_SIGNATURE:%v", getSignaturePrefix(st), sig.Hex)

		mutsWrite.Keys = append(mutsWrite.Keys, key)
		mutsWrite.Muts = append(mutsWrite.Muts, mut)
	}

	err := bigtable.WriteBulk(mutsWrite, bigtable.tableData, DEFAULT_BATCH_INSERTS)

	if err != nil {
		return err
	}

	return nil
}

// get a signature by it's hex representation
func (bigtable *Bigtable) GetSignature(hex string, st types.SignatureType) (*string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"hex": hex,
			"st":  st,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()
	key := fmt.Sprintf("1:%v_SIGNATURE:%v", getSignaturePrefix(st), hex)
	row, err := bigtable.tableData.ReadRow(ctx, key)
	if err != nil {
		logrus.Errorf("error reading signature imoprt status row %v: %v", row.Key(), err)
		return nil, err
	}
	if row == nil {
		return nil, nil
	}
	row_ := row[DEFAULT_FAMILY][0]
	s := string(row_.Value)
	return &s, nil
}

// get a method label for its byte signature with defaults
func (bigtable *Bigtable) GetMethodLabel(data []byte, interaction types.ContractInteractionType) string {
	id := data
	if len(data) > 3 {
		id = data[:4]
	}
	method := fmt.Sprintf("0x%x", id)

	switch interaction {
	case types.CONTRACT_NONE:
		return "Transfer"
	case types.CONTRACT_CREATION:
		return "Constructor"
	case types.CONTRACT_DESTRUCTION:
		return "Destruction"
	case types.CONTRACT_PRESENT:
		if len(id) == 4 {
			cacheKey := fmt.Sprintf("M:H2L:%s", method)
			if _, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Hour, &method); err != nil {
				if sig, err := bigtable.GetSignature(method, types.MethodSignature); err == nil {
					if sig != nil {
						method = utils.RemoveRoundBracketsIncludingContent(*sig)
					}
					cache.TieredCache.Set(cacheKey, method, time.Hour)
				}
			}
		}
	default:
		utils.LogError(nil, "unknown contract interaction type", 0, map[string]interface{}{"type": interaction})
	}
	return method
}

// get a method label for its byte signature with defaults
func (bigtable *Bigtable) GetAddressLabel(id string, invoke_overwrite types.ContractInteractionType) string {
	switch invoke_overwrite {
	case types.CONTRACT_CREATION:
		return "Contract Creation"
	case types.CONTRACT_DESTRUCTION:
		return "Contract Destruction"
	default:
		return id
	}
}

// get an event label for its byte signature with defaults
func (bigtable *Bigtable) GetEventLabel(id []byte) string {
	label := ""
	if len(id) > 0 {
		event := fmt.Sprintf("0x%x", id)
		cacheKey := fmt.Sprintf("E:H2L:%s", event)
		if _, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Hour, &label); err != nil {
			sig, err := bigtable.GetSignature(event, types.EventSignature)
			if err == nil {
				if sig != nil {
					label = *sig
				}
				cache.TieredCache.Set(cacheKey, label, time.Hour)
			}
		}
	}
	return label
}

func prefixSuccessor(prefix string, pos int) string {
	if prefix == "" {
		return "" // infinite range
	}
	split := strings.Split(prefix, ":")
	if len(split) > pos {
		prefix = strings.Join(split[:pos], ":")
	}
	n := len(prefix)
	for n--; n >= 0 && prefix[n] == '\xff'; n-- {
	}
	if n == -1 {
		return ""
	}
	ans := []byte(prefix[:n])
	ans = append(ans, prefix[n]+1)
	return string(ans)
}

func (bigtable *Bigtable) markBalanceUpdate(address []byte, token []byte, mutations *types.BulkMutations, cache *freecache.Cache) {
	balanceUpdateKey := fmt.Sprintf("%s:B:%x", bigtable.chainId, address)                        // format is B: for balance update as chainid:prefix:address (token id will be encoded as column name)
	balanceUpdateCacheKey := []byte(fmt.Sprintf("%s:B:%x:%x", bigtable.chainId, address, token)) // format is B: for balance update as chainid:prefix:address (token id will be encoded as column name)
	if _, err := cache.Get(balanceUpdateCacheKey); err != nil {
		mut := gcp_bigtable.NewMutation()
		mut.Set(DEFAULT_FAMILY, fmt.Sprintf("%x", token), gcp_bigtable.Timestamp(0), []byte{})

		mutations.Keys = append(mutations.Keys, balanceUpdateKey)
		mutations.Muts = append(mutations.Muts, mut)

		cache.Set(balanceUpdateCacheKey, []byte{0x1}, int((utils.Day * 2).Seconds()))
	}
}

var (
	GASNOW_RAPID_COLUMN    = "RAPI"
	GASNOW_FAST_COLUMN     = "FAST"
	GASNOW_STANDARD_COLUMN = "STAN"
	GASNOW_SLOW_COLUMN     = "SLOW"
)

func (bigtable *Bigtable) SaveGasNowHistory(slow, standard, rapid, fast *big.Int) error {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	ts := time.Now().Truncate(time.Minute)
	row := fmt.Sprintf("%s:GASNOW:%s", bigtable.chainId, reversePaddedBigtableTimestamp(timestamppb.New(ts)))

	gcpTs := gcp_bigtable.Time(ts)

	mut := gcp_bigtable.NewMutation()
	mut.Set(SERIES_FAMILY, GASNOW_SLOW_COLUMN, gcpTs, slow.Bytes())
	mut.Set(SERIES_FAMILY, GASNOW_STANDARD_COLUMN, gcpTs, standard.Bytes())
	mut.Set(SERIES_FAMILY, GASNOW_FAST_COLUMN, gcpTs, fast.Bytes())
	mut.Set(SERIES_FAMILY, GASNOW_RAPID_COLUMN, gcpTs, rapid.Bytes())

	err := bigtable.tableMetadata.Apply(ctx, row, mut)
	if err != nil {
		return fmt.Errorf("error saving gas now history to bigtable. err: %w", err)
	}
	return nil
}

func (bigtable *Bigtable) GetGasNowHistory(ts, pastTs time.Time) ([]types.GasNowHistory, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		logger.WithFields(logrus.Fields{
			"ts":     ts,
			"pastTs": pastTs,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	start := fmt.Sprintf("%s:GASNOW:%s", bigtable.chainId, reversePaddedBigtableTimestamp(timestamppb.New(ts)))
	end := fmt.Sprintf("%s:GASNOW:%s", bigtable.chainId, reversePaddedBigtableTimestamp(timestamppb.New(pastTs)))

	rowRange := gcp_bigtable.NewRange(start, end)
	famFilter := gcp_bigtable.FamilyFilter(SERIES_FAMILY)
	filter := gcp_bigtable.RowFilter(famFilter)

	history := make([]types.GasNowHistory, 0)

	scanner := func(row gcp_bigtable.Row) bool {
		if len(row[SERIES_FAMILY]) < 4 {
			logrus.Errorf("error reading row: %+v", row)
			return false
		}
		// Columns are returned alphabetically so fast, rapid, slow, standard should be the order
		history = append(history, types.GasNowHistory{
			Ts:       row[SERIES_FAMILY][0].Timestamp.Time(),
			Fast:     new(big.Int).SetBytes(row[SERIES_FAMILY][0].Value),
			Rapid:    new(big.Int).SetBytes(row[SERIES_FAMILY][1].Value),
			Slow:     new(big.Int).SetBytes(row[SERIES_FAMILY][2].Value),
			Standard: new(big.Int).SetBytes(row[SERIES_FAMILY][3].Value),
		})
		return true
	}

	err := bigtable.tableMetadata.ReadRows(ctx, rowRange, scanner, filter)
	if err != nil {
		return nil, fmt.Errorf("error getting gas now history to bigtable, err: %w", err)
	}
	return history, nil
}

func (bigtable *Bigtable) ReindexITxsFromNode(start, end, batchSize, concurrency int64, transforms []func(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error), cache *freecache.Cache) error {
	g := new(errgroup.Group)
	g.SetLimit(int(concurrency))

	if start == 0 && end == 0 {
		return fmt.Errorf("start or end block height can't be 0")
	}

	if end < start {
		return fmt.Errorf("end block must be grater or equal to start block")
	}

	logrus.Infof("reindexing txs for blocks from %d to %d", start, end)

	for i := start; i <= end; i += batchSize {
		firstBlock := i
		lastBlock := firstBlock + batchSize - 1
		if lastBlock > end {
			lastBlock = end
		}

		blockNumbers := make([]int64, 0, lastBlock-firstBlock+1)
		for b := firstBlock; b <= lastBlock; b++ {
			blockNumbers = append(blockNumbers, b)
		}

		g.Go(func() error {
			blocks, err := rpc.CurrentErigonClient.GetBlocksByBatch(blockNumbers)
			if err != nil {
				return fmt.Errorf("error getting blocks by batch from %v to %v: %v", firstBlock, lastBlock, err)
			}

			subG := new(errgroup.Group)
			subG.SetLimit(int(concurrency))

			for _, block := range blocks {
				currentBlock := block
				subG.Go(func() error {
					bulkMutsData := types.BulkMutations{}
					bulkMutsMetadataUpdate := types.BulkMutations{}
					for _, transform := range transforms {
						mutsData, mutsMetadataUpdate, err := transform(currentBlock, cache)
						if err != nil {
							logrus.WithError(err).Errorf("error transforming block [%v]", currentBlock.Number)
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
						err := bigtable.SaveBlockKeys(currentBlock.Number, currentBlock.Hash, metaKeys)
						if err != nil {
							return fmt.Errorf("error saving block [%v] keys to bigtable metadata updates table: %w", currentBlock.Number, err)
						}

						err = bigtable.WriteBulk(&bulkMutsData, bigtable.tableData, DEFAULT_BATCH_INSERTS)
						if err != nil {
							return fmt.Errorf("error writing block [%v] to bigtable data table: %w", currentBlock.Number, err)
						}
					}

					if len(bulkMutsMetadataUpdate.Keys) > 0 {
						err := bigtable.WriteBulk(&bulkMutsMetadataUpdate, bigtable.tableMetadataUpdates, DEFAULT_BATCH_INSERTS)
						if err != nil {
							return fmt.Errorf("error writing block [%v] to bigtable metadata updates table: %w", currentBlock.Number, err)
						}
					}

					return nil
				})
			}
			return subG.Wait()
		})

	}

	if err := g.Wait(); err == nil {
		logrus.Info("data table indexing completed")
	} else {
		utils.LogError(err, "wait group error", 0)
		return err
	}

	return nil
}
