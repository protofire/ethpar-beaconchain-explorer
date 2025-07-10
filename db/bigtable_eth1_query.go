package db

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/cache"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	rpc_types "github.com/protofire/ethpar-beaconchain-explorer/rpc/types"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	gcp_bigtable "cloud.google.com/go/bigtable"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/coocood/freecache"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"google.golang.org/protobuf/proto"
)

// Legacy chain configuration global variables
var (
	MaxWithdrawalsPerPayload uint64
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

func (bigtable *Bigtable) GetMostRecentBlockFromDataTable() (*types.Eth1BlockIndexed, error) {

	ctx, cleanup := withTimeoutAndWarning("get most recent block from data table", time.Second*30)
	defer cleanup()

	prefix := fmt.Sprintf("%s:B:", bigtable.chainId)

	rowRange := gcp_bigtable.PrefixRange(prefix)
	block := types.Eth1BlockIndexed{}

	rowHandler := func(row gcp_bigtable.Row) bool {
		c, err := strconv.Atoi(strings.Replace(row.Key(), prefix, "", 1))
		if err != nil {
			log.Errorf("error parsing block number from key %v: %v", row.Key(), err)
			return false
		}

		c = MAX_EL_BLOCK_NUMBER - c

		err = proto.Unmarshal(row[DEFAULT_FAMILY][0].Value, &block)
		if err != nil {
			log.Errorf("error could not unmarschal proto object, err: %v", err)
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
			log.Errorf("error could not unmarschal proto object, err: %v", err)
			return false
		}

		*blocks = append(*blocks, &block)
		return true
	}
}

// GetFullBlocksDescending streams all existing blocks in the inclusive range [high, low]
// from Bigtable, including all ranks from 0 to MAX_BLOCK_RANK for each block number.
//
// For each block number from high down to low (inclusive), the function attempts to read
// blocks with rank 0 through 4. Only blocks that actually exist in Bigtable are streamed.
// Missing ranks are silently skipped without error.
//
// Blocks are read from the blocks Bigtable table using keys of the form:
//   <chainId>:<reversedPaddedBlockNumber>:<rank>
//
// The block data is deserialized from the "data" column in the DEFAULT_FAMILY_BLOCKS
// column family and streamed to the caller via the provided channel.
//
// Returns an error only if a Bigtable read operation or protobuf unmarshal fails.
func (bigtable *Bigtable) GetFullBlocksDescending(
    stream chan<- *types.Eth1Block,
    high, low uint64,
) error {
	ctx, cleanup := withTimeoutAndWarning(
		fmt.Sprintf("getting full blocks with ranks: high %d low %d", high, low),
		3*time.Minute,
	)
	defer cleanup()

	if high < low {
		return fmt.Errorf("invalid block range: high %d < low %d", high, low)
	}

	for blockNum := high; blockNum >= low; blockNum-- {
		for rank := uint8(0); rank <= uint8(MAX_BLOCK_RANK); rank++ {
			key := makeBlockKey(bigtable.chainId, blockNum, rank)

			row, err := bigtable.tableBlocks.ReadRow(ctx, key,
				gcp_bigtable.RowFilter(gcp_bigtable.ColumnFilter("data")),
			)
			if err != nil {
				return fmt.Errorf("error reading block %d (rank %d): %w", blockNum, rank, err)
			}

			if len(row[DEFAULT_FAMILY_BLOCKS]) == 0 {
				// Some ranks might not exist
				continue
			}

			var block types.Eth1Block
			err = proto.Unmarshal(row[DEFAULT_FAMILY_BLOCKS][0].Value, &block)
			if err != nil {
				log.WithFields(logger.Fields{
					"blockNumber": blockNum,
					"rank":        rank,
					"key":         key,
				}).Errorf("failed to unmarshal block")
				return fmt.Errorf("unmarshal failed for block %d rank %d: %w", blockNum, rank, err)
			}

			stream <- &block
		}

		if blockNum == 0 {
			break
		}
	}

	return nil
}

// GetBlocksIndexedMultiple retrieves multiple Eth1BlockIndexed entries from Bigtable,
// including all available ranks (0–4) for each provided block number.
//
// For each block number in the input slice, the function constructs up to 5 row keys,
// one per possible rank (rank 0 is always expected to exist, others are optional).
// Row keys are constructed in the format:
//   <chainID>:B:<reversedPaddedBlockNumber>:<rank>
//
// Bigtable is queried in a batch using RowList to fetch all matching rows.
// Only rows with valid data in column "d" (as filtered by a ColumnFilter) are returned.
//
// The function respects the provided `limit` on the total number of rows returned.
// It returns a slice of deserialized Eth1BlockIndexed pointers.
//
// Returns an error if the Bigtable read operation fails.
func (bigtable *Bigtable) GetBlocksIndexedMultiple(blockNumbers []uint64, limit uint64) ([]*types.Eth1BlockIndexed, error) {

	ctx, cleanup := withTimeoutAndWarning(
		"get multiple indexeed blocks",
		time.Second*30,
	)
	defer cleanup()

	var rowList gcp_bigtable.RowList

	for _, block := range blockNumbers {
		reversed := reversedPaddedBlockNumber(block)

		for rank := 0; rank <= 4; rank++ {
			key := fmt.Sprintf("%s:B:%s:%02d", bigtable.chainId, reversed, rank)
			rowList = append(rowList, key)
		}
	}

	rowFilter := gcp_bigtable.RowFilter(gcp_bigtable.ColumnFilter("d"))

	blocks := make([]*types.Eth1BlockIndexed, 0, 100)

	rowHandler := getBlockHandler(&blocks)

	err := bigtable.tableData.ReadRows(ctx, rowList, rowHandler, rowFilter, gcp_bigtable.LimitRows(int64(limit)))
	if err != nil {
		return nil, err
	}

	return blocks, nil
}

// GetBlocksDescending retrieves up to `limit` blocks (including all available ranks)
// starting from the given `start` block number, in descending order.
//
// Each block may have up to 5 ranks (0–4). The method returns all available ranks
// for each block as separate Eth1BlockIndexed entries.
//
// Block 0 is handled explicitly and included if within range. The returned list is
// not guaranteed to be ordered.
func (bigtable *Bigtable) GetBlocksDescending(start, limit uint64) ([]*types.Eth1BlockIndexed, error) {

	ctx, cleanup := withTimeoutAndWarning(
		"get blocks вуысутвштп",
		time.Second*30,
	)
	defer cleanup()

	if limit == 0 {
		return nil, fmt.Errorf("limit = 0 would fetch no blocks")
	}

	if start == 0 {
		limit = 1 // only block 0 can be fetched
	}

	// Compute which blockNumbers to query
	var blockNumbers []uint64
	for i := start; i > 0 && uint64(len(blockNumbers)) < limit; i-- {
		blockNumbers = append(blockNumbers, i)
	}
	if start < limit {
		blockNumbers = append(blockNumbers, 0) // include block 0 explicitly
	}

	// Build full RowList including all possible ranks
	var rowList gcp_bigtable.RowList
	for _, block := range blockNumbers {
		rev := reversedPaddedBlockNumber(block)
		for rank := 0; rank <= 4; rank++ {
			key := fmt.Sprintf("%s:B:%s:%02d", bigtable.chainId, rev, rank)
			rowList = append(rowList, key)
		}
	}

	rowFilter := gcp_bigtable.RowFilter(gcp_bigtable.ColumnFilter("d"))
	allBlocks := make([]*types.Eth1BlockIndexed, 0, limit*5) // worst case
	rowHandler := getBlockHandler(&allBlocks)

	err := bigtable.tableData.ReadRows(ctx, rowList, rowHandler, rowFilter)
	if err != nil {
		return nil, err
	}

	// Post-process: group by block number and keep up to `limit` unique blocks
	grouped := make(map[uint64][]*types.Eth1BlockIndexed)
	for _, b := range allBlocks {
		grouped[b.Number] = append(grouped[b.Number], b)
	}

	// Sort block numbers descending
	keys := make([]uint64, 0, len(grouped))
	for num := range grouped {
		keys = append(keys, num)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] > keys[j] })

	final := make([]*types.Eth1BlockIndexed, 0, limit*5)
	for _, num := range keys {
		if uint64(len(final)) >= limit {
			break
		}
		final = append(final, grouped[num]...)
	}

	return final, nil
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
	iSplits := strings.Split(sbi.indexes[i], ":")
	jSplits := strings.Split(sbi.indexes[j], ":")
	if len(iSplits) != len(jSplits) || len(iSplits) < 7 {
		utils.LogError(nil, "unexpected bigtable transaction indices", 0, map[string]interface{}{"index_i": sbi.indexes[i], "index_j": sbi.indexes[j]})
		return false
	}

	// block
	if iSplits[5] != jSplits[5] {
		return iSplits[5] < jSplits[5]
	}
	// tx idx
	if iSplits[6] != jSplits[6] {
		if iSplits[6] == strconv.Itoa(TX_PER_BLOCK_LIMIT) || jSplits[6] == strconv.Itoa(TX_PER_BLOCK_LIMIT) {
			return jSplits[6] == strconv.Itoa(TX_PER_BLOCK_LIMIT)
		}
		return iSplits[6] < jSplits[6]
	}
	// itx idx
	if len(iSplits) > 7 && iSplits[7] != jSplits[7] {
		if iSplits[7] == strconv.Itoa(ITX_PER_TX_LIMIT) || jSplits[7] == strconv.Itoa(ITX_PER_TX_LIMIT) {
			return jSplits[7] == strconv.Itoa(ITX_PER_TX_LIMIT)
		}
		return iSplits[7] < jSplits[7]
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
			nextSplits := strings.Split(indexes[i], ":")
			if len(nextSplits) < 7 {
				utils.LogError(nil, "unexpected bigtable transaction index", 0, map[string]interface{}{"index": indexes[i]})
				continue
			}
			if nextSplits[5] != splits[5] || (len(splits) > 7 && nextSplits[6] != splits[6]) {
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

	ctx, cleanup := withTimeoutAndWarning(
		"getting eth1 txs for address",
		time.Second*30,
	)
	defer cleanup()

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
			log.Fatalf("error parsing Eth1TransactionIndexed data: %v", err)
		}
		keysMap[row.Key()] = b

		return true
	})
	if err != nil {
		log.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1TxsForAddress")
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
		log.WithFields(logger.Fields{
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

	ctx, cleanup := withTimeoutAndWarning(
		fmt.Sprintf("getting eth1 tx with hash: %s", txHash),
		time.Second*30,
	)
	defer cleanup()

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
		log.WithFields(logger.Fields{
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

	transactions, keys, err := bigtable.GetEth1TxsForAddress(pageToken, DefaultInfScrollRows)
	if err != nil {
		return nil, err
	}

	idxs := make([]int64, len(keys))
	for i, k := range keys {
		txIdx, err := strconv.Atoi(strings.Split(k, ":")[6])
		if err != nil {
			return nil, fmt.Errorf("error parsing Eth1InternalTransactionIndexed tx index: %v", err)
		}
		txIdx = TX_PER_BLOCK_LIMIT - txIdx
		if txIdx < 0 {
			return nil, fmt.Errorf("invalid Eth1InternalTransactionIndexed tx index: %d", txIdx)
		}

		idxs[i] = int64(txIdx)
	}

	contractInteractionTypes, err := bigtable.GetAddressContractInteractionsAtTransactions(transactions, idxs)
	if err != nil {
		utils.LogError(err, "error getting contract states", 0)
	}

	// retrieve metadata
	names := make(map[string]string)
	for _, t := range transactions {
		names[string(t.From)] = ""
		names[string(t.To)] = ""
	}
	names, _, err = bigtable.GetAddressesNamesArMetadata(&names, nil)
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
			utils.FormatTransactionHash(t.Hash, t.ErrorMsg == ""),
			utils.FormatMethod(bigtable.GetMethodLabel(t.MethodId, contractInteraction)),
			utils.FormatBlockNumber(t.BlockNumber),
			utils.FormatTimestamp(t.Time.AsTime().Unix()),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.From, fromName, false, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatInOutSelf(address, t.From, t.To),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.To, bigtable.GetAddressLabel(names[string(t.To)], contractInteraction), contractInteraction != types.CONTRACT_NONE, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
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
		log.WithFields(logger.Fields{
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
			log.Fatalf("error parsing Eth1BlockIndexed data: %v", err)
		}
		keysMap[row.Key()] = b

		return true
	})
	if err != nil {
		log.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1BlocksForAddress")
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
		log.WithFields(logger.Fields{
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

	blocks, lastKey, err := bigtable.GetEth1BlocksForAddress(pageToken, DefaultInfScrollRows)
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
		log.WithFields(logger.Fields{
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
			log.Fatalf("error parsing Eth1UncleIndexed data: %v", err)
		}
		keysMap[row.Key()] = b

		return true
	})
	if err != nil {
		log.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1UnclesForAddress")
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
		log.WithFields(logger.Fields{
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

	uncles, lastKey, err := bigtable.GetEth1UnclesForAddress(pageToken, DefaultInfScrollRows)
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
		log.WithFields(logger.Fields{
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
			log.Fatalf("error parsing Eth1BlobTransactionIndexed data: %v", err)
		}
		keysMap[row.Key()] = b
		return true
	})
	if err != nil {
		log.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1BtxForAddress")
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
		log.WithFields(logger.Fields{
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
	names, _, err = bigtable.GetAddressesNamesArMetadata(&names, nil)
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
		log.WithFields(logger.Fields{
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
			log.Fatalf("error parsing Eth1InternalTransactionIndexed data: %v", err)
		}

		// geth traces include zero-value staticalls
		if bytes.Equal(b.Value, []byte{}) {
			return true
		}
		keysMap[row.Key()] = b
		return true
	})
	if err != nil {
		log.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1ItxForAddress")
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
		log.WithFields(logger.Fields{
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
	names, _, err = bigtable.GetAddressesNamesArMetadata(&names, nil)
	if err != nil {
		return nil, err
	}

	idxs := make([][2]int64, len(keys))
	for i, k := range keys {
		txIdx, err := strconv.Atoi(strings.Split(k, ":")[6])
		if err != nil {
			return nil, fmt.Errorf("error parsing Eth1InternalTransactionIndexed tx index: %v", err)
		}
		txIdx = TX_PER_BLOCK_LIMIT - txIdx
		if txIdx < 0 {
			return nil, fmt.Errorf("invalid Eth1InternalTransactionIndexed tx index: %d", txIdx)
		}

		traceIdx, err := strconv.Atoi(strings.Split(k, ":")[7])
		if err != nil {
			return nil, fmt.Errorf("error parsing Eth1InternalTransactionIndexed trace index: %v", err)
		}
		traceIdx = ITX_PER_TX_LIMIT - traceIdx
		if txIdx < 0 {
			return nil, fmt.Errorf("invalid Eth1InternalTransactionIndexed trace index: %d", traceIdx)
		}
		idxs[i] = [2]int64{int64(txIdx), int64(traceIdx)}
	}
	contractInteractionTypes, err := bigtable.GetAddressContractInteractionsAtITransactions(itransactions, idxs)
	if err != nil {
		utils.LogError(err, "error getting contract states", 0)
	}

	tableData := make([][]interface{}, len(itransactions))
	for i, t := range itransactions {

		fromName := names[string(t.From)]
		toName := names[string(t.To)]

		var fromContractInteraction, toContractInteraction types.ContractInteractionType
		if len(contractInteractionTypes) > i {
			fromContractInteraction = contractInteractionTypes[i][0]
			toContractInteraction = contractInteractionTypes[i][1]
		}

		if t.Type == "suicide" {
			// erigon's "suicide" might be misleading for users
			t.Type = "selfdestruct"
		}

		tableData[i] = []interface{}{
			utils.FormatTransactionHash(t.ParentHash, true),
			utils.FormatBlockNumber(t.BlockNumber),
			utils.FormatTimestamp(t.Time.AsTime().Unix()),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.From, bigtable.GetAddressLabel(fromName, fromContractInteraction), fromContractInteraction != types.CONTRACT_NONE, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
			utils.FormatInOutSelf(address, t.From, t.To),
			utils.FormatAddressWithLimitsInAddressPageTable(address, t.To, bigtable.GetAddressLabel(toName, toContractInteraction), toContractInteraction != types.CONTRACT_NONE, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
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

func (bigtable *Bigtable) GetInternalTransfersForTransaction(transaction []byte, from []byte, parityTrace []*rpc_types.ParityTraceResult, currency string) ([]types.ITransaction, error) {

	names := make(map[string]string)
	for _, trace := range parityTrace {
		from, to, _, _ := trace.ConvertFields()
		names[string(from)] = ""
		names[string(to)] = ""
	}

	err := bigtable.GetAddressNames(names)
	if err != nil {
		return nil, err
	}

	contractInteractionTypes, err := bigtable.GetAddressContractInteractionsAtParityTraces(parityTrace)
	if err != nil {
		utils.LogError(err, "error getting contract states", 0)
	}

	if len(parityTrace) < 1 {
		// TODO: pruned node workaround: No internal transactions available from a pruned node.
    		log.Warnf("got parity trace with len < 1, len: %v", len(parityTrace))
		return []types.ITransaction{}, nil
	}
	
	log.Infof("parityTrace len: %v", len(parityTrace))
	data := make([]types.ITransaction, 0, len(parityTrace)-1)
	for i := 1; i < len(parityTrace); i++ {
		from, to, value, txType := parityTrace[i].ConvertFields()
		if txType == "suicide" {
			// erigon's "suicide" might be misleading for users
			txType = "selfdestruct"
		}
		input := make([]byte, 0)
		if len(parityTrace[i].Action.Input) > 2 {
			input, err = hex.DecodeString(parityTrace[i].Action.Input[2:])
			if err != nil {
				utils.LogError(err, "can't convert hex string", 0)
			}
		}

		var fromContractInteraction, toContractInteraction types.ContractInteractionType
		if len(contractInteractionTypes) > i {
			fromContractInteraction = contractInteractionTypes[i][0]
			toContractInteraction = contractInteractionTypes[i][1]
		}

		fromName := bigtable.GetAddressLabel(names[string(from)], fromContractInteraction)
		toName := bigtable.GetAddressLabel(names[string(to)], toContractInteraction)

		itx := types.ITransaction{
			From:      utils.FormatAddress(from, nil, fromName, false, fromContractInteraction != types.CONTRACT_NONE, true),
			To:        utils.FormatAddress(to, nil, toName, false, toContractInteraction != types.CONTRACT_NONE, true),
			Amount:    utils.FormatElCurrency(value, currency, 8, true, false, false, true),
			TracePath: utils.FormatTracePath(txType, parityTrace[i].TraceAddress, parityTrace[i].Error == "", bigtable.GetMethodLabel(input, fromContractInteraction)),
			Advanced:  txType == "delegatecall" || string(value) == "\x00",
		}

		gaslimit, err := strconv.ParseUint(parityTrace[i].Action.Gas, 0, 0)
		if err == nil {
			itx.Gas.Limit = gaslimit
		}

		data = append(data, itx)
		// gasusage, err := strconv.ParseUint(parityTrace[i].Result.GasUsed, 0, 0)
		// if err == nil {
		// 	itx.Gas.Usage = gasusage
		// }
	}
	return data, nil
}

// currently only erc20
func (bigtable *Bigtable) GetArbitraryTokenTransfersForTransaction(transaction []byte) ([]*types.Transfer, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
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
			log.Fatalf("error unmarshalling data for row %v: %v", row.Key(), err)
			return false
		}
		rowN, err := strconv.Atoi(strings.Split(row_.Row, ":")[3])
		if err != nil {
			log.Fatalf("error parsing data for row %v: %v", row.Key(), err)
			return false
		}
		rowN = ITX_PER_TX_LIMIT - rowN
		mux.Lock()
		transfers[rowN] = b
		mux.Unlock()
		return true
	}, gcp_bigtable.LimitRows(256))
	if err != nil {
		return nil, err
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
			return err
		}
		return nil
	})

	for address := range tokens {
		address := address
		g.Go(func() error {
			metadata, err := bigtable.GetERC20MetadataForAddress([]byte(address))
			if err != nil {
				return err
			}
			mux.Lock()
			tokensToAdd[address] = metadata
			mux.Unlock()
			return nil
		})
	}
	err = g.Wait()
	if err != nil {
		return nil, err
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
		log.WithFields(logger.Fields{
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
			log.Fatalf("error parsing Eth1ERC20Indexed data: %v", err)
		}
		keysMap[row.Key()] = b
		return true
	})
	if err != nil {
		log.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1ERC20ForAddress")
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
		log.WithFields(logger.Fields{
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
	names, tokens, err = bigtable.GetAddressesNamesArMetadata(&names, &tokens)
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
		log.WithFields(logger.Fields{
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
			log.Fatalf("error parsing Eth1ERC721Indexed data: %v", err)
		}
		keysMap[row.Key()] = b
		return true
	})
	if err != nil {
		log.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1ERC721ForAddress")
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
		log.WithFields(logger.Fields{
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
	names, _, err = bigtable.GetAddressesNamesArMetadata(&names, nil)
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
		log.WithFields(logger.Fields{
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
			log.Fatalf("error parsing ETh1ERC1155Indexed data: %v", err)
		}
		keysMap[row.Key()] = b
		return true
	})
	if err != nil {
		log.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1ERC1155ForAddress")
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
		log.WithFields(logger.Fields{
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
	names, _, err = bigtable.GetAddressesNamesArMetadata(&names, nil)
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

func (bigtable *Bigtable) GetMetadata(startToken string, limit int) ([]string, []*types.Eth1AddressBalance, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
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
		log.WithFields(logger.Fields{
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
		log.WithFields(logger.Fields{
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
		if val == nil || len(val) < 1 {
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
		log.WithFields(logger.Fields{
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
		log.Infof("retrieving metadata for token %x via rpc", address)

		metadata, err := bigtable.rpc.GetERC20TokenMetadata(address)
		if err != nil {
			log.Warnf("error retrieving metadata for token %x: %v", address, err)
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

	// log.Infof("retrieving metadata for token %x via bigtable", address)
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
		log.WithFields(logger.Fields{
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
		// log.Infof("retrieved name for address %x from cache", address)
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
		log.WithFields(logger.Fields{
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
		log.WithFields(logger.Fields{}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
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

// convenience function to get contract interaction status per parity trace
func (bigtable *Bigtable) GetAddressContractInteractionsAtParityTraces(traces []*rpc_types.ParityTraceResult) ([][2]types.ContractInteractionType, error) {
	requests := make([]contractInteractionAtRequest, 0, len(traces)*2)
	for i, itx := range traces {
		from, to, _, _ := itx.ConvertFields()
		requests = append(requests, contractInteractionAtRequest{
			address:  fmt.Sprintf("%x", from),
			block:    int64(itx.BlockNumber),
			txIdx:    int64(itx.TransactionPosition),
			traceIdx: int64(i),
		})
		requests = append(requests, contractInteractionAtRequest{
			address:  fmt.Sprintf("%x", to),
			block:    int64(itx.BlockNumber),
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
		log.WithFields(logger.Fields{
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
				log.Warnf("Hit rate limit when fetching contract metadata for address %x", address)
			} else {
				logAdditionalInfo := map[string]interface{}{"address": fmt.Sprintf("%x", address)}
				if strings.Contains(err.Error(), "unsupported arg type") {
					// open issue in the go-ethereum lib: https://github.com/ethereum/go-ethereum/issues/24572
					log.Warnf("could not parse ABI for %x: %v", address, err)
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
			log.Errorf("error saving contract metadata to bigtable: %v", err)
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
					log.Fatalf("error decoding abi for address 0x%x: %v", address, err)
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

func (bigtable *Bigtable) GetEth1TxForToken(prefix string, limit int64) ([]*types.Eth1ERC20Indexed, string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
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
			log.Fatalf("error parsing Eth1ERC20Indexed data: %v", err)
		}
		keysMap[row.Key()] = b

		return true
	})
	if err != nil {
		log.WithError(err).WithField("prefix", prefix).WithField("limit", limit).Errorf("error reading rows in bigtable_eth1 / GetEth1TxForToken")
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

	transactions, lastKey, err := bigtable.GetEth1TxForToken(pageToken, DefaultInfScrollRows)
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
	names, tokens, err = bigtable.GetAddressesNamesArMetadata(&names, &tokens)
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
		log.WithFields(logger.Fields{
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

// Get the status of the last signature import run
func (bigtable *Bigtable) GetSignatureImportStatus(st types.SignatureType) (*types.SignatureImportStatus, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"st": st,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()
	key := fmt.Sprintf("1:%v_SIGNATURE_IMPORT_STATUS", getSignaturePrefix(st))
	row, err := bigtable.tableData.ReadRow(ctx, key)
	if err != nil {
		log.Errorf("error reading signature imoprt status row %v: %v", row.Key(), err)
		return nil, err
	}
	s := &types.SignatureImportStatus{}
	if row == nil {
		return s, nil
	}
	row_ := row[DEFAULT_FAMILY][0]
	err = json.Unmarshal(row_.Value, s)
	if err != nil {
		log.Errorf("error unmarshalling signature import status for row %v: %v", row.Key(), err)
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
		log.WithFields(logger.Fields{
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
		log.Errorf("error reading signature imoprt status row %v: %v", row.Key(), err)
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
		log.WithFields(logger.Fields{
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
			log.Errorf("error reading row: %+v", row)
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

// GetAvailableRanksForExecBlock returns a list of ranks for a given execution block number
// by scanning all rows in Bigtable that match the block's key prefix.
// 
// For block number N, it scans rows with prefix:
//   <chainID>:<reversedPaddedBlockNumber(N)>:
//
// Then extracts the rank from the row key suffix.
// Returns a sorted list of available ranks (e.g., [0, 2, 4]).
func (bigtable *Bigtable) GetAvailableRanksForExecBlock(number uint64) ([]int, error) {
	ctx, cleanup := withTimeoutAndWarning(fmt.Sprintf("get ranks for exec block %d", number), 15*time.Second)
	defer cleanup()

	prefix := fmt.Sprintf("%s:%s:", bigtable.chainId, reversedPaddedBlockNumber(number))

	var ranks []int

	err := bigtable.tableBlocks.ReadRows(ctx, gcp_bigtable.PrefixRange(prefix), func(row gcp_bigtable.Row) bool {
		// Key format: <chainID>:<reversedPaddedBlockNumber>:<rank>
		parts := strings.Split(row.Key(), ":")
		if len(parts) != 3 {
			// unexpected key, skip
			return true
		}

		rankStr := parts[2]
		rank, err := strconv.Atoi(rankStr)
		if err != nil {
			// skip invalid ranks
			return true
		}

		ranks = append(ranks, rank)
		return true
	})

	if err != nil {
		return nil, fmt.Errorf("failed to read ranks from Bigtable for block %d: %w", number, err)
	}

	sort.Ints(ranks)
	return ranks, nil
}