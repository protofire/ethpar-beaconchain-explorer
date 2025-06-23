package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/erc20"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/eth1indexer"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
	"github.com/protofire/ethpar-beaconchain-explorer/version"

	"github.com/coocood/freecache"
	"github.com/ethereum/go-ethereum/common"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"

	_ "net/http/pprof"
)

// Init base logger
var mainLogger = logger.New(nil)

func main() {
	erigonEndpoint := flag.String("erigon", "", "Erigon archive node enpoint")
	block := flag.Int64("block", 0, "Index a specific block")

	reorgDepth := flag.Int("reorg.depth", 20, "Lookback to check and handle chain reorgs")

	concurrencyBlocks := flag.Int64("blocks.concurrency", 30, "Concurrency to use when indexing blocks from erigon")
	startBlocks := flag.Int64("blocks.start", 0, "Block to start indexing")
	endBlocks := flag.Int64("blocks.end", 0, "Block to finish indexing")
	bulkBlocks := flag.Int64("blocks.bulk", 8000, "Maximum number of blocks to be processed before saving")
	offsetBlocks := flag.Int64("blocks.offset", 100, "Blocks offset")
	checkBlocksGaps := flag.Bool("blocks.gaps", false, "Check for gaps in the blocks table")
	checkBlocksGapsLookback := flag.Int("blocks.gaps.lookback", 1000000, "Lookback for gaps check of the blocks table")
	traceMode := flag.String("blocks.tracemode", "parity/geth", "Trace mode to use, can bei either 'parity', 'geth' or 'parity/geth' for both")

	concurrencyData := flag.Int64("data.concurrency", 30, "Concurrency to use when indexing data from bigtable")
	startData := flag.Int64("data.start", 0, "Block to start indexing")
	endData := flag.Int64("data.end", 0, "Block to finish indexing")
	bulkData := flag.Int64("data.bulk", 8000, "Maximum number of blocks to be processed before saving")
	offsetData := flag.Int64("data.offset", 1000, "Data offset")
	checkDataGaps := flag.Bool("data.gaps", false, "Check for gaps in the data table")
	checkDataGapsLookback := flag.Int("data.gaps.lookback", 1000000, "Lookback for gaps check of the blocks table")

	enableBalanceUpdater := flag.Bool("balances.enabled", false, "Enable balance update process")
	enableFullBalanceUpdater := flag.Bool("balances.full.enabled", false, "Enable full balance update process")
	balanceUpdaterBatchSize := flag.Int("balances.batch", 1000, "Batch size for balance updates")

	tokenPriceExport := flag.Bool("token.price.enabled", false, "Enable token export process")
	tokenPriceExportList := flag.String("token.price.list", "", "Tokenlist path to use for the token price export")
	tokenPriceExportFrequency := flag.Duration("token.price.frequency", time.Hour, "Token price export interval")

	versionFlag := flag.Bool("version", false, "Print version and exit")

	configPath := flag.String("config", "", "Path to the config file, if empty string defaults will be used")

	enableEnsUpdater := flag.Bool("ens.enabled", false, "Enable ens update process")
	ensBatchSize := flag.Int64("ens.batch", 200, "Batch size for ens updates")

	flag.Parse()

	if *versionFlag {
		fmt.Println(version.Version)
		fmt.Println(version.GoVersion)
		return
	}

	cfg := &types.Config{}
	err := utils.ReadConfig(cfg, *configPath)
	if err != nil {
		mainLogger.Fatalf("error reading config file: %v", err)
	}
	utils.Config = cfg
	mainLogger.WithField("config", *configPath).WithField("version", version.Version).WithField("chainName", utils.Config.Chain.ClConfig.ConfigName).Printf("starting")

	if utils.Config.Metrics.Enabled {
		go func(addr string) {
			mainLogger.Infof("serving metrics on %v", addr)
			if err := metrics.Serve(addr); err != nil {
				mainLogger.WithError(err).Fatal("Error serving metrics")
			}
		}(utils.Config.Metrics.Address)
	}

	// enable pprof endpoint if requested
	if utils.Config.Pprof.Enabled {
		go func() {
			mainLogger.Infof("starting pprof http server on port %s", utils.Config.Pprof.Port)
			mainLogger.Info(http.ListenAndServe(fmt.Sprintf("localhost:%s", utils.Config.Pprof.Port), nil))
		}()
	}

	db.MustInitDB(&types.DatabaseConfig{
		Username:     cfg.WriterDatabase.Username,
		Password:     cfg.WriterDatabase.Password,
		Name:         cfg.WriterDatabase.Name,
		Host:         cfg.WriterDatabase.Host,
		Port:         cfg.WriterDatabase.Port,
		MaxOpenConns: cfg.WriterDatabase.MaxOpenConns,
		MaxIdleConns: cfg.WriterDatabase.MaxIdleConns,
		SSL:          cfg.WriterDatabase.SSL,
	}, &types.DatabaseConfig{
		Username:     cfg.ReaderDatabase.Username,
		Password:     cfg.ReaderDatabase.Password,
		Name:         cfg.ReaderDatabase.Name,
		Host:         cfg.ReaderDatabase.Host,
		Port:         cfg.ReaderDatabase.Port,
		MaxOpenConns: cfg.ReaderDatabase.MaxOpenConns,
		MaxIdleConns: cfg.ReaderDatabase.MaxIdleConns,
		SSL:          cfg.ReaderDatabase.SSL,
	}, "pgx", "postgres")
	defer db.ReaderDb.Close()
	defer db.WriterDb.Close()

	if erigonEndpoint == nil || *erigonEndpoint == "" {

		if utils.Config.Eth1ErigonEndpoint == "" {

			utils.LogFatal(nil, "no erigon node url provided", 0)
		} else {
			mainLogger.Info("applying erigon endpoint from config")
			*erigonEndpoint = utils.Config.Eth1ErigonEndpoint
		}

	}

	mainLogger.Infof("using erigon node at %v", *erigonEndpoint)
	client, err := rpc.NewErigonClient(*erigonEndpoint)
	if err != nil {
		utils.LogFatal(err, "erigon client creation error", 0)
	}

	chainId := strconv.FormatUint(utils.Config.Chain.ClConfig.DepositChainID, 10)

	balanceUpdaterPrefix := chainId + ":B:"

	nodeChainId, err := client.GetNativeClient().ChainID(context.Background())
	if err != nil {
		utils.LogFatal(err, "node chain id error", 0)
	}

	if nodeChainId.String() != chainId {
		mainLogger.Fatalf("node chain id mismatch, wanted %v got %v", chainId, nodeChainId.String())
	}

	// Initialize BigTable client
	bt, err := db.InitBigtable(utils.Config.Bigtable.Project, utils.Config.Bigtable.Instance, chainId, utils.Config.RedisCacheEndpoint)
	if err != nil {
		mainLogger.Fatalf("error connecting to bigtable: %v", err)
	}
	defer bt.Close()

	if *tokenPriceExport {
		go func() {
			for {
				err = UpdateTokenPrices(bt, client, *tokenPriceExportList)
				if err != nil {
					utils.LogError(err, "error while updating token prices", 0)
					time.Sleep(*tokenPriceExportFrequency)
				}
				time.Sleep(*tokenPriceExportFrequency)
			}
		}()
	}

	if *enableFullBalanceUpdater {
		ProcessMetadataUpdates(bt, client, balanceUpdaterPrefix, *balanceUpdaterBatchSize, -1)
		return
	}

	transforms := make([]func(blk *types.Eth1Block, cache *freecache.Cache) (*types.BulkMutations, *types.BulkMutations, error), 0)
	transforms = append(transforms,
		bt.TransformBlock,
		bt.TransformTx,
		bt.TransformItx,
		bt.TransformBlobTx,
		bt.TransformERC20,
		bt.TransformERC721,
		bt.TransformERC1155,
		bt.TransformUncle,
		bt.TransformWithdrawals,
		bt.TransformEnsNameRegistered,
		bt.TransformContract)

	cache := freecache.NewCache(100 * 1024 * 1024) // 100 MB limit

	if *block != 0 {
		err = eth1indexer.IndexFromNode(bt, client, *block, *block, *concurrencyBlocks, *traceMode, mainLogger)
		if err != nil {
			mainLogger.WithError(err).Fatalf("error indexing from node, start: %v end: %v concurrency: %v", *block, *block, *concurrencyBlocks)
		}
		err = bt.IndexEventsWithTransformers(*block, *block, transforms, *concurrencyData, cache)
		if err != nil {
			mainLogger.WithError(err).Fatalf("error indexing from bigtable")
		}
		cache.Clear()

		mainLogger.Infof("indexing of block %v completed", *block)
		return
	}

	if *checkBlocksGaps {
		bt.CheckForGapsInBlocksTable(*checkBlocksGapsLookback)
		return
	}

	if *checkDataGaps {
		bt.CheckForGapsInDataTable(*checkDataGapsLookback)
		return
	}

	if *endBlocks != 0 && *startBlocks < *endBlocks {
		err = eth1indexer.IndexFromNode(bt, client, *startBlocks, *endBlocks, *concurrencyBlocks, *traceMode, mainLogger)
		if err != nil {
			mainLogger.WithError(err).Fatalf("error indexing from node, start: %v end: %v concurrency: %v", *startBlocks, *endBlocks, *concurrencyBlocks)
		}
		return
	}

	if *endData != 0 && *startData < *endData {
		err = bt.IndexEventsWithTransformers(int64(*startData), int64(*endData), transforms, *concurrencyData, cache)
		if err != nil {
			mainLogger.WithError(err).Fatalf("error indexing from bigtable")
		}
		cache.Clear()
		return
	}

	var lastBlockFromNodeOld uint64
	var lastBlockFromNodeSameCount uint64
	lastSuccessulBlockIndexingTs := time.Now()
	for ; ; time.Sleep(time.Second * 14) {
		err := HandleChainReorgs(bt, client, *reorgDepth)
		if err != nil {
			mainLogger.Errorf("error handling chain reorgs: %v", err)
			continue
		}

		lastBlockFromNode, err := client.GetLatestEth1BlockNumber()
		if err != nil {
			lastBlockFromNodeSameCount++
			if lastBlockFromNodeSameCount > 20 { // nearly 5 minutes no new block
				utils.LogFatal(err, "no new block in 20 tries", 0, map[string]interface{}{
					"lastBlockFromNode": lastBlockFromNodeOld,
				})
			}
			mainLogger.Errorf("error retrieving latest eth block number: %v", err)
			continue
		}
		if lastBlockFromNode != lastBlockFromNodeOld {
			lastBlockFromNodeOld = lastBlockFromNode
			lastBlockFromNodeSameCount = 0
		} else {
			lastBlockFromNodeSameCount++
			if lastBlockFromNodeSameCount > 20 { // nearly 5 minutes no new block
				utils.LogFatal(nil, "no new block in 20 tries", 0, map[string]interface{}{
					"lastBlockFromNode": lastBlockFromNodeOld,
				})
			}
		}

		lastBlockFromBlocksTable, err := bt.GetLastBlockInBlocksTable()
		if err != nil {
			mainLogger.Errorf("error retrieving last blocks from blocks table: %v", err)
			continue
		}

		lastBlockFromDataTable, err := bt.GetLastBlockInDataTable()
		if err != nil {
			mainLogger.Errorf("error retrieving last blocks from data table: %v", err)
			continue
		}

		mainLogger.WithFields(
			logger.Fields{
				"node":   lastBlockFromNode,
				"blocks": lastBlockFromBlocksTable,
				"data":   lastBlockFromDataTable,
			},
		).Infof("last blocks")

		continueAfterError := false
		if lastBlockFromNode > 0 {
			if lastBlockFromBlocksTable < int(lastBlockFromNode) {
				mainLogger.Infof("missing blocks %v to %v in blocks table, indexing ...", lastBlockFromBlocksTable+1, lastBlockFromNode)

				startBlock := int64(lastBlockFromBlocksTable+1) - *offsetBlocks
				if startBlock < 0 {
					startBlock = 0
				}

				if *bulkBlocks <= 0 || *bulkBlocks > int64(lastBlockFromNode)-startBlock+1 {
					*bulkBlocks = int64(lastBlockFromNode) - startBlock + 1
				}

				for startBlock <= int64(lastBlockFromNode) && !continueAfterError {
					endBlock := startBlock + *bulkBlocks - 1
					if endBlock > int64(lastBlockFromNode) {
						endBlock = int64(lastBlockFromNode)
					}

					err = eth1indexer.IndexFromNode(bt, client, startBlock, endBlock, *concurrencyBlocks, *traceMode, mainLogger)
					if err != nil {
						errMsg := "error indexing from node"
						errFields := map[string]interface{}{
							"start":       startBlock,
							"end":         endBlock,
							"concurrency": *concurrencyBlocks}
						if time.Since(lastSuccessulBlockIndexingTs) > time.Minute*30 {
							utils.LogFatal(err, errMsg, 0, errFields)
						} else {
							utils.LogError(err, errMsg, 0, errFields)
						}
						continueAfterError = true
						continue
					} else {
						lastSuccessulBlockIndexingTs = time.Now()
					}

					startBlock = endBlock + 1
				}
				if continueAfterError {
					continue
				}
			}

			if lastBlockFromDataTable < int(lastBlockFromNode) {
				mainLogger.Infof("missing blocks %v to %v in data table, indexing ...", lastBlockFromDataTable+1, lastBlockFromNode)

				startBlock := int64(lastBlockFromDataTable+1) - *offsetData
				if startBlock < 0 {
					startBlock = 0
				}

				if *bulkData <= 0 || *bulkData > int64(lastBlockFromNode)-startBlock+1 {
					*bulkData = int64(lastBlockFromNode) - startBlock + 1
				}

				for startBlock <= int64(lastBlockFromNode) && !continueAfterError {
					endBlock := startBlock + *bulkData - 1
					if endBlock > int64(lastBlockFromNode) {
						endBlock = int64(lastBlockFromNode)
					}

					err = bt.IndexEventsWithTransformers(startBlock, endBlock, transforms, *concurrencyData, cache)
					if err != nil {
						utils.LogError(err, "error indexing from bigtable", 0, map[string]interface{}{"start": startBlock, "end": endBlock, "concurrency": *concurrencyData})
						cache.Clear()
						continueAfterError = true
						continue
					}
					cache.Clear()

					startBlock = endBlock + 1
				}
				if continueAfterError {
					continue
				}
			}
		}

		if *enableBalanceUpdater {
			ProcessMetadataUpdates(bt, client, balanceUpdaterPrefix, *balanceUpdaterBatchSize, 10)
		}

		if *enableEnsUpdater {
			err := bt.ImportEnsUpdates(client.GetNativeClient(), *ensBatchSize)
			if err != nil {
				utils.LogError(err, "error importing ens updates", 0, nil)
				continue
			}
		}

		mainLogger.Infof("index run completed")
		services.ReportStatus("eth1indexer", "Running", nil)
	}

	// utils.WaitForCtrlC()
}

func UpdateTokenPrices(bt *db.Bigtable, client *rpc.ErigonClient, tokenListPath string) error {

	tokenListContent, err := os.ReadFile(tokenListPath)
	if err != nil {
		return err
	}

	tokenList := &erc20.ERC20TokenList{}

	err = json.Unmarshal(tokenListContent, tokenList)
	if err != nil {
		return err
	}

	type defillamaPriceRequest struct {
		Coins []string `json:"coins"`
	}
	coinsList := make([]string, 0, len(tokenList.Tokens))
	for _, token := range tokenList.Tokens {
		coinsList = append(coinsList, "ethereum:"+token.Address)
	}

	req := &defillamaPriceRequest{
		Coins: coinsList,
	}

	reqEncoded, err := json.Marshal(req)
	if err != nil {
		return err
	}

	httpClient := &http.Client{Timeout: time.Second * 10}

	resp, err := httpClient.Post("https://coins.llama.fi/prices", "application/json", bytes.NewReader(reqEncoded))
	if err != nil {
		return err
	}

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("error querying defillama api: %v", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	type defillamaCoin struct {
		Decimals  int64            `json:"decimals"`
		Price     *decimal.Decimal `json:"price"`
		Symbol    string           `json:"symbol"`
		Timestamp int64            `json:"timestamp"`
	}

	type defillamaResponse struct {
		Coins map[string]defillamaCoin `json:"coins"`
	}

	respParsed := &defillamaResponse{}
	err = json.Unmarshal(body, respParsed)
	if err != nil {
		return err
	}

	tokenPrices := make([]*types.ERC20TokenPrice, 0, len(respParsed.Coins))
	for address, data := range respParsed.Coins {
		tokenPrices = append(tokenPrices, &types.ERC20TokenPrice{
			Token: common.FromHex(strings.TrimPrefix(address, "ethereum:0x")),
			Price: []byte(data.Price.String()),
		})
	}

	g := new(errgroup.Group)
	g.SetLimit(20)
	for i := range tokenPrices {
		i := i
		g.Go(func() error {

			metadata, err := client.GetERC20TokenMetadata(tokenPrices[i].Token)
			if err != nil {
				return err
			}
			tokenPrices[i].TotalSupply = metadata.TotalSupply
			// mainLogger.Infof("price for token %x is %s @ %v", tokenPrices[i].Token, tokenPrices[i].Price, new(big.Int).SetBytes(tokenPrices[i].TotalSupply))
			return nil
		})
	}
	err = g.Wait()
	if err != nil {
		return err
	}

	return bt.SaveERC20TokenPrices(tokenPrices)
}

func HandleChainReorgs(bt *db.Bigtable, client *rpc.ErigonClient, depth int) error {
	ctx := context.Background()
	// get latest block from the node
	latestNodeBlock, err := client.GetNativeClient().BlockByNumber(ctx, nil)
	if err != nil {
		mainLogger.Debugf("error getting latest node block: %v", err)
		return err
	}
	latestNodeBlockNumber := latestNodeBlock.NumberU64()

	// for each block check if block node hash and block db hash match
	if depth > int(latestNodeBlockNumber) {
		depth = int(latestNodeBlockNumber)
	}
	for i := latestNodeBlockNumber - uint64(depth); i <= latestNodeBlockNumber; i++ {
		nodeBlock, err := client.GetNativeClient().HeaderByNumber(ctx, big.NewInt(int64(i)))
		if err != nil {
			mainLogger.Debugf("error getting block header for block %s: %v", i, err)
			return err
		}

		dbBlock, err := bt.GetBlockFromBlocksTable(i)
		if err != nil {
			if err == db.ErrBlockNotFound { // exit if we hit a block that is not yet in the db
				return nil
			}
			return err
		}

		if !bytes.Equal(nodeBlock.Hash().Bytes(), dbBlock.Hash) {
			mainLogger.Warnf("found incosistency at height %v, node block hash: %x, db block hash: %x", i, nodeBlock.Hash().Bytes(), dbBlock.Hash)

			// first we set the cached marker of the last block in the blocks/data table to the block prior to the forked one
			if i > 0 {
				previousBlock := i - 1
				err := bt.SetLastBlockInBlocksTable(int64(previousBlock))
				if err != nil {
					return fmt.Errorf("error setting last block [%v] in blocks table: %w", previousBlock, err)
				}
				err = bt.SetLastBlockInDataTable(int64(previousBlock))
				if err != nil {
					return fmt.Errorf("error setting last block [%v] in data table: %w", previousBlock, err)
				}
				// now we can proceed to delete all blocks including and after the forked block
			}
			// delete all blocks starting from the fork block up to the latest block in the db
			for j := i; j <= latestNodeBlockNumber; j++ {
				dbBlock, err := bt.GetBlockFromBlocksTable(j)
				if err != nil {
					if err == db.ErrBlockNotFound { // exit if we hit a block that is not yet in the db
						return nil
					}
					return err
				}
				mainLogger.Infof("deleting block at height %v with hash %x", dbBlock.Number, dbBlock.Hash)

				err = bt.DeleteBlock(dbBlock.Number, dbBlock.Hash)
				if err != nil {
					return err
				}
			}
		} else {
			mainLogger.Infof("height %v, node block hash: %x, db block hash: %x", i, nodeBlock.Hash().Bytes(), dbBlock.Hash)
		}
	}

	return nil
}

func ProcessMetadataUpdates(bt *db.Bigtable, client *rpc.ErigonClient, prefix string, batchSize int, iterations int) {
	lastKey := prefix

	its := 0
	for {
		start := time.Now()
		keys, pairs, err := bt.GetMetadataUpdates(prefix, lastKey, batchSize)
		if err != nil {
			mainLogger.Errorf("error retrieving metadata updates from bigtable: %v", err)
			return
		}

		if len(keys) == 0 {
			return
		}

		balances := make([]*types.Eth1AddressBalance, 0, len(pairs))
		for b := 0; b < len(pairs); b += batchSize {
			start := b
			end := b + batchSize
			if len(pairs) < end {
				end = len(pairs)
			}

			mainLogger.Infof("processing batch %v with start %v and end %v", b, start, end)

			b, err := client.GetBalances(pairs[start:end], 2, 4)

			if err != nil {
				mainLogger.Errorf("error retrieving balances from node: %v", err)
				return
			}
			balances = append(balances, b...)
		}

		err = bt.SaveBalances(balances, keys)
		if err != nil {
			mainLogger.Errorf("error saving balances to bigtable: %v", err)
			return
		}

		lastKey = keys[len(keys)-1]
		mainLogger.Infof("retrieved %v balances in %v, currently at %v", len(balances), time.Since(start), lastKey)

		its++

		if iterations != -1 && its > iterations {
			return
		}
	}
}