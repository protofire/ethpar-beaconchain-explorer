package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/eth1indexer"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
	"github.com/protofire/ethpar-beaconchain-explorer/version"

	"github.com/coocood/freecache"
	_ "github.com/jackc/pgx/v5/stdlib"

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

	// Start token price export
	if *tokenPriceExport {
		go eth1indexer.StartTokenPriceUpdater(bt, client, *tokenPriceExportList, mainLogger, *&tokenPriceExportFrequency)
	}

	// Start unlimited balance updater
	if *enableFullBalanceUpdater {
		eth1indexer.ProcessMetadataUpdates(bt, client, balanceUpdaterPrefix, *balanceUpdaterBatchSize, -1, mainLogger)
		return
	}

	// Indexing parameters
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
	
	// Index a single block and exit
	indexingParams := eth1indexer.IndexingParams{
		StartBlock: *block,
		EndBlock: *block,
		ConcurrencyBlocks:  *concurrencyBlocks,
		ConcurrencyData: *concurrencyData,
		TraceMode: *traceMode,
		Cache: cache, 
		Bigtable: bt,
		Client: client,
		Log: mainLogger,
	}

	if err := eth1indexer.IndexSingleBlock(indexingParams); err != nil {
		mainLogger.Fatalf("failed to index block %v, error: %v", *block, err)
	}

	// TODO: close indexer if there are any gaps?
	if *checkBlocksGaps {
		bt.CheckForGapsInBlocksTable(*checkBlocksGapsLookback)
		return
	}

	// TODO: close indexer if there are any gaps?
	if *checkDataGaps {
		bt.CheckForGapsInDataTable(*checkDataGapsLookback)
		return
	}

	// Save specified block range from node to bigtable and exit
	if *endBlocks != 0 && *startBlocks < *endBlocks {
		err = eth1indexer.IndexFromNode(bt, client, *startBlocks, *endBlocks, *concurrencyBlocks, *traceMode, mainLogger)
		if err != nil {
			mainLogger.WithError(err).Fatalf("error indexing from node, start: %v end: %v concurrency: %v", *startBlocks, *endBlocks, *concurrencyBlocks)
		}
		return
	}

	// Transform specified block range from blocks to data bigtable tables and exit
	if *endData != 0 && *startData < *endData {
		err = bt.IndexEventsWithTransformers(int64(*startData), int64(*endData), transforms, *concurrencyData, cache)
		if err != nil {
			mainLogger.WithError(err).Fatalf("error indexing from bigtable")
		}
		cache.Clear()
		return
	}

	// Endless cycle if no ranges or single blocks specified
	var lastBlockFromNodeOld uint64
	var lastBlockFromNodeSameCount uint64
	lastSuccessulBlockIndexingTs := time.Now()
	for ; ; time.Sleep(time.Second * 14) {
		err := eth1indexer.HandleChainReorgs(bt, client, *reorgDepth, mainLogger)
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
			eth1indexer.ProcessMetadataUpdates(bt, client, balanceUpdaterPrefix, *balanceUpdaterBatchSize, 10, mainLogger)
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