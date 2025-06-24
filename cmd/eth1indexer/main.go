package main

import (
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/eth1indexer"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/erigon"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
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
	executionClient := flag.String("execution.client", "", "Execution client, can be 'erigon', 'geth'")
	executionEndpoint := flag.String("execution.endpoint", "", "Execution client JSON-RPC enpoint")
	mode := flag.String("mode", "live", "Indexer mode, can be 'single', 'blockrange', 'datarange' or 'live'")
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
	mainLogger.WithFields(logger.Fields{
		"config": *configPath,
		"version": version.Version,
		"chainName": utils.Config.Chain.ClConfig.ConfigName,
	}).Info("starting")

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
	
	var rpcClient execution.ExecutionClient
	
	switch *executionClient {
	case "erigon":
		rpcClient, err = erigon.NewErigonClient(*executionEndpoint)
		if err != nil {
			mainLogger.Fatalf("failed to create a new Erigon client: %v", err)
		}
	case "geth":
		// TODO implement
	default:
		mainLogger.Fatalf("unsupported execution client: %s", *executionClient)
	}

	chainId := strconv.FormatUint(utils.Config.Chain.ClConfig.DepositChainID, 10)

	balanceUpdaterPrefix := chainId + ":B:"

	nodeChainId := rpcClient.GetChainID()

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
		go eth1indexer.StartTokenPriceUpdater(bt, rpcClient, *tokenPriceExportList, mainLogger, *&tokenPriceExportFrequency)
	}

	// Start unlimited balance updater
	if *enableFullBalanceUpdater {
		eth1indexer.ProcessMetadataUpdates(bt, rpcClient, balanceUpdaterPrefix, *balanceUpdaterBatchSize, -1, mainLogger)
		return
	}
	
	// Compose indexing parameters
	indexingParams := eth1indexer.IndexingParams{
		StartBlock:        *startBlocks,
		EndBlock:          *endBlocks,
		StartData:         *startData,
		EndData:           *endData,
		ConcurrencyBlocks: *concurrencyBlocks,
		ConcurrencyData:   *concurrencyData,
		TraceMode:         *traceMode,
		Cache:             freecache.NewCache(100 * 1024 * 1024), // 100MB
		Bigtable:          bt,
		Client:            rpcClient,
		Log:               mainLogger,
	}

	// Validate and adjust parameters based on mode
	switch *mode {
	case "single":
		if block == nil {
			mainLogger.Fatal("--block is required in single mode")
		}
		indexingParams.StartBlock = *block
		indexingParams.EndBlock = *block
		if err := eth1indexer.IndexSingleBlock(indexingParams); err != nil {
			mainLogger.Fatalf("failed to index block %v, error: %v", *block, err)
		}

	case "blockrange":
		if *startBlocks == 0 || *endBlocks == 0 {
			mainLogger.Fatal("--blocks.start and --blocks.end are required in blockrange mode")
		}
		if *startBlocks > *endBlocks {
			mainLogger.Fatalf("invalid blockrange: start (%d) > end (%d)", *startBlocks, *endBlocks)
		}
		if err := eth1indexer.IndexBlockRange(indexingParams); err != nil {
			mainLogger.Fatalf("failed to index block range, start: %v, end: %v, error: %v", startBlocks, endBlocks, err)
		}

	case "datarange":
		if *startData == 0 || *endData == 0 {
			mainLogger.Fatal("--data.start and --data.end are required in datarange mode")
		}
		if *startData > *endData {
			mainLogger.Fatalf("invalid datarange: start (%d) > end (%d)", *startData, *endData)
		}
		if err := eth1indexer.IndexDataRange(indexingParams); err != nil {
			mainLogger.Fatalf("failed to index data range, start: %v, end: %v, error: %v", startData, endData, err)
		}

	case "live":
		// No additional validation needed for live mode

	default:
		mainLogger.Fatalf("unknown mode: %s", *mode)
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

	

	// utils.WaitForCtrlC()
}