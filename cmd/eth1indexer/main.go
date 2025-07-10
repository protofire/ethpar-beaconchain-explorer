package main

import (
	"fmt"
	"os"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/config"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/eth1indexer"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/profiling"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
	"github.com/protofire/ethpar-beaconchain-explorer/version"

	"github.com/coocood/freecache"
)

const cacheSize = 100 * 1024 * 1024

var log = logger.New(nil)

func main() {
	// TODO: make metrics conditional in all imported packages
	metrics.Init(nil)

	cfg, err := config.LoadEth1IndexerConfig(os.Args[1:])
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	// TODO: legacy chain parameters via global vars
	db.MaxWithdrawalsPerPayload = cfg.Chain.MaxWithdrawalsPerPayload

	if cfg.Version {
		fmt.Println(version.Version)
		fmt.Println(version.GoVersion)
		return
	}

	log.WithFields(logger.Fields{
		"config":  cfg.Config,
		"version": version.Version,
	}).Info("starting")

	metrics.StartMetrics(cfg.Metrics.Enabled, cfg.Metrics.Address)
	profiling.StartProfiling(cfg.Pprof.Enabled, cfg.Pprof.Address, cfg.Pprof.Port)

	rpcClient := execution.MustInitNewClient(cfg.JsonRpc.Client, cfg.JsonRpc.Endpoint)
	defer rpcClient.Close()

	if !rpcClient.ValidateChainIdFromConfig(cfg.Chain.Id) {
		log.Fatalf("chain ID mismatch: expected %v, got %v", cfg.Chain.Id, rpcClient.GetChainID())
	}

	// Initialize BigTable client
	bt := db.MustInitBigtable(&db.BigtableConfig{
		Project:      cfg.BigTable.Project,
		Instance:     cfg.BigTable.Instance,
		ChainId:      cfg.Chain.Id,
		CacheAddr:    cfg.Cache.Endpoint,
		Emulated:     cfg.BigTable.Emulated,
		EmulatorHost: cfg.BigTable.EmulatorHost,
		EmulatorPort: cfg.BigTable.EmulatorPort,
		Rpc:          rpcClient,
	})
	defer bt.Close()

	startAuxiliaryServices(cfg, rpcClient, bt)

	// Compose indexing parameters
	indexingParams := eth1indexer.IndexingParams{
		StartBlock:        cfg.Indexing.Blocks.Start,
		EndBlock:          cfg.Indexing.Blocks.End,
		BulkBlock:         cfg.Indexing.Blocks.Bulk,
		OffsetBlock:       cfg.Indexing.Blocks.Offset,
		StartData:         cfg.Indexing.Data.Start,
		EndData:           cfg.Indexing.Data.End,
		BulkData:          cfg.Indexing.Data.Bulk,
		OffsetData:        cfg.Indexing.Data.Offset,
		ConcurrencyBlocks: cfg.Indexing.Blocks.Concurrency,
		ConcurrencyData:   cfg.Indexing.Data.Concurrency,
		ReorgDepth:        cfg.Indexing.ReorgDepth,
		TraceMode:         cfg.Indexing.TraceMode,
		EnsUpdate:         cfg.Indexing.EnsUpdater.Enabled,
		EnsBatch:          cfg.Indexing.EnsUpdater.Batch,
		BalanceUpdate:     cfg.Indexing.BalanceUpdater.Enabled,
		BalanceBatch:      cfg.Indexing.BalanceUpdater.Batch,
		BalancePrefix:     fmt.Sprintf("%d:B:", cfg.Chain.Id),
		Cache:             freecache.NewCache(cacheSize),
		Bigtable:          bt,
		Client:            rpcClient,
		Log:               log,
		ReportStatus:      cfg.ReportStatus,
	}

	// Validate and adjust parameters based on mode
	switch cfg.Indexing.Mode {
	case "single":
		indexingParams.StartBlock = cfg.Indexing.Block
		indexingParams.EndBlock = cfg.Indexing.Block
		if err := eth1indexer.IndexSingleBlock(indexingParams); err != nil {
			log.Fatalf("failed to index block %v, error: %v", indexingParams.StartBlock, err)
		}

	case "blockrange":
		if err := eth1indexer.IndexBlockRange(indexingParams); err != nil {
			log.Fatalf("failed to index block range, start: %v, end: %v, error: %v", indexingParams.StartBlock, indexingParams.EndBlock, err)
		}

	case "datarange":
		if err := eth1indexer.IndexDataRange(indexingParams); err != nil {
			log.Fatalf("failed to index data range, start: %v, end: %v, error: %v", indexingParams.StartData, indexingParams.EndData, err)
		}

	case "live":
		if err := eth1indexer.IndexLive(indexingParams); err != nil {
			log.Fatalf("live indexing failed: %v", err)
		}
		utils.WaitForCtrlC()
	default:
		log.Fatalf("unknown mode: %s", cfg.Indexing.Mode)
	}
}

func startAuxiliaryServices(cfg *config.Eth1IndexerConfig, rpcClient execution.ExecutionClient, bt *db.Bigtable) {
	balanceUpdaterPrefix := fmt.Sprintf("%d:B:", cfg.Chain.Id)

	// Start token price export
	if cfg.Indexing.TokenPriceExporter.Enabled {
		go eth1indexer.StartTokenPriceUpdater(bt, rpcClient, cfg.Indexing.TokenPriceExporter.List, log, cfg.Indexing.TokenPriceExporter.Frequency)
	}

	// Start unlimited balance updater
	if cfg.Indexing.BalanceUpdater.Full {
		eth1indexer.ProcessMetadataUpdates(bt, rpcClient, balanceUpdaterPrefix, cfg.Indexing.BalanceUpdater.Batch, -1, log)
		return
	}

	// close indexer if there are any gaps???
	if cfg.Indexing.Blocks.CheckGaps {
		bt.CheckForGapsInBlocksTable(cfg.Indexing.Blocks.GapsLoopback)
		return
	}

	// close indexer if there are any gaps???
	if cfg.Indexing.Data.CheckGaps {
		bt.CheckForGapsInDataTable(cfg.Indexing.Data.GapsLoopback)
		return
	}
}
