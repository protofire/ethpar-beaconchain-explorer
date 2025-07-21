package main

import (
	"context"
	"os"
	"fmt"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/cache"
	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/eth2indexer"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/config"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/profiling"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
	"github.com/protofire/ethpar-beaconchain-explorer/version"

	_ "github.com/jackc/pgx/v5/stdlib"
)

var log = logger.New(nil)

func main() {
	// TODO: make metrics conditional in all imported packages
	metrics.Init(nil)

	// Load configuration
	cfg, err := config.LoadEth2IndexerConfig(os.Args[1:])
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	if cfg.Version {
		fmt.Println(version.Version)
		fmt.Println(version.GoVersion)
		return
	}

	// Load network parameters
	chainParams, err := config.LoadNetworkConfig(cfg.NetworkParams)
	if err != nil {
		log.Fatalf("error reading network params file: %v", err)
	}
	
	log.WithFields(logger.Fields{
		"version":   version.Version,
		"chainName": cfg.Chain}).Info("starting")

	metrics.StartMetrics(cfg.Metrics.Enabled, cfg.Metrics.Address)
	profiling.StartProfiling(cfg.Pprof.Enabled, cfg.Pprof.Address, cfg.Pprof.Port)

	sqlDb, err := db.MustInitDatabase(&cfg.Database, "pgx")
	if err != nil {
		log.Fatalf("failed to initialize database connection: %v", err)
	}
	defer sqlDb.Close()

	rpcClient := execution.MustInitNewClient(cfg.Execution.Client, cfg.Execution.Endpoint)
	defer rpcClient.Close()

	if !rpcClient.ValidateChainIdFromConfig(cfg.Chain.Id) {
		log.Fatalf("chain ID mismatch: expected %v, got %v", cfg.Chain.Id, rpcClient.GetChainID())
	}

	consClient := consensus.MustInitNewClient(cfg.Consensus.Client, cfg.Consensus.Endpoint, cfg.Chain.Id, chainParams)
	defer consClient.Close()

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

	cache.MustInitTieredCache(cfg.Cache.Endpoint)
	log.Infof("tiered Cache initialized, latest finalized epoch: %v", services.LatestFinalizedEpoch())
	
	if cfg.Metrics.Enabled {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		go metrics.MonitorDB(ctx, sqlDb.Db)
	}
	
	go eth2indexer.Start(&eth2indexer.IndexingParams{
		ConsClient:  consClient,
		ExecClient:  rpcClient,
		Bigtable:    bt,
		Database:    sqlDb,
		ChainParams: chainParams,
		Config:      cfg,
		Log:         log,
	})

	utils.WaitForCtrlC()

	log.Info("exiting...")
}
