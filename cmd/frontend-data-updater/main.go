package main

import (
	"flag"
	"fmt"
	"math/big"

	"github.com/protofire/ethpar-beaconchain-explorer/cache"
	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/price"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/lighthouse"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/teku"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
	"github.com/protofire/ethpar-beaconchain-explorer/version"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/sirupsen/logrus"

	"net/http"
	_ "net/http/pprof"
)

func main() {
	// TODO: make metrics conditional in all imported packages
	metrics.Init(nil)
	configPath := flag.String("config", "", "Path to the config file, if empty string defaults will be used")
	versionFlag := flag.Bool("version", false, "Print version and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Println(version.Version)
		fmt.Println(version.GoVersion)
		return
	}

	cfg := &types.Config{}
	err := utils.ReadConfig(cfg, *configPath)
	if err != nil {
		logrus.Fatalf("error reading config file: %v", err)
	}
	utils.Config = cfg
	logrus.WithField("config", *configPath).WithField("version", version.Version).WithField("chainName", utils.Config.Chain.ClConfig.ConfigName).Printf("starting")

	// enable pprof endpoint if requested
	if utils.Config.Pprof.Enabled {
		go func() {
			logrus.Infof("starting pprof http server on port %s", utils.Config.Pprof.Port)
			logrus.Info(http.ListenAndServe(fmt.Sprintf("localhost:%s", utils.Config.Pprof.Port), nil))
		}()
	}

	metrics.StartMetrics(utils.Config.Metrics.Enabled, utils.Config.Metrics.Address)

	// Initialize BigTable client
	bt := db.MustInitBigtable(&db.BigtableConfig{
		Project:      utils.Config.Bigtable.Project,
		Instance:     utils.Config.Bigtable.Project,
		ChainId:      utils.Config.Chain.ClConfig.DepositChainID,
		CacheAddr:    utils.Config.RedisCacheEndpoint,
		Emulated:     utils.Config.Bigtable.Emulator,
		EmulatorHost: utils.Config.Bigtable.EmulatorHost,
		EmulatorPort: uint16(utils.Config.Bigtable.EmulatorPort),
		Rpc:          nil,
	})
	defer bt.Close()

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

	if utils.Config.TieredCacheProvider == "redis" || len(utils.Config.RedisCacheEndpoint) != 0 {
		cache.MustInitTieredCache(utils.Config.RedisCacheEndpoint)
	}

	if utils.Config.TieredCacheProvider != "redis" {
		logrus.Fatalf("No cache provider set. Please set TierdCacheProvider (example redis, bigtable)")
	}

	logrus.Infof("initializing prices")
	price.Init(utils.Config.Chain.ClConfig.DepositChainID, utils.Config.Eth1ErigonEndpoint, utils.Config.Frontend.ClCurrency, utils.Config.Frontend.ElCurrency)

	var consClient consensus.ConsensusClient

	chainID := new(big.Int).SetUint64(utils.Config.Chain.ClConfig.DepositChainID)
	if utils.Config.Indexer.Node.Type == "lighthouse" {
		consClient, err = lighthouse.NewLighthouseClient("http://"+cfg.Indexer.Node.Host+":"+cfg.Indexer.Node.Port, chainID)
		if err != nil {
			utils.LogFatal(err, "new explorer lighthouse client error", 0)
		}
	} else if utils.Config.Indexer.Node.Type == "teku" {
		consClient, err = teku.NewTekuClient("http://"+cfg.Indexer.Node.Host+":"+cfg.Indexer.Node.Port, chainID)
		if err != nil {
			utils.LogFatal(err, "new explorer lighthouse client error", 0)
		}
	} else {
		logrus.Fatalf("invalid node type %v specified. supported node types are teku and lighthouse", utils.Config.Indexer.Node.Type)
	}

	logrus.Infof("initializing frontend services")
	services.Init(consClient, bt) // Init frontend services
	logrus.Infof("frontend services initiated")

	utils.WaitForCtrlC()

	logrus.Println("exiting...")
}

