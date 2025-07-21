package main

import (
	"context"
	"encoding/gob"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/cache"
	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/handlers"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/price"
	"github.com/protofire/ethpar-beaconchain-explorer/ratelimit"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/lighthouse"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/teku"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/static"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
	"github.com/protofire/ethpar-beaconchain-explorer/version"

	"github.com/sirupsen/logrus"

	_ "net/http/pprof"

	httpSwagger "github.com/swaggo/http-swagger"
	"github.com/gorilla/csrf"
	"github.com/gorilla/mux"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/phyber/negroni-gzip/gzip"
	"github.com/stripe/stripe-go/v72"
	"github.com/urfave/negroni"
	"github.com/zesik/proxyaddr"
)

func init() {
	gob.Register(types.DataTableSaveState{})
}

var frontendHttpServer *http.Server

func main() {
	// TODO: make metrics conditional in all imported packages
	metrics.Init(nil)

	configPath := flag.String("config", "", "Path to the config file, if empty string defaults will be used")
	versionFlag := flag.Bool("version", false, "Show version and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Println(version.Version)
		fmt.Println(version.GoVersion)
		return
	}
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	cfg := &types.Config{}
	err := utils.ReadConfig(cfg, *configPath)
	if err != nil {
		logrus.Fatalf("error reading config file: %v", err)
	}
	utils.Config = cfg
	logrus.WithFields(logrus.Fields{
		"config":    *configPath,
		"version":   version.Version,
		"chainName": utils.Config.Chain.ClConfig.ConfigName}).Printf("starting")

	if utils.Config.Chain.ClConfig.SlotsPerEpoch == 0 || utils.Config.Chain.ClConfig.SecondsPerSlot == 0 {
		utils.LogFatal(err, "invalid chain configuration specified, you must specify the slots per epoch, seconds per slot and genesis timestamp in the config file", 0)
	}

	if utils.Config.Pprof.Enabled {
		go func() {
			logrus.Infof("starting pprof http server on port %s", utils.Config.Pprof.Port)
			logrus.Info(http.ListenAndServe(fmt.Sprintf("0.0.0.0:%s", utils.Config.Pprof.Port), nil))
		}()
	}

	wg := &sync.WaitGroup{}

	wg.Add(1)
	go func() {
		defer wg.Done()
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
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		db.MustInitFrontendDB(&types.DatabaseConfig{
			Username:     cfg.Frontend.WriterDatabase.Username,
			Password:     cfg.Frontend.WriterDatabase.Password,
			Name:         cfg.Frontend.WriterDatabase.Name,
			Host:         cfg.Frontend.WriterDatabase.Host,
			Port:         cfg.Frontend.WriterDatabase.Port,
			MaxOpenConns: cfg.Frontend.WriterDatabase.MaxOpenConns,
			MaxIdleConns: cfg.Frontend.WriterDatabase.MaxIdleConns,
			SSL:          cfg.Frontend.WriterDatabase.SSL,
		}, &types.DatabaseConfig{
			Username:     cfg.Frontend.ReaderDatabase.Username,
			Password:     cfg.Frontend.ReaderDatabase.Password,
			Name:         cfg.Frontend.ReaderDatabase.Name,
			Host:         cfg.Frontend.ReaderDatabase.Host,
			Port:         cfg.Frontend.ReaderDatabase.Port,
			MaxOpenConns: cfg.Frontend.ReaderDatabase.MaxOpenConns,
			MaxIdleConns: cfg.Frontend.ReaderDatabase.MaxIdleConns,
			SSL:          cfg.Frontend.ReaderDatabase.SSL,
		}, "pgx", "postgres")
	}()

	if utils.Config.ClickHouseEnabled {
		wg.Add(1)
		go func() {
			defer wg.Done()
			db.MustInitClickhouseDB(nil, &types.DatabaseConfig{
				Username:     cfg.ClickHouse.ReaderDatabase.Username,
				Password:     cfg.ClickHouse.ReaderDatabase.Password,
				Name:         cfg.ClickHouse.ReaderDatabase.Name,
				Host:         cfg.ClickHouse.ReaderDatabase.Host,
				Port:         cfg.ClickHouse.ReaderDatabase.Port,
				MaxOpenConns: cfg.ClickHouse.ReaderDatabase.MaxOpenConns,
				MaxIdleConns: cfg.ClickHouse.ReaderDatabase.MaxIdleConns,
				SSL:          true,
			}, "clickhouse", "clickhouse")
		}()
	}

	rpcClient := execution.MustInitNewClient("erigon", utils.Config.Eth1ErigonEndpoint)
	defer rpcClient.Close()

	if !rpcClient.ValidateChainIdFromConfig(utils.Config.Chain.ClConfig.DepositChainID) {
		log.Fatalf("chain ID mismatch: expected %v, got %v", utils.Config.Chain.ClConfig.DepositChainID, rpcClient.GetChainID())
	}

	// Initialize BigTable client
	bt := db.MustInitBigtable(&db.BigtableConfig{
		Project:      utils.Config.Bigtable.Project,
		Instance:     utils.Config.Bigtable.Instance,
		ChainId:      utils.Config.Chain.ClConfig.DepositChainID,
		CacheAddr:    utils.Config.RedisCacheEndpoint,
		Emulated:     utils.Config.Bigtable.Emulator,
		EmulatorHost: utils.Config.Bigtable.EmulatorHost,
		EmulatorPort: uint16(utils.Config.Bigtable.EmulatorPort),
		Rpc:          rpcClient,
	})
	defer bt.Close()

	if utils.Config.TieredCacheProvider == "redis" || len(utils.Config.RedisCacheEndpoint) != 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cache.MustInitTieredCache(utils.Config.RedisCacheEndpoint)
			logrus.Infof("tiered Cache initialized, latest finalized epoch: %v", services.LatestFinalizedEpoch())

		}()
	}

	wg.Wait()

	if utils.Config.TieredCacheProvider != "redis" {
		logrus.Fatalf("no cache provider set, please set TierdCacheProvider (example redis)")
	}

	defer db.ReaderDb.Close()
	defer db.WriterDb.Close()
	defer db.FrontendReaderDB.Close()
	defer db.FrontendWriterDB.Close()

	
	if utils.Config.Metrics.Enabled {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		go metrics.MonitorDB(ctx, db.WriterDb)
		DBInfo := []string{
			cfg.WriterDatabase.Username,
			cfg.WriterDatabase.Password,
			cfg.WriterDatabase.Host,
			cfg.WriterDatabase.Port,
			cfg.WriterDatabase.Name}
		DBStr := strings.Join(DBInfo, "-")
		frontendDBInfo := []string{
			cfg.Frontend.WriterDatabase.Username,
			cfg.Frontend.WriterDatabase.Password,
			cfg.Frontend.WriterDatabase.Host,
			cfg.Frontend.WriterDatabase.Port,
			cfg.Frontend.WriterDatabase.Name}
		frontendDBStr := strings.Join(frontendDBInfo, "-")
		if DBStr != frontendDBStr {
			go metrics.MonitorDB(ctx, db.FrontendWriterDB)
		}
	}

	logrus.Infof("database connection established")

	if cfg.Frontend.Enabled {

		if cfg.Frontend.OnlyAPI {
			services.ReportStatus(cfg.ReportServiceStatus, "api", "Running", nil)
		} else {
			services.ReportStatus(cfg.ReportServiceStatus, "frontend", "Running", nil)
		}

		router := mux.NewRouter()

		apiV1Router := router.PathPrefix("/api/v1").Subrouter()
		router.PathPrefix("/api/v1/docs/").Handler(httpSwagger.WrapHandler)
		apiV1Router.HandleFunc("/latestState", handlers.ApiLatestState).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/epoch/{epoch}", handlers.ApiEpoch).Methods("GET", "OPTIONS")

		apiV1Router.HandleFunc("/epoch/{epoch}/blocks", handlers.ApiEpochSlots).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/epoch/{epoch}/slots", handlers.ApiEpochSlots).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/slot/{slotOrHash}", handlers.ApiSlots).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/slot/{slot}/attestations", handlers.ApiSlotAttestations).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/slot/{slot}/deposits", handlers.ApiSlotDeposits).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/slot/{slot}/attesterslashings", handlers.ApiSlotAttesterSlashings).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/slot/{slot}/proposerslashings", handlers.ApiSlotProposerSlashings).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/slot/{slot}/voluntaryexits", handlers.ApiSlotVoluntaryExits).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/slot/{slot}/withdrawals", handlers.ApiSlotWithdrawals).Methods("GET", "OPTIONS")

		// deprecated, use slot equivalents
		apiV1Router.HandleFunc("/block/{slotOrHash}", handlers.ApiSlots).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/block/{slot}/attestations", handlers.ApiSlotAttestations).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/block/{slot}/deposits", handlers.ApiSlotDeposits).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/block/{slot}/attesterslashings", handlers.ApiSlotAttesterSlashings).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/block/{slot}/proposerslashings", handlers.ApiSlotProposerSlashings).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/block/{slot}/voluntaryexits", handlers.ApiSlotVoluntaryExits).Methods("GET", "OPTIONS")

		apiV1Router.HandleFunc("/sync_committee/{period}", handlers.ApiSyncCommittee).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/eth1deposit/{txhash}", handlers.ApiEth1Deposit).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/leaderboard", handlers.ApiValidatorLeaderboard).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/{indexOrPubkey}", handlers.ApiValidatorGet(bt)).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator", handlers.ApiValidatorPost(bt)).Methods("POST", "OPTIONS")
		apiV1Router.HandleFunc("/validator/{indexOrPubkey}/withdrawals", handlers.ApiValidatorWithdrawals).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/{indexOrPubkey}/blsChange", handlers.ApiValidatorBlsChange).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/{indexOrPubkey}/balancehistory", handlers.ApiValidatorBalanceHistory(bt)).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/{indexOrPubkey}/incomedetailhistory", handlers.ApiValidatorIncomeDetailsHistory(bt)).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/{indexOrPubkey}/performance", handlers.ApiValidatorPerformance(bt)).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/{indexOrPubkey}/execution/performance", handlers.ApiValidatorExecutionPerformance(bt)).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/{indexOrPubkey}/attestations", handlers.ApiValidatorAttestations(bt)).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/{indexOrPubkey}/proposals", handlers.ApiValidatorProposals).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/{indexOrPubkey}/deposits", handlers.ApiValidatorDeposits).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/{indexOrPubkey}/attestationefficiency", handlers.ApiValidatorAttestationEfficiency(bt)).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/{indexOrPubkey}/attestationeffectiveness", handlers.ApiValidatorAttestationEffectiveness(bt)).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/stats/{index}", handlers.ApiValidatorDailyStats).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/eth1/{address}", handlers.ApiValidatorByEth1Address).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validator/withdrawalCredentials/{withdrawalCredentialsOrEth1address}", handlers.ApiWithdrawalCredentialsValidators).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validators/queue", handlers.ApiValidatorQueue).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/validators/proposalLuck", handlers.ApiProposalLuck).Methods("GET", "OPTIONS")

		apiV1Router.HandleFunc("/execution/gasnow", handlers.ApiEth1GasNowData).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/execution/block/{blockNumber}", handlers.ApiETH1ExecBlocks(bt)).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/execution/{addressIndexOrPubkey}/produced", handlers.ApiETH1AccountProducedBlocks(bt)).Methods("GET", "OPTIONS")

		apiV1Router.HandleFunc("/execution/address/{address}", handlers.ApiEth1Address(bt)).Methods("GET", "OPTIONS")
		apiV1Router.HandleFunc("/execution/address/{address}/erc20tokens", handlers.ApiEth1AddressERC20Tokens(bt)).Methods("GET", "OPTIONS")

		apiV1Router.HandleFunc("/ens/lookup/{domain}", handlers.ResolveEnsDomain).Methods("GET", "OPTIONS")
		apiV1Router.Use(utils.CORSMiddleware)

		
		router.HandleFunc("/api/healthz", handlers.ApiHealthz).Methods("GET", "HEAD")
		router.HandleFunc("/api/healthz-loadbalancer", handlers.ApiHealthzLoadbalancer).Methods("GET", "HEAD")

		logrus.Infof("initializing prices")
		price.Init(utils.Config.Chain.ClConfig.DepositChainID, utils.Config.Eth1ErigonEndpoint, utils.Config.Frontend.ClCurrency, utils.Config.Frontend.ElCurrency)

		logrus.Infof("prices initialized")

		if cfg.Frontend.SessionSecret == "" {
			logrus.Fatal("session secret is empty, please provide a secure random string.")
			return
		}

		utils.InitSessionStore(cfg.Frontend.SessionSecret)

		if !utils.Config.Frontend.OnlyAPI {
			if utils.Config.Frontend.SiteDomain == "" {
				utils.Config.Frontend.SiteDomain = "beaconcha.in"
			}

			router.HandleFunc("/", handlers.Index).Methods("GET")
			router.HandleFunc("/latestState", handlers.LatestState).Methods("GET")
			router.HandleFunc("/launchMetrics", handlers.SlotVizMetrics).Methods("GET")
			router.HandleFunc("/index/data", handlers.IndexPageData).Methods("GET")
			router.HandleFunc("/slot/{slotOrHash}", handlers.Slot(bt, rpcClient)).Methods("GET")
			router.HandleFunc("/slot/{slotOrHash}/deposits", handlers.SlotDepositData).Methods("GET")
			router.HandleFunc("/slot/{slotOrHash}/votes", handlers.SlotVoteData).Methods("GET")
			router.HandleFunc("/slot/{slot}/attestations", handlers.SlotAttestationsData).Methods("GET")
			router.HandleFunc("/slot/{slot}/withdrawals", handlers.SlotWithdrawalData).Methods("GET")
			router.HandleFunc("/slot/{slot}/blsChange", handlers.SlotBlsChangeData).Methods("GET")
			router.HandleFunc("/slots/finder", handlers.SlotFinder).Methods("GET")
			router.HandleFunc("/slots", handlers.Slots).Methods("GET")
			router.HandleFunc("/slots/data", handlers.SlotsData).Methods("GET")
			router.HandleFunc("/blocks", handlers.Eth1Blocks).Methods("GET")
			router.HandleFunc("/blocks/data", handlers.Eth1BlocksData(bt)).Methods("GET")
			router.HandleFunc("/blocks/highest", handlers.Eth1BlocksHighest).Methods("GET")
			router.HandleFunc("/address/{address}", handlers.Eth1Address(bt, rpcClient)).Methods("GET")
			router.HandleFunc("/address/{address}/blocks", handlers.Eth1AddressBlocksMined(bt)).Methods("GET")
			router.HandleFunc("/address/{address}/uncles", handlers.Eth1AddressUnclesMined(bt)).Methods("GET")
			router.HandleFunc("/address/{address}/withdrawals", handlers.Eth1AddressWithdrawals()).Methods("GET")
			router.HandleFunc("/address/{address}/transactions", handlers.Eth1AddressTransactions(bt)).Methods("GET")
			router.HandleFunc("/address/{address}/internalTxns", handlers.Eth1AddressInternalTransactions(bt)).Methods("GET")
			router.HandleFunc("/address/{address}/blobTxns", handlers.Eth1AddressBlobTransactions(bt)).Methods("GET")
			router.HandleFunc("/address/{address}/erc20", handlers.Eth1AddressErc20Transactions(bt)).Methods("GET")
			router.HandleFunc("/address/{address}/erc721", handlers.Eth1AddressErc721Transactions(bt)).Methods("GET")
			router.HandleFunc("/address/{address}/erc1155", handlers.Eth1AddressErc1155Transactions(bt)).Methods("GET")
			router.HandleFunc("/token/{token}", handlers.Eth1Token(bt)).Methods("GET")
			router.HandleFunc("/token/{token}/transfers", handlers.Eth1TokenTransfers(bt)).Methods("GET")
			router.HandleFunc("/transactions", handlers.Eth1Transactions(bt)).Methods("GET")
			router.HandleFunc("/block/{block}", handlers.Eth1Block(bt, rpcClient)).Methods("GET")
			router.HandleFunc("/block/{block}/rank/{rank}", handlers.Eth1Block(bt, rpcClient)).Methods("GET")
			router.HandleFunc("/block/{block}/transactions", handlers.BlockTransactionsData(bt, rpcClient)).Methods("GET")
			router.HandleFunc("/tx/{hash}", handlers.Eth1TransactionTx(rpcClient, bt)).Methods("GET")
			router.HandleFunc("/tx/{hash}/data", handlers.Eth1TransactionTxData(rpcClient, bt)).Methods("GET")
			router.HandleFunc("/mempool", handlers.MempoolView).Methods("GET")
			router.HandleFunc("/gasnow", handlers.GasNow(bt)).Methods("GET")
			router.HandleFunc("/gasnow/data", handlers.GasNowData).Methods("GET")

			router.HandleFunc("/vis", handlers.Vis).Methods("GET")
			router.HandleFunc("/vis/blocks", handlers.VisBlocks).Methods("GET")
			router.HandleFunc("/vis/votes", handlers.VisVotes).Methods("GET")
			router.HandleFunc("/epoch/{epoch}", handlers.Epoch).Methods("GET")
			router.HandleFunc("/epochs", handlers.Epochs).Methods("GET")
			router.HandleFunc("/epochs/data", handlers.EpochsData).Methods("GET")

			router.HandleFunc("/validator/{index}", handlers.Validator(bt)).Methods("GET")
			router.HandleFunc("/validator/{index}/proposedblocks", handlers.ValidatorProposedBlocks).Methods("GET")
			router.HandleFunc("/validator/{index}/attestations", handlers.ValidatorAttestations(bt)).Methods("GET")
			router.HandleFunc("/validator/{index}/withdrawals", handlers.ValidatorWithdrawals).Methods("GET")
			router.HandleFunc("/validator/{index}/sync", handlers.ValidatorSync(bt)).Methods("GET")
			router.HandleFunc("/validator/{index}/history", handlers.ValidatorHistory(bt)).Methods("GET")
			router.HandleFunc("/validator/{pubkey}/deposits", handlers.ValidatorDeposits(bt, )).Methods("GET")
			router.HandleFunc("/validator/{index}/slashings", handlers.ValidatorSlashings).Methods("GET")
			router.HandleFunc("/validator/{index}/effectiveness", handlers.ValidatorAttestationInclusionEffectiveness(bt)).Methods("GET")
			router.HandleFunc("/validator/{pubkey}/name", handlers.SaveValidatorName(bt)).Methods("POST")
			router.HandleFunc("/validator/{index}/stats", handlers.ValidatorStatsTable).Methods("GET")
			router.HandleFunc("/validators", handlers.Validators).Methods("GET")
			router.HandleFunc("/validators/data", handlers.ValidatorsData(bt)).Methods("GET")
			router.HandleFunc("/validators/slashings", handlers.ValidatorsSlashings).Methods("GET")
			router.HandleFunc("/validators/slashings/data", handlers.ValidatorsSlashingsData).Methods("GET")
			router.HandleFunc("/validators/leaderboard", handlers.ValidatorsLeaderboard).Methods("GET")
			router.HandleFunc("/validators/leaderboard/data", handlers.ValidatorsLeaderboardData).Methods("GET")
			router.HandleFunc("/validators/withdrawals", handlers.Withdrawals).Methods("GET")
			router.HandleFunc("/validators/withdrawals/data", handlers.WithdrawalsData(bt)).Methods("GET")
			router.HandleFunc("/validators/withdrawals/bls", handlers.BLSChangeData(bt)).Methods("GET")
			router.HandleFunc("/validators/deposits", handlers.Deposits).Methods("GET")
			router.HandleFunc("/validators/initiated-deposits", handlers.Eth1Deposits).Methods("GET") // deprecated, will redirect to /validators/deposits
			router.HandleFunc("/validators/initiated-deposits/data", handlers.Eth1DepositsData).Methods("GET")
			router.HandleFunc("/validators/deposit-leaderboard", handlers.Eth1DepositsLeaderboard).Methods("GET")
			router.HandleFunc("/validators/deposit-leaderboard/data", handlers.Eth1DepositsLeaderboardData).Methods("GET")
			router.HandleFunc("/validators/included-deposits", handlers.Eth2Deposits).Methods("GET") // deprecated, will redirect to /validators/deposits
			router.HandleFunc("/validators/included-deposits/data", handlers.Eth2DepositsData).Methods("GET")

			router.HandleFunc("/search", handlers.Search).Methods("POST")
			router.HandleFunc("/search/{type}/{search}", handlers.SearchAhead(bt)).Methods("GET")

			router.HandleFunc("/tables/{tableId}/state", handlers.GetDataTableStateChanges).Methods("GET")
			router.HandleFunc("/tables/{tableId}/state", handlers.SetDataTableStateChanges).Methods("PUT")
			router.HandleFunc("/ens/{search}", handlers.EnsSearch).Methods("GET")

			// confirming the email update should not require auth
			router.HandleFunc("/settings/email/{hash}", handlers.UserConfirmUpdateEmail).Methods("GET")

			router.HandleFunc("/monitoring/{module}", handlers.Monitoring).Methods("GET", "OPTIONS")

			if utils.Config.Frontend.Debug {
				// serve files from local directory when debugging, instead of from go embed file
				templatesHandler := http.FileServer(http.Dir("templates"))
				router.PathPrefix("/templates").Handler(http.StripPrefix("/templates/", templatesHandler))

				cssHandler := http.FileServer(http.Dir("static/css"))
				router.PathPrefix("/css").Handler(http.StripPrefix("/css/", cssHandler))

				jsHandler := http.FileServer(http.Dir("static/js"))
				router.PathPrefix("/js").Handler(http.StripPrefix("/js/", jsHandler))
			}
			fileSys := http.FS(static.Files)
			router.PathPrefix("/").Handler(handlers.CustomFileServer(http.FileServer(fileSys), fileSys, handlers.NotFound))

		}

		if utils.Config.Metrics.Enabled {
			router.Use(metrics.Middleware)
		}

		ratelimit.Init()
		router.Use(ratelimit.HttpMiddleware)

		n := negroni.New(negroni.NewRecovery())
		n.Use(gzip.Gzip(gzip.DefaultCompression))

		pa := &proxyaddr.ProxyAddr{}
		pa.Init(proxyaddr.CIDRLoopback)
		n.Use(pa)

		n.UseHandler(utils.SessionStore.SCS.LoadAndSave(router))

		if utils.Config.Frontend.HttpWriteTimeout == 0 {
			utils.Config.Frontend.HttpWriteTimeout = time.Second * 15
		}
		if utils.Config.Frontend.HttpReadTimeout == 0 {
			utils.Config.Frontend.HttpReadTimeout = time.Second * 15
		}
		if utils.Config.Frontend.HttpIdleTimeout == 0 {
			utils.Config.Frontend.HttpIdleTimeout = time.Minute
		}
		frontendHttpServer = &http.Server{
			Addr:         cfg.Frontend.Server.Host + ":" + cfg.Frontend.Server.Port,
			WriteTimeout: utils.Config.Frontend.HttpWriteTimeout,
			ReadTimeout:  utils.Config.Frontend.HttpReadTimeout,
			IdleTimeout:  utils.Config.Frontend.HttpIdleTimeout,
			Handler:      n,
		}

		logrus.Printf("http server listening on %v", frontendHttpServer.Addr)
		go func() {
			if err := frontendHttpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logrus.WithError(err).Fatal("Error serving frontend")
			}
		}()
	}

	metrics.StartMetrics(utils.Config.Metrics.Enabled, utils.Config.Metrics.Address)

	utils.WaitForCtrlC()

	if frontendHttpServer != nil {
		logrus.Infof("shutting down frontendHttpServer")
		ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
		defer cancel()
		if err := frontendHttpServer.Shutdown(ctx); err != nil {
			logrus.WithError(err).Error("error shutting down frontend server")
		}
	}

	logrus.Println("exiting...")
}
