package main

import (
	"flag"
	"fmt"
	"math/big"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/exporter"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/lighthouse"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/teku"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
	"github.com/protofire/ethpar-beaconchain-explorer/version"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

func main() {

	configPath := flag.String("config", "", "Path to the config file, if empty string defaults will be used")

	start := flag.Uint64("start", 1, "Start epoch")
	end := flag.Uint64("end", 1, "End epoch")
	concurrency := flag.Int("concurrency", 1, "Number of parallel epoch exports")

	versionFlag := flag.Bool("version", false, "Show version and exit")
	flag.Parse()

	if *versionFlag {
		fmt.Println(version.Version)
		return
	}

	if *start == 1 && *end == 1 {
		monitor(*configPath)
	}

	cfg := &types.Config{}
	err := utils.ReadConfig(cfg, *configPath)
	if err != nil {
		logrus.Fatalf("error reading config file: %v", err)
	}
	utils.Config = cfg

	bt, err := db.InitBigtable(utils.Config.Bigtable.Project, utils.Config.Bigtable.Instance, fmt.Sprintf("%d", utils.Config.Chain.ClConfig.DepositChainID), utils.Config.RedisCacheEndpoint)
	if err != nil {
		logrus.Fatalf("error connecting to bigtable: %v", err)
	}
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
	defer db.FrontendReaderDB.Close()
	defer db.FrontendWriterDB.Close()

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

	gOuter := errgroup.Group{}
	gOuter.SetLimit(*concurrency)
	for epoch := *start; epoch <= *end; epoch++ {
		epoch := epoch
		gOuter.Go(func() error {
			logrus.Infof("exporting epoch %v", epoch)
			start := time.Now()

			startGetEpochData := time.Now()
			logrus.Printf("retrieving data for epoch %v", epoch)

			data, err := consClient.GetEpochData(epoch, false)
			if err != nil {
				logrus.Fatalf("error retrieving epoch data: %v", err)
			}
			logrus.WithFields(logrus.Fields{"duration": time.Since(startGetEpochData), "epoch": epoch}).Info("completed getting epoch-data")
			logrus.Printf("data for epoch %v retrieved, took %v", epoch, time.Since(start))

			if len(data.Validators) == 0 {
				logrus.Fatal("error retrieving epoch data: no validators received for epoch")
			}

			// export epoch data to bigtable
			g := new(errgroup.Group)
			g.SetLimit(6)
			g.Go(func() error {
				err := db.BigtableClient.SaveValidatorBalances(epoch, data.Validators)
				if err != nil {
					return fmt.Errorf("error exporting validator balances to bigtable for epoch %v: %w", epoch, err)
				}
				return nil
			})
			g.Go(func() error {
				err := db.BigtableClient.SaveAttestationDuties(data.AttestationDuties)
				if err != nil {
					return fmt.Errorf("error exporting attestations to bigtable for epoch %v: %w", epoch, err)
				}
				return nil
			})
			g.Go(func() error {
				err := db.BigtableClient.SaveSyncComitteeDuties(data.SyncDuties)
				if err != nil {
					return fmt.Errorf("error exporting sync committee duties to bigtable for epoch %v: %w", epoch, err)
				}
				return nil
			})
			g.Go(func() error {
				err := db.BigtableClient.MigrateIncomeDataV1V2Schema(epoch)
				if err != nil {
					return fmt.Errorf("error migrating income data to v2 schema for epoch %v: %w", epoch, err)
				}
				return nil
			})

			err = g.Wait()
			if err != nil {
				return fmt.Errorf("error during bigtable export for epoch %v: %w", epoch, err)
			}
			logrus.WithFields(logrus.Fields{"duration": time.Since(start), "epoch": epoch}).Info("completed exporting epoch")
			return nil
		})
	}

	err = gOuter.Wait()
	if err != nil {
		logrus.Fatalf("error during bigtable export: %v", err)
	}

}

func monitor(configPath string) {
	cfg := &types.Config{}
	err := utils.ReadConfig(cfg, configPath)
	if err != nil {
		logrus.Fatalf("error reading config file: %v", err)
	}
	utils.Config = cfg

	bt, err := db.InitBigtable(utils.Config.Bigtable.Project, utils.Config.Bigtable.Instance, fmt.Sprintf("%d", utils.Config.Chain.ClConfig.DepositChainID), utils.Config.RedisCacheEndpoint)
	if err != nil {
		logrus.Fatalf("error connecting to bigtable: %v", err)
	}
	defer bt.Close()

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

	current := uint64(0)

	for ; ; time.Sleep(time.Second * 12) {
		head, err := consClient.GetChainHead()
		if err != nil {
			utils.LogFatal(err, "getting chain head from lighthouse in monitor error", 0)
		}

		logrus.Infof("current is %v, head is %v, finalized is %v", current, head.HeadEpoch, head.FinalizedEpoch)

		if current == head.HeadEpoch {
			continue
		}

		tx, err := db.WriterDb.Beginx()
		if err != nil {
			logrus.Errorf("error starting tx: %v", err)
			continue
		}

		for i := head.FinalizedEpoch; i <= head.HeadEpoch; i++ {
			logrus.Infof("exporting epoch %v", i)
			for slot := i * cfg.Chain.ClConfig.SlotsPerEpoch; i <= (i+1)*cfg.Chain.ClConfig.SlotsPerEpoch-1; i++ {
				err := exporter.ExportSlot(consClient, slot, false, tx)
				if err != nil {
					logrus.Errorf("error exporting slot: %v", err)
					tx.Rollback()
					continue
				}
			}
		}

		err = tx.Commit()
		if err != nil {
			logrus.Errorf("error committing tx: %v", err)
			continue
		}

		current = head.HeadEpoch
	}
}
