package eth2indexer

import (
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/config"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
)

type IndexingParams struct {
	ConsClient                  consensus.ConsensusClient
	ExecClient                  execution.ExecutionClient
	Bigtable                    *db.Bigtable
	Database                    *db.Postgres
	ChainParams                 *config.NetworkConfig
	Eth1DepositContractFirstBlock uint64
	HistoricalPrice             bool
	PubKeyTagsExporter          bool
	SyncCommitteesExporter      bool 
	SyncCommitteesCountExporter bool
	PendingQueueIndexer         bool
	SsvExporter                 bool
	RocketPoolExporter          bool
	MevBoostRelayExporter       bool  
	EnsTransformer struct {
		ValidRegistrarContracts []string
	}
	Log                         *logger.Logger
}

// Start will start the export of data from rpc into the database
func Start(p *IndexingParams) {
	go networkLivenessUpdater(p)
	go genesisDepositsExporter(p)
	go eth1DepositsExporter(p)
	// go checkSubscriptions()
	// go syncCommitteesExporter(client)
	// go syncCommitteesCountExporter()
	// if cfg.Indexing.SsvExporter {
	// 	go ssvExporter()
	// }
	// if cfg.Indexing.RocketPoolExporter {
	// 	go rocketpoolExporter()
	// }

	// if cfg.Indexing.PubKeyTagsExporter {
	// 	go UpdatePubkeyTag()
	// }

	// if cfg.Indexing.MevBoostRelayExporter {
	// 	go mevBoostRelaysExporter()
	// }
	// wait until the beacon-node is available
	for {
		head, err := p.ConsClient.GetChainHead()
		if err == nil {
			p.Log.Infof("beacon node is available with head slot: %v", head.HeadSlot)
			break
		}
		p.Log.Errorf("beacon-node seems to be unavailable: %v", err)
		time.Sleep(time.Second * 10)
	}

	firstRun := true

	minWaitTimeBetweenRuns := time.Second * time.Duration(p.ChainParams.Time.SecondsPerSlot)
	for {
		start := time.Now()
		err := runSlotExporter(p.ConsClient, firstRun, p.Bigtable)
		if err != nil {
			p.Log.Errorf("error during slot export run: %v", err)
		} else if err == nil && firstRun {
			firstRun = false
		}

		p.Log.Info("update run completed")
		elapsed := time.Since(start)
		if elapsed < minWaitTimeBetweenRuns {
			time.Sleep(minWaitTimeBetweenRuns - elapsed)
		}

		services.ReportStatus(true, "slotExporter", "Running", nil)
	}
}