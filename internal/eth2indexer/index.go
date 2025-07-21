package eth2indexer

import (
	"context"
	"sync"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/config"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
)

type IndexingParams struct {
	ConsClient  consensus.ConsensusClient
	ExecClient  execution.ExecutionClient
	Bigtable    *db.Bigtable
	Database    *db.Postgres
	ChainParams *config.NetworkConfig
	Config      *config.Eth2IndexerConfig
	Log         *logger.Logger
}

var exporters = []func(context.Context, *IndexingParams){
	networkLivenessUpdater,
	genesisDepositsExporter,
	// eth1DepositsExporter,
	syncCommitteesExporter,
	syncCommitteesCountExporter,
}

// Start will start the export of data from rpc into the database
func Start(p *IndexingParams) {
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	go utils.WaitForCtrlCAndCancelGoRoutines(cancel)

	// Wait for beacon node to become available
	waitForBeaconNode(ctx, p)

	var wg sync.WaitGroup

	for _, exporter := range exporters {
		wg.Add(1)
		go func(fn func(context.Context, *IndexingParams)) {
			defer wg.Done()
			fn(ctx, p)
		}(exporter)
	}

	wg.Wait()

	startSlotIndexingLoop(ctx, p)
}

func waitForBeaconNode(ctx context.Context, p *IndexingParams) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.Log.Info("startup cancelled before beacon node became available")
			return
		case <-ticker.C:
			head, err := p.ConsClient.GetChainHead()
			if err == nil {
				p.Log.Infof("beacon node is available with head slot: %v", head.HeadSlot)
				return
			}
			p.Log.Errorf("beacon-node seems to be unavailable: %v", err)
		}
	}
}