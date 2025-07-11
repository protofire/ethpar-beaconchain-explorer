package exporter

import (
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/config"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	//"github.com/protofire/ethpar-beaconchain-explorer/utils"

	"github.com/sirupsen/logrus"
)

// Start will start the export of data from rpc into the database
func Start(
	client consensus.ConsensusClient, 
	bt *db.Bigtable,
	sqlDb *db.Postgres,
	cfg *config.Eth2IndexerConfig,
	chainParams *config.NetworkConfig,
) {
	go networkLivenessUpdater(client, sqlDb, chainParams)
	go eth1DepositsExporter()
	go genesisDepositsExporter(client, sqlDb)
	go checkSubscriptions()
	go syncCommitteesExporter(client)
	go syncCommitteesCountExporter()
	if cfg.Indexing.SsvExporter {
		go ssvExporter()
	}
	if cfg.Indexing.RocketPoolExporter {
		go rocketpoolExporter()
	}

	if cfg.Indexing.PubKeyTagsExporter {
		go UpdatePubkeyTag()
	}

	if cfg.Indexing.MevBoostRelayExporter {
		go mevBoostRelaysExporter()
	}
	// wait until the beacon-node is available
	for {
		head, err := client.GetChainHead()
		if err == nil {
			log.Infof("beacon node is available with head slot: %v", head.HeadSlot)
			break
		}
		log.Errorf("beacon-node seems to be unavailable: %v", err)
		time.Sleep(time.Second * 10)
	}

	firstRun := true

	minWaitTimeBetweenRuns := time.Second * time.Duration(chainParams.Time.SecondsPerSlot)
	for {
		start := time.Now()
		err := RunSlotExporter(client, firstRun, bt)
		if err != nil {
			logrus.Errorf("error during slot export run: %v", err)
		} else if err == nil && firstRun {
			firstRun = false
		}

		logrus.Info("update run completed")
		elapsed := time.Since(start)
		if elapsed < minWaitTimeBetweenRuns {
			time.Sleep(minWaitTimeBetweenRuns - elapsed)
		}

		services.ReportStatus(true, "slotExporter", "Running", nil)
	}
}

// networkLivenessUpdater periodically polls the beacon node's chain head and stores
// liveness-related metadata (head epoch, finalized epoch, justified epoch, and previous
// justified epoch) into the PostgreSQL database.
//
// It runs in a loop synchronized to the beacon chain slot duration and ensures that:
//   - The beacon node is synced before recording data.
//   - Duplicate entries for the same epoch are avoided.
//
// This function is intended to track network progress and consensus stability over time.
func networkLivenessUpdater(client consensus.ConsensusClient, sqlDb *db.Postgres, chainParams *config.NetworkConfig) {
	prevHeadEpoch, err := sqlDb.GetPrevHeadEpoch()
	if err != nil {
		log.Fatalf("getting previous head epoch from db error: %v", err)
	}

	epochDuration := time.Second * time.Duration(chainParams.Time.SecondsPerSlot*chainParams.Time.SlotsPerEpoch)
	slotDuration := time.Second * time.Duration(chainParams.Time.SecondsPerSlot)

	for {
		head, err := client.GetChainHead()
		if err != nil {
			log.Errorf("error getting chainhead when exporting networkliveness: %v", err)
			time.Sleep(slotDuration)
			continue
		}

		if prevHeadEpoch == head.HeadEpoch {
			time.Sleep(slotDuration)
			continue
		}

		// wait for node to be synced
		if time.Now().Add(-epochDuration).After(utils.EpochToTime(head.HeadEpoch)) {
			time.Sleep(slotDuration)
			continue
		}

		if err := sqlDb.InsertNetworkLivenessSnapshot(
			head.HeadEpoch, 
			head.FinalizedEpoch, 
			head.JustifiedEpoch, 
			head.PreviousJustifiedEpoch,
		); err != nil {
			log.Errorf("error saving networkliveness: %v", err)
		} else {
			log.Infof("updated networkliveness for epoch %v", head.HeadEpoch)
			prevHeadEpoch = head.HeadEpoch
		}

		time.Sleep(slotDuration)
	}
}

// genesisDepositsExporter exports the initial deposit records for genesis validators
// into the blocks_deposits table. This is a one-time operation that executes
// after the chain has started (epoch > 0) and no genesis deposits have yet been stored.
//
// It retrieves all validators active at slot 0 via the Beacon node RPC,
// inserts them into the database, and attempts to associate each validator
// with a corresponding ETH1 deposit signature if available.
//
// Genesis deposits are used to establish the initial validator set and provide
// context for historical and auditing views in the explorer.
func genesisDepositsExporter(client consensus.ConsensusClient, sqlDb *db.Postgres) {
	for {
		// check if the beaconchain has started
		latestEpoch, err := sqlDb.GetLatestEpoch()
		if err != nil {
			log.Errorf("error retrieving latest epoch from the database: %v", err)
			time.Sleep(time.Second * 10)
			continue
		}

		if latestEpoch == 0 {
			time.Sleep(time.Minute)
			continue
		}

		// check if genesis-deposits have already been exported
		genesisDepositsCount, err := sqlDb.GetGenesisDepositsCount()
		if err != nil {
			log.Errorf("error retrieving genesis-deposits-count when exporting genesis-deposits: %v", err)
			time.Sleep(time.Minute)
			continue
		}

		// if genesis-deposits have already been exported exit this go-routine
		if genesisDepositsCount > 0 {
			return
		}

		genesisValidators, err := client.GetValidatorState(0)
		if err != nil {
			log.Errorf("error retrieving genesis validator data for genesis-epoch when exporting genesis-deposits: %v", err)
			time.Sleep(time.Minute)
			continue
		}

		tx, err := db.WriterDb.Beginx()
		if err != nil {
			logger.Errorf("error beginning db-tx when exporting genesis-deposits: %v", err)
			time.Sleep(time.Minute)
			continue
		}

		logger.Infof("exporting deposit data for %v genesis validators", len(genesisValidators.Data))
		for i, validator := range genesisValidators.Data {
			if i%1000 == 0 {
				logger.Infof("exporting deposit data for genesis validator %v (%v/%v)", validator.Index, i, len(genesisValidators.Data))
			}
			_, err = tx.Exec(`INSERT INTO blocks_deposits (block_slot, block_root, block_index, publickey, withdrawalcredentials, amount, signature)
			VALUES (0, '\x01', $1, $2, $3, $4, $5) ON CONFLICT DO NOTHING`,
				validator.Index, utils.MustParseHex(validator.Validator.Pubkey), utils.MustParseHex(validator.Validator.WithdrawalCredentials), validator.Balance, []byte{0x0},
			)
			if err != nil {
				tx.Rollback()
				logger.Errorf("error exporting genesis-deposits: %v", err)
				time.Sleep(time.Minute)
				continue
			}
		}

		// hydrate the eth1 deposit signature for all genesis validators that have a corresponding eth1 deposit
		_, err = tx.Exec(`
			UPDATE blocks_deposits 
			SET signature = a.signature 
			FROM (
				SELECT DISTINCT ON(publickey) publickey, signature 
				FROM eth1_deposits 
				WHERE valid_signature = true) AS a 
			WHERE block_slot = 0 AND blocks_deposits.publickey = a.publickey AND blocks_deposits.signature = '\x'`)
		if err != nil {
			tx.Rollback()
			logger.Errorf("error hydrating eth1 data into genesis-deposits: %v", err)
			time.Sleep(time.Minute)
			continue
		}

		// update deposits-count
		_, err = tx.Exec("UPDATE blocks SET depositscount = $1 WHERE slot = 0", len(genesisValidators.Data))
		if err != nil {
			tx.Rollback()
			logger.Errorf("error updating deposit count for the genesis slot: %v", err)
			time.Sleep(time.Minute)
			continue
		}

		err = tx.Commit()
		if err != nil {
			tx.Rollback()
			logger.Errorf("error committing db-tx when exporting genesis-deposits: %v", err)
			time.Sleep(time.Minute)
			continue
		}

		logger.Infof("exported genesis-deposits for %v genesis-validators", len(genesisValidators.Data))
		return
	}
}



func genesisDepositsExporter(client consensus.ConsensusClient, sqlDb *db.Postgres) {
	for {
		latestEpoch, err := sqlDb.GetLatestEpoch()
		if err != nil {
			log.Errorf("error retrieving latest epoch: %v", err)
			time.Sleep(10 * time.Second)
			continue
		}

		if latestEpoch == 0 {
			time.Sleep(time.Minute)
			continue
		}

		count, err := sqlDb.GetGenesisDepositsCount()
		if err != nil {
			log.Errorf("error retrieving genesis deposits count: %v", err)
			time.Sleep(time.Minute)
			continue
		}
		if count > 0 {
			return
		}

		genesisValidators, err := client.GetValidatorState(0)
		if err != nil {
			log.Errorf("error fetching genesis validators: %v", err)
			time.Sleep(time.Minute)
			continue
		}

		log.Infof("exporting genesis deposits for %d validators", len(genesisValidators.Data))

		if err := sqlDb.ExportGenesisDeposits(context.Background(), genesisValidators.Data); err != nil {
			log.Errorf("error exporting genesis deposits: %v", err)
			time.Sleep(time.Minute)
			continue
		}

		log.Infof("exported genesis deposits for %d validators", len(genesisValidators.Data))
		return
	}
}
