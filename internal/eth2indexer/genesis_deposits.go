package eth2indexer

import (
	"time"
)

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
func genesisDepositsExporter(p *IndexingParams) {
	for {
		// check if the beaconchain has started
		latestEpoch, err := p.Database.GetLatestEpoch()
		if err != nil {
			p.Log.Errorf("error retrieving latest epoch from the database: %v", err)
			time.Sleep(time.Second * 10)
			continue
		}

		if latestEpoch == 0 {
			time.Sleep(time.Minute)
			continue
		}

		// check if genesis-deposits have already been exported
		genesisDepositsCount, err := p.Database.GetGenesisDepositsCount()
		if err != nil {
			p.Log.Errorf("error retrieving genesis-deposits-count when exporting genesis-deposits: %v", err)
			time.Sleep(time.Minute)
			continue
		}

		// if genesis-deposits have already been exported exit this go-routine
		if genesisDepositsCount > 0 {
			return
		}

		genesisValidators, err := p.ConsClient.GetValidatorState(0)
		if err != nil {
			p.Log.Errorf("error retrieving genesis validator data for genesis-epoch when exporting genesis-deposits: %v", err)
			time.Sleep(time.Minute)
			continue
		}

		if err := p.Database.SaveGenesisDeposits(genesisValidators); err != nil {
			time.Sleep(time.Minute)
			continue
		}
		return
	}
}