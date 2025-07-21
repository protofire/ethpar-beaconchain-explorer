package eth2indexer

import (
	"context"
	"time"
)

// genesisDepositsExporter exports initial validator deposit records from epoch 0.
// It waits until the beacon chain has started (latestEpoch > 0), then retrieves
// validator state from the beacon node and stores it into the DB.
// This function is safe to run multiple times and exits if data is already present.
func genesisDepositsExporter(ctx context.Context, p *IndexingParams) {
	p.Log.Info("genesisDepositsExporter started")

	for {
		select {
		case <-ctx.Done():
			p.Log.Info("genesisDepositsExporter cancelled")
			return
		default:
			// check if the beacon chain has started
			latestEpoch, err := p.Database.GetLatestEpoch()
			if err != nil {
				p.Log.Errorf("error retrieving latest epoch from the database: %v", err)
				wait(ctx, 10*time.Second)
				continue
			}

			if latestEpoch == 0 {
				wait(ctx, time.Minute)
				continue
			}

			// check if already done
			count, err := p.Database.GetGenesisDepositsCount()
			if err != nil {
				p.Log.Errorf("error retrieving genesis-deposits-count: %v", err)
				wait(ctx, time.Minute)
				continue
			}
			if count > 0 {
				p.Log.Info("genesis deposits already exported; skipping")
				return
			}

			validators, err := p.ConsClient.GetValidatorState(0)
			if err != nil {
				p.Log.Errorf("error retrieving genesis validator state: %v", err)
				wait(ctx, time.Minute)
				continue
			}

			if err := p.Database.SaveGenesisDeposits(validators); err != nil {
				p.Log.Errorf("error saving genesis deposits: %v", err)
				wait(ctx, time.Minute)
				continue
			}

			p.Log.Infof("exported %d genesis deposits", len(validators.Data))
			return
		}
	}
}

func wait(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}