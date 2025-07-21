package eth2indexer

import (
	"context"
	"fmt"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
)

// syncCommitteesCountExporter runs a periodic background task that calculates
// and stores sync committee participation statistics. It exits cleanly when
// the provided context is cancelled.
func syncCommitteesCountExporter(ctx context.Context, params *IndexingParams) {
	if !params.Config.Indexing.SyncCommitteesCountExporter {
		return
	}

	ticker := time.NewTicker(12 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			params.Log.Info("syncCommitteesCountExporter shutting down")
			return
		case <-ticker.C:
			if err := exportSyncCommitteesCount(params); err != nil {
				params.Log.Errorf("error exporting sync committees count per validator: %v", err)
			}
		}
	}
}


// exportSyncCommitteesCount calculates and stores the cumulative sync committee
// participation count per validator for all periods from the Altair fork up to
// the latest finalized epoch. It resumes from the last exported period, ensuring
// continuity and avoiding duplicate writes.
func exportSyncCommitteesCount(params *IndexingParams) error {
	rowCount, err := params.Database.CountSyncCommitteePeriods()
	if err != nil {
		return err
	}

	latestFinalizedEpoch, err := params.Database.GetLatestFinalizedEpoch()
	if err != nil {
		params.Log.Errorf("error retrieving latest exported finalized epoch from the database: %v", err)
	}

	currentPeriod := utils.SyncPeriodOfEpoch(latestFinalizedEpoch)
	firstPeriod := utils.SyncPeriodOfEpoch(params.ChainParams.Forks.AltairForkEpoch)

	var countSoFar float64
	if rowCount > 0 {
		maxPeriod, err := params.Database.GetLatestSyncCommitteePeriod()
		if err != nil {
			return err
		}

		if firstPeriod <= maxPeriod {
			// continue where we left off last time
			firstPeriod = maxPeriod + 1
		}

		countSoFar, err = params.Database.GetSyncCommitteeValidatorCountAtPeriod(maxPeriod)
		if err != nil {
			return err
		}
	}

	return processSyncCommitteePeriods(params, firstPeriod, currentPeriod, countSoFar)
}

// processSyncCommitteePeriods iterates through sync committee periods and stores
// cumulative participation metrics for each, updating countSoFar incrementally.
func processSyncCommitteePeriods(
	params *IndexingParams,
	firstPeriod, currentPeriod uint64,
	initialCount float64,
) error {
	countSoFar := initialCount

	for period := firstPeriod; period <= currentPeriod; period++ {
		start := time.Now()
		newCount, err := computeAndStoreSyncCommitteeParticipation(params, period, countSoFar)
		if err != nil {
			return fmt.Errorf("error exporting sync-committee count at period %v: %w", period, err)
		}

		countSoFar = newCount

		params.Log.WithFields(logger.Fields{
			"period":   period,
			"duration": time.Since(start),
		}).Infof("exported sync_committees_count_per_validator")
	}

	return nil
}

// computeAndStoreSyncCommitteeParticipation calculates the cumulative validator
// sync committee participation at a given period and stores it in the database.
func computeAndStoreSyncCommitteeParticipation(params *IndexingParams, period uint64, countSoFar float64) (float64, error) {
	count := 0.0
	if period > 0 {
		e := utils.FirstEpochOfSyncPeriod(period - 1)
		totalValidatorsCount, err := params.Database.GetValidatorCountForEpoch(e)
		if err != nil {
			return 0, fmt.Errorf("error retrieving validatorscount for epoch %v: %v", e, err)
		}
		count = countSoFar + (float64(params.ChainParams.SyncCommittee.SyncCommitteeSize) / float64(totalValidatorsCount))
	}

	if err := params.Database.UpsertSyncCommitteeValidatorCount(period, count); err != nil {
		return 0, err
	}

	return count, nil
}
