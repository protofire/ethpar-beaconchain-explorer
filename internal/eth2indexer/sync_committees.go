package eth2indexer

import (
	"fmt"
	"strconv"
	"time"
	"context"

	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
)

type SyncCommittee struct {
	Period         uint64 `json:"period"`
	ValidatorIndex uint64 `json:"validatorindex"`
	CommitteeIndex uint64 `json:"committeeindex"`
}

// syncCommitteesExporter continuously exports sync committee data from the Beacon node
// to the local database (`sync_committees` table).
//
// It runs in an infinite loop, checking for missing sync committee periods starting from
// the Altair fork, fetching assignments (validator indices and committee positions),
// and storing them with deduplication logic.
//
// The exporter only runs if enabled via configuration (`Config.Indexing.SyncCommitteesExporter`).
// Errors during export are logged but do not stop the loop.
//
// Note: The loop sleeps between iterations based on the chain’s slot duration.
//
// Returns: nothing (runs indefinitely with side effects: DB writes and logs).
func syncCommitteesExporter(ctx context.Context, p *IndexingParams) {
	if !p.Config.Indexing.SyncCommitteesExporter {
		return
	}
	ticker := time.NewTicker(time.Second * time.Duration(p.ChainParams.Time.SecondsPerSlot))
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			p.Log.Info("syncCommitteesExporter stopped gracefully")
			return
		case <-ticker.C:
			t0 := time.Now()
			err := exportSyncCommittees(p)
			if err != nil {
				p.Log.WithFields(logger.Fields{
					"duration":  time.Since(t0),
					"submodule": "Sync Committees Exporter",
				}).Errorf("error exporting sync_committees: %v", err)
			}
		}
	}
}

func exportSyncCommittees(params *IndexingParams) error {
	
	existingPeriods, err := params.Database.GetExportedSyncCommitteePeriods()
	if err != nil {
		return err
	}

	existing := make(map[uint64]bool, len(existingPeriods))
	for _, period := range existingPeriods {
		existing[period] = true
	}

	currEpoch := services.LatestFinalizedEpoch()
	if currEpoch > 0 { 
		currEpoch -= 1 // guard against underflows
	}

	firstPeriod := utils.SyncPeriodOfEpoch(params.ChainParams.Forks.AltairForkEpoch)
	lastPeriod := utils.SyncPeriodOfEpoch(uint64(currEpoch)) + 1 // allow peeking ahead
	
	var exportedCount int
	for period := firstPeriod; period <= lastPeriod; period++ {
		if existing[period] {
			continue
		}

		t0 := time.Now()
		if err := exportSyncCommitteeAtPeriod(params, period); err != nil {
			return fmt.Errorf("error exporting sync-committee at period %v: %w", period, err)
		}

		params.Log.WithFields(logger.Fields{
			"period":   period,
			"epoch":    utils.FirstEpochOfSyncPeriod(period),
			"duration": time.Since(t0),
		}).Info("exported sync_committee")

		exportedCount++
	}

	if exportedCount == 0 {
		params.Log.Debug("no new sync committees to export")
	}

	return nil
}

func exportSyncCommitteeAtPeriod(params *IndexingParams, period uint64) error {
	committees, err := getSyncCommitteeAtPeriod(params, period)
	if err != nil {
		return err
	}

	entries := make([][3]uint64, len(committees))
	for i, c := range committees {
		entries[i] = [3]uint64{c.Period, c.ValidatorIndex, c.CommitteeIndex}
	}

	return params.Database.InsertSyncCommittees(context.Background(), entries)
}

// getSyncCommitteeAtPeriod retrieves the sync committee assignments for a given period
// from the Beacon node and returns them as a slice of SyncCommittee structs.
//
// Inputs:
//   - params: Indexing parameters including chain constants and consensus client
//   - period: Sync committee period to fetch
//
// Returns:
//   - []SyncCommittee: List of validator assignments with position and index
//   - error: If retrieval or parsing fails
func getSyncCommitteeAtPeriod(params *IndexingParams, period uint64) ([]SyncCommittee, error) {
	timeCfg := params.ChainParams.Time
	forkEpoch := params.ChainParams.Forks.AltairForkEpoch

	// Determine stateID (slot) to fetch committee from
	var stateID uint64
	if period > 0 {
		stateID = utils.FirstEpochOfSyncPeriod(period-1) * timeCfg.SlotsPerEpoch
	} else {
		stateID = forkEpoch * timeCfg.SlotsPerEpoch
	}

	// Make sure we never query before Altair fork
	if stateID/timeCfg.SlotsPerEpoch <= forkEpoch {
		stateID = forkEpoch * timeCfg.SlotsPerEpoch
	}

	epoch := utils.FirstEpochOfSyncPeriod(period)
	firstEpoch := epoch
	lastEpoch := firstEpoch + params.ChainParams.SyncCommittee.EpochsPerSyncCommitteePeriod - 1

	params.Log.Infof("exporting sync committee assignments for period %v (epoch %v to %v)", period, firstEpoch, lastEpoch)

	committeeResp, err := params.ConsClient.GetSyncCommittee(fmt.Sprintf("%d", stateID), epoch)
	if err != nil {
		return nil, fmt.Errorf("failed to get sync committee for period %d: %w", period, err)
	}

	// Preallocate exact size, no need for append
	committee := make([]SyncCommittee, len(committeeResp.Validators))
	for i, idxStr := range committeeResp.Validators {
		idx, err := strconv.ParseUint(idxStr, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid validator index at position %d: %w", i, err)
		}
		committee[i] = SyncCommittee{
			Period:         period,
			ValidatorIndex: idx,
			CommitteeIndex: uint64(i),
		}
	}

	return committee, nil
}