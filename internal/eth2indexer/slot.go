package eth2indexer

import (
	"bytes"
	"database/sql"
	"fmt"
	"time"
	"context"

	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	"github.com/jmoiron/sqlx"
)

// startSlotIndexingLoop continuously indexes new slots as the chain progresses.
// It waits for new slots and triggers processing while maintaining a minimum interval.
func startSlotIndexingLoop(ctx context.Context, params *IndexingParams) {
	firstRun := true
	minWait := time.Second * time.Duration(params.ChainParams.Time.SecondsPerSlot)

	for {
		select {
		case <-ctx.Done():
			params.Log.Info("slotExporter shutting down")
			return
		default:
			start := time.Now()
			if err := processSlotIndexing(params, firstRun); err != nil {
				params.Log.Errorf("error during slot export: %v", err)
			} else if firstRun {
				firstRun = false
			}

			elapsed := time.Since(start)
			if elapsed < minWait {
				select {
				case <-time.After(minWait - elapsed):
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// processSlotIndexing performs a single slot indexing pass:
// - recovers missing slots on first run,
// - exports new slots up to the chain head,
// - finalizes non-finalized slots and updates metadata.
func processSlotIndexing(params *IndexingParams, firstRun bool) error {
	head, err := params.ConsClient.GetChainHead()
	if err != nil {
		return fmt.Errorf("error retrieving chain head: %w", err)
	}

	tx, err := params.Database.Db.Beginx()
	if err != nil {
		return fmt.Errorf("error starting transaction: %w", err)
	}

	committed := false
	defer func() {
		if !committed {
			if err := tx.Rollback(); err != nil && err != sql.ErrTxDone {
				params.Log.Errorf("rollback failed: %v", err)
			}
		}
	}()

	if firstRun {
		if err := recoverMissingSlots(params, tx, head); err != nil {
			return err
		}
	}

	var lastDbSlot uint64
	if err := tx.Get(&lastDbSlot, "SELECT slot FROM blocks ORDER BY slot DESC LIMIT 1"); err != nil {
		if err == sql.ErrNoRows {
			params.Log.Infof("database is empty, exporting genesis slot")
			if err := exportSlot(params, 0, utils.EpochOfSlot(0) == head.HeadEpoch, tx); err != nil {
				return fmt.Errorf("error exporting genesis slot: %w", err)
			}
			lastDbSlot = 0
		} else {
			return fmt.Errorf("error retrieving last db slot: %w", err)
		}
	}

	commitEarly, err := exportNewChainHeadSlots(params, tx, lastDbSlot, head)
	if err != nil {
		return err
	}
	if commitEarly {
		return nil // already committed
	}

	if err := finalizeAndUpdateNonFinalizedSlots(params, tx, head); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("error committing transaction: %w", err)
	}
	params.Log.Infof("commit successful")
	committed = true

	return nil
}

// recoverMissingSlots checks the DB for gaps in historical slot data.
// If the genesis slot is missing or there are gaps between existing slots,
// it backfills the missing slots by exporting them one by one.
func recoverMissingSlots(params *IndexingParams, tx *sqlx.Tx, head *types.ChainHead) error {
	dbSlots, err := params.Database.GetAllSlots(tx)
	if err != nil {
		return fmt.Errorf("error retrieving all db slots: %w", err)
	}

	if len(dbSlots) > 0 && dbSlots[0] != 0 {
		params.Log.Infof("exporting genesis slot as it is missing in the database")
		if err := exportSlot(params, 0, utils.EpochOfSlot(0) == head.HeadEpoch, tx); err != nil {
			return fmt.Errorf("error exporting genesis slot: %w", err)
		}
		dbSlots, err = params.Database.GetAllSlots(tx)
		if err != nil {
			return fmt.Errorf("error retrieving all db slots: %w", err)
		}
	}

	if len(dbSlots) > 1 {
		for i := 1; i < len(dbSlots); i++ {
			prev := dbSlots[i-1]
			curr := dbSlots[i]
			if prev != curr-1 {
				params.Log.Infof("slots between %v and %v are missing, exporting them", prev, curr)
				for slot := prev + 1; slot < curr; slot++ {
					if err := exportSlot(params, slot, false, tx); err != nil {
						return fmt.Errorf("error exporting slot %v: %w", slot, err)
					}
				}
			}
		}
	}

	return nil
}

// exportNewChainHeadSlots exports all new slots from the last known DB slot up to the current head.
// If more than 10 epochs worth of slots are pending, it exports only a batch and commits early.
func exportNewChainHeadSlots(params *IndexingParams, tx *sqlx.Tx, lastSlot uint64, head *types.ChainHead) (bool, error) {
	if lastSlot == head.HeadSlot {
		return false, nil // nothing to do
	}

	slotsExported := 0
	maxSlots := int(params.ChainParams.Time.SlotsPerEpoch) * 10

	for slot := lastSlot + 1; slot <= head.HeadSlot; slot++ {
		if err := exportSlot(params, slot, utils.EpochOfSlot(slot) == head.HeadEpoch, tx); err != nil {
			return false, fmt.Errorf("error exporting slot %v: %w", slot, err)
		}
		slotsExported++

		if slotsExported == maxSlots {
			if err := tx.Commit(); err != nil {
				return false, fmt.Errorf("error committing tx: %w", err)
			}
			params.Log.Infof("Committed after %d slots (10 epochs)", slotsExported)
			return true, nil
		}
	}

	return false, nil
}


// exportSlot handles the export of a single slot, including retrieving the block,
// saving duties (if not in pruned mode), and initiating epoch export if this is
// the first slot of a new epoch.
func exportSlot(params *IndexingParams, slot uint64, isHeadEpoch bool, tx *sqlx.Tx) error {
	isFirstSlotOfEpoch := slot%params.ChainParams.Time.SlotsPerEpoch == 0
	epoch := utils.EpochOfSlot(slot)

	if isFirstSlotOfEpoch {
		params.Log.Infof("exporting slot %v (epoch transition into epoch %v)", slot, epoch)
	} else {
		params.Log.Infof("exporting slot %v", slot)
	}
	start := time.Now()

	// retrieve the data for the slot from the node
	// the first slot of an epoch will also contain all validator duties for the whole epoch
	block, err := params.ConsClient.GetBlockBySlot(slot)
	if err != nil {
		return fmt.Errorf("error retrieving data for slot %v: %w", slot, err)
	}

	if isFirstSlotOfEpoch {
		params.Log.Infof("exporting duties, balances, validators, queue deposits and metadata for epoch %v", epoch)
		if err := exportEpochData(params, block, epoch, isHeadEpoch, tx); err != nil {
			return err
		}
	}

	if params.Config.Consensus.Mode != "pruned" {
		exportSlotDuties(params, block)
	}

	// save the block data to the db
	err = params.Database.SaveBlock(block, false, tx)
	if err != nil {
		return fmt.Errorf("error saving slot to the db: %w", err)
	}

	params.Log.WithFields(
		logger.Fields{
			"slot":      block.Slot,
			"blockRoot": fmt.Sprintf("%x", block.BlockRoot),
		},
	).Infof("! export of slot completed, took %v", time.Since(start))

	return nil
}

// prepareSlotSyncDuties builds a structure mapping a single slot to its sync committee duties,
// where each validator index maps to a boolean duty flag.
func prepareSlotSyncDuties(slot types.Slot, duties map[types.ValidatorIndex]bool) map[types.Slot]map[types.ValidatorIndex]bool {
	result := make(map[types.Slot]map[types.ValidatorIndex]bool)
	result[slot] = make(map[types.ValidatorIndex]bool)

	for validator, duty := range duties {
		result[slot][types.ValidatorIndex(validator)] = duty
	}

	return result
}

// prepareSlotAttDuties constructs attestation duties for a single slot.
// It maps each attested slot to validator indices and the slots in which they are attesting.
func prepareSlotAttDuties(currentSlot types.Slot, duties map[types.ValidatorIndex][]types.Slot) map[types.Slot]map[types.ValidatorIndex][]types.Slot {
	result := make(map[types.Slot]map[types.ValidatorIndex][]types.Slot)

	for validator, attestedSlots := range duties {
		valIndex := types.ValidatorIndex(validator)

		for _, attestedSlot := range attestedSlots {
			slot := types.Slot(attestedSlot)
			if result[slot] == nil {
				result[slot] = make(map[types.ValidatorIndex][]types.Slot)
			}
			result[slot][valIndex] = append(result[slot][valIndex], currentSlot)
		}
	}

	return result
}

// exportSlotDuties writes the sync committee and attestation duties of a single slot to Bigtable.
// This should only be called when not operating in pruned mode.
func exportSlotDuties(params *IndexingParams, block *types.Block) error {
	syncDuties := prepareSlotSyncDuties(types.Slot(block.Slot), block.SyncDuties)
	attDuties := prepareSlotAttDuties(types.Slot(block.Slot), block.AttestationDuties)

	if err := params.Bigtable.SaveAttestationDuties(attDuties); err != nil {
		return fmt.Errorf("error exporting attestations to bigtable for slot %v: %w", block.Slot, err)
	}
	if err := params.Bigtable.SaveSyncComitteeDuties(syncDuties); err != nil {
		return fmt.Errorf("error exporting sync committee duties to bigtable for slot %v: %w", block.Slot, err)
	}

	return nil
}

// finalizeAndUpdateNonFinalizedSlots checks all non-finalized slots in the DB,
// updates their status based on consensus data (proposed, missed, orphaned),
// and re-exports slots if reorgs occurred. Also updates epoch metadata if needed.
func finalizeAndUpdateNonFinalizedSlots(params *IndexingParams, tx *sqlx.Tx, head *types.ChainHead) error {
	dbNonFinalSlots, err := params.Database.GetAllNonFinalizedSlots()
	if err != nil {
		return fmt.Errorf("error retrieving non-finalized slots: %w", err)
	}

	for _, dbSlot := range dbNonFinalSlots {
		header, err := params.ConsClient.GetBlockHeader(dbSlot.Slot)
		if err != nil {
			return fmt.Errorf("error retrieving block header for slot %v: %w", dbSlot.Slot, err)
		}

		nodeFinalized := dbSlot.Slot <= head.FinalizedSlot
		matches := header != nil && bytes.Equal(dbSlot.BlockRoot, utils.MustParseHex(header.Data.Root))

		switch {
		case nodeFinalized && matches:
			params.Log.Infof("setting slot %v as finalized (proposed)", dbSlot.Slot)
			if err := params.Database.SetSlotFinalizationAndStatus(dbSlot.Slot, true, dbSlot.Status, tx); err != nil {
				return fmt.Errorf("error finalizing slot %v: %w", dbSlot.Slot, err)
			}

		case nodeFinalized && header == nil && len(dbSlot.BlockRoot) < 32:
			params.Log.Infof("setting slot %v as finalized (missed)", dbSlot.Slot)
			if err := params.Database.SetSlotFinalizationAndStatus(dbSlot.Slot, true, "2", tx); err != nil {
				return fmt.Errorf("error finalizing missed slot %v: %w", dbSlot.Slot, err)
			}

		case nodeFinalized && header == nil && len(dbSlot.BlockRoot) == 32:
			params.Log.Infof("setting slot %v as finalized (orphaned)", dbSlot.Slot)
			if err := params.Database.SetSlotFinalizationAndStatus(dbSlot.Slot, true, "3", tx); err != nil {
				return fmt.Errorf("error finalizing orphaned slot %v: %w", dbSlot.Slot, err)
			}

		case nodeFinalized && !matches:
			params.Log.Infof("setting slot %v as orphaned and exporting new slot", dbSlot.Slot)
			if err := params.Database.SetSlotFinalizationAndStatus(dbSlot.Slot, true, "3", tx); err != nil {
				return fmt.Errorf("error marking orphaned slot %v: %w", dbSlot.Slot, err)
			}
			if err := exportSlot(params, dbSlot.Slot, utils.EpochOfSlot(dbSlot.Slot) == head.HeadEpoch, tx); err != nil {
				return fmt.Errorf("error re-exporting orphaned slot %v: %w", dbSlot.Slot, err)
			}

		case !nodeFinalized && len(dbSlot.BlockRoot) < 32 && header != nil:
			params.Log.Infof("exporting new late slot %v", dbSlot.Slot)
			if err := exportSlot(params, dbSlot.Slot, utils.EpochOfSlot(dbSlot.Slot) == head.HeadEpoch, tx); err != nil {
				return fmt.Errorf("error exporting late slot %v: %w", dbSlot.Slot, err)
			}
		}

		if nodeFinalized &&
			dbSlot.Slot%params.ChainParams.Time.SlotsPerEpoch == 0 &&
			dbSlot.Slot > params.ChainParams.Time.SlotsPerEpoch-1 {

			if err := updateFinalizedEpochMetadata(params, tx, dbSlot.Slot, head); err != nil {
				return err
			}
		}
	}

	return nil
}