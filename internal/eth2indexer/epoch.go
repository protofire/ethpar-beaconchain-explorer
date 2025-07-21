package eth2indexer

import (
	"fmt"
	"math/big"
	"strings"
	"strconv"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	rpc_types "github.com/protofire/ethpar-beaconchain-explorer/rpc/types"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	"github.com/jmoiron/sqlx"
	"golang.org/x/sync/errgroup"
)

// exportEpochData handles the export of all epoch-level data including
// duties, balances, validators, and epoch metadata. Executes tasks in parallel.
func exportEpochData(
	params *IndexingParams,
	block *types.Block,
	epoch uint64,
	isHeadEpoch bool,
	tx *sqlx.Tx,
) error {
	g := errgroup.Group{}

	if params.Config.Consensus.Mode != "pruned" || isHeadEpoch {
		startSlot := epoch * params.ChainParams.Time.SlotsPerEpoch
		endSlot := (epoch+1)*params.ChainParams.Time.SlotsPerEpoch - 1

		syncDutiesEpoch := prepareEpochSyncDuties(startSlot, endSlot, block.EpochAssignments.SyncAssignments)

		attDutiesEpoch, err := prepareEpochAttDuties(block.EpochAssignments.AttestorAssignments)
		if err != nil {
			return fmt.Errorf("error preparing attestation duties: %w", err)
		}

		g.Go(func() error {
			if err := params.Bigtable.SaveAttestationDuties(attDutiesEpoch); err != nil {
				return fmt.Errorf("error exporting attestation assignments to bigtable for slot %v: %w", block.Slot, err)
			}
			return nil
		})

		g.Go(func() error {
			if err := params.Bigtable.SaveSyncComitteeDuties(syncDutiesEpoch); err != nil {
				return fmt.Errorf("error exporting sync committee assignments to bigtable for slot %v: %w", block.Slot, err)
			}
			return nil
		})

		g.Go(func() error {
			if err := params.Bigtable.SaveValidatorBalances(epoch, block.Validators); err != nil {
				return fmt.Errorf("error exporting validator balances to bigtable for slot %v: %w", block.Slot, err)
			}
			return nil
		})
	}

	if isHeadEpoch {
		g.Go(func() error {
			if err := params.Database.SaveValidators(epoch, block.Validators, params.ConsClient, 10000, tx, params.Bigtable, params.Log); err != nil {
				return fmt.Errorf("error saving validators for epoch %v: %w", epoch, err)
			}

			if err := params.Database.UpdateQueueDeposits(tx, params.Log); err != nil {
				return fmt.Errorf("error updating queue deposits cache: %w", err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}

	if err := exportEpoch(params.Database, tx, epoch, block, params.ConsClient, params.Log); err != nil {
		return fmt.Errorf("failed to export epoch metadata: %w", err)
	}

	return nil
}

// exportEpoch computes and saves epoch-level metadata to the database,
// including validator statistics and participation rates.
func exportEpoch(
	db *db.Postgres,
	tx *sqlx.Tx,
	epoch uint64,
	block *types.Block,
	consClient consensus.ConsensusClient,
	log *logger.Logger,
) error {
	log.Infof("exporting epoch metadata for epoch %v", epoch)

	row, err := buildEpochRowWithStats(epoch, block, consClient, log)
	if err != nil {
		return fmt.Errorf("failed to build epoch row: %w", err)
	}

	if err := db.SaveEpochRow(tx, row); err != nil {
		return fmt.Errorf("failed to save epoch row: %w", err)
	}

	return nil
}

// buildEpochRowWithStats constructs the epoch metadata row by computing validator stats
// and optionally including participation metrics if available.
func buildEpochRowWithStats(
	epoch uint64,
	block *types.Block,
	consClient consensus.ConsensusClient,
	log *logger.Logger,
) (db.EpochRow, error) {
	validatorStats := computeValidatorStats(block.Validators, epoch)

	// Compose basic row
	row := db.EpochRow{
		Epoch:                   epoch,
		BlockCount:              0,
		ProposerSlashingsCount:  len(block.ProposerSlashings),
		AttesterSlashingsCount:  len(block.AttesterSlashings),
		AttestationsCount:       len(block.Attestations),
		DepositCount:            len(block.Deposits),
		WithdrawalCount:         len(block.ExecutionPayload.Withdrawals),
		VoluntaryExitCount:      len(block.VoluntaryExits),
		ValidatorCount:          validatorStats.ValidatorCount,
		AverageValidatorBalance: validatorStats.AverageBalance.Uint64(),
		TotalValidatorBalance:   validatorStats.BalanceSum.Uint64(),
		EffectiveBalanceSum:     validatorStats.EffectiveBalanceSum.Uint64(),
		GlobalParticipationRate: 0,
		VotedEther:              0,
		Finalized:               false,
	}

	// compute participation if epoch > 0
	if epoch > 0 {
		log.Infof("fetching participation stats for epoch %v", epoch-1)

		head, err := consClient.GetChainHead()
		if err != nil {
			return row, fmt.Errorf("failed to get head: %w", err)
		}

		rpcResp, err := consClient.GetValidatorInclusion(epoch - 1)
		if err != nil {
			return row, fmt.Errorf("error retrieving participation: %w", err)
		}

		participation := computeParticipationStats(epoch-1, head, rpcResp)

		row.GlobalParticipationRate = float64(participation.GlobalParticipationRate)
		row.VotedEther = participation.VotedEther
		row.EffectiveBalanceSum = participation.EligibleEther
		row.Finalized = participation.Finalized
	}

	return row, nil
}

type EpochValidatorStats struct {
	ValidatorCount      int
	BalanceSum          *big.Int
	EffectiveBalanceSum *big.Int
	AverageBalance      *big.Int
}

type EpochParticipationStats struct {
	GlobalParticipationRate float32
	VotedEther              uint64
	EligibleEther           uint64
	Finalized               bool
}

// computeValidatorStats aggregates statistics (balance sum, count, average, etc.)
// for active validators in the given epoch.
func computeValidatorStats(validators []*types.Validator, epoch uint64) EpochValidatorStats {
	count := 0
	balanceSum := big.NewInt(0)
	effectiveSum := big.NewInt(0)

	for _, v := range validators {
		if v.ExitEpoch > epoch && v.ActivationEpoch <= epoch {
			count++
			balanceSum.Add(balanceSum, new(big.Int).SetUint64(v.Balance))
			effectiveSum.Add(effectiveSum, new(big.Int).SetUint64(v.EffectiveBalance))
		}
	}

	avg := big.NewInt(0)
	if count > 0 {
		avg.Div(balanceSum, big.NewInt(int64(count)))
	}

	return EpochValidatorStats{
		ValidatorCount:      count,
		BalanceSum:          balanceSum,
		EffectiveBalanceSum: effectiveSum,
		AverageBalance:      avg,
	}
}

// computeParticipationStats calculates participation rate, voted ether,
// and finalization status for the specified epoch based on beacon node data.
func computeParticipationStats(epoch uint64, head *types.ChainHead, rpcResp rpc_types.StandardValidatorParticipationResponse) EpochParticipationStats {
	isFinalized := epoch <= head.FinalizedEpoch && head.JustifiedEpoch > 0

	var rate float32
	var voted uint64
	var eligible uint64

	if epoch+1 < head.HeadEpoch {
		// Use data from 'previous'
		eligible = uint64(rpcResp.Data.PreviousEpochActiveGwei)
		voted = uint64(rpcResp.Data.PreviousEpochTargetAttestingGwei)
		rate = float32(utils.SafeDivideFloat(rpcResp.Data.PreviousEpochTargetAttestingGwei, rpcResp.Data.PreviousEpochActiveGwei))
	} else {
		// Use data from 'current'
		eligible = uint64(rpcResp.Data.CurrentEpochActiveGwei)
		voted = uint64(rpcResp.Data.CurrentEpochTargetAttestingGwei)
		rate = float32(utils.SafeDivideFloat(rpcResp.Data.CurrentEpochTargetAttestingGwei, rpcResp.Data.CurrentEpochActiveGwei))
	}

	return EpochParticipationStats{
		GlobalParticipationRate: rate,
		VotedEther:              voted,
		EligibleEther:           eligible,
		Finalized:               isFinalized,
	}
}

// prepareEpochSyncDuties builds a map of sync committee duties for each slot in the epoch,
// marking all assigned validators with a default `false` flag for later use.
func prepareEpochSyncDuties(startSlot, endSlot uint64, validatorIndices []uint64) map[types.Slot]map[types.ValidatorIndex]bool {
	syncDutiesEpoch := make(map[types.Slot]map[types.ValidatorIndex]bool)

	for slot := startSlot; slot <= endSlot; slot++ {
		syncDutiesEpoch[types.Slot(slot)] = make(map[types.ValidatorIndex]bool)
		for _, validatorIndex := range validatorIndices {
			syncDutiesEpoch[types.Slot(slot)][types.ValidatorIndex(validatorIndex)] = false
		}
	}

	return syncDutiesEpoch
}

// prepareEpochAttDuties parses assignment keys of the form "slot-validator"
// and constructs a mapping from attested slots to validators.
func prepareEpochAttDuties(assignments map[string]uint64) (map[types.Slot]map[types.ValidatorIndex][]types.Slot, error) {
	attDutiesEpoch := make(map[types.Slot]map[types.ValidatorIndex][]types.Slot)

	for key, validatorIndex := range assignments {
		keySplit := strings.Split(key, "-")
		if len(keySplit) != 2 {
			return nil, fmt.Errorf("invalid attestation key format: %s", key)
		}

		attestedSlot, err := strconv.ParseUint(keySplit[0], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing attested slot from attestation key: %w", err)
		}

		slot := types.Slot(attestedSlot)
		validator := types.ValidatorIndex(validatorIndex)

		if attDutiesEpoch[slot] == nil {
			attDutiesEpoch[slot] = make(map[types.ValidatorIndex][]types.Slot)
		}

		attDutiesEpoch[slot][validator] = []types.Slot{}
	}

	return attDutiesEpoch, nil
}

// updateFinalizedEpochMetadata fetches participation data for a finalized epoch,
// updates epoch-level participation stats in the DB, and saves the validator queue state.
func updateFinalizedEpochMetadata(params *IndexingParams, tx *sqlx.Tx, slot uint64, head *types.ChainHead) error {
	epoch := utils.EpochOfSlot(slot)

	rpcResp, err := params.ConsClient.GetValidatorInclusion(epoch - 1)
	if err != nil {
		return fmt.Errorf("error retrieving validator inclusion data for epoch %v: %w", epoch-1, err)
	}

	stats := computeParticipationStats(epoch-1, head, rpcResp)

	params.Log.Infof("updating epoch %v with participation rate %v", epoch, stats.GlobalParticipationRate)

	if err := params.Database.UpdateEpochStatus(&types.ValidatorParticipation{
		Epoch:                  epoch,
		GlobalParticipationRate: stats.GlobalParticipationRate,
		VotedEther:              stats.VotedEther,
		EligibleEther:           stats.EligibleEther,
		Finalized:               true,
	}, tx); err != nil {
		return fmt.Errorf("error updating epoch status for epoch %v: %w", epoch, err)
	}

	params.Log.Infof("exporting validator queue")
	queue, err := params.ConsClient.GetValidatorQueue()
	if err != nil {
		return fmt.Errorf("error retrieving validator queue: %w", err)
	}

	if err := params.Database.SaveValidatorQueue(queue, tx); err != nil {
		return fmt.Errorf("error saving validator queue: %w", err)
	}

	return nil
}