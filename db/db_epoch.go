package db

import (
	"fmt"

	"github.com/jmoiron/sqlx"

)

const (
	ScheduledSlotRoot = byte(0x0)
	MissedBlockRoot   = byte(0x1)
)

type EpochRow struct {
	Epoch                   uint64
	BlockCount              int
	ProposerSlashingsCount  int
	AttesterSlashingsCount  int
	AttestationsCount       int
	DepositCount            int
	WithdrawalCount         int
	VoluntaryExitCount      int
	ValidatorCount          int
	AverageValidatorBalance uint64
	TotalValidatorBalance   uint64
	EffectiveBalanceSum     uint64
	GlobalParticipationRate float64
	VotedEther              uint64
	Finalized               bool
}

// SaveEpoch will save the epoch data into the database
// exporter
func (pg *Postgres) SaveEpochRow(tx *sqlx.Tx, row EpochRow) error {
	const query = `INSERT INTO epochs (
			epoch, 
			blockscount, 
			proposerslashingscount, 
			attesterslashingscount, 
			attestationscount, 
			depositscount,
			withdrawalcount,
			voluntaryexitscount, 
			validatorscount, 
			averagevalidatorbalance, 
			totalvalidatorbalance,
			eligibleether, 
			globalparticipationrate, 
			votedether,
			finalized
		)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
		ON CONFLICT (epoch) DO UPDATE SET 
			blockscount             = excluded.blockscount, 
			proposerslashingscount  = excluded.proposerslashingscount,
			attesterslashingscount  = excluded.attesterslashingscount,
			attestationscount       = excluded.attestationscount,
			depositscount           = excluded.depositscount,
			withdrawalcount         = excluded.withdrawalcount,
			voluntaryexitscount     = excluded.voluntaryexitscount,
			validatorscount         = excluded.validatorscount,
			averagevalidatorbalance = excluded.averagevalidatorbalance,
			totalvalidatorbalance   = excluded.totalvalidatorbalance,
			eligibleether           = excluded.eligibleether,
			globalparticipationrate = excluded.globalparticipationrate,
			votedether              = excluded.votedether,
			finalized               = excluded.finalized;
	`
	
	_, err := tx.Exec(
		query,
		row.Epoch,
		row.BlockCount,
		row.ProposerSlashingsCount,
		row.AttesterSlashingsCount,
		row.AttestationsCount,
		row.DepositCount,
		row.WithdrawalCount,
		row.VoluntaryExitCount,
		row.ValidatorCount,
		row.AverageValidatorBalance,
		row.TotalValidatorBalance,
		row.EffectiveBalanceSum,
		row.GlobalParticipationRate,
		row.VotedEther,
		row.Finalized,
	)
	if err != nil {
		return fmt.Errorf("error saving epoch row: %w", err)
	}
	return nil
}

func (pg *Postgres) DeleteDuplicateBlocks(tx *sqlx.Tx, fromEpoch uint64, blockRoot []byte) error {
	const query = `
		DELETE FROM blocks
		WHERE slot IN (
			SELECT slot FROM blocks
			WHERE epoch >= $1
			GROUP BY slot
			HAVING COUNT(*) > 1
		) AND blockroot = $2;
	`

	_, err := tx.Exec(query, fromEpoch, blockRoot)
	if err != nil {
		return fmt.Errorf("error deleting duplicate blocks with root %x: %w", blockRoot, err)
	}
	return nil
}
