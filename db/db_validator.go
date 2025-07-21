package db

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/protofire/ethpar-beaconchain-explorer/types"

	"github.com/lib/pq"
)

// CountSyncCommitteePeriods returns the number of sync committee periods
// for which validator participation data has been recorded.
func (pg *Postgres) CountSyncCommitteePeriods() (uint64, error) {
	const query = `SELECT COUNT(*) FROM sync_committees_count_per_validator`

	var count uint64
	if err := pg.Db.Get(&count, query); err != nil {
		return 0, fmt.Errorf("failed to count sync committee periods: %w", err)
	}
	return count, nil
}

// GetLatestSyncCommitteePeriod returns the highest sync committee period
// that has been recorded in the sync_committees_count_per_validator table.
func (pg *Postgres) GetLatestSyncCommitteePeriod() (uint64, error) {
	const query = `SELECT MAX(period) FROM sync_committees_count_per_validator`

	var maxPeriod uint64
	if err := pg.Db.Get(&maxPeriod, query); err != nil {
		return 0, fmt.Errorf("failed to get latest sync committee period: %w", err)
	}

	return maxPeriod, nil
}

// GetSyncCommitteeValidatorCountAtPeriod returns the number of validators
// that have participated in sync committees up to the given period.
func (pg *Postgres) GetSyncCommitteeValidatorCountAtPeriod(period uint64) (float64, error) {
	const query = `SELECT count_so_far FROM sync_committees_count_per_validator WHERE period = $1`

	var count float64
	err := pg.Db.Get(&count, query, period)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil // No data for this period yet
		}
		return 0, fmt.Errorf("failed to get validator count for period %d: %w", period, err)
	}

	return count, nil
}

// GetValidatorCountForEpoch returns the number of active validators
// recorded in the epochs table for the given epoch.
func (pg *Postgres) GetValidatorCountForEpoch(epoch uint64) (uint64, error) {
	const query = `SELECT validatorscount FROM epochs WHERE epoch = $1`

	var count uint64
	err := pg.Db.Get(&count, query, epoch)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil // Epoch not found
		}
		return 0, fmt.Errorf("failed to get validator count for epoch %d: %w", epoch, err)
	}

	return count, nil
}

// UpsertSyncCommitteeValidatorCount inserts or updates the count_so_far
// for the given sync committee period.
func (pg *Postgres) UpsertSyncCommitteeValidatorCount(period uint64, count float64) error {
	const query = `
		INSERT INTO sync_committees_count_per_validator (period, count_so_far)
		VALUES ($1, $2)
		ON CONFLICT (period) DO UPDATE SET
			count_so_far = EXCLUDED.count_so_far;
	`

	_, err := pg.Db.Exec(query, period, count)
	if err != nil {
		return fmt.Errorf("failed to upsert validator count for period %d: %w", period, err)
	}

	return nil
}

func (pg *Postgres) GetValidatorPageData(validators []uint64) ([]types.ValidatorPageData, error) {
	const query = `
		SELECT validatorindex, balanceactivation
		FROM validators 
		WHERE validatorindex = ANY($1)
		ORDER BY validatorindex ASC
	`
	
	filter := pq.Array(validators)
	var data []types.ValidatorPageData

	err := pg.Db.Select(&data, query, filter)
	if err != nil {
		return nil, err
	}
	return data, nil
}