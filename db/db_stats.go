package db

import (
	"errors"
	"database/sql"

	"github.com/protofire/ethpar-beaconchain-explorer/types"
)

// GetTop5Eth1Depositors returns the top 5 depositors on the eth1 side by number of deposits.
// Only deposits with a valid signature are counted.
//
// Returns:
//   - Slice of StatsTopDepositors
//   - Error, if any database issue occurs (except no rows)
func (pg *Postgres) GetTop5Eth1Depositors() ([]types.StatsTopDepositors, error) {
	const query = `
		SELECT 
			ENCODE(from_address::bytea, 'hex') AS from_address, 
			COUNT(from_address) AS count
		FROM eth1_deposits
		WHERE valid_signature = true
		GROUP BY from_address
		ORDER BY count DESC
		LIMIT 5;
	`

	var topDepositors []types.StatsTopDepositors

	if err := pg.Db.Select(&topDepositors, query); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return nil, err
	}

	return topDepositors, nil
}

// CountInvalidDeposits returns the number of eth1 deposits with invalid signatures.
//
// Returns:
//   - uint64: number of invalid deposits
//   - error: if a database error occurs
func (pg *Postgres) CountInvalidDeposits() (uint64, error) {
	const query = `
		SELECT COUNT(*) 
		FROM eth1_deposits
		WHERE valid_signature = false
	`

	var count uint64
	err := pg.Db.Get(&count, query)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}

	return count, nil
}

// CountUniqueValidators returns the number of unique eth1 depositors
// whose valid deposits meet or exceed the 32 ETH threshold.
//
// Returns:
//   - uint64: number of unique validators
//   - error: if a database error occurs
func (pg *Postgres) CountUniqueValidators() (uint64, error) {
	const query = `
		SELECT COUNT(*) 
		FROM (
			SELECT publickey
			FROM eth1_deposits
			WHERE valid_signature = true
			GROUP BY publickey
			HAVING SUM(amount) >= 32e9
		) AS q
	`
	var count uint64

	if err := pg.Db.Get(&count, query); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}

	return count, nil
}