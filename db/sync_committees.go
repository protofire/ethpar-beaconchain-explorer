package db

import (
	"context"
	"fmt"
	"strings"
)

// GetExportedSyncCommitteePeriods returns all unique sync committee periods
// currently stored in the `sync_committees` table.
//
// Returns:
//   - []uint64: A list of exported sync committee periods.
//   - error: An error if the database query fails, or nil on success.
func (pg *Postgres) GetExportedSyncCommitteePeriods() ([]uint64, error) {
	const query = `SELECT period FROM sync_committees GROUP BY period`
	
	var periods []uint64
	if err := pg.Db.Select(&periods, query); err != nil {
		return nil, err
	}
	return periods, nil
}

func (pg *Postgres) InsertSyncCommittees(ctx context.Context, entries [][3]uint64) error {
	tx, err := pg.Db.BeginTxx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	const nArgs = 3
	valueArgs := make([]interface{}, len(entries)*nArgs)
	valueIds := make([]string, len(entries))

	for i, e := range entries {
		valueArgs[i*nArgs+0] = e[0] // period
		valueArgs[i*nArgs+1] = e[1] // validatorIndex
		valueArgs[i*nArgs+2] = e[2] // committeeIndex
		valueIds[i] = fmt.Sprintf("($%d,$%d,$%d)", i*nArgs+1, i*nArgs+2, i*nArgs+3)
	}

	query := fmt.Sprintf(`
		INSERT INTO sync_committees (period, validatorindex, committeeindex)
		VALUES %s
		ON CONFLICT (period, validatorindex, committeeindex) DO NOTHING`, strings.Join(valueIds, ","))

	if _, err := tx.ExecContext(ctx, query, valueArgs...); err != nil {
		return err
	}

	return tx.Commit()
}
