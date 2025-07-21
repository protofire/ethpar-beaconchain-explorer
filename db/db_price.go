package db

import (
	"github.com/protofire/ethpar-beaconchain-explorer/types"
)

// GetPricesBetween returns price records for the given time range [start, end].
//
// Parameters:
//   - start: Unix timestamp (in seconds) for the start of the range
//   - end:   Unix timestamp (in seconds) for the end of the range
//
// Returns:
//   - []types.Price: historical prices ordered from newest to oldest
//   - error: if the database query fails
func (pg *Postgres) GetPricesBetween(start, end uint64) ([]types.Price, error) {
	const query = `
		SELECT ts, eur, usd, gbp, cad, jpy, cny, rub, aud
		FROM price
		WHERE ts >= TO_TIMESTAMP($1) AND ts <= TO_TIMESTAMP($2)
		ORDER BY ts DESC
	`

	var prices []types.Price
	if err := pg.Db.Select(&prices, query, start, end); err != nil {
		return nil, err
	}

	return prices, nil
}
