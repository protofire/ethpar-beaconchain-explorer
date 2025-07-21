package db

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
)

func (pg *Postgres) GetEnsNamesForAddress(addressMap map[string]string) (map[string]string, error) {
	const query = `
		SELECT address, ens_name 
		FROM ens
		WHERE address = ANY($1)
		  AND is_primary_name
		  AND valid_to >= now();
	`

	if len(addressMap) == 0 {
		return map[string]string{}, nil
	}

	type pair struct {
		Address []byte `db:"address"`
		EnsName string `db:"ens_name"`
	}

	dbAddresses := []pair{}
	addresses := make([][]byte, 0, len(addressMap))
	for add := range addressMap {
		addresses = append(addresses, []byte(add))
	}

	err := pg.Db.Select(&dbAddresses, query, addresses)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string, len(dbAddresses))
	for _, found := range dbAddresses {
		result[string(found.Address)] = found.EnsName
	}

	return result, nil
}

func (pg *Postgres) GetEnsNameForAddress(ctx context.Context, address []byte) (string, error) {
	const query = `
		SELECT ens_name
		FROM ens
		WHERE address = $1
		  AND is_primary_name = true
		  AND valid_to >= now()
		LIMIT 1;
	`

	var name string
	err := pg.Db.GetContext(ctx, &name, query, address)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return "", nil
		}
		return "", fmt.Errorf("GetEnsNameForAddress query failed: %w", err)
	}

	return name, nil
}

func (pg *Postgres) GetAddressForEnsName(name string) (address *common.Address, err error) {
	addressBytes := []byte{}
	err = pg.Db.Get(&addressBytes, `
	SELECT address 
	FROM ens
	WHERE
		ens_name = $1 AND
		valid_to >= now()
	`, name)
	if err == nil && addressBytes != nil {
		add := common.BytesToAddress(addressBytes)
		address = &add
	}
	return address, err
}
