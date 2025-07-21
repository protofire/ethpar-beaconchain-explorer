package services

import (
	"context"
	"fmt"
	"sync"

	"github.com/protofire/ethpar-beaconchain-explorer/db"

	"golang.org/x/sync/errgroup"
)

type AddressNamesService struct {
	BT  *db.Bigtable
	PG  *db.Postgres
	Ctx context.Context
}

func (svc *AddressNamesService) GetNamesForAddresses(addresses map[string]string) (map[string]string, error) {
	result := make(map[string]string, len(addresses))
	mux := sync.Mutex{}

	g := new(errgroup.Group)
	g.SetLimit(25)

	for addrStr := range addresses {
		addr := addrStr

		g.Go(func() error {
			var name string
			var err error

			// 1. Check ENS from Postgres
			name, err = svc.PG.GetEnsNameForAddress(svc.Ctx, []byte(addr))
			if err != nil {
				return fmt.Errorf("failed to get ENS name from Postgres for address %s: %w", addr, err)
			}

			// 2. If ENS not set get name from Bigtable
			if name == "" {
				name, err = svc.BT.GetAddressName(svc.Ctx, []byte(addr))
				if err != nil {
					return fmt.Errorf("failed to get name from Bigtable for address %s: %w", addr, err)
				}
			}

			if name != "" {
				mux.Lock()
				result[addr] = name
				mux.Unlock()
			}

			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return result, nil
}