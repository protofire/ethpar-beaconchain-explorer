package db

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/cache"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	"github.com/lib/pq"
	"golang.org/x/sync/errgroup"
)

func (pg *Postgres) GetValidatorIncomeHistory(validatorIndices []uint64, lowerBoundDay uint64, upperBoundDay uint64, lastFinalizedEpoch uint64, bt *Bigtable) ([]types.ValidatorIncomeHistory, error) {
	if len(validatorIndices) == 0 {
		return []types.ValidatorIncomeHistory{}, nil
	}

	if upperBoundDay == 0 {
		upperBoundDay = 65536
	}

	validatorIndices = utils.SortedUniqueUint64(validatorIndices)
	validatorIndicesStr := make([]string, len(validatorIndices))
	for i, v := range validatorIndices {
		validatorIndicesStr[i] = fmt.Sprintf("%d", v)
	}

	validatorIndicesPqArr := pq.Array(validatorIndices)

	cacheDur := time.Second * time.Duration(utils.Config.Chain.ClConfig.SecondsPerSlot*utils.Config.Chain.ClConfig.SlotsPerEpoch+10) // updates every epoch, keep 10sec longer
	cacheKey := fmt.Sprintf("%d:validatorIncomeHistory:%d:%d:%d:%s", utils.Config.Chain.ClConfig.DepositChainID, lowerBoundDay, upperBoundDay, lastFinalizedEpoch, strings.Join(validatorIndicesStr, ","))
	cached := []types.ValidatorIncomeHistory{}
	if _, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, cacheDur, &cached); err == nil {
		return cached, nil
	}

	var result []types.ValidatorIncomeHistory
	err := pg.Db.Select(&result, `
		SELECT 
			day, 
			SUM(COALESCE(cl_rewards_gwei, 0)) AS cl_rewards_gwei,
			SUM(COALESCE(end_balance, 0)) AS end_balance
		FROM validator_stats 
		WHERE validatorindex = ANY($1) AND day BETWEEN $2 AND $3 
		GROUP BY day 
		ORDER BY day
	;`, validatorIndicesPqArr, lowerBoundDay, upperBoundDay)
	if err != nil {
		return nil, err
	}

	// retrieve rewards for epochs not yet in stats
	if upperBoundDay == 65536 {
		lastDay := int64(0)
		if len(result) > 0 {
			lastDay = int64(result[len(result)-1].Day)
		} else {
			lastDayDb, err := pg.GetLastExportedStatisticDay()
			if err == nil {
				lastDay = int64(lastDayDb)
			} else if err == ErrNoStats {
				lastDay = -1
			} else {
				return nil, err
			}
		}

		currentDay := lastDay + 1
		firstSlot := uint64(0)
		if lastDay > -1 {
			firstSlot = utils.GetLastBalanceInfoSlotForDay(uint64(lastDay)) + 1
		}
		lastSlot := lastFinalizedEpoch * utils.Config.Chain.ClConfig.SlotsPerEpoch

		totalBalance := uint64(0)

		g := errgroup.Group{}
		g.Go(func() error {
			latestBalances, err := bt.GetValidatorBalanceHistory(validatorIndices, lastFinalizedEpoch, lastFinalizedEpoch)
			if err != nil {
				pg.Logger.Errorf("error in GetValidatorIncomeHistory calling BigtableClient.GetValidatorBalanceHistory: %v", err)
				return err
			}

			for _, balance := range latestBalances {
				if len(balance) == 0 {
					continue
				}

				totalBalance += balance[0].Balance
			}
			return nil
		})

		var lastBalance uint64
		g.Go(func() error {

			if lastDay < 0 {
				return pg.GetValidatorActivationBalance(validatorIndices, &lastBalance)
			} else {
				return pg.GetValidatorBalanceForDay(validatorIndices, uint64(lastDay), &lastBalance)
			}
		})

		var lastDeposits uint64
		g.Go(func() error {
			deposits, err := pg.GetValidatorDepositsAndIncomingConsolidations(&SlotRange{StartSlot: firstSlot, EndSlot: lastSlot}, validatorIndices)
			if err != nil {
				return err
			}
			for _, deposit := range deposits {
				lastDeposits += deposit.DepositsAmount
			}
			return nil
		})

		var lastWithdrawals uint64
		g.Go(func() error {
			return pg.GetValidatorWithdrawalsForSlots(validatorIndices, firstSlot, lastSlot, &lastWithdrawals)
		})

		err = g.Wait()
		if err != nil {
			return nil, err
		}

		result = append(result, types.ValidatorIncomeHistory{
			Day:        int64(currentDay),
			ClRewards:  int64(totalBalance - lastBalance - lastDeposits + lastWithdrawals),
			EndBalance: sql.NullInt64{Int64: int64(totalBalance), Valid: true}, // show the latest balance for todays income
		})
	}

	go func() {
		err := cache.TieredCache.Set(cacheKey, &result, cacheDur)
		if err != nil {
			pg.Logger.WithField("cache-key", cacheKey).Errorf("error setting tieredCache for GetValidatorIncomeHistory with key %v", err)
		}
	}()

	return result, nil
}