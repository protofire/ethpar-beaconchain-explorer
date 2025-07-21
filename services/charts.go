package services

import (
	"fmt"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/cache"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
)

// LatestChartsPageData returns the latest chart page data
func LatestChartsPageData() []*types.ChartsPageDataChart {
	wanted := &[]*types.ChartsPageDataChart{}
	cacheKey := fmt.Sprintf("%d:frontend:chartsPageData", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Hour, wanted); err == nil {
		return *wanted.(*[]*types.ChartsPageDataChart)
	} else {
		log.Errorf("error retrieving chartsPageData from cache: %v", err)
	}

	return nil
}