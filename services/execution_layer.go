package services

import (
	"fmt"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/cache"
)

const latestBlockNumberCacheKey = "latestEth1BlockNumber"
const latestBlockHashRootCacheKey = "latestEth1BlockRootHash"

// LatestEth1BlockNumber will return most recent eth1 block number
func LatestEth1BlockNumber(chainId uint64) uint64 {
	cacheKey := fmt.Sprintf("%d:frontend:%s", chainId, latestBlockNumberCacheKey)

	if wanted, err := cache.TieredCache.GetUint64WithLocalTimeout(cacheKey, time.Second*5); err == nil {
		return wanted
	} else {
		log.Errorf("error retrieving latest block number from cache: %v", err)
	}
	return 0
}

// Eth1HeadBlockRootHash will return the hash of the current chain head block
func Eth1HeadBlockRootHash(chainId uint64) []byte {
	cacheKey := fmt.Sprintf("%d:frontend:%s", chainId, latestBlockHashRootCacheKey)

	if wanted, err := cache.TieredCache.GetStringWithLocalTimeout(cacheKey, time.Second*5); err == nil {
		return []byte(wanted)
	} else {
		log.Errorf("error retrieving latest blockroot hash from cache: %v", err)
	}
	return []byte{}
}
