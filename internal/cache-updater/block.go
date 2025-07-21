package cacheupdater

// latestBlockUpdater updates the most recent eth1 block number variable
func latestBlockUpdater(wg *sync.WaitGroup, bt *db.Bigtable) {
	firstRun := true

	for {
		recent, err := bt.GetMostRecentBlockFromDataTable()
		if err != nil {
			utils.LogError(err, "error getting most recent eth1 block", 0)
		}
		cacheKey := fmt.Sprintf("%d:frontend:%s", utils.Config.Chain.ClConfig.DepositChainID, latestBlockNumberCacheKey)
		err = cache.TieredCache.SetUint64(cacheKey, recent.GetNumber(), utils.Day)
		if err != nil {
			utils.LogError(err, fmt.Sprintf("error caching latest block number with cache key %s", latestBlockNumberCacheKey), 0)
		}

		if firstRun {
			log.Info("initialized eth1 block updater")
			wg.Done()
			firstRun = false
		}
		ReportStatus(true, "latestBlockUpdater", "Running", nil)
		time.Sleep(time.Second * 10)
	}
}

// headBlockRootHashUpdater updates the hash of the current chain head block
func headBlockRootHashUpdater(pg *db.Postgres, wg *sync.WaitGroup) {
	firstRun := true

	for {
		blockRootHash := []byte{}
		err := pg.Db.Get(&blockRootHash, `
		SELECT blockroot
		FROM blocks
		WHERE status = '1'
		ORDER BY slot DESC
		LIMIT 1`)

		if err != nil {
			utils.LogError(err, "error getting blockroot hash for chain head", 0)
		}
		cacheKey := fmt.Sprintf("%d:frontend:%s", utils.Config.Chain.ClConfig.DepositChainID, latestBlockHashRootCacheKey)
		err = cache.TieredCache.SetString(cacheKey, string(blockRootHash), utils.Day)
		if err != nil {
			utils.LogError(err, fmt.Sprintf("error caching latest blockroot hash with cache key %s", latestBlockHashRootCacheKey), 0)
		}

		if firstRun {
			log.Info("initialized eth1 head block root hash updater")
			wg.Done()
			firstRun = false
		}
		ReportStatus(true, "headBlockRootHashUpdater", "Running", nil)
		time.Sleep(time.Second * 10)
	}
}