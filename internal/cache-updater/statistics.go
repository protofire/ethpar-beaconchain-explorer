package cacheupdater


func latestExportedStatisticDayUpdater(wg *sync.WaitGroup) {
	firstRun := true
	cacheKey := fmt.Sprintf("%d:frontend:lastExportedStatisticDay", utils.Config.Chain.ClConfig.DepositChainID)
	for {
		lastDay, err := db.GetLastExportedStatisticDay()
		if err != nil {
			log.Errorf("error retrieving last exported statistics day: %v", err)
			time.Sleep(time.Second * 10)
			continue
		}

		err = cache.TieredCache.Set(cacheKey, lastDay, utils.Day)
		if err != nil {
			log.Errorf("error caching last exported statistics day: %v", err)
		}
		if firstRun {
			firstRun = false
			wg.Done()
			log.Info("initialized last exported statistics day updater")
		}
		ReportStatus(true, "lastExportedStatisticDay", "Running", nil)
		time.Sleep(time.Minute * 2)
	}
}
