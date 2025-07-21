package cacheupdater

func epochUpdater(wg *sync.WaitGroup) {
	firstRun := true
	for {
		// latest epoch acording to the node
		var epochNode uint64
		err := db.WriterDb.Get(&epochNode, "SELECT headepoch FROM network_liveness order by headepoch desc LIMIT 1")
		if err != nil {
			log.Errorf("error retrieving latest node epoch from the database: %v", err)
		} else {
			cacheKey := fmt.Sprintf("%d:frontend:latestNodeEpoch", utils.Config.Chain.ClConfig.DepositChainID)
			err := cache.TieredCache.SetUint64(cacheKey, epochNode, utils.Day)
			if err != nil {
				log.Errorf("error caching latestNodeEpoch: %v", err)
			}
		}

		// latest finalized epoch acording to the node
		var latestNodeFinalized uint64
		err = db.WriterDb.Get(&latestNodeFinalized, "SELECT finalizedepoch FROM network_liveness order by headepoch desc LIMIT 1")
		if err != nil {
			log.Errorf("error retrieving latest node finalized epoch from the database: %v", err)
		} else {
			cacheKey := fmt.Sprintf("%d:frontend:latestNodeFinalizedEpoch", utils.Config.Chain.ClConfig.DepositChainID)
			err := cache.TieredCache.SetUint64(cacheKey, latestNodeFinalized, utils.Day)
			if err != nil {
				log.Errorf("error caching latestNodeFinalized: %v", err)
			}
		}

		// latest exported epoch
		var epoch uint64
		err = db.WriterDb.Get(&epoch, "SELECT COALESCE(MAX(epoch), 0) FROM epochs")
		if err != nil {
			log.Errorf("error retrieving latest exported epoch from the database: %v", err)
		} else {
			cacheKey := fmt.Sprintf("%d:frontend:latestEpoch", utils.Config.Chain.ClConfig.DepositChainID)
			err := cache.TieredCache.SetUint64(cacheKey, epoch, utils.Day)
			if err != nil {
				log.Errorf("error caching latestEpoch: %v", err)
			}
		}

		// latest exported finalized epoch

		latestFinalizedEpoch, err := db.GetLatestFinalizedEpoch()
		if err != nil {
			log.Errorf("error retrieving latest exported finalized epoch from the database: %v", err)
		} else {
			cacheKey := fmt.Sprintf("%d:frontend:latestFinalized", utils.Config.Chain.ClConfig.DepositChainID)
			err := cache.TieredCache.SetUint64(cacheKey, latestFinalizedEpoch, utils.Day)
			if err != nil {
				log.Errorf("error caching latestFinalizedEpoch: %v", err)
			}
			if firstRun {
				log.Info("initialized epoch updater")
				wg.Done()
				firstRun = false
			}
		}
		ReportStatus(true, "epochUpdater", "Running", nil)
		time.Sleep(time.Second)
	}
}