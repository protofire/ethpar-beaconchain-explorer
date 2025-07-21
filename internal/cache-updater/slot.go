package cacheupdater

func slotUpdater(wg *sync.WaitGroup) {
	firstRun := true

	for {
		var slot uint64
		err := db.WriterDb.Get(&slot, "SELECT COALESCE(MAX(slot), 0) FROM blocks where slot < $1", utils.TimeToSlot(uint64(time.Now().Add(time.Second*10).Unix())))

		if err != nil {
			log.Errorf("error retrieving latest slot from the database: %v", err)

			if err.Error() == "sql: database is closed" {
				log.Fatalf("error retrieving latest slot from the database: %v", err)
			}
		} else {
			cacheKey := fmt.Sprintf("%d:frontend:slot", utils.Config.Chain.ClConfig.DepositChainID)
			err := cache.TieredCache.SetUint64(cacheKey, slot, utils.Day)
			if err != nil {
				log.Errorf("error caching slot: %v", err)
			}
			if firstRun {
				log.Info("initialized slot updater")
				wg.Done()
				firstRun = false
			}
		}
		ReportStatus(true, "slotUpdater", "Running", nil)
		time.Sleep(time.Second)
	}
}

func latestProposedSlotUpdater(wg *sync.WaitGroup) {
	firstRun := true

	for {
		var slot uint64
		err := db.WriterDb.Get(&slot, "SELECT COALESCE(MAX(slot), 0) FROM blocks WHERE status = '1'")

		if err != nil {
			log.Errorf("error retrieving latest proposed slot from the database: %v", err)
		} else {

			cacheKey := fmt.Sprintf("%d:frontend:latestProposedSlot", utils.Config.Chain.ClConfig.DepositChainID)
			err = cache.TieredCache.SetUint64(cacheKey, slot, utils.Day)
			if err != nil {
				log.Errorf("error caching latestProposedSlot: %v", err)
			}
			if firstRun {
				log.Info("initialized last proposed slot updater")
				wg.Done()
				firstRun = false
			}
		}
		ReportStatus(true, "latestProposedSlotUpdater", "Running", nil)
		time.Sleep(time.Second)
	}
}