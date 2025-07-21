package cacheupdater


func mempoolUpdater(wg *sync.WaitGroup) {
	firstRun := true
	errorCount := 0

	var client *geth_rpc.Client

	for {
		var err error

		if client == nil {
			client, err = geth_rpc.Dial(utils.Config.Eth1GethEndpoint)
			if err != nil {
				utils.LogError(err, "can't connect to geth node", 0)
				time.Sleep(time.Second * 30)
				continue
			}
		}

		var mempoolTx types.RawMempoolResponse

		err = client.Call(&mempoolTx, "txpool_content")
		if err != nil {
			errorCount++
			if errorCount < 5 {
				logrus.Warnf("error calling txpool_content request (x%d): %v", errorCount, err)
			} else {
				logrus.Errorf("error calling txpool_content request (x%d): %v", errorCount, err)
			}
			time.Sleep(time.Second * 10)
			continue
		} else {
			errorCount = 0
		}

		mempoolTx.TxsByHash = make(map[common.Hash]*types.RawMempoolTransaction)

		for _, txs := range mempoolTx.Pending {
			for _, tx := range txs {
				mempoolTx.TxsByHash[tx.Hash] = tx

				if tx.GasPrice == nil {
					tx.GasPrice = tx.GasFeeCap
				}
				tx.Input = nil // nil inputs to save space
			}
		}
		for _, txs := range mempoolTx.Queued {
			for _, tx := range txs {
				mempoolTx.TxsByHash[tx.Hash] = tx

				if tx.GasPrice == nil {
					tx.GasPrice = tx.GasFeeCap
				}
				tx.Input = nil // nil inputs to save space
			}
		}
		for _, txs := range mempoolTx.BaseFee {
			for _, tx := range txs {
				mempoolTx.TxsByHash[tx.Hash] = tx

				if tx.GasPrice == nil {
					tx.GasPrice = tx.GasFeeCap
				}
				tx.Input = nil // nil inputs to save space
			}
		}

		cacheKey := fmt.Sprintf("%d:frontend:mempool", utils.Config.Chain.ClConfig.DepositChainID)
		err = cache.TieredCache.Set(cacheKey, mempoolTx, utils.Day)
		if err != nil {
			log.Errorf("error caching mempool data: %v", err)
		}
		if firstRun {
			log.Info("initialized mempool updater")
			wg.Done()
			firstRun = false
		}
		ReportStatus(true, "mempoolUpdater", "Running", nil)
		time.Sleep(time.Second * 5)
	}
}
