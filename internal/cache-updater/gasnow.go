package cacheupdater

func gasNowUpdater(wg *sync.WaitGroup, bt *db.Bigtable) {
	firstRun := true

	for {
		data, err := getGasNowData(bt)
		if err != nil {
			log.Warnf("error retrieving gas now data: %v", err)
			time.Sleep(time.Second * 5)
			continue
		}

		cacheKey := fmt.Sprintf("%d:frontend:gasNow", utils.Config.Chain.ClConfig.DepositChainID)
		err = cache.TieredCache.Set(cacheKey, data, utils.Day)
		if err != nil {
			log.Errorf("error caching latestFinalizedEpoch: %v", err)
		}
		if firstRun {
			wg.Done()
			firstRun = false
		}
		time.Sleep(time.Second * 15)
	}
}

func getGasNowData(bt *db.Bigtable) (*types.GasNowPageData, error) {
	gpoData := &types.GasNowPageData{}
	gpoData.Code = 200
	gpoData.Data.Timestamp = time.Now().UnixNano() / 1e6

	client, err := geth_rpc.Dial(utils.Config.Eth1GethEndpoint)
	if err != nil {
		return nil, err
	}
	var raw json.RawMessage
	err = client.Call(&raw, "eth_getBlockByNumber", "pending", true)
	if err != nil {
		return nil, fmt.Errorf("error retrieving pending block data: %.1000s", err) // limit error message to 1000 characters
	}

	// var res map[string]interface{}
	// err = json.Unmarshal(raw, &res)
	// if err != nil {
	// 	return nil, err
	// }

	var header *geth_types.Header
	var body rpcBlock

	err = json.Unmarshal(raw, &header)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(raw, &body)
	if err != nil {
		return nil, err
	}
	txs := body.Transactions

	sort.Slice(txs, func(i, j int) bool {
		return txs[i].tx.GasPrice().Cmp(txs[j].tx.GasPrice()) > 0
	})
	if len(txs) > 1 {
		medianGasPrice := txs[len(txs)/2].tx.GasPrice()
		tailGasPrice := txs[len(txs)-1].tx.GasPrice()

		gpoData.Data.Rapid = medianGasPrice
		gpoData.Data.Fast = tailGasPrice
	} else {
		gpoData.Data.Rapid = new(big.Int)
		gpoData.Data.Fast = new(big.Int)
	}

	err = client.Call(&raw, "txpool_content")
	if err != nil {
		return nil, fmt.Errorf("error getting raw json data from txpool_content: %w", err)
	}

	txPoolContent := &TxPoolContent{}
	err = json.Unmarshal(raw, txPoolContent)
	if err != nil {
		return nil, fmt.Errorf("unmarshal txpoolcontent json error: %w", err)
	}

	pendingTxs := make([]*TxPoolContentTransaction, 0, len(txPoolContent.Pending))

	for _, account := range txPoolContent.Pending {
		lowestNonce := 9223372036854775807
		for n := range account {
			if n < int(lowestNonce) {
				lowestNonce = n
			}
		}

		pendingTxs = append(pendingTxs, account[lowestNonce])
	}
	sort.Slice(pendingTxs, func(i, j int) bool {
		return pendingTxs[i].GetGasPrice().Cmp(pendingTxs[j].GetGasPrice()) > 0
	})

	standardIndex := int(math.Max(float64(2*len(txs)), 500))

	slowIndex := int(math.Max(float64(5*len(txs)), 1000))
	if standardIndex < len(pendingTxs) {
		gpoData.Data.Standard = pendingTxs[standardIndex].GetGasPrice()
	} else {
		gpoData.Data.Standard = header.BaseFee
	}

	if gpoData.Data.Standard.Cmp(header.BaseFee) < 0 {
		gpoData.Data.Standard = header.BaseFee
	}

	if slowIndex < len(pendingTxs) {
		gpoData.Data.Slow = pendingTxs[slowIndex].GetGasPrice()
	} else {
		gpoData.Data.Slow = header.BaseFee
	}

	if gpoData.Data.Slow.Cmp(header.BaseFee) < 0 {
		gpoData.Data.Slow = header.BaseFee
	}

	err = bt.SaveGasNowHistory(gpoData.Data.Slow, gpoData.Data.Standard, gpoData.Data.Fast, gpoData.Data.Rapid)
	if err != nil {
		logrus.WithError(err).Error("error updating gas now history")
	}

	gpoData.Data.Price = price.GetPrice(utils.Config.Frontend.ElCurrency, "USD")
	gpoData.Data.Currency = "USD"

	// gpoData.RapidUSD = gpoData.Rapid * 21000 * params.GWei / params.Ether * usd
	// gpoData.FastUSD = gpoData.Fast * 21000 * params.GWei / params.Ether * usd
	// gpoData.StandardUSD = gpoData.Standard * 21000 * params.GWei / params.Ether * usd
	// gpoData.SlowUSD = gpoData.Slow * 21000 * params.GWei / params.Ether * usd
	return gpoData, nil
}