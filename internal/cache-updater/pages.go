package cacheupdater


func indexPageDataUpdater(wg *sync.WaitGroup, bt *db.Bigtable) {
	firstRun := true

	for {
		log.Infof("updating index page data")
		start := time.Now()
		data, err := getIndexPageData(bt)
		if err != nil {
			log.Errorf("error retrieving index page data: %v", err)
			time.Sleep(time.Second * 10)
			continue
		}
		log.WithFields(logger.Fields{
			"genesis": data.Genesis, 
			"currentEpoch": data.CurrentEpoch, 
			"networkName": data.NetworkName, 
			"networkStartTs": data.NetworkStartTs,
		}).Infof("index page data update completed in %v", time.Since(start))

		cacheKey := fmt.Sprintf("%d:frontend:indexPageData", utils.Config.Chain.ClConfig.DepositChainID)
		err = cache.TieredCache.Set(cacheKey, data, utils.Day)
		if err != nil {
			log.Errorf("error caching indexPageData: %v", err)
		}
		if firstRun {
			log.Info("initialized index page updater")
			wg.Done()
			firstRun = false
		}
		ReportStatus(true, "indexPageDataUpdater", "Running", nil)
		time.Sleep(time.Second * 10)
	}
}


func slotVizUpdater(wg *sync.WaitGroup) {
	firstRun := true

	for {
		latestEpoch := LatestEpoch()
		epochData, err := db.GetSlotVizData(latestEpoch)
		if err != nil {
			log.Errorf("error retrieving slot viz data from database: %v latest epoch: %v", err, latestEpoch)
		} else {
			cacheKey := fmt.Sprintf("%d:frontend:slotVizMetrics", utils.Config.Chain.ClConfig.DepositChainID)
			err = cache.TieredCache.Set(cacheKey, epochData, utils.Day)
			if err != nil {
				log.Errorf("error caching slotVizMetrics: %v", err)
			}
			if firstRun {
				log.Info("initialized slotViz metrics")
				wg.Done()
				firstRun = false
			}
		}
		ReportStatus(true, "slotVizUpdater", "Running", nil)
		time.Sleep(time.Second)
	}
}

func getIndexPageData(bt *db.Bigtable) (*types.IndexPageData, error) {
	currency := utils.Config.Frontend.MainCurrency

	data := &types.IndexPageData{}
	data.Mainnet = utils.Config.Chain.ClConfig.ConfigName == "mainnet"
	data.NetworkName = utils.Config.Chain.ClConfig.ConfigName
	data.DepositContract = utils.Config.Chain.ClConfig.DepositContractAddress

	var epoch uint64
	err := db.ReaderDb.Get(&epoch, "SELECT COALESCE(MAX(epoch), 0) FROM epochs")
	if err != nil {
		return nil, fmt.Errorf("error retrieving latest epoch from the database: %v", err)
	}
	data.CurrentEpoch = epoch

	cutoffSlot := utils.TimeToSlot(uint64(time.Now().Add(time.Second * 10).Unix()))

	// If we are before the genesis block show the first 20 slots by default
	startSlotTime := utils.SlotToTime(0)
	genesisTransition := utils.SlotToTime(160)
	now := time.Now()

	// run deposit query until the Genesis period is over
	if now.Before(genesisTransition) || startSlotTime == time.Unix(0, 0) {
		if cutoffSlot < 15 {
			cutoffSlot = 15
		}
		type Deposit struct {
			Total   uint64    `db:"total"`
			BlockTs time.Time `db:"block_ts"`
		}

		deposit := Deposit{}
		err = db.ReaderDb.Get(&deposit, `
			SELECT COUNT(*) as total, COALESCE(MAX(block_ts),NOW()) AS block_ts
			FROM (
				SELECT publickey, SUM(amount) AS amount, MAX(block_ts) as block_ts
				FROM eth1_deposits
				WHERE valid_signature = true
				GROUP BY publickey
				HAVING SUM(amount) >= 32e9
			) a`)
		if err != nil {
			return nil, fmt.Errorf("error retrieving eth1 deposits: %v", err)
		}

		if deposit.Total == 0 { // see if there are any genesis validators
			err = db.ReaderDb.Get(&deposit.Total, "SELECT COALESCE(MAX(validatorindex), 0) FROM validators")
			if err != nil {
				return nil, fmt.Errorf("error retrieving max validator index: %v", err)
			}

			if deposit.Total > 0 {
				deposit.Total = (deposit.Total + 1) * 32
				deposit.BlockTs = time.Now()
			}
		}

		data.DepositThreshold = float64(utils.Config.Chain.ClConfig.MinGenesisActiveValidatorCount) * 32
		data.DepositedTotal = float64(deposit.Total)

		data.ValidatorsRemaining = (data.DepositThreshold - data.DepositedTotal) / 32
		// genesisDelay := time.Duration(int64(utils.Config.Chain.ClConfig.GenesisDelay) * 1000 * 1000 * 1000) // convert seconds to nanoseconds

		minGenesisTime := time.Unix(int64(utils.Config.Chain.ClConfig.MinGenesisTime), 0)

		data.MinGenesisTime = minGenesisTime.Unix()
		data.NetworkStartTs = minGenesisTime.Add(time.Second * time.Duration(utils.Config.Chain.ClConfig.GenesisDelay)).Unix()
	}

	// has genesis occurred
	if now.After(startSlotTime) {
		data.Genesis = true
	} else {
		data.Genesis = false
	}
	// show the transition view one hour before the first slot and until epoch 30 is reached
	if now.Add(utils.Day).After(startSlotTime) && now.Before(genesisTransition) {
		data.GenesisPeriod = true
	} else {
		data.GenesisPeriod = false
	}

	if startSlotTime == time.Unix(0, 0) {
		data.Genesis = false
	}

	var scheduledCount uint8
	err = db.WriterDb.Get(&scheduledCount, `
		select count(*) from blocks where status = '0' and epoch = $1;
	`, epoch)
	if err != nil {
		return nil, fmt.Errorf("error retrieving scheduledCount from blocks: %v", err)
	}
	data.ScheduledCount = scheduledCount

	latestFinalizedEpoch := LatestFinalizedEpoch()
	var epochs []*types.IndexPageDataEpochs
	err = db.ReaderDb.Select(&epochs, `SELECT epoch, finalized , eligibleether, globalparticipationrate, votedether FROM epochs ORDER BY epochs DESC LIMIT 15`)
	if err != nil {
		return nil, fmt.Errorf("error retrieving index epoch data: %v", err)
	}
	epochsMap := make(map[uint64]bool)
	for _, epoch := range epochs {
		epoch.Ts = utils.EpochToTime(epoch.Epoch)
		epoch.FinalizedFormatted = utils.FormatYesNo(epoch.Finalized)
		epoch.VotedEtherFormatted = utils.FormatBalance(epoch.VotedEther, currency)
		epoch.EligibleEtherFormatted = utils.FormatEligibleBalance(epoch.EligibleEther, currency)
		epoch.GlobalParticipationRateFormatted = utils.FormatGlobalParticipationRate(epoch.VotedEther, epoch.GlobalParticipationRate, currency)
		epochsMap[epoch.Epoch] = true
	}

	var blocks []*types.IndexPageDataBlocks
	err = db.ReaderDb.Select(&blocks, `
		SELECT
			blocks.epoch,
			blocks.slot,
			blocks.proposer,
			blocks.blockroot,
			blocks.parentroot,
			blocks.attestationscount,
			blocks.depositscount,
			COALESCE(blocks.withdrawalcount,0) as withdrawalcount, 
			blocks.voluntaryexitscount,
			blocks.proposerslashingscount,
			blocks.attesterslashingscount,
			blocks.status,
			COALESCE(blocks.exec_block_number, 0) AS exec_block_number,
			COALESCE(validator_names.name, '') AS name
		FROM blocks 
		LEFT JOIN validators ON blocks.proposer = validators.validatorindex
		LEFT JOIN validator_names ON validators.pubkey = validator_names.publickey
		WHERE blocks.slot < $1
		ORDER BY blocks.slot DESC LIMIT 20`, cutoffSlot)
	if err != nil {
		return nil, fmt.Errorf("error retrieving index block data: %v", err)
	}

	blocksMap := make(map[uint64]*types.IndexPageDataBlocks)
	for _, block := range blocks {
		if blocksMap[block.Slot] == nil || len(block.BlockRoot) > len(blocksMap[block.Slot].BlockRoot) {
			blocksMap[block.Slot] = block
		}
	}
	blocks = make([]*types.IndexPageDataBlocks, 0, len(blocks))
	for _, b := range blocksMap {
		blocks = append(blocks, b)
	}
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Slot > blocks[j].Slot
	})
	if err := enrichExecutionRanks(blocks, bt); err != nil {
		return nil, fmt.Errorf("error retrieving block ranks from BigTable: %v", err)
	}
	data.Blocks = blocks

	if len(data.Blocks) > 15 {
		data.Blocks = data.Blocks[:15]
	}

	for _, block := range data.Blocks {
		block.StatusFormatted = utils.FormatBlockStatus(block.Status, block.Slot)
		block.ProposerFormatted = utils.FormatValidatorWithName(block.Proposer, block.ProposerName)
		block.BlockRootFormatted = fmt.Sprintf("%x", block.BlockRoot)

		if !epochsMap[block.Epoch] {
			epochs = append(epochs, &types.IndexPageDataEpochs{
				Epoch:                            block.Epoch,
				Ts:                               utils.EpochToTime(block.Epoch),
				Finalized:                        false,
				FinalizedFormatted:               utils.FormatYesNo(false),
				EligibleEther:                    0,
				EligibleEtherFormatted:           utils.FormatEligibleBalance(0, currency),
				GlobalParticipationRate:          0,
				GlobalParticipationRateFormatted: utils.FormatGlobalParticipationRate(0, 1, ""),
				VotedEther:                       0,
				VotedEtherFormatted:              "",
			})
			epochsMap[block.Epoch] = true
		}
	}
	sort.Slice(epochs, func(i, j int) bool {
		return epochs[i].Epoch > epochs[j].Epoch
	})

	data.Epochs = epochs

	if len(data.Epochs) > 15 {
		data.Epochs = data.Epochs[:15]
	}

	// Fetch recent slots data for the dashboard, similar to blocks
	var slots []*types.IndexPageDataSlots
	err = db.ReaderDb.Select(&slots, `
		SELECT
			blocks.epoch,
			blocks.slot,
			blocks.proposer,
			blocks.blockroot,
			blocks.attestationscount,
			blocks.depositscount,
			COALESCE(blocks.withdrawalcount,0) as withdrawalcount, 
			blocks.voluntaryexitscount,
			blocks.status,
			COALESCE(blocks.syncaggregate_participation, 0) as syncaggregate_participation,
			COALESCE(blocks.exec_block_number, 0) AS exec_block_number,
			COALESCE(validator_names.name, '') AS name
		FROM blocks 
		LEFT JOIN validators ON blocks.proposer = validators.validatorindex
		LEFT JOIN validator_names ON validators.pubkey = validator_names.publickey
		WHERE blocks.slot < $1
		ORDER BY blocks.slot DESC LIMIT 15`, cutoffSlot)
	if err != nil {
		return nil, fmt.Errorf("error retrieving index slot data: %v", err)
	}

	// Process slots data - format the fields for display
	for _, slot := range slots {
		slot.Ts = utils.SlotToTime(slot.Slot)
		slot.StatusFormatted = utils.FormatBlockStatus(slot.Status, slot.Slot)
		slot.ProposerFormatted = utils.FormatValidatorWithName(slot.Proposer, slot.ProposerName)
		slot.BlockRootFormatted = fmt.Sprintf("%x", slot.BlockRoot)
		// TODO: For EthPar parallel blocks, query and set ParallelBlocksCount
		// This would require a query like:
		// SELECT COUNT(*) FROM parallel_blocks WHERE slot = slot.Slot
		slot.ParallelBlocksCount = 0 // Default to 0 for now
	}
	data.Slots = slots

	if data.GenesisPeriod {
		for _, blk := range blocks {
			if blk.Status != 0 {
				data.CurrentSlot = blk.Slot
			}
		}
	} else if len(blocks) > 0 {
		data.CurrentSlot = blocks[0].Slot
	}

	for _, block := range data.Blocks {
		block.Ts = utils.SlotToTime(block.Slot)
	}
	queueCount := struct {
		EnteringValidators uint64 `db:"entering_validators_count"`
		ExitingValidators  uint64 `db:"exiting_validators_count"`
	}{}
	err = db.ReaderDb.Get(&queueCount, "SELECT entering_validators_count, exiting_validators_count FROM queue ORDER BY ts DESC LIMIT 1")
	if err != nil && err != sql.ErrNoRows {
		return nil, fmt.Errorf("error retrieving validator queue count: %v", err)
	}
	data.EnteringValidators = queueCount.EnteringValidators
	data.ExitingValidators = queueCount.ExitingValidators

	var epochLowerBound uint64
	if epochLowerBound = 0; epoch > 1600 {
		epochLowerBound = epoch - 1600
	}
	var epochHistory []*types.IndexPageEpochHistory
	err = db.WriterDb.Select(&epochHistory, "SELECT epoch, eligibleether, validatorscount, (epoch <= $3) AS finalized, averagevalidatorbalance FROM epochs WHERE epoch < $1 and epoch > $2 ORDER BY epoch", epoch, epochLowerBound, latestFinalizedEpoch)
	if err != nil {
		return nil, fmt.Errorf("error retrieving staked ether history: %v", err)
	}

	if len(epochHistory) > 0 {
		for i := len(epochHistory) - 1; i >= 0; i-- {
			if epochHistory[i].Finalized {
				data.CurrentFinalizedEpoch = epochHistory[i].Epoch
				data.FinalityDelay = FinalizationDelay()
				data.AverageBalance = string(utils.FormatBalance(uint64(epochHistory[i].AverageValidatorBalance), currency))
				break
			}
		}

		data.StakedEther = string(utils.FormatBalance(epochHistory[len(epochHistory)-1].EligibleEther, currency))
		data.ActiveValidators = epochHistory[len(epochHistory)-1].ValidatorsCount
	}

	data.StakedEtherChartData = make([][]float64, len(epochHistory))
	data.ActiveValidatorsChartData = make([][]float64, len(epochHistory))
	for i, history := range epochHistory {
		data.StakedEtherChartData[i] = []float64{float64(utils.EpochToTime(history.Epoch).Unix() * 1000), utils.ClToMainCurrency(history.EligibleEther).InexactFloat64()}
		data.ActiveValidatorsChartData[i] = []float64{float64(utils.EpochToTime(history.Epoch).Unix() * 1000), float64(history.ValidatorsCount)}
	}

	data.Title = template.HTML(utils.Config.Frontend.SiteTitle)
	data.Subtitle = template.HTML(utils.Config.Frontend.SiteSubtitle)

	return data, nil
}

func enrichExecutionRanks(blocks []*types.IndexPageDataBlocks, bt *db.Bigtable) error {
    for _, b := range blocks {
        if b.ExecutionBlockNumber == 0 {
            continue
        }

        ranks, err := bt.GetAvailableRanksForExecBlock(uint64(b.ExecutionBlockNumber))
        if err != nil {
            return fmt.Errorf("failed to get ranks for exec block %d: %w", b.ExecutionBlockNumber, err)
        }

        b.ExecutionRanks = ranks
    }
    return nil
}
