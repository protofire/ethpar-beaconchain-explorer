package cacheupdater


func statsUpdater(pg *db.Postgres, params *config.NetworkConfig, wg *sync.WaitGroup) {
	sleepDuration := time.Duration(time.Duration(utils.Config.Chain.ClConfig.SlotsPerEpoch*utils.Config.Chain.ClConfig.SecondsPerSlot) * time.Second)

	log.Infof("sleep duration is %v", sleepDuration)
	firstrun := true
	for {
		latestEpoch := LatestEpoch()

		now := time.Now()
		statResult, err := calculateStats(pg, params)
		if err != nil {
			log.WithField("epoch", latestEpoch).Errorf("error updating stats: %v", err)
			time.Sleep(sleepDuration)
			continue
		}
		log.WithField("epoch", latestEpoch).WithField("duration", time.Since(now)).Info("stats update completed")

		cacheKey := fmt.Sprintf("%d:frontend:latestStats", utils.Config.Chain.ClConfig.DepositChainID)
		err = cache.TieredCache.Set(cacheKey, statResult, utils.Day)
		if err != nil {
			log.Errorf("error caching latestStats: %v", err)
		}
		if firstrun {
			wg.Done()
			firstrun = false
		}
		ReportStatus(pg, true, "statsUpdater", "Running", nil)
		time.Sleep(sleepDuration)
	}
}

func calculateStats(pg *db.Postgres, params *config.NetworkConfig) (*types.Stats, error) {
	stats := types.Stats{}

	topDeposits, err := pg.GetTop5Eth1Depositors()
	if err != nil {
		return nil, err
	}
	stats.TopDepositors = &topDeposits
	invalidCount, err := pg.CountInvalidDeposits()
	if err != nil {
		return nil, err
	}
	stats.InvalidDepositCount = &invalidCount

	uniqueValidatorCount, err := pg.CountUniqueValidators()
	if err != nil {
		return nil, err
	}
	stats.UniqueValidatorCount = &uniqueValidatorCount

	totalValidatorCount, err := pg.GetTotalValidatorsCount()
	if err != nil {
		log.WithError(err).Error("error getting total validator count")
	}
	stats.TotalValidatorCount = &totalValidatorCount

	activeValidatorCount, err := pg.GetActiveValidatorCount()
	if err != nil {
		log.WithError(err).Error("error getting active validator count")
	}

	stats.ActiveValidatorCount = &activeValidatorCount

	pendingValidatorCount, err := pg.GetPendingValidatorCount()
	if err != nil {
		log.WithError(err).Error("error getting pending validator count")
	}

	stats.PendingValidatorCount = &pendingValidatorCount

	validatorChurnLimit, err := getValidatorChurnLimit(params, activeValidatorCount)
	if err != nil {
		log.WithError(err).Error("error getting total validator churn limit")
	}

	stats.ValidatorChurnLimit = &validatorChurnLimit

	epoch := LatestEpoch()
	validatorActivationChurnLimit, err := getValidatorActivationChurnLimit(params, activeValidatorCount, epoch)
	if err != nil {
		log.WithError(err).Error("error getting total validator churn limit")
	}

	stats.ValidatorActivationChurnLimit = &validatorActivationChurnLimit

	LatestValidatorWithdrawalIndex, err := pg.GetMostRecentWithdrawalValidator()
	if err != nil {
		log.WithError(err).Error("error getting most recent withdrawal validator index")
	}

	stats.LatestValidatorWithdrawalIndex = &LatestValidatorWithdrawalIndex

	WithdrawableValidatorCount, err := pg.GetWithdrawableValidatorCount(epoch)
	if err != nil {
		log.WithError(err).Error("error getting withdrawable validator count")
	}

	stats.WithdrawableValidatorCount = &WithdrawableValidatorCount

	PendingBLSChangeValidatorCount, err := pg.GetPendingBLSChangeValidatorCount()
	if err != nil {
		log.WithError(err).Error("error getting withdrawable validator count")
	}

	stats.PendingBLSChangeValidatorCount = &PendingBLSChangeValidatorCount

	TotalAmountWithdrawn, WithdrawalCount, err := pg.GetTotalAmountWithdrawn()
	if err != nil {
		log.WithError(err).Error("error getting total amount withdrawn")
	}
	stats.TotalAmountWithdrawn = &TotalAmountWithdrawn
	stats.WithdrawalCount = &WithdrawalCount

	TotalAmountDeposited, err := pg.GetTotalAmountDeposited()
	if err != nil {
		log.WithError(err).Error("error getting total deposited")
	}

	stats.TotalAmountDeposited = &TotalAmountDeposited

	BLSChangeCount, err := pg.GetBLSChangeCount()
	if err != nil {
		log.WithError(err).Error("error getting bls change count")
	}

	stats.BLSChangeCount = &BLSChangeCount

	return &stats, nil
}

// getValidatorActivationChurnLimit returns the rate at which validators can enter the system, see https://eips.ethereum.org/EIPS/eip-7514
func getValidatorActivationChurnLimit(params *config.NetworkConfig, validatorCount, epoch uint64) (uint64, error) {
	vcl, err := getValidatorChurnLimit(params, validatorCount)
	if err != nil {
		return 0, err
	}
	if params.Forks.DenebForkEpoch > epoch {
		return vcl, nil
	}
	if vcl > params.Validator.MaxPerEpochActivationChurnLimit {
		return params.Validator.MaxPerEpochActivationChurnLimit, nil
	}
	return vcl, nil
}

// getValidatorChurnLimit returns the rate at which validators can leave the system
func getValidatorChurnLimit(params *config.NetworkConfig, validatorCount uint64) (uint64, error) {
	min := params.Validator.MinPerEpochChurnLimit

	adaptable := uint64(0)
	if validatorCount > 0 {
		adaptable = validatorCount / params.Validator.ChurnLimitQuotient
	}

	if min > adaptable {
		return min, nil
	}

	return adaptable, nil
}