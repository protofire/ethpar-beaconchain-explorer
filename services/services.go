package services

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"math"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/cache"
	"github.com/protofire/ethpar-beaconchain-explorer/db"
	ethclients "github.com/protofire/ethpar-beaconchain-explorer/ethClients"
	"github.com/protofire/ethpar-beaconchain-explorer/price"
	"github.com/protofire/ethpar-beaconchain-explorer/ratelimit"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	itypes "github.com/gobitfly/eth-rewards/types"
	"github.com/shopspring/decimal"

	"github.com/ethereum/go-ethereum/common"
	"github.com/sirupsen/logrus"

	geth_types "github.com/ethereum/go-ethereum/core/types"
	geth_rpc "github.com/ethereum/go-ethereum/rpc"
)

// Init will initialize the services
func Init(client consensus.ConsensusClient, bt *db.Bigtable) {
	ready := &sync.WaitGroup{}
	ready.Add(1)
	go epochUpdater(ready)

	ready.Add(1)
	go slotUpdater(ready)

	ready.Add(1)
	go latestProposedSlotUpdater(ready)

	ready.Add(1)
	go latestBlockUpdater(ready, bt)

	ready.Add(1)
	go headBlockRootHashUpdater(ready)

	ready.Add(1)
	go slotVizUpdater(ready)

	ready.Add(1)
	go indexPageDataUpdater(ready, bt)

	ready.Add(1)
	go poolsUpdater(ready)

	ready.Add(1)
	go relaysUpdater(ready)

	ready.Add(1)
	go chartsPageDataUpdater(ready, client)

	ready.Add(1)
	go statsUpdater(ready)

	ready.Add(1)
	go mempoolUpdater(ready)

	ready.Add(1)
	go burnUpdater(ready, bt)

	ready.Add(1)
	go gasNowUpdater(ready, bt)

	ready.Add(1)
	go ethStoreStatisticsDataUpdater(ready)

	ready.Add(1)
	go startMonitoringService(ready, bt)

	ready.Add(1)
	go latestExportedStatisticDayUpdater(ready)

	if utils.Config.RatelimitUpdater.Enabled {
		go ratelimit.DBUpdater()
	}

	ready.Wait()
}

func InitNotificationSender() {
	log.Infof("starting notifications-sender")
	go notificationSender()
}

func InitNotificationCollector(pubkeyCachePath string, bt *db.Bigtable) {
	err := initPubkeyCache(pubkeyCachePath)
	if err != nil {
		log.Fatalf("error initializing pubkey cache path for notifications: %v", err)
	}

	go ethclients.Init()

	go notificationCollector(bt)
}

func getRelaysPageData() (*types.RelaysResp, error) {
	start := time.Now()
	defer func() {
		log.WithField("duration", time.Since(start)).Info("completed caching relays page data")
	}()
	var relaysData types.RelaysResp

	relaysData.LastUpdated = start

	networkParticipationQuery, err := db.ReaderDb.Preparex(`
		SELECT 
			(SELECT
				COUNT(DISTINCT block_slot) AS block_count
			FROM relays_blocks
			WHERE 
				block_slot > $1 AND 
				block_root NOT IN (SELECT bt.blockroot FROM blocks_tags bt WHERE bt.tag_id='invalid-relay-reward') 
			) / (MAX(blocks.slot) - $1)::float AS network_participation
		FROM blocks`)
	if err != nil {
		log.Errorf("failed to prepare networkParticipationQuery: %v", err)
		return nil, err
	}
	defer networkParticipationQuery.Close()

	overallStatsQuery, err := db.ReaderDb.Preparex(`
		WITH stats AS (
			SELECT 
				tag_id AS relay_id,
				COUNT(*) AS block_count,
				SUM(value) AS total_value,
				ROUND(avg(value)) AS avg_value,
				COUNT(DISTINCT builder_pubkey) AS unique_builders,
				MAX(value) AS max_value,
				(SELECT rb2.block_slot FROM relays_blocks rb2 WHERE rb2.value = MAX(rb.value) AND rb2.tag_id = rb.tag_id LIMIT 1) AS max_value_slot
			FROM relays_blocks rb
			WHERE 
				rb.block_slot > $1 AND 
				rb.block_root NOT IN (SELECT bt.blockroot FROM blocks_tags bt WHERE bt.tag_id='invalid-relay-reward') 
			GROUP BY tag_id 
		)
		SELECT 
			tags.metadata ->> 'name' AS "name",
			relays.public_link AS link,
			relays.is_censoring AS censors,
			relays.is_ethical AS ethical,
			stats.block_count / (SELECT MAX(blocks.slot) - $1 FROM blocks)::float AS network_usage,
			stats.relay_id,
			stats.block_count,
			stats.total_value,
			stats.avg_value,
			stats.unique_builders,
			stats.max_value,
			stats.max_value_slot
		FROM relays
		LEFT JOIN stats ON stats.relay_id = relays.tag_id
		LEFT JOIN tags ON tags.id = relays.tag_id 
		WHERE stats.relay_id = tag_id 
		ORDER BY stats.block_count DESC`)
	if err != nil {
		log.Errorf("failed to prepare overallStatsQuery: %v", err)
		return nil, err
	}
	defer overallStatsQuery.Close()

	dayInSlots := uint64(utils.Day/time.Second) / utils.Config.Chain.ClConfig.SecondsPerSlot

	tmp := [3]types.RelayInfoContainer{{Days: 7}, {Days: 31}, {Days: 180}}
	latest := LatestSlot()
	for i := 0; i < len(tmp); i++ {
		tmp[i].IsFirst = i == 0
		var forSlot uint64 = 0
		if latest > tmp[i].Days*dayInSlots {
			forSlot = latest - (tmp[i].Days * dayInSlots)
		}

		// calculate total adoption
		err = networkParticipationQuery.Get(&tmp[i].NetworkParticipation, forSlot)
		if err != nil {
			return nil, err
		}
		err = overallStatsQuery.Select(&tmp[i].RelaysInfo, forSlot)
		if err != nil {
			return nil, err
		}

	}
	relaysData.RelaysInfoContainers = tmp

	var forSlot uint64 = 0
	if latest > (14 * dayInSlots) {
		forSlot = latest - (14 * dayInSlots)
	}
	err = db.ReaderDb.Select(&relaysData.TopBuilders, `
		select 
			builder_pubkey,
			SUM(c) as c,
			jsonb_agg(tags.metadata) as tags,
			max(latest_slot) as latest_slot
		from (
			select 
				builder_pubkey,
				count(*) as c,
				tag_id,
				(
					select block_slot
					from relays_blocks rb2
					where
						rb2.builder_pubkey = rb.builder_pubkey
					order by block_slot desc
					limit 1
				) as latest_slot
			from (
				select builder_pubkey, tag_id
				from relays_blocks
				where block_slot > $1
				order by block_slot desc) rb
			group by builder_pubkey, tag_id 
		) foo
		left join tags on tags.id = foo.tag_id
		group by builder_pubkey 
		order by c desc`, forSlot)
	if err != nil {
		log.Errorf("failed to get builder ranking %v", err)
		return nil, err
	}

	err = db.ReaderDb.Select(&relaysData.RecentBlocks, `
		select
			jsonb_agg(tags.metadata order by id) as tags,
			max(relays_blocks.value) as value,
			relays_blocks.block_slot as slot,
			relays_blocks.builder_pubkey as builder_pubkey,
			relays_blocks.proposer_fee_recipient as proposer_fee_recipient,
			validators.validatorindex as proposer,
			encode(exec_extra_data, 'hex') as block_extra_data
		from (
			select blockroot, exec_extra_data
			from blocks
			where blockroot in (
				select rb.block_root
				from relays_blocks rb
			) 
			order by blocks.slot desc
			limit 15
		) as blocks
		left join relays_blocks
			on relays_blocks.block_root = blocks.blockroot
		left join tags 
			on tags.id = relays_blocks.tag_id 
		left join validators
			on validators.pubkey = relays_blocks.proposer_pubkey  
		where validators.validatorindex is not null
		group by 
			blockroot, 
			relays_blocks.block_slot,
			relays_blocks.builder_pubkey,
			relays_blocks.proposer_fee_recipient,
			blocks.exec_extra_data,
			validators.validatorindex 
		order by relays_blocks.block_slot desc`)
	if err != nil {
		log.Errorf("failed to get latest blocks for relays page %v", err)
		return nil, err
	}

	err = db.ReaderDb.Select(&relaysData.TopBlocks, `
		select
			jsonb_agg(tags.metadata order by id) as tags,
			max(relays_blocks.value) as value,
			relays_blocks.block_slot as slot,
			relays_blocks.builder_pubkey as builder_pubkey,
			relays_blocks.proposer_fee_recipient as proposer_fee_recipient,
			validators.validatorindex as proposer,
			encode(exec_extra_data, 'hex') as block_extra_data
		from (
			select value, block_slot, builder_pubkey, proposer_fee_recipient, block_root, tag_id, proposer_pubkey
			from relays_blocks
			where relays_blocks.block_root not in (select bt.blockroot from blocks_tags bt where bt.tag_id='invalid-relay-reward') 
			order by relays_blocks.value desc
			limit 15
		) as relays_blocks 
		left join blocks
			on relays_blocks.block_root = blocks.blockroot
		left join tags 
			on tags.id = relays_blocks.tag_id 
		left join validators
			on validators.pubkey = relays_blocks.proposer_pubkey  
		group by 
			blockroot, 
			relays_blocks.block_slot,
			relays_blocks.builder_pubkey,
			relays_blocks.proposer_fee_recipient,
			blocks.exec_fee_recipient,
			blocks.exec_extra_data,
			validators.validatorindex 
		order by value desc`)
	if err != nil {
		log.Errorf("failed to get top blocks for relays page %v", err)
		return nil, err
	}

	return &relaysData, nil
}

func relaysUpdater(wg *sync.WaitGroup) {
	firstRun := true

	for {
		data, err := getRelaysPageData()
		if err != nil {
			log.Errorf("error retrieving relays page data: %v", err)
			time.Sleep(time.Second * 10)
			continue
		}

		cacheKey := fmt.Sprintf("%d:frontend:relaysData", utils.Config.Chain.ClConfig.DepositChainID)
		err = cache.TieredCache.Set(cacheKey, data, utils.Day)
		if err != nil {
			log.Errorf("error caching relaysData: %v", err)
		}
		if firstRun {
			log.Info("initialized relays page updater")
			wg.Done()
			firstRun = false
		}
		ReportStatus(true, "relaysUpdater", "Running", nil)
		time.Sleep(time.Minute)
	}
}

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

func poolsUpdater(wg *sync.WaitGroup) {
	firstRun := true

	for {
		data, err := getPoolsPageData()
		if err != nil {
			log.Errorf("error retrieving pools page data: %v", err)
			time.Sleep(time.Second * 10)
			continue
		}

		cacheKey := fmt.Sprintf("%d:frontend:poolsData", utils.Config.Chain.ClConfig.DepositChainID)
		err = cache.TieredCache.Set(cacheKey, data, utils.Day)
		if err != nil {
			log.Errorf("error caching poolsData: %v", err)
		}
		if firstRun {
			log.Info("initialized pools page updater")
			wg.Done()
			firstRun = false
		}
		ReportStatus(true, "poolsUpdater", "Running", nil)
		time.Sleep(time.Minute * 10)
	}
}

func getPoolsPageData() (*types.PoolsResp, error) {
	var poolData types.PoolsResp
	err := db.ReaderDb.Select(&poolData.PoolInfos, `
	select pool as name, validators as count, apr * 100 as avg_performance_1d, (select avg(apr) from historical_pool_performance as hpp1 where hpp1.pool = hpp.pool AND hpp1.day > hpp.day - 7) * 100 as avg_performance_7d, (select avg(apr) from historical_pool_performance as hpp1 where hpp1.pool = hpp.pool AND hpp1.day > hpp.day - 31) * 100 as avg_performance_31d from historical_pool_performance hpp where day = (select max(day) from historical_pool_performance) order by validators desc;
	`)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	ethstoreData := &types.PoolInfo{}
	err = db.ReaderDb.Get(ethstoreData, `
	select 'ETH.STORE' as name, -1 as count, apr * 100 as avg_performance_1d, (select avg(apr) from eth_store_stats as e1 where e1.validator = -1 AND e1.day > e.day - 7) * 100 as avg_performance_7d, (select avg(apr) from eth_store_stats as e1 where e1.validator = -1 AND e1.day > e.day - 31) * 100 as avg_performance_31d from eth_store_stats e where day = (select max(day) from eth_store_stats) LIMIT 1;
	`)
	if err != nil && err != sql.ErrNoRows {
		return nil, err
	}

	for _, pool := range poolData.PoolInfos {
		pool.EthstoreComparison1d = pool.AvgPerformance1d*100/ethstoreData.AvgPerformance1d - 100
		pool.EthstoreComparison7d = pool.AvgPerformance7d*100/ethstoreData.AvgPerformance7d - 100
		pool.EthstoreComparison31d = pool.AvgPerformance31d*100/ethstoreData.AvgPerformance31d - 100
	}
	poolData.PoolInfos = append([]*types.PoolInfo{ethstoreData}, poolData.PoolInfos...)

	return &poolData, nil
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

func ethStoreStatisticsDataUpdater(wg *sync.WaitGroup) {
	firstRun := true
	for {
		data, err := getEthStoreStatisticsData()
		if err != nil {
			log.Errorf("error retrieving ETH.STORE statistics data: %v", err)
			time.Sleep(time.Second * 10)
			continue
		}

		cacheKey := fmt.Sprintf("%d:frontend:ethStoreStatistics", utils.Config.Chain.ClConfig.DepositChainID)
		err = cache.TieredCache.Set(cacheKey, data, utils.Day)
		if err != nil {
			log.Errorf("error caching ETH.STORE statistics data: %v", err)
		}
		if firstRun {
			firstRun = false
			wg.Done()
			log.Info("initialized ETH.STORE statistics data updater")
		}
		ReportStatus(true, "ethStoreStatistics", "Running", nil)
		time.Sleep(time.Second * 90)
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

func getEthStoreStatisticsData() (*types.EthStoreStatistics, error) {
	var ethStoreDays []types.EthStoreDay
	err := db.ReaderDb.Select(&ethStoreDays, `
		SELECT
			day,
			apr,
			effective_balances_sum_wei,
			total_rewards_wei
		FROM eth_store_stats
		WHERE validator = -1
		ORDER BY DAY ASC`)
	if err != nil {
		return nil, fmt.Errorf("error getting eth store stats from db: %v", err)
	}
	daysLastIndex := len(ethStoreDays) - 1

	if daysLastIndex < 0 {
		return nil, fmt.Errorf("no eth store stats found in db")
	}

	effectiveBalances := [][]float64{}
	totalRewards := [][]float64{}
	aprs := [][]float64{}
	for _, stat := range ethStoreDays {
		ts := float64(utils.EpochToTime(stat.Day*utils.EpochsPerDay()).Unix()) * 1000

		effectiveBalances = append(effectiveBalances, []float64{
			ts,
			stat.EffectiveBalancesSum.Div(decimal.NewFromInt(1e18)).Round(0).InexactFloat64(),
		})

		totalRewards = append(totalRewards, []float64{
			ts,
			stat.TotalRewardsWei.Div(decimal.NewFromInt(1e18)).Round(6).InexactFloat64(),
		})

		aprs = append(aprs, []float64{
			ts,
			stat.APR.Mul(decimal.NewFromInt(100)).Round(3).InexactFloat64(),
		})
	}

	data := &types.EthStoreStatistics{
		EffectiveBalances:         effectiveBalances,
		TotalRewards:              totalRewards,
		APRs:                      aprs,
		ProjectedAPR:              ethStoreDays[daysLastIndex].APR.Mul(decimal.NewFromInt(100)).InexactFloat64(),
		StartEpoch:                ethStoreDays[daysLastIndex].Day * utils.EpochsPerDay(),
		YesterdayRewards:          ethStoreDays[daysLastIndex].TotalRewardsWei.Div(decimal.NewFromInt(1e18)).InexactFloat64(),
		YesterdayEffectiveBalance: ethStoreDays[daysLastIndex].EffectiveBalancesSum.Div(decimal.NewFromInt(1e18)).InexactFloat64(),
		YesterdayTs:               utils.EpochToTime(ethStoreDays[daysLastIndex].Day * utils.EpochsPerDay()).Unix(),
	}

	return data, nil
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

		// if minGenesisTime.Before(time.Now()) {
		// 	minGenesisTime = time.Now()
		// }

		// log.Infof("start ts is :%v", data.NetworkStartTs)

		// enough deposits
		// if data.DepositedTotal > data.DepositThreshold {
		// 	if depositThresholdReached.Load() == nil {
		// 		eth1BlockDepositReached.Store(*threshold)
		// 		depositThresholdReached.Store(true)
		// 	}
		// 	eth1Block := eth1BlockDepositReached.Load().(time.Time)

		// 	if !(startSlotTime == time.Unix(0, 0)) && eth1Block.Add(genesisDelay).After(minGenesisTime) {
		// 		// Network starts after min genesis time
		// 		data.NetworkStartTs = eth1Block.Add(time.Second * time.Duration(utils.Config.Chain.ClConfig.GenesisDelay)).Unix()
		// 	} else {
		// 		data.NetworkStartTs = minGenesisTime.Unix()
		// 	}
		// }
		// log.Infof("start ts is :%v", data.NetworkStartTs)

		latestChartsPageData := LatestChartsPageData()
		if len(latestChartsPageData) != 0 {
			for _, c := range latestChartsPageData {
				if c.Path == "deposits" {
					data.DepositChart = c
				} else if c.Path == "deposits_distribution" {
					data.DepositDistribution = c
				}
			}
		}
	}
	if data.DepositChart != nil && data.DepositChart.Data != nil && data.DepositChart.Data.Series != nil {
		series := data.DepositChart.Data.Series
		if len(series) > 2 {
			points, ok := series[1].Data.([][]float64)
			if !ok {
				log.Errorf("error parsing deposit chart data could not convert  series to [][]float64 series: %+v", series[1].Data)
			} else {
				periodDays := float64(len(points))
				avgDepositPerDay := data.DepositedTotal / periodDays
				daysUntilThreshold := (data.DepositThreshold - data.DepositedTotal) / avgDepositPerDay
				estimatedTimeToThreshold := time.Now().Add(utils.Day * time.Duration(daysUntilThreshold))
				if estimatedTimeToThreshold.After(time.Unix(data.NetworkStartTs, 0)) {
					data.NetworkStartTs = estimatedTimeToThreshold.Add(time.Duration(int64(utils.Config.Chain.ClConfig.GenesisDelay) * 1000 * 1000 * 1000)).Unix()
				}
			}
		}
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
	}
	
	log.Infof("About to enrich slots with execution ranks, slots count: %d", len(slots))
	// Enrich slots with execution ranks data from BigTable
	if err := enrichSlotsExecutionRanks(slots, bt); err != nil {
		log.Errorf("error enriching slots with execution ranks: %v", err)
	} else {
		log.Infof("Successfully enriched slots with execution ranks")
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

func enrichSlotsExecutionRanks(slots []*types.IndexPageDataSlots, bt *db.Bigtable) error {
	log.Infof("enrichSlotsExecutionRanks: Processing %d slots", len(slots))
	
	for i, slot := range slots {
		log.Infof("enrichSlotsExecutionRanks: Processing slot %d (index %d), ExecutionBlockNumber: %d", slot.Slot, i, slot.ExecutionBlockNumber)
		
		if slot.ExecutionBlockNumber == 0 {
			log.Infof("enrichSlotsExecutionRanks: Slot %d has no execution block", slot.Slot)
			slot.ExecutionRanks = []types.ExecutionRankData{}
			slot.ParallelBlocksCount = 0
			continue
		}

		// Get available ranks for this execution block number
		log.Infof("enrichSlotsExecutionRanks: Getting ranks for exec block %d", slot.ExecutionBlockNumber)
		ranks, err := bt.GetAvailableRanksForExecBlock(uint64(slot.ExecutionBlockNumber))
		if err != nil {
			log.Errorf("enrichSlotsExecutionRanks: failed to get ranks for exec block %d: %v", slot.ExecutionBlockNumber, err)
			slot.ExecutionRanks = []types.ExecutionRankData{}
			slot.ParallelBlocksCount = 0
			continue
		}

		log.Infof("enrichSlotsExecutionRanks: Found %d ranks for exec block %d: %v", len(ranks), slot.ExecutionBlockNumber, ranks)

		// Convert ranks to ExecutionRankData
		var executionRanks []types.ExecutionRankData
		for _, rank := range ranks {
			executionRanks = append(executionRanks, types.ExecutionRankData{
				Rank:        rank,
				BlockNumber: uint64(slot.ExecutionBlockNumber),
				GasUsed:     0, // We'll fetch this separately if needed
			})
		}

		slot.ExecutionRanks = executionRanks
		slot.ParallelBlocksCount = uint64(len(executionRanks))
		log.Infof("enrichSlotsExecutionRanks: Set %d execution ranks for slot %d", len(executionRanks), slot.Slot)
	}
	return nil
}

// LatestEpoch will return the latest epoch
func LatestEpoch() uint64 {
	cacheKey := fmt.Sprintf("%d:frontend:latestEpoch", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetUint64WithLocalTimeout(cacheKey, time.Second*5); err == nil {
		return wanted
	} else {
		log.Errorf("error retrieving latestEpoch from cache: %v", err)
	}

	return 0
}

func LatestNodeEpoch() uint64 {
	cacheKey := fmt.Sprintf("%d:frontend:latestNodeEpoch", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetUint64WithLocalTimeout(cacheKey, time.Second*5); err == nil {
		return wanted
	} else {
		log.Errorf("error retrieving latestNodeEpoch from cache: %v", err)
	}

	return 0
}

func LatestNodeFinalizedEpoch() uint64 {
	cacheKey := fmt.Sprintf("%d:frontend:latestNodeFinalizedEpoch", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetUint64WithLocalTimeout(cacheKey, time.Second*5); err == nil {
		return wanted
	} else {
		log.Errorf("error retrieving latestNodeFinalizedEpoch from cache: %v", err)
	}

	return 0
}

// LatestFinalizedEpoch will return the most recent epoch that has been finalized.
func LatestFinalizedEpoch() uint64 {
	cacheKey := fmt.Sprintf("%d:frontend:latestFinalized", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetUint64WithLocalTimeout(cacheKey, time.Second*5); err == nil {
		return wanted
	} else {
		log.Errorf("error retrieving latestFinalized for key: %v from cache: %v", cacheKey, err)
	}
	return 0
}

// LatestSlot will return the latest slot
func LatestSlot() uint64 {
	cacheKey := fmt.Sprintf("%d:frontend:slot", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetUint64WithLocalTimeout(cacheKey, time.Second*5); err == nil {
		return wanted
	} else {
		log.Errorf("error retrieving latest slot from cache: %v", err)
	}
	return 0
}

// FinalizationDelay will return the current Finalization Delay
func FinalizationDelay() uint64 {
	return LatestNodeEpoch() - LatestNodeFinalizedEpoch()
}

// LatestProposedSlot will return the latest proposed slot
func LatestProposedSlot() uint64 {
	cacheKey := fmt.Sprintf("%d:frontend:latestProposedSlot", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetUint64WithLocalTimeout(cacheKey, time.Second*5); err == nil {
		return wanted
	} else {
		log.Errorf("error retrieving latestProposedSlot from cache: %v", err)
	}
	return 0
}

func LatestMempoolTransactions() *types.RawMempoolResponse {
	wanted := &types.RawMempoolResponse{}
	cacheKey := fmt.Sprintf("%d:frontend:mempool", utils.Config.Chain.ClConfig.DepositChainID)
	if wanted, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Minute, wanted); err == nil {
		return wanted.(*types.RawMempoolResponse)
	} else {
		log.Errorf("error retrieving mempool data from cache: %v", err)
	}
	return &types.RawMempoolResponse{}
}

func LatestBurnData() *types.BurnPageData {
	wanted := &types.BurnPageData{}
	cacheKey := fmt.Sprintf("%d:frontend:burn", utils.Config.Chain.ClConfig.DepositChainID)
	if wanted, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Minute, wanted); err == nil {
		return wanted.(*types.BurnPageData)
	} else {
		log.Errorf("error retrieving burn data from cache: %v", err)
	}
	return &types.BurnPageData{}
}

func LatestEthStoreStatistics() *types.EthStoreStatistics {
	wanted := &types.EthStoreStatistics{}
	cacheKey := fmt.Sprintf("%d:frontend:ethStoreStatistics", utils.Config.Chain.ClConfig.DepositChainID)
	if wanted, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Minute, wanted); err == nil {
		return wanted.(*types.EthStoreStatistics)
	} else {
		log.Errorf("error retrieving ETH.STORE statistics data from cache: %v", err)
	}
	return &types.EthStoreStatistics{}
}

func EthStoreDisclaimer() string {
	return "ETH.STORE® is not made available for use as a benchmark, whether in relation to a financial instrument, financial contract or to measure the performance of an investment fund, or otherwise in a way that would require it to be administered by a benchmark administrator pursuant to the EU Benchmarks Regulation. Currently Bitfly does not grant any right to access or use ETH.STORE® for such purpose."
}

// LatestIndexPageData returns the latest index page data
func LatestIndexPageData() *types.IndexPageData {
	wanted := &types.IndexPageData{}
	cacheKey := fmt.Sprintf("%d:frontend:indexPageData", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Second*5, wanted); err == nil {
		return wanted.(*types.IndexPageData)
	} else {
		log.Errorf("error retrieving indexPageData from cache: %v", err)
	}

	return &types.IndexPageData{}
}

// LatestPoolsPageData returns the latest pools page data
func LatestPoolsPageData() *types.PoolsResp {

	wanted := &types.PoolsResp{}
	cacheKey := fmt.Sprintf("%d:frontend:poolsData", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Second*5, wanted); err == nil {
		return wanted.(*types.PoolsResp)
	} else {
		log.Errorf("error retrieving poolsData from cache: %v", err)
	}

	return &types.PoolsResp{
		PoolsDistribution:       types.ChartsPageDataChart{},
		HistoricPoolPerformance: types.ChartsPageDataChart{},
		PoolInfos:               []*types.PoolInfo{},
	}
}

func LatestGasNowData() *types.GasNowPageData {
	wanted := &types.GasNowPageData{}
	cacheKey := fmt.Sprintf("%d:frontend:gasNow", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Second*5, wanted); err == nil {
		return wanted.(*types.GasNowPageData)
	} else {
		log.Errorf("error retrieving gasNow from cache: %v", err)
	}

	return nil
}

func LatestRelaysPageData() *types.RelaysResp {
	wanted := &types.RelaysResp{}
	cacheKey := fmt.Sprintf("%d:frontend:relaysData", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Second*5, wanted); err == nil {
		return wanted.(*types.RelaysResp)
	} else {
		log.Errorf("error retrieving relaysData from cache: %v", err)
	}

	return nil
}

func LatestSlotVizMetrics() []*types.SlotVizEpochs {
	wanted := &[]*types.SlotVizEpochs{}
	cacheKey := fmt.Sprintf("%d:frontend:slotVizMetrics", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Second*5, wanted); err == nil {
		w := wanted.(*[]*types.SlotVizEpochs)
		return *w
	} else {
		log.Errorf("error retrieving slotVizMetrics from cache: %v", err)
	}

	return []*types.SlotVizEpochs{}
}

// LatestState returns statistics about the current eth2 state
func LatestState() *types.LatestState {
	data := &types.LatestState{}
	data.CurrentEpoch = LatestEpoch()
	data.CurrentSlot = LatestSlot()
	data.CurrentFinalizedEpoch = LatestFinalizedEpoch()
	data.LastProposedSlot = LatestProposedSlot()
	data.FinalityDelay = FinalizationDelay()
	data.IsSyncing = IsSyncing()
	data.Rates = GetRates(utils.Config.Frontend.MainCurrency)

	return data
}

func GetRates(selectedCurrency string) *types.Rates {
	r := types.Rates{}

	if !price.IsAvailableCurrency(selectedCurrency) {
		logrus.Warnf("setting selectedCurrency to mainCurrency since selected is not available: %v", selectedCurrency)
		selectedCurrency = utils.Config.Frontend.MainCurrency
	}

	r.SelectedCurrency = selectedCurrency
	r.SelectedCurrencySymbol = price.GetCurrencySymbol(r.SelectedCurrency)

	r.MainCurrency = utils.Config.Frontend.MainCurrency
	r.ClCurrency = utils.Config.Frontend.ClCurrency
	r.ElCurrency = utils.Config.Frontend.ElCurrency
	r.TickerCurrency = selectedCurrency
	if r.TickerCurrency == utils.Config.Frontend.MainCurrency {
		r.TickerCurrency = "USD"
		if !price.IsAvailableCurrency(r.TickerCurrency) {
			r.TickerCurrency = utils.Config.Frontend.MainCurrency
		}
	}

	r.MainCurrencySymbol = price.GetCurrencySymbol(utils.Config.Frontend.MainCurrency)
	r.ElCurrencySymbol = price.GetCurrencySymbol(utils.Config.Frontend.ElCurrency)
	r.ClCurrencySymbol = price.GetCurrencySymbol(utils.Config.Frontend.ClCurrency)
	r.TickerCurrencySymbol = price.GetCurrencySymbol(r.TickerCurrency)

	r.MainCurrencyPrice = price.GetPrice(utils.Config.Frontend.MainCurrency, r.SelectedCurrency)
	r.ClCurrencyPrice = price.GetPrice(utils.Config.Frontend.ClCurrency, r.SelectedCurrency)
	r.ElCurrencyPrice = price.GetPrice(utils.Config.Frontend.ElCurrency, r.SelectedCurrency)
	r.MainCurrencyTickerPrice = price.GetPrice(utils.Config.Frontend.MainCurrency, r.TickerCurrency)

	r.MainCurrencyPriceFormatted = utils.FormatAddCommas(uint64(r.MainCurrencyPrice))
	r.ClCurrencyPriceFormatted = utils.FormatAddCommas(uint64(r.ClCurrencyPrice))
	r.ElCurrencyPriceFormatted = utils.FormatAddCommas(uint64(r.ElCurrencyPrice))
	r.MainCurrencyTickerPriceFormatted = utils.FormatAddCommas(uint64(r.MainCurrencyTickerPrice))

	r.MainCurrencyPriceKFormatted = utils.KFormatterEthPrice(uint64(r.MainCurrencyPrice))
	r.ClCurrencyPriceKFormatted = utils.KFormatterEthPrice(uint64(r.ClCurrencyPrice))
	r.ElCurrencyPriceKFormatted = utils.KFormatterEthPrice(uint64(r.ElCurrencyPrice))
	r.MainCurrencyTickerPriceKFormatted = utils.FormatAddCommas(uint64(r.MainCurrencyTickerPrice))

	r.MainCurrencyPrices = map[string]types.RatesPrice{}
	for _, c := range price.GetAvailableCurrencies() {
		p := types.RatesPrice{}
		p.Symbol = price.GetCurrencySymbol(c)
		cPrice := price.GetPrice(utils.Config.Frontend.MainCurrency, c)
		p.RoundPrice = uint64(cPrice)
		p.TruncPrice = utils.KFormatterEthPrice(uint64(cPrice))
		r.MainCurrencyPrices[c] = p
	}

	return &r
}

func GetLatestStats() *types.Stats {
	wanted := &types.Stats{}
	cacheKey := fmt.Sprintf("%d:frontend:latestStats", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetWithLocalTimeout(cacheKey, time.Second*5, wanted); err == nil {
		return wanted.(*types.Stats)
	} else {
		utils.LogError(err, "error retrieving latestStats from cache", 0)
	}

	// create an empty stats object if no stats exist (genesis)
	return &types.Stats{
		TopDepositors: &[]types.StatsTopDepositors{
			{
				Address:      "000",
				DepositCount: 0,
			},
			{
				Address:      "000",
				DepositCount: 0,
			},
		},
		InvalidDepositCount:            new(uint64),
		UniqueValidatorCount:           new(uint64),
		TotalValidatorCount:            new(uint64),
		ActiveValidatorCount:           new(uint64),
		PendingValidatorCount:          new(uint64),
		ValidatorChurnLimit:            new(uint64),
		ValidatorActivationChurnLimit:  new(uint64),
		LatestValidatorWithdrawalIndex: new(uint64),
	}
}

var globalNotificationMessage = template.HTML("")
var globalNotificationMessageTs time.Time
var globalNotificationMux = &sync.Mutex{}

func GlobalNotificationMessage() template.HTML {
	globalNotificationMux.Lock()
	defer globalNotificationMux.Unlock()

	if time.Since(globalNotificationMessageTs) > time.Minute*10 {
		globalNotificationMessageTs = time.Now()

		err := db.WriterDb.Get(&globalNotificationMessage, "SELECT content FROM global_notifications WHERE target = $1 AND enabled", utils.Config.Chain.Name)

		if err != nil && err != sql.ErrNoRows {
			log.Errorf("error updating global notification message: %v", err)
			globalNotificationMessage = ""
			return globalNotificationMessage
		}
	}
	return globalNotificationMessage
}

// IsSyncing returns true if the chain is still syncing
func IsSyncing() bool {
	return time.Now().Add(time.Minute * -10).After(utils.EpochToTime(LatestEpoch()))
}

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

type TxPoolContent struct {
	Pending map[string]map[int]*TxPoolContentTransaction
}

type TxPoolContentTransaction struct {
	GasPrice     string `json:"gasPrice"`
	MaxFeePerGas string `json:"maxFeePerGas"`
	Hash         string `json:"hash"`
}

func (tx *TxPoolContentTransaction) GetGasPrice() *big.Int {
	if tx.MaxFeePerGas != "" {
		gasPrice, ok := new(big.Int).SetString(strings.Replace(tx.MaxFeePerGas, "0x", "", 1), 16)
		if !ok {
			log.Warnf("error parsing gas price value of %s in tx %s", tx.MaxFeePerGas, tx.Hash)
			return big.NewInt(0)
		}

		return gasPrice
	} else if tx.GasPrice != "" {
		gasPrice, ok := new(big.Int).SetString(strings.Replace(tx.GasPrice, "0x", "", 1), 16)
		if !ok {
			log.Warnf("error parsing gas price value of %s in tx %s", tx.GasPrice, tx.Hash)
			return big.NewInt(0)
		}
		return gasPrice
	} else {
		big.NewInt(0)
		//log.Warnf("tx %v has neither gasPrice not maxFeePerGas set", tx.Hash)
	}

	return big.NewInt(0)
}

type rpcTransaction struct {
	tx *geth_types.Transaction
	txExtraInfo
}

type txExtraInfo struct {
	BlockNumber *string         `json:"blockNumber,omitempty"`
	BlockHash   *common.Hash    `json:"blockHash,omitempty"`
	From        *common.Address `json:"from,omitempty"`
}

type rpcBlock struct {
	Transactions []rpcTransaction `json:"transactions"`
}

func (tx *rpcTransaction) UnmarshalJSON(msg []byte) error {
	if err := json.Unmarshal(msg, &tx.tx); err != nil {
		return err
	}
	return json.Unmarshal(msg, &tx.txExtraInfo)
}

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

func burnUpdater(wg *sync.WaitGroup, bt *db.Bigtable) {
	firstRun := true
	for ; ; time.Sleep(time.Minute * 15) { // only update once every 15 minutes
		data, err := getBurnPageData(bt)
		if err != nil {
			log.Errorf("error retrieving burn page data: %v", err)
			continue
		}
		cacheKey := fmt.Sprintf("%d:frontend:burn", utils.Config.Chain.ClConfig.DepositChainID)
		err = cache.TieredCache.Set(cacheKey, data, utils.Day)
		if err != nil {
			log.Errorf("error caching burn data: %v", err)
		}
		if firstRun {
			log.Infof("initialized burn updater")
			wg.Done()
			firstRun = false
		}
	}
}

func getBurnPageData(bt *db.Bigtable) (*types.BurnPageData, error) {
	data := &types.BurnPageData{}
	start := time.Now()

	latestFinalizedEpoch := LatestFinalizedEpoch()
	latestBlock := LatestEth1BlockNumber()

	lookbackEpoch := latestFinalizedEpoch - 10
	if lookbackEpoch > latestFinalizedEpoch {
		lookbackEpoch = 0
	}
	lookbackDayEpoch := latestFinalizedEpoch - utils.EpochsPerDay()
	if lookbackDayEpoch > latestFinalizedEpoch {
		lookbackDayEpoch = 0
	}

	// Check db to have at least one entry (will error otherwise anyway)
	burnedFeesCount := 0
	if err := db.ReaderDb.Get(&burnedFeesCount, "SELECT COUNT(*) FROM chart_series WHERE indicator = 'BURNED_FEES'"); err != nil {
		return nil, fmt.Errorf("error get BURNED_FEES count from chart_series: %w", err)
	}
	if burnedFeesCount <= 0 {
		return data, nil
	}

	// Retrieve the total amount of burned Ether
	if err := db.ReaderDb.Get(&data.TotalBurned, "SELECT SUM(value) FROM chart_series WHERE indicator = 'BURNED_FEES'"); err != nil {
		return nil, fmt.Errorf("error retrieving total burned amount from chart_series table: %w", err)
	}

	cutOff := time.Time{}
	if err := db.ReaderDb.Get(&cutOff, "SELECT ( SELECT MAX(time) FROM chart_series WHERE indicator = 'BURNED_FEES' ) + interval '24 hours'"); err != nil {
		return nil, fmt.Errorf("error retrieving cutoff date from chart_series table: %w", err)
	}

	cutOffEpoch := utils.TimeToEpoch(cutOff)

	additionalBurned := float64(0)
	// log.Infof("using epoch limit %d", cutOffEpoch)
	err := db.ReaderDb.Get(&additionalBurned, "SELECT COALESCE(SUM(exec_base_fee_per_gas::numeric * exec_gas_used::numeric), 0) AS burnedfees FROM blocks WHERE epoch > $1", cutOffEpoch)
	if err != nil {
		return nil, fmt.Errorf("error retrieving additional burned eth from blocks table: %v", err)
	}
	// log.Infof("additonal burn: %v", additionalBurned)
	data.TotalBurned += additionalBurned

	// log.Infof("using epoch limit %d", lookbackEpoch)
	err = db.ReaderDb.Get(&data.BurnRate1h, "SELECT COALESCE(SUM(exec_base_fee_per_gas::numeric * exec_gas_used::numeric) / 60, 0) AS burnedfees FROM blocks WHERE epoch > $1", lookbackEpoch)
	if err != nil {
		return nil, fmt.Errorf("error retrieving burn rate (1h) from blocks table: %v", err)
	}

	income, err := bt.GetTotalValidatorIncomeDetailsHistory(lookbackEpoch, latestFinalizedEpoch)
	if err != nil {
		log.WithError(err).Error("error getting validator income history")
	}

	total := &itypes.ValidatorEpochIncome{}

	for _, details := range income {
		total.AttestationHeadReward += details.AttestationHeadReward
		total.AttestationSourceReward += details.AttestationSourceReward
		total.AttestationSourcePenalty += details.AttestationSourcePenalty
		total.AttestationTargetReward += details.AttestationTargetReward
		total.AttestationTargetPenalty += details.AttestationTargetPenalty
		total.FinalityDelayPenalty += details.FinalityDelayPenalty
		total.ProposerSlashingInclusionReward += details.ProposerSlashingInclusionReward
		total.ProposerAttestationInclusionReward += details.ProposerAttestationInclusionReward
		total.ProposerSyncInclusionReward += details.ProposerSyncInclusionReward
		total.SyncCommitteeReward += details.SyncCommitteeReward
		total.SyncCommitteePenalty += details.SyncCommitteePenalty
		total.SlashingReward += details.SlashingReward
		total.SlashingPenalty += details.SlashingPenalty
		total.TxFeeRewardWei = utils.AddBigInts(total.TxFeeRewardWei, details.TxFeeRewardWei)
	}

	rewards := decimal.NewFromBigInt(new(big.Int).SetBytes(total.TxFeeRewardWei), 0)

	rewards = rewards.Add(decimal.NewFromBigInt(new(big.Int).SetUint64(total.AttestationHeadReward), 0))
	rewards = rewards.Add(decimal.NewFromBigInt(new(big.Int).SetUint64(total.AttestationSourceReward), 0))
	rewards = rewards.Add(decimal.NewFromBigInt(new(big.Int).SetUint64(total.AttestationTargetReward), 0))
	rewards = rewards.Add(decimal.NewFromBigInt(new(big.Int).SetUint64(total.ProposerSlashingInclusionReward), 0))
	rewards = rewards.Add(decimal.NewFromBigInt(new(big.Int).SetUint64(total.ProposerAttestationInclusionReward), 0))
	rewards = rewards.Add(decimal.NewFromBigInt(new(big.Int).SetUint64(total.ProposerSyncInclusionReward), 0))
	rewards = rewards.Add(decimal.NewFromBigInt(new(big.Int).SetUint64(total.SyncCommitteeReward), 0))
	rewards = rewards.Add(decimal.NewFromBigInt(new(big.Int).SetUint64(total.SlashingReward), 0))

	rewards = rewards.Sub(decimal.NewFromBigInt(new(big.Int).SetUint64(total.AttestationTargetPenalty), 0))
	rewards = rewards.Sub(decimal.NewFromBigInt(new(big.Int).SetUint64(total.FinalityDelayPenalty), 0))
	rewards = rewards.Sub(decimal.NewFromBigInt(new(big.Int).SetUint64(total.SyncCommitteePenalty), 0))
	rewards = rewards.Sub(decimal.NewFromBigInt(new(big.Int).SetUint64(total.AttestationSourcePenalty), 0))
	rewards = rewards.Sub(decimal.NewFromBigInt(new(big.Int).SetUint64(total.SlashingPenalty), 0))

	// rewards per min
	rewards = rewards.Div(decimal.NewFromInt(64))

	// emission per minute
	data.Emission = rewards.InexactFloat64() - data.BurnRate1h

	log.Infof("burn rate per min: %v inflation per min: %v emission: %v", data.BurnRate1h, rewards.InexactFloat64(), data.Emission)
	// log.Infof("calculated emission: %v", data.Emission)

	// log.Infof("using epoch limit %d", lookbackDayEpoch)
	err = db.ReaderDb.Get(&data.BurnRate24h, "select COALESCE(SUM(exec_base_fee_per_gas::numeric * exec_gas_used::numeric) / (60 * 24), 0) as burnedfees from blocks where epoch >= $1", lookbackDayEpoch)
	if err != nil {
		return nil, fmt.Errorf("error retrieving burn rate (24h) from blocks table: %v", err)
	}

	err = db.ReaderDb.Get(&data.BlockUtilization, "select avg(exec_gas_used::numeric * 100 / exec_gas_limit) from blocks where epoch >= $1 and exec_gas_used > 0 and exec_gas_limit > 0", lookbackDayEpoch)
	if err != nil {
		return nil, fmt.Errorf("error retrieving block utilization from blocks table: %v", err)
	}

	blocks, err := bt.GetBlocksDescending(latestBlock, 1000)
	if err != nil {
		return nil, err
	}

	data.Blocks = make([]*types.BurnPageDataBlock, 0, 1000)
	for _, blk := range blocks {

		blockNumber := blk.GetNumber()
		baseFee := new(big.Int).SetBytes(blk.GetBaseFee())
		// gasHalf := float64(blk.GetGasLimit()) / 2.0
		txReward := new(big.Int).SetBytes(blk.GetTxReward())

		burned := new(big.Int).Mul(baseFee, big.NewInt(int64(blk.GetGasUsed())))

		blockReward := new(big.Int).Add(utils.Eth1BlockReward(blockNumber, blk.GetDifficulty()), new(big.Int).Add(txReward, new(big.Int).SetBytes(blk.GetUncleReward())))

		data.Blocks = append(data.Blocks, &types.BurnPageDataBlock{
			Number:        int64(blockNumber),
			Hash:          hex.EncodeToString(blk.Hash),
			GasTarget:     int64(blk.GasLimit),
			GasUsed:       int64(blk.GasUsed),
			Txn:           int(blk.TransactionCount),
			Age:           blk.Time.AsTime(),
			BaseFeePerGas: float64(baseFee.Int64()),
			BurnedFees:    float64(burned.Int64()),
			Rewards:       float64(blockReward.Int64()),
		})
	}

	if len(data.Blocks) > 100 {
		if data.Blocks[0].BaseFeePerGas < data.Blocks[100].BaseFeePerGas {
			data.BaseFeeTrend = -1
		} else if data.Blocks[0].BaseFeePerGas == data.Blocks[100].BaseFeePerGas {
			data.BaseFeeTrend = 0
		} else {
			data.BaseFeeTrend = 1
		}
	} else {
		data.BaseFeeTrend = 1
	}

	for _, b := range data.Blocks {
		if b.Number > 12965000 {
			b.GasTarget = b.GasTarget / 2
		}
	}
	log.Infof("epoch burn page export took: %v seconds", time.Since(start).Seconds())
	return data, nil
}

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

// LatestExportedStatisticDay will return the last exported day in the validator_stats table
func LatestExportedStatisticDay() (uint64, error) {
	cacheKey := fmt.Sprintf("%d:frontend:lastExportedStatisticDay", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetUint64WithLocalTimeout(cacheKey, time.Second*5); err == nil {
		return wanted, nil
	}
	wanted, err := db.GetLastExportedStatisticDay()

	if err != nil {
		return 0, err
	}
	return wanted, nil
}