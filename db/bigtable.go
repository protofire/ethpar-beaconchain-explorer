package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	gcp_bigtable "cloud.google.com/go/bigtable"
	"github.com/go-redis/redis/v8"
	itypes "github.com/gobitfly/eth-rewards/types"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/option"
	"google.golang.org/protobuf/proto"
)

const (
	DEFAULT_FAMILY                        = "f"
	VALIDATOR_BALANCES_FAMILY             = "vb"
	VALIDATOR_HIGHEST_ACTIVE_INDEX_FAMILY = "ha"
	ATTESTATIONS_FAMILY                   = "at"
	PROPOSALS_FAMILY                      = "pr"
	SYNC_COMMITTEES_FAMILY                = "sc"
	INCOME_DETAILS_COLUMN_FAMILY          = "id"
	STATS_COLUMN_FAMILY                   = "stats"
	MACHINE_METRICS_COLUMN_FAMILY         = "mm"
	SERIES_FAMILY                         = "series"

	SUM_COLUMN = "sum"

	MAX_CL_BLOCK_NUMBER = 1000000000 - 1
	MAX_EL_BLOCK_NUMBER = 1000000000
	MAX_EPOCH           = 1000000000 - 1

	MAX_BATCH_MUTATIONS   = 100000
	DEFAULT_BATCH_INSERTS = 10000

	REPORT_TIMEOUT = time.Second * 10
)

type BigtableConfig struct {
	Project      string
	Instance     string
	ChainId      uint64
	CacheAddr    string
	Emulated     bool
	EmulatorHost string
	EmulatorPort uint16
	Rpc          execution.ExecutionClient
}

type Bigtable struct {
	client                         *gcp_bigtable.Client
	tableBeaconchain               *gcp_bigtable.Table
	tableValidators                *gcp_bigtable.Table
	tableValidatorsHistory         *gcp_bigtable.Table
	tableData                      *gcp_bigtable.Table
	tableBlocks                    *gcp_bigtable.Table
	tableMetadataUpdates           *gcp_bigtable.Table
	tableMetadata                  *gcp_bigtable.Table
	tableMachineMetrics            *gcp_bigtable.Table
	redisCache                     *redis.Client
	LastAttestationCache           map[uint64]uint64
	LastAttestationCacheMux        *sync.Mutex
	chainId                        string
	machineMetricsQueuedWritesChan chan (types.BulkMutation)
	log                            *logger.Logger
	rpc                            execution.ExecutionClient
}

var log = logger.New(nil).WithField("module", "bigtable")

// MustInitBigtable constructs a Bigtable helper and terminates the process
// if any dependency (emulator env, Bigtable client, Redis cache) is missing.
func MustInitBigtable(cfg *BigtableConfig) *Bigtable {

	btLog := logger.New(nil).WithField("module", "bt_client")

	if cfg == nil {
		btLog.Fatal("nil BigtableConfig")
	}

	if cfg.Emulated {
		host := cfg.EmulatorHost
		if host == "" {
			host = "127.0.0.1"
		}
		addr := fmt.Sprintf("%s:%d", host, cfg.EmulatorPort)
		btLog.Infof("using Bigtable emulator at %s", addr)
		if err := os.Setenv("BIGTABLE_EMULATOR_HOST", addr); err != nil {
			btLog.Fatalf("set BIGTABLE_EMULATOR_HOST: %v", err)
		}
	}

	rootCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		btClient *gcp_bigtable.Client
		redisCli *redis.Client
	)
	g, ctx := errgroup.WithContext(rootCtx)

	g.Go(func() error {
		cl, err := gcp_bigtable.NewClient(
			ctx,
			cfg.Project,
			cfg.Instance,
			option.WithGRPCConnectionPool(50),
		)
		if err != nil {
			return fmt.Errorf("create Bigtable client: %w", err)
		}
		btClient = cl
		return nil
	})

	g.Go(func() error {
		rc := redis.NewClient(&redis.Options{
			Addr:        cfg.CacheAddr,
			ReadTimeout: 20 * time.Second,
		})
		if err := rc.Ping(ctx).Err(); err != nil {
			return fmt.Errorf("redis ping: %w", err)
		}
		redisCli = rc
		return nil
	})

	if err := g.Wait(); err != nil {
		btLog.Fatalf("dependency initialisation: %v", err)
	}

	bt := &Bigtable{
		client:                         btClient,
		tableData:                      btClient.Open("data"),
		tableBlocks:                    btClient.Open("blocks"),
		tableMetadataUpdates:           btClient.Open("metadata_updates"),
		tableMetadata:                  btClient.Open("metadata"),
		tableBeaconchain:               btClient.Open("beaconchain"),
		tableMachineMetrics:            btClient.Open("machine_metrics"),
		tableValidators:                btClient.Open("beaconchain_validators"),
		tableValidatorsHistory:         btClient.Open("beaconchain_validators_history"),
		chainId:                        strconv.FormatUint(cfg.ChainId, 10),
		redisCache:                     redisCli,
		LastAttestationCacheMux:        &sync.Mutex{},
		machineMetricsQueuedWritesChan: make(chan types.BulkMutation, MAX_BATCH_MUTATIONS),
		log:                            btLog,
		rpc:                            cfg.Rpc,
	}

	return bt
}

func (bigtable *Bigtable) Close() {
	close(bigtable.machineMetricsQueuedWritesChan)
	time.Sleep(time.Second * 5)
	bigtable.client.Close()
}

func (bigtable *Bigtable) SaveMachineMetric(process string, userID uint64, machine string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*30)
	defer cancel()

	rowKeyData := fmt.Sprintf("u:%s:p:%s:m:%v", reversePaddedUserID(userID), process, machine)

	ts := gcp_bigtable.Now()
	rateLimitKey := fmt.Sprintf("%s:%d", rowKeyData, ts.Time().Minute())
	keySet, err := bigtable.redisCache.SetNX(ctx, rateLimitKey, "1", time.Minute).Result()
	if err != nil {
		return err
	}
	if !keySet {
		return fmt.Errorf("rate limit, last metric insert was less than 1 min ago")
	}

	// for limiting machines per user, add the machine field to a redis set
	// bucket period is 15mins
	machineLimitKey := fmt.Sprintf("%s:%d", reversePaddedUserID(userID), ts.Time().Minute()%15)
	pipe := bigtable.redisCache.Pipeline()
	pipe.SAdd(ctx, machineLimitKey, machine)
	pipe.Expire(ctx, machineLimitKey, time.Minute*15)
	_, err = pipe.Exec(ctx)
	if err != nil {
		return err
	}

	dataMut := gcp_bigtable.NewMutation()
	dataMut.Set(MACHINE_METRICS_COLUMN_FAMILY, "v1", ts, data)

	bulkMut := types.BulkMutation{ // schedule the mutation for writing
		Key: rowKeyData,
		Mut: dataMut,
	}

	bigtable.machineMetricsQueuedWritesChan <- bulkMut

	return nil
}

func (bigtable Bigtable) getMachineMetricNamesMap(userID uint64, searchDepth int) (map[string]bool, error) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	rangePrefix := fmt.Sprintf("u:%s:p:", reversePaddedUserID(userID))

	filter := gcp_bigtable.ChainFilters(
		gcp_bigtable.FamilyFilter(MACHINE_METRICS_COLUMN_FAMILY),
		gcp_bigtable.LatestNFilter(searchDepth),
		gcp_bigtable.TimestampRangeFilter(time.Now().Add(time.Duration(searchDepth*-1)*time.Minute), time.Now()),
		gcp_bigtable.StripValueFilter(),
	)

	machineNames := make(map[string]bool)

	err := bigtable.tableMachineMetrics.ReadRows(ctx, gcp_bigtable.PrefixRange(rangePrefix), func(r gcp_bigtable.Row) bool {
		success, _, machine, _ := machineMetricRowParts(r.Key())
		if !success {
			return false
		}
		machineNames[machine] = true

		return true
	}, gcp_bigtable.RowFilter(filter))
	if err != nil {
		return machineNames, err
	}

	return machineNames, nil
}

func (bigtable Bigtable) GetMachineMetricsMachineNames(userID uint64) ([]string, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"userId": userID,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	names, err := bigtable.getMachineMetricNamesMap(userID, 300)
	if err != nil {
		return nil, err
	}

	result := []string{}
	for key := range names {
		result = append(result, key)
	}

	return result, nil
}

func (bigtable Bigtable) GetMachineMetricsMachineCount(userID uint64) (uint64, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"userId": userID,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	machineLimitKey := fmt.Sprintf("%s:%d", reversePaddedUserID(userID), time.Now().Minute()%15)

	card, err := bigtable.redisCache.SCard(ctx, machineLimitKey).Result()
	if err != nil {
		return 0, err
	}
	return uint64(card), nil
}

func (bigtable Bigtable) GetMachineMetricsNode(userID uint64, limit, offset int) ([]*types.MachineMetricNode, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"userId": userID,
			"limit":  limit,
			"offset": offset,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	return getMachineMetrics(bigtable, "beaconnode", userID, limit, offset,
		func(data []byte, machine string) *types.MachineMetricNode {
			obj := &types.MachineMetricNode{}
			err := proto.Unmarshal(data, obj)
			if err != nil {
				return nil
			}
			obj.Machine = &machine
			return obj
		},
	)
}

func (bigtable Bigtable) GetMachineMetricsValidator(userID uint64, limit, offset int) ([]*types.MachineMetricValidator, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"userId": userID,
			"limit":  limit,
			"offset": offset,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	return getMachineMetrics(bigtable, "validator", userID, limit, offset,
		func(data []byte, machine string) *types.MachineMetricValidator {
			obj := &types.MachineMetricValidator{}
			err := proto.Unmarshal(data, obj)
			if err != nil {
				return nil
			}
			obj.Machine = &machine
			return obj
		},
	)
}

func (bigtable Bigtable) GetMachineMetricsSystem(userID uint64, limit, offset int) ([]*types.MachineMetricSystem, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"userId": userID,
			"limit":  limit,
			"offset": offset,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	return getMachineMetrics(bigtable, "system", userID, limit, offset,
		func(data []byte, machine string) *types.MachineMetricSystem {
			obj := &types.MachineMetricSystem{}
			err := proto.Unmarshal(data, obj)
			if err != nil {
				return nil
			}
			obj.Machine = &machine
			return obj
		},
	)
}

func getMachineMetrics[T types.MachineMetricSystem | types.MachineMetricNode | types.MachineMetricValidator](bigtable Bigtable, process string, userID uint64, limit, offset int, marshler func(data []byte, machine string) *T) ([]*T, error) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*30))
	defer cancel()

	rangePrefix := fmt.Sprintf("u:%s:p:%s:m:", reversePaddedUserID(userID), process)
	res := make([]*T, 0)
	if offset <= 0 {
		offset = 1
	}

	filter := gcp_bigtable.ChainFilters(
		gcp_bigtable.FamilyFilter(MACHINE_METRICS_COLUMN_FAMILY),
		gcp_bigtable.LatestNFilter(limit),
		gcp_bigtable.CellsPerRowOffsetFilter(offset),
	)
	gapSize := getMachineStatsGap(uint64(limit))
	err := bigtable.tableMachineMetrics.ReadRows(ctx, gcp_bigtable.PrefixRange(rangePrefix), func(r gcp_bigtable.Row) bool {
		success, _, machine, _ := machineMetricRowParts(r.Key())
		if !success {
			return false
		}
		var count = -1
		for _, ri := range r[MACHINE_METRICS_COLUMN_FAMILY] {
			count++
			if count%gapSize != 0 {
				continue
			}

			obj := marshler(ri.Value, machine)
			if obj == nil {
				return false
			}

			res = append(res, obj)
		}
		return true
	}, gcp_bigtable.RowFilter(filter))
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (bigtable Bigtable) GetMachineRowKey(userID uint64, process string, machine string) string {
	return fmt.Sprintf("u:%s:p:%s:m:%s", reversePaddedUserID(userID), process, machine)
}

// Returns a map[userID]map[machineName]machineData
// machineData contains the latest machine data in CurrentData
// and 5 minute old data in fiveMinuteOldData (defined in limit)
// as well as the insert timestamps of both
func (bigtable Bigtable) GetMachineMetricsForNotifications(rowKeys gcp_bigtable.RowList) (map[uint64]map[string]*types.MachineMetricSystemUser, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"rowKeys": rowKeys,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second*200))
	defer cancel()

	res := make(map[uint64]map[string]*types.MachineMetricSystemUser) // userID -> machine -> data

	limit := 5

	filter := gcp_bigtable.ChainFilters(
		gcp_bigtable.FamilyFilter(MACHINE_METRICS_COLUMN_FAMILY),
		gcp_bigtable.LatestNFilter(limit),
	)

	err := bigtable.tableMachineMetrics.ReadRows(ctx, rowKeys, func(r gcp_bigtable.Row) bool {
		success, userID, machine, _ := machineMetricRowParts(r.Key())
		if !success {
			return false
		}

		count := 0
		for _, ri := range r[MACHINE_METRICS_COLUMN_FAMILY] {

			obj := &types.MachineMetricSystem{}
			err := proto.Unmarshal(ri.Value, obj)
			if err != nil {
				return false
			}

			if _, found := res[userID]; !found {
				res[userID] = make(map[string]*types.MachineMetricSystemUser)
			}

			last, found := res[userID][machine]

			if found && count == limit-1 {
				res[userID][machine] = &types.MachineMetricSystemUser{
					UserID:                    userID,
					Machine:                   machine,
					CurrentData:               last.CurrentData,
					FiveMinuteOldData:         obj,
					CurrentDataInsertTs:       last.CurrentDataInsertTs,
					FiveMinuteOldDataInsertTs: ri.Timestamp.Time().Unix(),
				}
			} else {
				res[userID][machine] = &types.MachineMetricSystemUser{
					UserID:                    userID,
					Machine:                   machine,
					CurrentData:               obj,
					FiveMinuteOldData:         nil,
					CurrentDataInsertTs:       ri.Timestamp.Time().Unix(),
					FiveMinuteOldDataInsertTs: 0,
				}
			}
			count++

		}
		return true
	}, gcp_bigtable.RowFilter(filter))
	if err != nil {
		return nil, err
	}

	return res, nil
}

// exporter
func (bigtable *Bigtable) SaveValidatorBalances(epoch uint64, validators []*types.Validator) error {

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	// start := time.Now()
	ts := gcp_bigtable.Timestamp(0)

	muts := types.NewBulkMutations(len(validators))

	highestActiveIndex := uint64(0)
	epochKey := reversedPaddedEpoch(epoch)

	for _, validator := range validators {

		if validator.Balance > 0 && validator.Index > highestActiveIndex {
			highestActiveIndex = validator.Index
		}

		balanceEncoded := make([]byte, 8)
		binary.LittleEndian.PutUint64(balanceEncoded, validator.Balance)
		effectiveBalanceEncoded := uint8(validator.EffectiveBalance / 1e9) // we can encode the effective balance in 1 byte as it is capped at 32ETH and only decrements in 1 ETH steps

		combined := append(balanceEncoded, effectiveBalanceEncoded)
		mut := &gcp_bigtable.Mutation{}
		mut.Set(VALIDATOR_BALANCES_FAMILY, "b", ts, combined)
		key := fmt.Sprintf("%s:%s:%s:%s", bigtable.chainId, validatorIndexToKey(validator.Index), VALIDATOR_BALANCES_FAMILY, epochKey)

		muts.Add(key, mut)
	}

	err := bigtable.WriteBulk(muts, bigtable.tableValidatorsHistory, MAX_BATCH_MUTATIONS)
	if err != nil {
		return err
	}

	// store the highes active validator index for that epoch
	highestActiveIndexEncoded := make([]byte, 8)
	binary.LittleEndian.PutUint64(highestActiveIndexEncoded, highestActiveIndex)

	mut := &gcp_bigtable.Mutation{}
	mut.Set(VALIDATOR_HIGHEST_ACTIVE_INDEX_FAMILY, VALIDATOR_HIGHEST_ACTIVE_INDEX_FAMILY, ts, highestActiveIndexEncoded)
	key := fmt.Sprintf("%s:%s:%s", bigtable.chainId, VALIDATOR_HIGHEST_ACTIVE_INDEX_FAMILY, epochKey)
	err = bigtable.tableValidatorsHistory.Apply(ctx, key, mut)
	if err != nil {
		return err
	}
	return nil
}

// exporter
func (bigtable *Bigtable) SaveAttestationDuties(duties map[types.Slot]map[types.ValidatorIndex][]types.Slot) error {

	// Initialize in memory last attestation cache lazily
	bigtable.LastAttestationCacheMux.Lock()
	if bigtable.LastAttestationCache == nil {
		t := time.Now()
		var err error
		bigtable.LastAttestationCache, err = bigtable.GetLastAttestationSlots([]uint64{})

		if err != nil {
			bigtable.LastAttestationCacheMux.Unlock()
			return err
		}
		log.Infof("initialized in memory last attestation slot cache with %v validators in %v", len(bigtable.LastAttestationCache), time.Since(t))

	}
	bigtable.LastAttestationCacheMux.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*5)
	defer cancel()

	start := time.Now()

	mutsInclusionSlot := types.NewBulkMutations(MAX_BATCH_MUTATIONS)

	mutLastAttestationSlot := gcp_bigtable.NewMutation()
	mutLastAttestationSlotCount := 0

	for attestedSlot, validators := range duties {
		for validator, inclusions := range validators {

			epoch := utils.EpochOfSlot(uint64(attestedSlot))
			bigtable.LastAttestationCacheMux.Lock()
			if len(inclusions) == 0 { // for missed attestations we write the max block number which will yield a cell ts of 0
				inclusions = append(inclusions, MAX_CL_BLOCK_NUMBER)
			}
			for _, inclusionSlot := range inclusions {
				key := fmt.Sprintf("%s:%s:%s:%s", bigtable.chainId, validatorIndexToKey(uint64(validator)), ATTESTATIONS_FAMILY, reversedPaddedEpoch(epoch))

				mutInclusionSlot := gcp_bigtable.NewMutation()
				mutInclusionSlot.Set(ATTESTATIONS_FAMILY, fmt.Sprintf("%d", attestedSlot), gcp_bigtable.Timestamp((MAX_CL_BLOCK_NUMBER-inclusionSlot)*1000), []byte{})

				mutsInclusionSlot.Add(key, mutInclusionSlot)

				if inclusionSlot != MAX_CL_BLOCK_NUMBER && uint64(attestedSlot) > bigtable.LastAttestationCache[uint64(validator)] {
					mutLastAttestationSlot.Set(ATTESTATIONS_FAMILY, fmt.Sprintf("%d", validator), gcp_bigtable.Timestamp((attestedSlot)*1000), []byte{})
					bigtable.LastAttestationCache[uint64(validator)] = uint64(attestedSlot)
					mutLastAttestationSlotCount++

					if mutLastAttestationSlotCount == MAX_BATCH_MUTATIONS {
						mutStart := time.Now()
						err := bigtable.tableValidators.Apply(ctx, fmt.Sprintf("%s:lastAttestationSlot", bigtable.chainId), mutLastAttestationSlot)
						if err != nil {
							bigtable.LastAttestationCacheMux.Unlock()
							return fmt.Errorf("error applying last attestation slot mutations: %v", err)
						}
						mutLastAttestationSlot = gcp_bigtable.NewMutation()
						mutLastAttestationSlotCount = 0
						log.Infof("applyied last attestation slot mutations in %v", time.Since(mutStart))
					}
				}

			}
			bigtable.LastAttestationCacheMux.Unlock()
		}
	}

	err := bigtable.WriteBulk(mutsInclusionSlot, bigtable.tableValidatorsHistory, MAX_BATCH_MUTATIONS)

	if err != nil {
		return fmt.Errorf("error writing attestation inclusion slot mutations: %v", err)
	}

	if mutLastAttestationSlotCount > 0 {
		err := bigtable.tableValidators.Apply(ctx, fmt.Sprintf("%s:lastAttestationSlot", bigtable.chainId), mutLastAttestationSlot)
		if err != nil {
			return fmt.Errorf("error applying last attestation slot mutations: %v", err)
		}
	}

	log.Infof("exported %v attestations to bigtable in %v", mutsInclusionSlot.Len(), time.Since(start))
	return nil
}

// exporter
func (bigtable *Bigtable) SaveSyncComitteeDuties(duties map[types.Slot]map[types.ValidatorIndex]bool) error {
	start := time.Now()

	if len(duties) == 0 {
		log.Infof("no sync duties to export")
		return nil
	}

	muts := types.NewBulkMutations(int(config.ChainParams.Time.SlotsPerEpoch*utils.Config.Chain.ClConfig.SyncCommitteeSize + 1))

	for slot, validators := range duties {
		for validator, participated := range validators {
			mut := gcp_bigtable.NewMutation()
			if participated {
				mut.Set(SYNC_COMMITTEES_FAMILY, "s", gcp_bigtable.Timestamp((MAX_CL_BLOCK_NUMBER-slot)*1000), []byte{})
			} else {
				mut.Set(SYNC_COMMITTEES_FAMILY, "s", gcp_bigtable.Timestamp(0), []byte{})
			}
			key := fmt.Sprintf("%s:%s:%s:%s:%s", bigtable.chainId, validatorIndexToKey(uint64(validator)), SYNC_COMMITTEES_FAMILY, reversedPaddedEpoch(utils.EpochOfSlot(uint64(slot))), reversedPaddedSlot(uint64(slot)))

			muts.Add(key, mut)
		}
	}

	err := bigtable.WriteBulk(muts, bigtable.tableValidatorsHistory, MAX_BATCH_MUTATIONS)
	if err != nil {
		return err
	}

	log.Infof("exported %v sync committee duties to bigtable in %v", muts.Len(), time.Since(start))
	return nil
}

func (bigtable *Bigtable) GetValidatorBalanceHistory(validators []uint64, startEpoch uint64, endEpoch uint64) (map[uint64][]*types.ValidatorBalance, error) {
	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"validators_count": len(validators),
			"startEpoch":       startEpoch,
			"endEpoch":         endEpoch,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	if len(validators) == 0 {
		return nil, fmt.Errorf("passing empty validator array is unsupported")
	}

	batchSize := 1000
	concurrency := 10

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute*5))
	defer cancel()

	res := make(map[uint64][]*types.ValidatorBalance, len(validators))
	resMux := &sync.Mutex{}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for i := 0; i < len(validators); i += batchSize {

		upperBound := i + batchSize
		if len(validators) < upperBound {
			upperBound = len(validators)
		}
		vals := validators[i:upperBound]

		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
			}
			ranges := bigtable.getValidatorsEpochRanges(vals, VALIDATOR_BALANCES_FAMILY, startEpoch, endEpoch)
			ro := gcp_bigtable.LimitRows(int64(endEpoch-startEpoch+1) * int64(len(vals)))

			handleRow := func(r gcp_bigtable.Row) bool {
				// log.Info(r.Key())
				keySplit := strings.Split(r.Key(), ":")

				epoch, err := strconv.ParseUint(keySplit[3], 10, 64)
				if err != nil {
					log.Errorf("error parsing epoch from row key %v: %v", r.Key(), err)
					return false
				}

				validator, err := validatorKeyToIndex(keySplit[1])
				if err != nil {
					log.Errorf("error parsing validator index from row key %v: %v", r.Key(), err)
					return false
				}
				resMux.Lock()
				if res[validator] == nil {
					res[validator] = make([]*types.ValidatorBalance, 0)
				}
				resMux.Unlock()

				for _, ri := range r[VALIDATOR_BALANCES_FAMILY] {
					balances := ri.Value

					balanceBytes := balances[0:8]
					balance := binary.LittleEndian.Uint64(balanceBytes)
					var effectiveBalance uint64
					if len(balances) == 9 { // in new schema the effective balance is encoded in 1 byte
						effectiveBalance = uint64(balances[8]) * 1e9
					} else {
						effectiveBalanceBytes := balances[8:16]
						effectiveBalance = binary.LittleEndian.Uint64(effectiveBalanceBytes)
					}

					resMux.Lock()
					res[validator] = append(res[validator], &types.ValidatorBalance{
						Epoch:            MAX_EPOCH - epoch,
						Balance:          balance,
						EffectiveBalance: effectiveBalance,
						Index:            validator,
						PublicKey:        []byte{},
					})
					resMux.Unlock()
				}
				return true
			}

			err := bigtable.tableValidatorsHistory.ReadRows(gCtx, ranges, handleRow, ro)
			if err != nil {
				return err
			}

			// logrus.Infof("retrieved data for validators %v - %v", vals[0], vals[len(vals)-1])
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return res, nil
}

func (bigtable *Bigtable) GetValidatorAttestationHistory(
	validators []uint64,
	startEpoch uint64,
	endEpoch uint64,
	missedSlotsMap, orphanedSlotsMap map[uint64]bool,
) (map[uint64][]*types.ValidatorAttestation, error) {
	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"validatorsCount": len(validators),
			"startEpoch":      startEpoch,
			"endEpoch":        endEpoch,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	if len(validators) == 0 {
		return nil, fmt.Errorf("passing empty validator array is unsupported")
	}

	batchSize := 1000
	concurrency := 10

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute*5))
	defer cancel()

	res := make(map[uint64][]*types.ValidatorAttestation, len(validators))
	resMux := &sync.Mutex{}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	attestationsMap := make(map[types.ValidatorIndex]map[types.Slot][]*types.ValidatorAttestation)

	for i := 0; i < len(validators); i += batchSize {

		upperBound := i + batchSize
		if len(validators) < upperBound {
			upperBound = len(validators)
		}
		vals := validators[i:upperBound]

		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
			}
			ranges := bigtable.getValidatorsEpochRanges(vals, ATTESTATIONS_FAMILY, startEpoch, endEpoch)
			filter := gcp_bigtable.LimitRows(int64(endEpoch-startEpoch+1) * int64(len(vals))) // max is one row per epoch
			err := bigtable.tableValidatorsHistory.ReadRows(ctx, ranges, func(r gcp_bigtable.Row) bool {
				keySplit := strings.Split(r.Key(), ":")

				validator, err := validatorKeyToIndex(keySplit[1])
				if err != nil {
					log.Errorf("error parsing validator from row key %v: %v", r.Key(), err)
					return false
				}

				for _, ri := range r[ATTESTATIONS_FAMILY] {
					attesterSlotString := strings.Replace(ri.Column, ATTESTATIONS_FAMILY+":", "", 1)
					attesterSlot, err := strconv.ParseUint(attesterSlotString, 10, 64)
					if err != nil {
						log.Errorf("error parsing slot from row key %v: %v", r.Key(), err)
						return false
					}
					inclusionSlot := MAX_CL_BLOCK_NUMBER - uint64(ri.Timestamp)/1000

					status := uint64(1)
					if inclusionSlot == MAX_CL_BLOCK_NUMBER {
						inclusionSlot = 0
						status = 0
					}

					resMux.Lock()
					if attestationsMap[types.ValidatorIndex(validator)] == nil {
						attestationsMap[types.ValidatorIndex(validator)] = make(map[types.Slot][]*types.ValidatorAttestation)
					}

					if attestationsMap[types.ValidatorIndex(validator)][types.Slot(attesterSlot)] == nil {
						attestationsMap[types.ValidatorIndex(validator)][types.Slot(attesterSlot)] = make([]*types.ValidatorAttestation, 0)
					}

					attestationsMap[types.ValidatorIndex(validator)][types.Slot(attesterSlot)] = append(attestationsMap[types.ValidatorIndex(validator)][types.Slot(attesterSlot)], &types.ValidatorAttestation{
						InclusionSlot: inclusionSlot,
						Status:        status,
					})
					resMux.Unlock()

				}
				return true
			}, filter)

			return err
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Find all missed and orphaned slots
	slots := []uint64{}
	maxSlot := ((endEpoch + 1) * utils.Config.Chain.ClConfig.SlotsPerEpoch) - 1
	for slot := startEpoch * utils.Config.Chain.ClConfig.SlotsPerEpoch; slot <= maxSlot; slot++ {
		slots = append(slots, slot)
	}

	// Convert the attestationsMap info to the return format
	// Set the delay of the inclusionSlot
	for validator, attestations := range attestationsMap {
		if res[uint64(validator)] == nil {
			res[uint64(validator)] = make([]*types.ValidatorAttestation, 0)
		}
		for attesterSlot, att := range attestations {
			currentAttInfo := att[0]
			for _, attInfo := range att {
				if orphanedSlotsMap[attInfo.InclusionSlot] {
					attInfo.Status = 0
				}

				if currentAttInfo.Status != 1 && attInfo.Status == 1 {
					currentAttInfo.Status = attInfo.Status
					currentAttInfo.InclusionSlot = attInfo.InclusionSlot
				}
			}

			missedSlotsCount := uint64(0)
			for slot := uint64(attesterSlot) + 1; slot < currentAttInfo.InclusionSlot; slot++ {
				if missedSlotsMap[slot] || orphanedSlotsMap[slot] {
					missedSlotsCount++
				}
			}
			currentAttInfo.Index = uint64(validator)
			currentAttInfo.Epoch = uint64(attesterSlot) / utils.Config.Chain.ClConfig.SlotsPerEpoch
			currentAttInfo.CommitteeIndex = 0
			currentAttInfo.AttesterSlot = uint64(attesterSlot)
			currentAttInfo.Delay = int64(currentAttInfo.InclusionSlot - uint64(attesterSlot) - missedSlotsCount - 1)

			res[uint64(validator)] = append(res[uint64(validator)], currentAttInfo)
		}
	}

	// Sort the result by attesterSlot desc
	for validator, att := range res {
		sort.Slice(att, func(i, j int) bool {
			return att[i].AttesterSlot > att[j].AttesterSlot
		})
		res[validator] = att
	}

	return res, nil
}

func (bigtable *Bigtable) GetLastAttestationSlots(validators []uint64) (map[uint64]uint64, error) {

	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"validatorsCount": len(validators),
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	valLen := len(validators)

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute*5))
	defer cancel()

	res := make(map[uint64]uint64, len(validators))

	columnFilters := []gcp_bigtable.Filter{}
	if valLen < 1000 {
		columnFilters = make([]gcp_bigtable.Filter, 0, len(validators))
		for _, validator := range validators {
			columnFilters = append(columnFilters, gcp_bigtable.ColumnFilter(fmt.Sprintf("%d", validator)))
		}
	}

	filter := gcp_bigtable.ChainFilters(
		gcp_bigtable.FamilyFilter(ATTESTATIONS_FAMILY),
		gcp_bigtable.InterleaveFilters(columnFilters...),
		gcp_bigtable.LatestNFilter(1),
	)

	if len(columnFilters) == 1 { // special case to retrieve data for one validators
		filter = gcp_bigtable.ChainFilters(
			gcp_bigtable.FamilyFilter(ATTESTATIONS_FAMILY),
			columnFilters[0],
			gcp_bigtable.LatestNFilter(1),
		)
	} else if len(columnFilters) == 0 { // special case to retrieve data for all validators
		filter = gcp_bigtable.ChainFilters(
			gcp_bigtable.FamilyFilter(ATTESTATIONS_FAMILY),
			gcp_bigtable.LatestNFilter(1),
		)
	}

	key := fmt.Sprintf("%s:lastAttestationSlot", bigtable.chainId)

	row, err := bigtable.tableValidators.ReadRow(ctx, key, gcp_bigtable.RowFilter(filter))
	if err != nil {
		return nil, err
	}

	for _, ri := range row[ATTESTATIONS_FAMILY] {
		attestedSlot := uint64(ri.Timestamp) / 1000

		validator, err := strconv.ParseUint(strings.TrimPrefix(ri.Column, ATTESTATIONS_FAMILY+":"), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing validator from column key %v: %v", ri.Column, err)
		}

		res[validator] = attestedSlot
	}

	return res, nil
}

func (bigtable *Bigtable) GetValidatorMissedAttestationHistory(
	validators []uint64,
	startEpoch uint64,
	endEpoch uint64,
	orphanedSlotsMap map[uint64]bool,
) (map[uint64]map[uint64]bool, error) {
	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"validatorsCount": len(validators),
			"startEpoch":      startEpoch,
			"endEpoch":        endEpoch,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	if len(validators) == 0 {
		return nil, fmt.Errorf("passing empty validator array is unsupported")
	}

	batchSize := 1000
	concurrency := 10

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute*20))
	defer cancel()

	slots := []uint64{}

	for slot := startEpoch * utils.Config.Chain.ClConfig.SlotsPerEpoch; slot < (endEpoch+1)*utils.Config.Chain.ClConfig.SlotsPerEpoch; slot++ {
		slots = append(slots, slot)
	}

	res := make(map[uint64]map[uint64]bool)
	foundValid := make(map[uint64]map[uint64]bool)

	resMux := &sync.Mutex{}

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for i := 0; i < len(validators); i += batchSize {

		upperBound := i + batchSize
		if len(validators) < upperBound {
			upperBound = len(validators)
		}
		vals := validators[i:upperBound]

		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
			}
			ranges := bigtable.getValidatorsEpochRanges(vals, ATTESTATIONS_FAMILY, startEpoch, endEpoch)

			filter := gcp_bigtable.LimitRows(int64(endEpoch-startEpoch+1) * int64(len(vals))) // max is one row per epoch

			err := bigtable.tableValidatorsHistory.ReadRows(ctx, ranges, func(r gcp_bigtable.Row) bool {
				keySplit := strings.Split(r.Key(), ":")

				validator, err := validatorKeyToIndex(keySplit[1])
				if err != nil {
					log.Errorf("error parsing validator from row key %v: %v", r.Key(), err)
					return false
				}

				for _, ri := range r[ATTESTATIONS_FAMILY] {
					attesterSlotString := strings.Replace(ri.Column, ATTESTATIONS_FAMILY+":", "", 1)
					attesterSlot, err := strconv.ParseUint(attesterSlotString, 10, 64)
					if err != nil {
						log.Errorf("error parsing slot from row key %v: %v", r.Key(), err)
						return false
					}

					inclusionSlot := MAX_CL_BLOCK_NUMBER - uint64(ri.Timestamp)/1000

					status := uint64(1)
					if inclusionSlot == MAX_CL_BLOCK_NUMBER {
						status = 0
					}

					resMux.Lock()
					// only if the attestation was not included in another slot we count it as missed
					if (status == 0 || orphanedSlotsMap[inclusionSlot]) && (foundValid[validator] == nil || !foundValid[validator][attesterSlot]) {
						if res[validator] == nil {
							res[validator] = make(map[uint64]bool, 0)
						}
						res[validator][attesterSlot] = true
					} else {
						if res[validator] != nil && res[validator][attesterSlot] {
							delete(res[validator], attesterSlot)
						}
						if foundValid[validator] == nil {
							foundValid[validator] = make(map[uint64]bool, 0)
						}
						foundValid[validator][attesterSlot] = true
					}
					resMux.Unlock()
				}
				return true
			}, filter)

			return err
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return res, nil
}

// GetValidatorSyncDutiesHistory returns the sync participation status for the given validators ranging from startSlot to endSlot (both inclusive)
//
// The returned map uses the following keys: [validatorIndex][slot]
func (bigtable *Bigtable) GetValidatorSyncDutiesHistory(validators []uint64, startSlot uint64, endSlot uint64) (map[uint64]map[uint64]*types.ValidatorSyncParticipation, error) {
	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"validatorsCount": len(validators),
			"startSlot":       startSlot,
			"endSlot":         endSlot,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	if len(validators) == 0 {
		return nil, fmt.Errorf("passing empty validator array is unsupported")
	}

	batchSize := 1000
	concurrency := 10

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Minute*20))
	defer cancel()

	res := make(map[uint64]map[uint64]*types.ValidatorSyncParticipation, len(validators))
	resMux := &sync.Mutex{}

	filter := gcp_bigtable.LatestNFilter(1)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for i := 0; i < len(validators); i += batchSize {

		i := i
		upperBound := i + batchSize
		if len(validators) < upperBound {
			upperBound = len(validators)
		}
		vals := validators[i:upperBound]

		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
			}
			ranges := bigtable.getValidatorSlotRanges(vals, SYNC_COMMITTEES_FAMILY, startSlot, endSlot)

			err := bigtable.tableValidatorsHistory.ReadRows(ctx, ranges, func(r gcp_bigtable.Row) bool {
				keySplit := strings.Split(r.Key(), ":")

				validator, err := validatorKeyToIndex(keySplit[1])
				if err != nil {
					log.Errorf("error parsing validator from row key %v: %v", r.Key(), err)
					return false
				}
				slot, err := strconv.ParseUint(keySplit[4], 10, 64)
				if err != nil {
					log.Errorf("error parsing slot from row key %v: %v", r.Key(), err)
					return false
				}
				slot = MAX_CL_BLOCK_NUMBER - slot

				for _, ri := range r[SYNC_COMMITTEES_FAMILY] {

					inclusionSlot := MAX_CL_BLOCK_NUMBER - uint64(ri.Timestamp)/1000

					status := uint64(1) // 1: participated
					if inclusionSlot == MAX_CL_BLOCK_NUMBER {
						inclusionSlot = 0
						status = 0 // 0: missed
					}

					resMux.Lock()
					if res[validator] == nil {
						res[validator] = make(map[uint64]*types.ValidatorSyncParticipation, 0)
					}

					if len(res[validator]) > 0 && res[validator][slot] != nil {
						res[validator][slot].Status = status
					} else {
						res[validator][slot] = &types.ValidatorSyncParticipation{
							Slot:   slot,
							Status: status,
						}
					}
					resMux.Unlock()

				}
				return true
			}, gcp_bigtable.RowFilter(filter))

			return err
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return res, nil
}

// returns the validator attestation effectiveness in %
func (bigtable *Bigtable) GetValidatorEffectiveness(
	validators []uint64,
	epoch uint64,
	orphanedSlotsMap, missedSlotsMap map[uint64]bool,
) ([]*types.ValidatorEffectiveness, error) {
	end := epoch
	start := uint64(0)
	lookback := uint64(99)
	if end > lookback {
		start = end - lookback
	}
	data, err := bigtable.GetValidatorAttestationHistory(validators, start, end, missedSlotsMap, orphanedSlotsMap)

	if err != nil {
		return nil, err
	}

	res := make([]*types.ValidatorEffectiveness, 0, len(validators))
	type readings struct {
		Count uint64
		Sum   float64
	}

	aggEffectiveness := make(map[uint64]*readings)

	for validator, history := range data {
		for _, attestation := range history {
			if aggEffectiveness[validator] == nil {
				aggEffectiveness[validator] = &readings{}
			}
			if attestation.InclusionSlot > 0 {
				// log.Infof("adding %v for epoch %v %.2f%%", attestation.InclusionSlot, attestation.AttesterSlot, 1.0/float64(attestation.InclusionSlot-attestation.AttesterSlot)*100)
				aggEffectiveness[validator].Sum += 1.0 / float64(attestation.InclusionSlot-attestation.AttesterSlot)
				aggEffectiveness[validator].Count++
			} else {
				aggEffectiveness[validator].Sum += 0 // missed attestations get a penalty of 32 slots
				aggEffectiveness[validator].Count++
			}
		}
	}
	for validator, reading := range aggEffectiveness {
		res = append(res, &types.ValidatorEffectiveness{
			Validatorindex:        validator,
			AttestationEfficiency: float64(reading.Sum) / float64(reading.Count) * 100,
		})
	}

	return res, nil
}

func (bigtable *Bigtable) SaveValidatorIncomeDetails(epoch uint64, rewards map[uint64]*itypes.ValidatorEpochIncome) error {

	start := time.Now()
	ts := gcp_bigtable.Timestamp(utils.EpochToTime(epoch).UnixMicro())

	total := &itypes.ValidatorEpochIncome{}

	muts := types.NewBulkMutations(len(rewards))

	for i, rewardDetails := range rewards {

		data, err := proto.Marshal(rewardDetails)

		if err != nil {
			return err
		}

		mut := &gcp_bigtable.Mutation{}
		mut.Set(INCOME_DETAILS_COLUMN_FAMILY, "i", ts, data)
		key := fmt.Sprintf("%s:%s:%s:%s", bigtable.chainId, validatorIndexToKey(i), INCOME_DETAILS_COLUMN_FAMILY, reversedPaddedEpoch(epoch))

		muts.Add(key, mut)

		total.AttestationHeadReward += rewardDetails.AttestationHeadReward
		total.AttestationSourceReward += rewardDetails.AttestationSourceReward
		total.AttestationSourcePenalty += rewardDetails.AttestationSourcePenalty
		total.AttestationTargetReward += rewardDetails.AttestationTargetReward
		total.AttestationTargetPenalty += rewardDetails.AttestationTargetPenalty
		total.FinalityDelayPenalty += rewardDetails.FinalityDelayPenalty
		total.ProposerSlashingInclusionReward += rewardDetails.ProposerSlashingInclusionReward
		total.ProposerAttestationInclusionReward += rewardDetails.ProposerAttestationInclusionReward
		total.ProposerSyncInclusionReward += rewardDetails.ProposerSyncInclusionReward
		total.SyncCommitteeReward += rewardDetails.SyncCommitteeReward
		total.SyncCommitteePenalty += rewardDetails.SyncCommitteePenalty
		total.SlashingReward += rewardDetails.SlashingReward
		total.SlashingPenalty += rewardDetails.SlashingPenalty
		total.TxFeeRewardWei = utils.AddBigInts(total.TxFeeRewardWei, rewardDetails.TxFeeRewardWei)
	}

	sum, err := proto.Marshal(total)
	if err != nil {
		return err
	}

	mut := &gcp_bigtable.Mutation{}
	mut.Set(STATS_COLUMN_FAMILY, SUM_COLUMN, ts, sum)

	muts.Add(fmt.Sprintf("%s:%s:%s", bigtable.chainId, SUM_COLUMN, reversedPaddedEpoch(epoch)), mut)

	err = bigtable.WriteBulk(muts, bigtable.tableValidatorsHistory, MAX_BATCH_MUTATIONS)

	if err != nil {
		return err
	}

	log.Infof("exported validator income details for epoch %v to bigtable in %v", epoch, time.Since(start))
	return nil
}

// GetValidatorIncomeDetailsHistory returns the validator income details
// startEpoch & endEpoch are inclusive
func (bigtable *Bigtable) GetValidatorIncomeDetailsHistory(validators []uint64, startEpoch uint64, endEpoch uint64) (map[uint64]map[uint64]*itypes.ValidatorEpochIncome, error) {
	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"validatorsCount": len(validators),
			"startEpoch":      startEpoch,
			"endEpoch":        endEpoch,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	if len(validators) == 0 {
		return nil, fmt.Errorf("passing empty validator array is unsupported")
	}

	batchSize := 1000
	concurrency := 10

	if startEpoch > endEpoch {
		startEpoch = 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	res := make(map[uint64]map[uint64]*itypes.ValidatorEpochIncome, len(validators))
	resMux := &sync.Mutex{}

	filter := gcp_bigtable.LatestNFilter(1)

	g, gCtx := errgroup.WithContext(ctx)
	g.SetLimit(concurrency)

	for i := 0; i < len(validators); i += batchSize {

		upperBound := i + batchSize
		if len(validators) < upperBound {
			upperBound = len(validators)
		}
		vals := validators[i:upperBound]

		g.Go(func() error {
			select {
			case <-gCtx.Done():
				return gCtx.Err()
			default:
			}
			ranges := bigtable.getValidatorsEpochRanges(vals, INCOME_DETAILS_COLUMN_FAMILY, startEpoch, endEpoch)
			err := bigtable.tableValidatorsHistory.ReadRows(ctx, ranges, func(r gcp_bigtable.Row) bool {
				keySplit := strings.Split(r.Key(), ":")

				validator, err := validatorKeyToIndex(keySplit[1])
				if err != nil {
					log.Errorf("error parsing validator from row key %v: %v", r.Key(), err)
					return false
				}

				epoch, err := strconv.ParseUint(keySplit[3], 10, 64)
				if err != nil {
					log.Errorf("error parsing epoch from row key %v: %v", r.Key(), err)
					return false
				}

				for _, ri := range r[INCOME_DETAILS_COLUMN_FAMILY] {
					incomeDetails := &itypes.ValidatorEpochIncome{}
					err = proto.Unmarshal(ri.Value, incomeDetails)
					if err != nil {
						log.Errorf("error decoding validator income data for row %v: %v", r.Key(), err)
						return false
					}

					resMux.Lock()
					if res[validator] == nil {
						res[validator] = make(map[uint64]*itypes.ValidatorEpochIncome)
					}

					res[validator][MAX_EPOCH-epoch] = incomeDetails
					resMux.Unlock()
				}
				return true
			}, gcp_bigtable.RowFilter(filter))

			return err
		})
	}

	if err := g.Wait(); err != nil {
		return nil, err
	}

	return res, nil
}

// GetTotalValidatorIncomeDetailsHistory returns the total validator income for a given range of epochs
// It is considerably faster than fetching the individual income for each validator and aggregating it
// startEpoch & endEpoch are inclusive
func (bigtable *Bigtable) GetTotalValidatorIncomeDetailsHistory(startEpoch uint64, endEpoch uint64) (map[uint64]*itypes.ValidatorEpochIncome, error) {
	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.WithFields(logger.Fields{
			"startEpoch": startEpoch,
			"endEpoch":   endEpoch,
		}).Warnf("%s call took longer than %v", utils.GetCurrentFuncName(), REPORT_TIMEOUT)
	})
	defer tmr.Stop()

	if startEpoch > endEpoch {
		startEpoch = 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Minute*3)
	defer cancel()

	res := make(map[uint64]*itypes.ValidatorEpochIncome, endEpoch-startEpoch+1)

	filter := gcp_bigtable.LimitRows(int64(endEpoch - startEpoch + 1))

	rowRange := bigtable.getTotalIncomeEpochRanges(startEpoch, endEpoch)
	err := bigtable.tableValidatorsHistory.ReadRows(ctx, rowRange, func(r gcp_bigtable.Row) bool {
		keySplit := strings.Split(r.Key(), ":")

		epoch, err := strconv.ParseUint(keySplit[2], 10, 64)
		if err != nil {
			log.Errorf("error parsing epoch from row key %v: %v", r.Key(), err)
			return false
		}

		for _, ri := range r[STATS_COLUMN_FAMILY] {
			incomeDetails := &itypes.ValidatorEpochIncome{}
			err = proto.Unmarshal(ri.Value, incomeDetails)
			if err != nil {
				log.Errorf("error decoding validator income data for row %v: %v", r.Key(), err)
				return false
			}

			res[MAX_EPOCH-epoch] = incomeDetails
		}
		return true
	}, filter)

	if err != nil {
		return nil, err
	}
	return res, nil
}

func (bigtable *Bigtable) getValidatorsEpochRanges(validatorIndices []uint64, prefix string, startEpoch uint64, endEpoch uint64) gcp_bigtable.RowRangeList {
	if endEpoch > math.MaxInt64 {
		endEpoch = 0
	}
	if endEpoch < startEpoch { // handle overflows
		startEpoch = 0
	}

	ranges := make(gcp_bigtable.RowRangeList, 0, int((endEpoch-startEpoch+1))*len(validatorIndices))

	for _, validatorIndex := range validatorIndices {
		validatorKey := validatorIndexToKey(validatorIndex)

		// epochs are sorted descending, so start with the largest epoch and end with the smallest
		// add \x00 to make the range inclusive
		rangeEnd := fmt.Sprintf("%s:%s:%s:%s%s", bigtable.chainId, validatorKey, prefix, reversedPaddedEpoch(startEpoch), "\x00")
		rangeStart := fmt.Sprintf("%s:%s:%s:%s", bigtable.chainId, validatorKey, prefix, reversedPaddedEpoch(endEpoch))
		ranges = append(ranges, gcp_bigtable.NewRange(rangeStart, rangeEnd))
	}
	return ranges
}

func (bigtable *Bigtable) getTotalIncomeEpochRanges(startEpoch uint64, endEpoch uint64) gcp_bigtable.RowRange {
	if endEpoch > math.MaxInt64 {
		endEpoch = 0
	}
	if endEpoch < startEpoch { // handle overflows
		startEpoch = 0
	}

	rangeEnd := fmt.Sprintf("%s:%s:%s%s", bigtable.chainId, SUM_COLUMN, reversedPaddedEpoch(startEpoch), "\x00")
	rangeStart := fmt.Sprintf("%s:%s:%s", bigtable.chainId, SUM_COLUMN, reversedPaddedEpoch(endEpoch))

	return gcp_bigtable.NewRange(rangeStart, rangeEnd)
}

func (bigtable *Bigtable) getValidatorSlotRanges(validatorIndices []uint64, prefix string, startSlot uint64, endSlot uint64) gcp_bigtable.RowRangeList {
	if endSlot > math.MaxInt64 {
		endSlot = 0
	}
	if endSlot < startSlot { // handle overflows
		startSlot = 0
	}

	startEpoch := utils.EpochOfSlot(startSlot)
	endEpoch := utils.EpochOfSlot(endSlot)

	ranges := make(gcp_bigtable.RowRangeList, 0, len(validatorIndices))

	for _, validatorIndex := range validatorIndices {
		validatorKey := validatorIndexToKey(validatorIndex)

		rangeEnd := fmt.Sprintf("%s:%s:%s:%s:%s%s", bigtable.chainId, validatorKey, prefix, reversedPaddedEpoch(startEpoch), reversedPaddedSlot(startSlot), "\x00")
		rangeStart := fmt.Sprintf("%s:%s:%s:%s:%s", bigtable.chainId, validatorKey, prefix, reversedPaddedEpoch(endEpoch), reversedPaddedSlot(endSlot))
		ranges = append(ranges, gcp_bigtable.NewRange(rangeStart, rangeEnd))

	}
	return ranges
}

func (bt *Bigtable) GetCurrentDayClIncome(
	validatorIndices []uint64,
	lastExportedDay uint64,
) (map[uint64]int64, error) {
	dayIncome := make(map[uint64]int64)

	currentDay := uint64(lastExportedDay + 1)
	startEpoch := currentDay * utils.EpochsPerDay()
	endEpoch := startEpoch + utils.EpochsPerDay() - 1
	income, err := bt.GetValidatorIncomeDetailsHistory(validatorIndices, startEpoch, endEpoch)
	if err != nil {
		return dayIncome, err
	}

	// agregate all epoch income data to total day income for each validator
	for validatorIndex, validatorIncome := range income {
		if len(validatorIncome) == 0 {
			continue
		}
		for _, validatorEpochIncome := range validatorIncome {
			dayIncome[validatorIndex] += validatorEpochIncome.TotalClRewards()
		}
	}

	return dayIncome, nil
}
