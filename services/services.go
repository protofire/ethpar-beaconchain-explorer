package services

import (
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/cache"
	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/price"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	"github.com/ethereum/go-ethereum/common"
	"github.com/sirupsen/logrus"

	geth_types "github.com/ethereum/go-ethereum/core/types"
)

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
		log.Errorf("error retrieving latestStats from cache: %v", err)
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

// IsSyncing returns true if the chain is still syncing
func IsSyncing() bool {
	return time.Now().Add(time.Minute * -10).After(utils.EpochToTime(LatestEpoch()))
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

// LatestExportedStatisticDay will return the last exported day in the validator_stats table
func LatestExportedStatisticDay(pg *db.Postgres) (uint64, error) {
	cacheKey := fmt.Sprintf("%d:frontend:lastExportedStatisticDay", utils.Config.Chain.ClConfig.DepositChainID)

	if wanted, err := cache.TieredCache.GetUint64WithLocalTimeout(cacheKey, time.Second*5); err == nil {
		return wanted, nil
	}
	wanted, err := pg.GetLastExportedStatisticDay()

	if err != nil {
		return 0, err
	}
	return wanted, nil
}