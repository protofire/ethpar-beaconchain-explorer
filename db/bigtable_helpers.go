package db

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	gcp_bigtable "cloud.google.com/go/bigtable"
	"github.com/ethereum/go-ethereum/common"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func makeBlockKey(chainID string, number uint64, rank uint8) string {
	return fmt.Sprintf("%s:%s:%02d", chainID, reversedPaddedBlockNumber(number), rank)
}

func reversedPaddedBlockNumber(blockNumber uint64) string {
	return fmt.Sprintf("%09d", MAX_EL_BLOCK_NUMBER-blockNumber)
}

func reversePaddedUserID(userID uint64) string {
	return fmt.Sprintf("%09d", ^uint64(0)-userID)
}

func reversedPaddedEpoch(epoch uint64) string {
	return fmt.Sprintf("%09d", MAX_EPOCH-epoch)
}

func reversedPaddedSlot(slot uint64) string {
	return fmt.Sprintf("%09d", MAX_CL_BLOCK_NUMBER-slot)
}

func reversePaddedBigtableTimestamp(timestamp *timestamppb.Timestamp) string {
	if timestamp == nil {
		log.Fatalf("unknown timestamp: %v", timestamp)
	}
	return fmt.Sprintf("%019d", MAX_INT-timestamp.Seconds)
}

func reversePaddedIndex(i int, maxValue int) string {
	if i > maxValue {
		log.Fatalf("padded index %v is greater than the max index of %v", i, maxValue)
	}
	length := fmt.Sprintf("%d", len(fmt.Sprintf("%d", maxValue))-1)
	fmtStr := "%0" + length + "d"
	return fmt.Sprintf(fmtStr, maxValue-i)
}

// custom timestamp
func encodeIsContractUpdateTs(blockNumber, txIdx, traceIdx uint64) (gcp_bigtable.Timestamp, error) {
	var res uint64

	if blockNumber >= TIMESTAMP_BLOCK_SCALE {
		return 0, fmt.Errorf("error encoding IsContractTimestamp: block idx is >= %d (block %d, tx %d, trace %d)", TIMESTAMP_BLOCK_SCALE, blockNumber, txIdx, traceIdx)
	}
	res += blockNumber

	if txIdx >= TIMESTAMP_TX_SCALE {
		return 0, fmt.Errorf("error encoding IsContractTimestamp: tx idx is >= %d (block %d, tx %d, trace %d)", TIMESTAMP_TX_SCALE, blockNumber, txIdx, traceIdx)
	}
	res *= TIMESTAMP_TX_SCALE
	res += txIdx

	if traceIdx >= TIMESTAMP_TRACE_SCALE {
		return 0, fmt.Errorf("error encoding IsContractTimestamp: trace idx is >= %d (block %d, tx %d, trace %d)", TIMESTAMP_TRACE_SCALE, blockNumber, txIdx, traceIdx)
	}
	res *= TIMESTAMP_TRACE_SCALE
	res += traceIdx

	return gcp_bigtable.Timestamp(res * TIMESTAMP_GBT_SCALE), nil
}

func decodeIsContractUpdateTs(ts gcp_bigtable.Timestamp) (blockNumber, txIdx, traceIdx uint64) {
	n := uint64(ts)
	n /= TIMESTAMP_GBT_SCALE

	traceIdx = n % TIMESTAMP_TRACE_SCALE
	n /= TIMESTAMP_TRACE_SCALE

	txIdx = n % TIMESTAMP_TX_SCALE

	blockNumber = n / TIMESTAMP_TX_SCALE

	return blockNumber, txIdx, traceIdx
}

// withTimeoutAndWarning returns a context with the specified timeout and a cleanup function,
// and also sets up a watchdog timer that logs a warning if the operation exceeds the expected duration.
//
// Parameters:
//   - label: a human-readable string describing the operation (used in log messages)
//   - timeout: the duration after which the context will be automatically canceled
//
// The returned cleanup function must be deferred by the caller to stop the watchdog and release the context.
//
// Logs a warning like:
//
//	"fetchBlock (fetch latest block) call took longer than 30s"
func withTimeoutAndWarning(label string, timeout time.Duration) (context.Context, func()) {
	tmr := time.AfterFunc(REPORT_TIMEOUT, func() {
		log.Warnf("%s (%s) call took longer than %v", utils.GetCurrentFuncName(), label, timeout)
	})
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(timeout))

	cleanup := func() {
		tmr.Stop()
		cancel()
	}

	return ctx, cleanup
}

func getSignaturePrefix(st types.SignatureType) string {
	if st == types.EventSignature {
		return "e"
	}
	return "m"
}

func prefixSuccessor(prefix string, pos int) string {
	if prefix == "" {
		return "" // infinite range
	}
	split := strings.Split(prefix, ":")
	if len(split) > pos {
		prefix = strings.Join(split[:pos], ":")
	}
	n := len(prefix)
	for n--; n >= 0 && prefix[n] == '\xff'; n-- {
	}
	if n == -1 {
		return ""
	}
	ans := []byte(prefix[:n])
	ans = append(ans, prefix[n]+1)
	return string(ans)
}

// calculateMevFromBlock
//
// Deprecated: consider removing it from block transform
func calculateMevFromBlock(block *types.Eth1Block) *big.Int {
	mevReward := big.NewInt(0)

	for _, tx := range block.GetTransactions() {
		for _, itx := range tx.GetItx() {
			if common.BytesToAddress(itx.To) == common.BytesToAddress(block.GetCoinbase()) {
				mevReward = new(big.Int).Add(mevReward, new(big.Int).SetBytes(itx.GetValue()))
			}
		}

	}
	return mevReward
}

// calculateTxFeeFromTransaction
func CalculateTxFeeFromTransaction(tx *types.Eth1Transaction, blockBaseFee *big.Int) *big.Int {
	// calculate tx fee depending on tx type
	txFee := new(big.Int).SetUint64(tx.GasUsed)
	switch tx.Type {
	case 0, 1:
		txFee.Mul(txFee, new(big.Int).SetBytes(tx.GasPrice))
	case 2, 3:
		// multiply gasused with min(baseFee + maxpriorityfee, maxfee)
		if normalGasPrice, maxGasPrice := new(big.Int).Add(blockBaseFee, new(big.Int).SetBytes(tx.MaxPriorityFeePerGas)), new(big.Int).SetBytes(tx.MaxFeePerGas); normalGasPrice.Cmp(maxGasPrice) <= 0 {
			txFee.Mul(txFee, normalGasPrice)
		} else {
			txFee.Mul(txFee, maxGasPrice)
		}
	default:
		log.Errorf("unknown tx type %v", tx.Type)
	}
	return txFee
}

func validatorIndexToKey(index uint64) string {
	return utils.ReverseString(fmt.Sprintf("%d", index))
}

func validatorKeyToIndex(key string) (uint64, error) {
	key = utils.ReverseString(key)
	indexKey, err := strconv.ParseUint(key, 10, 64)

	if err != nil {
		return 0, err
	}
	return indexKey, nil
}

func machineMetricRowParts(r string) (bool, uint64, string, string) {
	keySplit := strings.Split(r, ":")

	userID, err := strconv.ParseUint(keySplit[1], 10, 64)
	if err != nil {
		log.Errorf("error parsing slot from row key %v: %v", r, err)
		return false, 0, "", ""
	}
	userID = ^uint64(0) - userID

	machine := ""
	if len(keySplit) >= 6 {
		machine = keySplit[5]
	}

	process := keySplit[3]

	return true, userID, machine, process
}

func verifyName(name string) error {
	// limited by max capacity of db (caused by btrees of indexes); tests showed maximum of 2684 (added buffer)
	if len(name) > 2048 {
		return fmt.Errorf("name too long: %v", name)
	}
	return nil
}
