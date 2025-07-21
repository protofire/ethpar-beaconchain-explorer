package db

import (
	"bytes"
	"fmt"
	"math/big"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/erc1155"
	"github.com/protofire/ethpar-beaconchain-explorer/erc20"
	"github.com/protofire/ethpar-beaconchain-explorer/erc721"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/metrics"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	gcp_bigtable "cloud.google.com/go/bigtable"

	"github.com/coocood/freecache"
	"github.com/ethereum/go-ethereum/common"
	eth_types "github.com/ethereum/go-ethereum/core/types"
	ensContracts "github.com/protofire/ethpar-beaconchain-explorer/contracts/ens"

	"google.golang.org/protobuf/proto"
)

// TransformBlock extracts blocks from bigtable more specifically from the table blocks.
// It transforms the block and strips any information that is not necessary for a blocks view
//
// It writes blocks to table data:
//
//	Row:    <chainID>:B:<reversePaddedBlockNumber>:<rank>
//	Family: f
//	Column: data
//	Cell:   Proto<Eth1BlockIndexed> (includes Rank)
//
// It also creates a secondary index by miner address to support reverse lookup:
//
//	Row:    <chainID>:I:B:<Miner>:<reversePaddedBlockNumber>:<rank>
//	Family: f
//	Column: <chainID>:B:<reversePaddedBlockNumber>:<rank>
//	Cell:   nil
func (bigtable *Bigtable) TransformBlock(block *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_block").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	idx := types.Eth1BlockIndexed{
		Hash:       block.GetHash(),
		ParentHash: block.GetParentHash(),
		UncleHash:  block.GetUncleHash(),
		Coinbase:   block.GetCoinbase(),
		Difficulty: block.GetDifficulty(),
		Number:     block.GetNumber(),
		GasLimit:   block.GetGasLimit(),
		GasUsed:    block.GetGasUsed(),
		Time:       block.GetTime(),
		BaseFee:    block.GetBaseFee(),
		// Duration:               uint64(block.GetTime().AsTime().Unix() - previous.GetTime().AsTime().Unix()),
		UncleCount:       uint64(len(block.GetUncles())),
		TransactionCount: uint64(len(block.GetTransactions())),
		// BaseFeeChange:          new(big.Int).Sub(new(big.Int).SetBytes(block.GetBaseFee()), new(big.Int).SetBytes(previous.GetBaseFee())).Bytes(),
		// BlockUtilizationChange: new(big.Int).Sub(new(big.Int).Div(big.NewInt(int64(block.GetGasUsed())), big.NewInt(int64(block.GetGasLimit()))), new(big.Int).Div(big.NewInt(int64(previous.GetGasUsed())), big.NewInt(int64(previous.GetGasLimit())))).Bytes(),
		BlobGasUsed:   block.GetBlobGasUsed(),
		ExcessBlobGas: block.GetExcessBlobGas(),
		Rank:          block.GetRank(),
	}

	uncleReward := big.NewInt(0)
	r := new(big.Int)

	for _, uncle := range block.Uncles {

		if len(block.Difficulty) == 0 { // no uncle rewards in PoS
			continue
		}

		r.Add(big.NewInt(int64(uncle.GetNumber())), big.NewInt(8))
		r.Sub(r, big.NewInt(int64(block.GetNumber())))
		r.Mul(r, utils.Eth1BlockReward(block.GetNumber(), block.Difficulty))
		r.Div(r, big.NewInt(8))

		r.Div(utils.Eth1BlockReward(block.GetNumber(), block.Difficulty), big.NewInt(32))
		uncleReward.Add(uncleReward, r)
	}

	idx.UncleReward = uncleReward.Bytes()

	var maxGasPrice *big.Int
	var minGasPrice *big.Int
	txReward := big.NewInt(0)

	for _, t := range block.GetTransactions() {
		price := new(big.Int).SetBytes(t.GasPrice)

		if minGasPrice == nil {
			minGasPrice = price
		}
		if maxGasPrice == nil {
			maxGasPrice = price
		}

		if price.Cmp(maxGasPrice) > 0 {
			maxGasPrice = price
		}

		if price.Cmp(minGasPrice) < 0 {
			minGasPrice = price
		}

		txFee := new(big.Int).Mul(new(big.Int).SetBytes(t.GasPrice), big.NewInt(int64(t.GasUsed)))

		if len(block.BaseFee) > 0 {
			effectiveGasPrice := utils.BigMin(new(big.Int).Add(new(big.Int).SetBytes(t.MaxPriorityFeePerGas), new(big.Int).SetBytes(block.BaseFee)), new(big.Int).SetBytes(t.MaxFeePerGas))
			proposerGasPricePart := new(big.Int).Sub(effectiveGasPrice, new(big.Int).SetBytes(block.BaseFee))

			if proposerGasPricePart.Cmp(big.NewInt(0)) >= 0 {
				txFee = new(big.Int).Mul(proposerGasPricePart, big.NewInt(int64(t.GasUsed)))
			} else {
				log.Errorf("error minerGasPricePart is below 0 for tx %v: %v", t.Hash, proposerGasPricePart)
				txFee = big.NewInt(0)
			}

		}

		txReward.Add(txReward, txFee)

		for _, itx := range t.Itx {
			if itx.Path == "[]" || bytes.Equal(itx.Value, []byte{0x0}) { // skip top level call & empty calls
				continue
			}
			idx.InternalTransactionCount++
		}

		if t.GetType() == 3 {
			idx.BlobTransactionCount++
		}

	}

	idx.TxReward = txReward.Bytes()

	if maxGasPrice != nil {
		idx.LowestGasPrice = minGasPrice.Bytes()

	}
	if minGasPrice != nil {
		idx.HighestGasPrice = maxGasPrice.Bytes()
	}

	idx.Mev = calculateMevFromBlock(block).Bytes() // deprecated but we still write the value to keep all blocks consistent

	// Mark Coinbase for balance update
	bigtable.markBalanceUpdate(idx.Coinbase, []byte{0x0}, bulkMetadataUpdates, cache)

	// <chainID>:B:<reverse number>:<rank>
	key := fmt.Sprintf("%s:B:%s:%02d", bigtable.chainId, reversedPaddedBlockNumber(block.GetNumber()), block.GetRank())
	mut := gcp_bigtable.NewMutation()

	b, err := proto.Marshal(&idx)
	if err != nil {
		return nil, nil, fmt.Errorf("error marshalling proto object err: %w", err)
	}

	mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

	bulkData.Keys = append(bulkData.Keys, key)
	bulkData.Muts = append(bulkData.Muts, mut)

	indexes := []string{
		// Index blocks by the miners address
		fmt.Sprintf("%s:I:B:%x:%02d:TIME:%s", bigtable.chainId, block.GetCoinbase(), block.GetRank(), reversePaddedBigtableTimestamp(block.Time)),
	}

	for _, idx := range indexes {
		mut := gcp_bigtable.NewMutation()
		mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

		bulkData.Keys = append(bulkData.Keys, idx)
		bulkData.Muts = append(bulkData.Muts, mut)
	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformTx extracts and indexes all transactions from a given Eth1 block.
//
// It writes transactions to table data:
//
//	Row:    <chainID>:TX:<txHash>
//	Family: f
//	Column: data
//	Cell:   Proto<Eth1TransactionIndexed>
//
// It also creates a secondary indexes by attributes such as sender, receiver,
// block number, method ID, timestamp, error, contract creation, etc.
//
//	Row:    <chainID>:I:TX:<address>:<DIMENSION>:...:<reverseIndex>
//	Family: f
//	Column: <chainID>:TX:<txHash>
//	Cell:   nil
//
// Where DIMENSION ∈ {TO, TIME, BLOCK, METHOD, FROM, ERROR, CONTRACT}
// and <reverseIndex> enables sorting by position or time (descending).
func (bigtable *Bigtable) TransformTx(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_tx").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	for i, tx := range blk.Transactions {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		iReverse := reversePaddedIndex(i, TX_PER_BLOCK_LIMIT)
		to := tx.GetTo()
		isContract := false
		if !bytes.Equal(tx.GetContractAddress(), ZERO_ADDRESS) {
			to = tx.GetContractAddress()
			isContract = true
		}

		method := make([]byte, 0)
		if len(tx.GetData()) > 3 {
			method = tx.GetData()[:4]
		}

		key := fmt.Sprintf("%s:TX:%x", bigtable.chainId, tx.GetHash())
		fee := new(big.Int).Mul(new(big.Int).SetBytes(tx.GetGasPrice()), big.NewInt(int64(tx.GetGasUsed()))).Bytes()
		blobFee := new(big.Int).Mul(new(big.Int).SetBytes(tx.GetBlobGasPrice()), big.NewInt(int64(tx.GetBlobGasUsed()))).Bytes()
		indexedTx := &types.Eth1TransactionIndexed{
			Hash:               tx.GetHash(),
			BlockNumber:        blk.GetNumber(),
			Time:               blk.GetTime(),
			MethodId:           method,
			From:               tx.GetFrom(),
			To:                 to,
			Value:              tx.GetValue(),
			TxFee:              fee,
			GasPrice:           tx.GetGasPrice(),
			BlobTxFee:          blobFee,
			BlobGasPrice:       tx.GetBlobGasPrice(),
			IsContractCreation: isContract,
			ErrorMsg:           tx.GetErrorMsg(),
			Rank:               blk.GetRank(),
		}
		// Mark Sender and Recipient for balance update
		bigtable.markBalanceUpdate(indexedTx.From, []byte{0x0}, bulkMetadataUpdates, cache)
		bigtable.markBalanceUpdate(indexedTx.To, []byte{0x0}, bulkMetadataUpdates, cache)

		if len(indexedTx.Hash) != 32 {
			log.Fatalf("retrieved hash of length %v for a tx in block %v", len(indexedTx.Hash), blk.GetNumber())
		}

		b, err := proto.Marshal(indexedTx)
		if err != nil {
			return nil, nil, err
		}

		mut := gcp_bigtable.NewMutation()
		mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

		bulkData.Keys = append(bulkData.Keys, key)
		bulkData.Muts = append(bulkData.Muts, mut)

		indexes := []string{
			fmt.Sprintf("%s:I:TX:%x:TO:%x:%s:%s", bigtable.chainId, tx.GetFrom(), to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:TIME:%s:%s", bigtable.chainId, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:BLOCK:%s:%s", bigtable.chainId, tx.GetFrom(), reversedPaddedBlockNumber(blk.GetNumber()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:METHOD:%x:%s:%s", bigtable.chainId, tx.GetFrom(), method, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:FROM:%x:%s:%s", bigtable.chainId, to, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:TIME:%s:%s", bigtable.chainId, to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:BLOCK:%s:%s", bigtable.chainId, to, reversedPaddedBlockNumber(blk.GetNumber()), iReverse),
			fmt.Sprintf("%s:I:TX:%x:METHOD:%x:%s:%s", bigtable.chainId, to, method, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
		}

		if indexedTx.ErrorMsg != "" {
			indexes = append(indexes, fmt.Sprintf("%s:I:TX:%x:ERROR:%s:%s", bigtable.chainId, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse))
			indexes = append(indexes, fmt.Sprintf("%s:I:TX:%x:ERROR:%s:%s", bigtable.chainId, to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse))
		}

		if indexedTx.IsContractCreation {
			indexes = append(indexes, fmt.Sprintf("%s:I:TX:%x:CONTRACT:%s:%s", bigtable.chainId, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse))
			indexes = append(indexes, fmt.Sprintf("%s:I:TX:%x:CONTRACT:%s:%s", bigtable.chainId, to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse))
		}

		for _, idx := range indexes {
			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

			bulkData.Keys = append(bulkData.Keys, idx)
			bulkData.Muts = append(bulkData.Muts, mut)
		}

	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformBlobTx extracts and indexes all type-3 ("blob") transactions from a given Eth1 block.
//
// It writes each blob transaction to the data table as a serialized record:
//
//	Row:    <chainID>:BTX:<txHash>
//	Family: f
//	Column: data
//	Cell:   Proto<Eth1BlobTransactionIndexed> (includes Rank)
//
// Additionally, it builds secondary indexes for efficient lookup by sender, recipient,
// timestamp, block number, and error status:
//
//	Row:    <chainID>:I:BTX:<address>:<DIMENSION>:...:<reverseIndex>
//	Family: f
//	Column: <chainID>:BTX:<txHash>
//	Cell:   nil
//
// Where DIMENSION ∈ {TO, TIME, BLOCK, FROM, ERROR}
// and <reverseIndex> allows reverse-ordered scans by time or position.
func (bigtable *Bigtable) TransformBlobTx(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_blob_tx").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	for i, tx := range blk.Transactions {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		if tx.Type != 3 {
			// skip non blob-txs
			continue
		}
		iReverse := reversePaddedIndex(i, TX_PER_BLOCK_LIMIT)
		to := tx.GetTo()

		key := fmt.Sprintf("%s:BTX:%x", bigtable.chainId, tx.GetHash())
		fee := new(big.Int).Mul(new(big.Int).SetBytes(tx.GetGasPrice()), big.NewInt(int64(tx.GetGasUsed()))).Bytes()
		blobFee := new(big.Int).Mul(new(big.Int).SetBytes(tx.GetBlobGasPrice()), big.NewInt(int64(tx.GetBlobGasUsed()))).Bytes()
		indexedTx := &types.Eth1BlobTransactionIndexed{
			Hash:                tx.GetHash(),
			BlockNumber:         blk.GetNumber(),
			Time:                blk.GetTime(),
			From:                tx.GetFrom(),
			To:                  to,
			Value:               tx.GetValue(),
			TxFee:               fee,
			GasPrice:            tx.GetGasPrice(),
			BlobTxFee:           blobFee,
			BlobGasPrice:        tx.GetBlobGasPrice(),
			ErrorMsg:            tx.GetErrorMsg(),
			BlobVersionedHashes: tx.GetBlobVersionedHashes(),
			Rank:                blk.GetRank(),
		}
		// Mark Sender and Recipient for balance update
		bigtable.markBalanceUpdate(indexedTx.From, []byte{0x0}, bulkMetadataUpdates, cache)
		bigtable.markBalanceUpdate(indexedTx.To, []byte{0x0}, bulkMetadataUpdates, cache)

		if len(indexedTx.Hash) != 32 {
			log.Fatalf("retrieved hash of length %v for a tx in block %v", len(indexedTx.Hash), blk.GetNumber())
		}

		b, err := proto.Marshal(indexedTx)
		if err != nil {
			return nil, nil, err
		}

		mut := gcp_bigtable.NewMutation()
		mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

		bulkData.Keys = append(bulkData.Keys, key)
		bulkData.Muts = append(bulkData.Muts, mut)

		indexes := []string{
			fmt.Sprintf("%s:I:BTX:%x:TO:%x:%s:%s", bigtable.chainId, tx.GetFrom(), to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:BTX:%x:TIME:%s:%s", bigtable.chainId, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:BTX:%x:BLOCK:%s:%s", bigtable.chainId, tx.GetFrom(), reversedPaddedBlockNumber(blk.GetNumber()), iReverse),
			fmt.Sprintf("%s:I:BTX:%x:FROM:%x:%s:%s", bigtable.chainId, to, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:BTX:%x:TIME:%s:%s", bigtable.chainId, to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse),
			fmt.Sprintf("%s:I:BTX:%x:BLOCK:%s:%s", bigtable.chainId, to, reversedPaddedBlockNumber(blk.GetNumber()), iReverse),
		}

		if indexedTx.ErrorMsg != "" {
			indexes = append(indexes, fmt.Sprintf("%s:I:BTX:%x:ERROR:%s:%s", bigtable.chainId, tx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReverse))
			indexes = append(indexes, fmt.Sprintf("%s:I:BTX:%x:ERROR:%s:%s", bigtable.chainId, to, reversePaddedBigtableTimestamp(blk.GetTime()), iReverse))
		}

		for _, idx := range indexes {
			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

			bulkData.Keys = append(bulkData.Keys, idx)
			bulkData.Muts = append(bulkData.Muts, mut)
		}

	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformContract processes contract creation and destruction events found in internal transactions.
//
// It writes contract lifecycle events to the metadata table:
//
//	Row:    <chainID>:S:<contractAddress>
//	Family: ACCOUNT_METADATA_FAMILY
//	Column: ACCOUNT_IS_CONTRACT
//	Timestamp: encoded from <blockNumber>, <txIndex>, <itxIndex>
//	Cell:   Proto<IsContractUpdate>
//
// A contract is marked as created if an internal transaction of type "create" is found.
// A contract is marked as destroyed if an internal transaction of type "suicide" is found.
// Success flag reflects whether both the internal call and its parent transaction succeeded.
func (bigtable *Bigtable) TransformContract(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_contract").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}
	contractUpdateWrites := &types.BulkMutations{}

	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}

		for j, itx := range tx.GetItx() {
			if j >= ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of internal transactions in block expected at most %d but got: %v, tx: %x", ITX_PER_TX_LIMIT, j, tx.GetHash())
			}

			if itx.GetType() == "create" || itx.GetType() == "suicide" {
				contractUpdate := &types.IsContractUpdate{
					IsContract: itx.GetType() == "create",
					// also use success status of enclosing transaction, as even successful sub-calls can still be reverted later in the tx
					Success: itx.GetErrorMsg() == "" && tx.GetErrorMsg() == "",
				}
				b, err := proto.Marshal(contractUpdate)
				if err != nil {
					return nil, nil, err
				}
				address := itx.GetTo()
				if itx.GetType() == "suicide" {
					address = itx.GetFrom()
				}

				mutWrite := gcp_bigtable.NewMutation()
				ts, err := encodeIsContractUpdateTs(blk.GetNumber(), uint64(i), uint64(j))
				if err != nil {
					bigtable.log.Errorf("error generating bigtable isContract timestamp: %v", err)
				} else {
					mutWrite.Set(ACCOUNT_METADATA_FAMILY, ACCOUNT_IS_CONTRACT, ts, b)
					contractUpdateWrites.Keys = append(contractUpdateWrites.Keys, fmt.Sprintf("%s:S:%x", bigtable.chainId, address))
					contractUpdateWrites.Muts = append(contractUpdateWrites.Muts, mutWrite)
				}
			}
		}
	}

	err = bigtable.WriteBulk(contractUpdateWrites, bigtable.tableMetadata, DEFAULT_BATCH_INSERTS)
	return bulkData, bulkMetadataUpdates, err
}

// TransformItx extracts and indexes all non-trivial internal transactions from a given Eth1 block.
//
// It writes each internal transaction to the data table:
//
//	Row:    <chainID>:ITX:<txHash>:<reversedItxIndex>
//	Family: f
//	Column: data
//	Cell:   Proto<Eth1InternalTransactionIndexed>
//
// For each valid internal transaction (excluding top-level and empty calls), secondary indexes are written:
//
//	Row:    <chainID>:I:ITX:<from>:TO:<to>:<reversedTimestamp>:<reversedTxIndex>:<reversedItxIndex>
//	Row:    <chainID>:I:ITX:<to>:FROM:<from>:<reversedTimestamp>:<reversedTxIndex>:<reversedItxIndex>
//	Row:    <chainID>:I:ITX:<from>:TIME:<reversedTimestamp>:<reversedTxIndex>:<reversedItxIndex>
//	Row:    <chainID>:I:ITX:<to>:TIME:<reversedTimestamp>:<reversedTxIndex>:<reversedItxIndex>
//	Family: f
//	Column: <chainID>:ITX:<txHash>:<reversedItxIndex>
//	Cell:   nil
//
// Special handling exists for internal transactions of type "delegatecall":
//   - All corresponding cells in both data and index tables are deleted instead of written.
//   - This is to avoid polluting the index with non-state-changing calls.
//
// Each internal transaction is linked to its parent transaction via `ParentHash`, and
// is annotated with block number, timestamp, from/to, type, and rank for disambiguation.
func (bigtable *Bigtable) TransformItx(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_itx").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		iReversed := reversePaddedIndex(i, TX_PER_BLOCK_LIMIT)

		for j, itx := range tx.GetItx() {
			if j >= ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of internal transactions in block expected at most %d but got: %v, tx: %x", ITX_PER_TX_LIMIT, j, tx.GetHash())
			}
			jReversed := reversePaddedIndex(j, ITX_PER_TX_LIMIT)

			if itx.Path == "[]" || bytes.Equal(itx.Value, []byte{0x0}) { // skip top level and empty calls
				continue
			}

			key := fmt.Sprintf("%s:ITX:%x:%s", bigtable.chainId, tx.GetHash(), jReversed)
			indexedItx := &types.Eth1InternalTransactionIndexed{
				ParentHash:  tx.GetHash(),
				BlockNumber: blk.GetNumber(),
				Time:        blk.GetTime(),
				Type:        itx.GetType(),
				From:        itx.GetFrom(),
				To:          itx.GetTo(),
				Value:       itx.GetValue(),
				Rank:        blk.GetRank(),
			}

			bigtable.markBalanceUpdate(indexedItx.To, []byte{0x0}, bulkMetadataUpdates, cache)
			bigtable.markBalanceUpdate(indexedItx.From, []byte{0x0}, bulkMetadataUpdates, cache)

			indexes := []string{
				// fmt.Sprintf("%s:i:ITX::%s:%s:%s", bigtable.chainId, reversePaddedBigtableTimestamp(blk.GetTime()), fmt.Sprintf("%04d", i), fmt.Sprintf("%05d", j)),
				fmt.Sprintf("%s:I:ITX:%x:TO:%x:%s:%s:%s", bigtable.chainId, itx.GetFrom(), itx.GetTo(), reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ITX:%x:FROM:%x:%s:%s:%s", bigtable.chainId, itx.GetTo(), itx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ITX:%x:TIME:%s:%s:%s", bigtable.chainId, itx.GetFrom(), reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ITX:%x:TIME:%s:%s:%s", bigtable.chainId, itx.GetTo(), reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
			}

			// Delete existing delegatecall data or add/update other data
			if itx.GetType() == "delegatecall" {
				mut := gcp_bigtable.NewMutation()
				mut.DeleteCellsInColumn(DEFAULT_FAMILY, DATA_COLUMN)

				bulkData.Keys = append(bulkData.Keys, key)
				bulkData.Muts = append(bulkData.Muts, mut)

				for _, idx := range indexes {
					mut := gcp_bigtable.NewMutation()
					mut.DeleteCellsInColumn(DEFAULT_FAMILY, key)

					bulkData.Keys = append(bulkData.Keys, idx)
					bulkData.Muts = append(bulkData.Muts, mut)
				}
			} else {
				b, err := proto.Marshal(indexedItx)
				if err != nil {
					return nil, nil, err
				}

				mut := gcp_bigtable.NewMutation()
				mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

				bulkData.Keys = append(bulkData.Keys, key)
				bulkData.Muts = append(bulkData.Muts, mut)

				for _, idx := range indexes {
					mut := gcp_bigtable.NewMutation()
					mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

					bulkData.Keys = append(bulkData.Keys, idx)
					bulkData.Muts = append(bulkData.Muts, mut)
				}
			}
		}
	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformERC20 extracts and indexes all ERC-20 `Transfer` events from logs of transactions in a given Eth1 block.
//
// It writes each valid ERC-20 transfer log into the data table:
//
//	Row:    <chainID>:ERC20:<txHash>:<reversedLogIndex>
//	Family: f
//	Column: data
//	Cell:   Proto<Eth1ERC20Indexed>
//
// It also generates a variety of secondary indexes based on address, token, timestamp, direction, etc.:
//
//	Row:    <chainID>:I:ERC20:<from>:TIME:<reversedTimestamp>:<reversedTxIndex>:<reversedLogIndex>
//	Row:    <chainID>:I:ERC20:<to>:TIME:<reversedTimestamp>:<reversedTxIndex>:<reversedLogIndex>
//	Row:    <chainID>:I:ERC20:<token>:ALL:TIME:<reversedTimestamp>:<reversedTxIndex>:<reversedLogIndex>
//	Row:    <chainID>:I:ERC20:<token>:<from>:TIME:<reversedTimestamp>:...
//	Row:    <chainID>:I:ERC20:<token>:<to>:TIME:<reversedTimestamp>:...
//	Row:    <chainID>:I:ERC20:<from>:TO:<to>:...
//	Row:    <chainID>:I:ERC20:<to>:FROM:<from>:...
//	Row:    <chainID>:I:ERC20:<from>:TOKEN_SENT:<token>:...
//	Row:    <chainID>:I:ERC20:<to>:TOKEN_RECEIVED:<token>:...
//
// All indexes use the transaction index and log index to guarantee uniqueness and maintain order.
//
// Balance updates for `from` and `to` addresses (per token) are marked for metadata refresh.
func (bigtable *Bigtable) TransformERC20(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_erc20").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	filterer, err := erc20.NewErc20Filterer(common.Address{}, nil)
	if err != nil {
		log.Errorf("error creating filterer: %v", err)
	}

	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		iReversed := reversePaddedIndex(i, TX_PER_BLOCK_LIMIT)
		for j, log := range tx.GetLogs() {
			if j >= ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of logs in block expected at most %d but got: %v tx: %x", ITX_PER_TX_LIMIT-1, j, tx.GetHash())
			}
			jReversed := reversePaddedIndex(j, ITX_PER_TX_LIMIT)
			if len(log.GetTopics()) != 3 || !bytes.Equal(log.GetTopics()[0], erc20.TransferTopic) {
				continue
			}

			topics := make([]common.Hash, 0, len(log.GetTopics()))

			for _, lTopic := range log.GetTopics() {
				topics = append(topics, common.BytesToHash(lTopic))
			}

			ethLog := eth_types.Log{
				Address:     common.BytesToAddress(log.GetAddress()),
				Data:        log.Data,
				Topics:      topics,
				BlockNumber: blk.GetNumber(),
				TxHash:      common.BytesToHash(tx.GetHash()),
				TxIndex:     uint(i),
				BlockHash:   common.BytesToHash(blk.GetHash()),
				Index:       uint(j),
				Removed:     log.GetRemoved(),
			}

			transfer, _ := filterer.ParseTransfer(ethLog)
			if transfer == nil {
				continue
			}

			value := []byte{}
			if transfer != nil && transfer.Value != nil {
				value = transfer.Value.Bytes()
			}

			key := fmt.Sprintf("%s:ERC20:%x:%s", bigtable.chainId, tx.GetHash(), jReversed)
			indexedLog := &types.Eth1ERC20Indexed{
				ParentHash:   tx.GetHash(),
				BlockNumber:  blk.GetNumber(),
				Time:         blk.GetTime(),
				TokenAddress: log.Address,
				From:         transfer.From.Bytes(),
				To:           transfer.To.Bytes(),
				Value:        value,
				Rank:         blk.GetRank(),
			}
			bigtable.markBalanceUpdate(indexedLog.From, indexedLog.TokenAddress, bulkMetadataUpdates, cache)
			bigtable.markBalanceUpdate(indexedLog.To, indexedLog.TokenAddress, bulkMetadataUpdates, cache)

			b, err := proto.Marshal(indexedLog)
			if err != nil {
				return nil, nil, err
			}

			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

			bulkData.Keys = append(bulkData.Keys, key)
			bulkData.Muts = append(bulkData.Muts, mut)

			indexes := []string{
				fmt.Sprintf("%s:I:ERC20:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC20:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),

				fmt.Sprintf("%s:I:ERC20:%x:ALL:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC20:%x:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC20:%x:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),

				fmt.Sprintf("%s:I:ERC20:%x:TO:%x:%s:%s:%s", bigtable.chainId, indexedLog.From, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC20:%x:FROM:%x:%s:%s:%s", bigtable.chainId, indexedLog.To, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC20:%x:TOKEN_SENT:%x:%s:%s:%s", bigtable.chainId, indexedLog.From, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC20:%x:TOKEN_RECEIVED:%x:%s:%s:%s", bigtable.chainId, indexedLog.To, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
			}

			for _, idx := range indexes {
				mut := gcp_bigtable.NewMutation()
				mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

				bulkData.Keys = append(bulkData.Keys, idx)
				bulkData.Muts = append(bulkData.Muts, mut)
			}
		}
	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformERC721 extracts and indexes all ERC-721 `Transfer` events from transaction logs in a given Eth1 block.
//
// It writes each valid ERC-721 transfer log into the data table:
//
//	Row:    <chainID>:ERC721:<txHash>:<reversedLogIndex>
//	Family: f
//	Column: data
//	Cell:   Proto<Eth1ERC721Indexed>
//
// It also generates a wide set of secondary indexes based on the sender, receiver,
// token address, token ID, and timestamp:
//
//	Row:    <chainID>:I:ERC721:<from>:TIME:<reversedTimestamp>:<reversedTxIndex>:<reversedLogIndex>
//	Row:    <chainID>:I:ERC721:<to>:TIME:<reversedTimestamp>:...
//	Row:    <chainID>:I:ERC721:<tokenAddress>:ALL:TIME:<...>
//	Row:    <chainID>:I:ERC721:<tokenAddress>:<from>:TIME:<...>
//	Row:    <chainID>:I:ERC721:<tokenAddress>:<to>:TIME:<...>
//	Row:    <chainID>:I:ERC721:<from>:TO:<to>:<...>
//	Row:    <chainID>:I:ERC721:<to>:FROM:<from>:<...>
//	Row:    <chainID>:I:ERC721:<from>:TOKEN_SENT:<tokenAddress>:<...>
//	Row:    <chainID>:I:ERC721:<to>:TOKEN_RECEIVED:<tokenAddress>:<...>
//
// The index structure ensures efficient lookups for wallet activity, token-specific histories,
// and aggregate analytics by participant or asset.
func (bigtable *Bigtable) TransformERC721(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_erc721").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	filterer, err := erc721.NewErc721Filterer(common.Address{}, nil)
	if err != nil {
		log.Errorf("error creating filterer: %v", err)
	}

	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		iReversed := reversePaddedIndex(i, TX_PER_BLOCK_LIMIT)
		for j, log := range tx.GetLogs() {
			if j >= ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of logs in block expected at most %d but got: %v tx: %x", ITX_PER_TX_LIMIT-1, j, tx.GetHash())
			}
			if len(log.GetTopics()) != 4 || !bytes.Equal(log.GetTopics()[0], erc721.TransferTopic) {
				continue
			}
			jReversed := reversePaddedIndex(j, ITX_PER_TX_LIMIT)

			topics := make([]common.Hash, 0, len(log.GetTopics()))

			for _, lTopic := range log.GetTopics() {
				topics = append(topics, common.BytesToHash(lTopic))
			}

			ethLog := eth_types.Log{
				Address:     common.BytesToAddress(log.GetAddress()),
				Data:        log.Data,
				Topics:      topics,
				BlockNumber: blk.GetNumber(),
				TxHash:      common.BytesToHash(tx.GetHash()),
				TxIndex:     uint(i),
				BlockHash:   common.BytesToHash(blk.GetHash()),
				Index:       uint(j),
				Removed:     log.GetRemoved(),
			}

			transfer, _ := filterer.ParseTransfer(ethLog)
			if transfer == nil {
				continue
			}

			tokenId := new(big.Int)
			if transfer != nil && transfer.TokenId != nil {
				tokenId = transfer.TokenId
			}

			key := fmt.Sprintf("%s:ERC721:%x:%s", bigtable.chainId, tx.GetHash(), jReversed)
			indexedLog := &types.Eth1ERC721Indexed{
				ParentHash:   tx.GetHash(),
				BlockNumber:  blk.GetNumber(),
				Time:         blk.GetTime(),
				TokenAddress: log.Address,
				From:         transfer.From.Bytes(),
				To:           transfer.To.Bytes(),
				TokenId:      tokenId.Bytes(),
				Rank:         blk.GetRank(),
			}

			b, err := proto.Marshal(indexedLog)
			if err != nil {
				return nil, nil, err
			}

			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

			bulkData.Keys = append(bulkData.Keys, key)
			bulkData.Muts = append(bulkData.Muts, mut)

			indexes := []string{
				// fmt.Sprintf("%s:I:ERC721:%s:%s:%s", bigtable.chainId, reversePaddedBigtableTimestamp(blk.GetTime()), fmt.Sprintf("%04d", i), fmt.Sprintf("%05d", j)),
				fmt.Sprintf("%s:I:ERC721:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC721:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),

				fmt.Sprintf("%s:I:ERC721:%x:ALL:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC721:%x:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC721:%x:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),

				fmt.Sprintf("%s:I:ERC721:%x:TO:%x:%s:%s:%s", bigtable.chainId, indexedLog.From, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC721:%x:FROM:%x:%s:%s:%s", bigtable.chainId, indexedLog.To, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC721:%x:TOKEN_SENT:%x:%s:%s:%s", bigtable.chainId, indexedLog.From, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC721:%x:TOKEN_RECEIVED:%x:%s:%s:%s", bigtable.chainId, indexedLog.To, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
			}

			for _, idx := range indexes {
				mut := gcp_bigtable.NewMutation()
				mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

				bulkData.Keys = append(bulkData.Keys, idx)
				bulkData.Muts = append(bulkData.Muts, mut)
			}
		}
	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformERC1155 extracts and indexes every ERC-1155 `TransferSingle` and
// `TransferBatch` event found in the logs of each transaction in a given
// Eth1 block.
//
// Primary storage
//
//	Row:    <chainID>:ERC1155:<txHash>:<reversedLogIndex>
//	Family: f
//	Column: data
//	Cell:   Proto<Eth1ERC1155Indexed> (includes Rank)
//
//	• For a `TransferSingle`, one record is written.
//	• For a `TransferBatch`, a separate record is written for **each**
//	  (id,value) pair in the batch.
//
// Secondary indexes (reverse-sorted for efficient time scans)
//
//	Row examples:
//	  <chainID>:I:ERC1155:<from>:TIME:<revTs>:<revTxIdx>:<revLogIdx>
//	  <chainID>:I:ERC1155:<to>:TIME:<revTs>:...                      // mirror view
//	  <chainID>:I:ERC1155:<tokenAddr>:ALL:TIME:<revTs>:...           // token-wide
//	  <chainID>:I:ERC1155:<tokenAddr>:<from>:TIME:<revTs>:...        // token + from
//	  <chainID>:I:ERC1155:<tokenAddr>:<to>:TIME:<revTs>:...          // token + to
//	  <chainID>:I:ERC1155:<from>:TO:<to>:<revTs>:...                 // pair flow
//	  <chainID>:I:ERC1155:<to>:FROM:<from>:<revTs>:...
//	  <chainID>:I:ERC1155:<from>:TOKEN_SENT:<tokenAddr>:<revTs>:...
//	  <chainID>:I:ERC1155:<to>:TOKEN_RECEIVED:<tokenAddr>:<revTs>:...
//
//	Family: f
//	Column: <chainID>:ERC1155:<txHash>:<reversedLogIndex> (value = nil)
//
// Balance updates
//   - For each transfer, the sender (`From`) and recipient (`To`) are queued
//     for per-token balance refresh via `markBalanceUpdate`.
//
// Limits & safeguards
//   - Respects `TX_PER_BLOCK_LIMIT` and `ITX_PER_TX_LIMIT` to avoid runaway
//     memory usage.
//   - Ignores logs whose signature does not match ERC-1155 transfer topics.
//
// Returns two `BulkMutations` objects ready for `WriteBulk`, or an error if
// marshalling fails or limits are exceeded.
func (bigtable *Bigtable) TransformERC1155(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_erc1155").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	filterer, err := erc1155.NewErc1155Filterer(common.Address{}, nil)
	if err != nil {
		log.Errorf("error creating filterer: %v", err)
	}

	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		iReversed := reversePaddedIndex(i, TX_PER_BLOCK_LIMIT)
		for j, txlog := range tx.GetLogs() {
			if j >= ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of logs in block expected at most %d but got: %v tx: %x", ITX_PER_TX_LIMIT-1, j, tx.GetHash())
			}
			jReversed := reversePaddedIndex(j, ITX_PER_TX_LIMIT)

			key := fmt.Sprintf("%s:ERC1155:%x:%s", bigtable.chainId, tx.GetHash(), jReversed)

			// no events emitted continue
			if len(txlog.GetTopics()) != 4 || (!bytes.Equal(txlog.GetTopics()[0], erc1155.TransferBulkTopic) && !bytes.Equal(txlog.GetTopics()[0], erc1155.TransferSingleTopic)) {
				continue
			}

			topics := make([]common.Hash, 0, len(txlog.GetTopics()))

			for _, lTopic := range txlog.GetTopics() {
				topics = append(topics, common.BytesToHash(lTopic))
			}

			ethLog := eth_types.Log{
				Address:     common.BytesToAddress(txlog.GetAddress()),
				Data:        txlog.Data,
				Topics:      topics,
				BlockNumber: blk.GetNumber(),
				TxHash:      common.BytesToHash(tx.GetHash()),
				TxIndex:     uint(i),
				BlockHash:   common.BytesToHash(blk.GetHash()),
				Index:       uint(j),
				Removed:     txlog.GetRemoved(),
			}

			indexedLog := &types.ETh1ERC1155Indexed{}
			transferBatch, _ := filterer.ParseTransferBatch(ethLog)
			transferSingle, _ := filterer.ParseTransferSingle(ethLog)
			if transferBatch == nil && transferSingle == nil {
				continue
			}

			if transferBatch != nil {
				ids := make([][]byte, 0, len(transferBatch.Ids))
				for _, id := range transferBatch.Ids {
					ids = append(ids, id.Bytes())
				}

				values := make([][]byte, 0, len(transferBatch.Values))
				for _, val := range transferBatch.Values {
					values = append(values, val.Bytes())
				}

				if len(ids) != len(values) {
					log.Errorf("error parsing erc1155 batch transfer logs. Expected len(ids): %v len(values): %v to be the same", len(ids), len(values))
					continue
				}
				for ti := range ids {
					indexedLog.BlockNumber = blk.GetNumber()
					indexedLog.Time = blk.GetTime()
					indexedLog.ParentHash = tx.GetHash()
					indexedLog.From = transferBatch.From.Bytes()
					indexedLog.To = transferBatch.To.Bytes()
					indexedLog.Operator = transferBatch.Operator.Bytes()
					indexedLog.TokenId = ids[ti]
					indexedLog.Value = values[ti]
					indexedLog.TokenAddress = txlog.GetAddress()
					indexedLog.Rank = blk.GetRank()
				}
			} else if transferSingle != nil {
				indexedLog.BlockNumber = blk.GetNumber()
				indexedLog.Time = blk.GetTime()
				indexedLog.ParentHash = tx.GetHash()
				indexedLog.From = transferSingle.From.Bytes()
				indexedLog.To = transferSingle.To.Bytes()
				indexedLog.Operator = transferSingle.Operator.Bytes()
				indexedLog.TokenId = transferSingle.Id.Bytes()
				indexedLog.Value = transferSingle.Value.Bytes()
				indexedLog.TokenAddress = txlog.GetAddress()
				indexedLog.Rank = blk.GetRank()
			}

			b, err := proto.Marshal(indexedLog)
			if err != nil {
				return nil, nil, err
			}

			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

			bulkData.Keys = append(bulkData.Keys, key)
			bulkData.Muts = append(bulkData.Muts, mut)

			indexes := []string{
				// fmt.Sprintf("%s:I:ERC1155:%s:%s:%s", bigtable.chainId, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),

				fmt.Sprintf("%s:I:ERC1155:%x:ALL:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:%x:TIME:%s:%s:%s", bigtable.chainId, indexedLog.TokenAddress, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),

				fmt.Sprintf("%s:I:ERC1155:%x:TO:%x:%s:%s:%s", bigtable.chainId, indexedLog.From, indexedLog.To, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:FROM:%x:%s:%s:%s", bigtable.chainId, indexedLog.To, indexedLog.From, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:TOKEN_SENT:%x:%s:%s:%s", bigtable.chainId, indexedLog.From, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
				fmt.Sprintf("%s:I:ERC1155:%x:TOKEN_RECEIVED:%x:%s:%s:%s", bigtable.chainId, indexedLog.To, indexedLog.TokenAddress, reversePaddedBigtableTimestamp(blk.GetTime()), iReversed, jReversed),
			}

			for _, idx := range indexes {
				mut := gcp_bigtable.NewMutation()
				mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

				bulkData.Keys = append(bulkData.Keys, idx)
				bulkData.Muts = append(bulkData.Muts, mut)
			}
		}
	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformUncle accepts an eth1 block and creates bigtable mutations.
// It transforms the uncles contained within a block, extracts the necessary information to create a view and writes that information to bigtable
// It writes uncles to table data:
// Row:    <chainID>:U:<reversePaddedNumber>
// Family: f
// Column: data
// Cell:   Proto<Eth1UncleIndexed>
// Example scan: "1:U:" returns mainnet uncles mined in desc order
// Example scan: "1:U:984886725" returns mainnet uncles mined after block 15113275 (1000000000 - 984886725)
//
// It indexes uncles by:
// Row:    <chainID>:I:U:<Miner>:TIME:<reversePaddedBigtableTimestamp>
// Family: f
// Column: <chainID>:U:<reversePaddedNumber>
// Cell:   nil
// Example lookup: "1:I:U:ea674fdde714fd979de3edf0f56aa9716b898ec8:TIME:" returns mainnet uncles mined by ethermine in desc order
//
// Deprecated: Ethereum no longer includes uncles in blocks after the Merge (PoS).
func (bigtable *Bigtable) TransformUncle(block *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_uncle").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	for i, uncle := range block.Uncles {
		if i > 99 {
			return nil, nil, fmt.Errorf("unexpected number of uncles in block expected at most 99 but got: %v", i)
		}
		iReversed := reversePaddedIndex(i, 10)
		r := new(big.Int)

		if len(block.Difficulty) > 0 {
			r.Add(big.NewInt(int64(uncle.GetNumber())), big.NewInt(8))
			r.Sub(r, big.NewInt(int64(block.GetNumber())))
			r.Mul(r, utils.Eth1BlockReward(block.GetNumber(), block.Difficulty))
			r.Div(r, big.NewInt(8))

			r.Div(utils.Eth1BlockReward(block.GetNumber(), block.Difficulty), big.NewInt(32))
		}

		uncleIndexed := types.Eth1UncleIndexed{
			Number:      uncle.GetNumber(),
			BlockNumber: block.GetNumber(),
			GasLimit:    uncle.GetGasLimit(),
			GasUsed:     uncle.GetGasUsed(),
			BaseFee:     uncle.GetBaseFee(),
			Difficulty:  uncle.GetDifficulty(),
			Time:        uncle.GetTime(),
			Reward:      r.Bytes(),
		}

		bigtable.markBalanceUpdate(uncle.Coinbase, []byte{0x0}, bulkMetadataUpdates, cache)

		// store uncles in with the key <chainid>:U:<reversePaddedBlockNumber>:<reversePaddedUncleIndex>
		key := fmt.Sprintf("%s:U:%s:%s", bigtable.chainId, reversedPaddedBlockNumber(block.GetNumber()), iReversed)
		mut := gcp_bigtable.NewMutation()

		b, err := proto.Marshal(&uncleIndexed)
		if err != nil {
			return nil, nil, fmt.Errorf("error marshalling proto object err: %w", err)
		}

		mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

		bulkData.Keys = append(bulkData.Keys, key)
		bulkData.Muts = append(bulkData.Muts, mut)

		indexes := []string{
			// Index uncle by the miners address
			fmt.Sprintf("%s:I:U:%x:TIME:%s:%s", bigtable.chainId, uncle.GetCoinbase(), reversePaddedBigtableTimestamp(block.Time), iReversed),
		}

		for _, idx := range indexes {
			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

			bulkData.Keys = append(bulkData.Keys, idx)
			bulkData.Muts = append(bulkData.Muts, mut)
		}
	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformWithdrawals accepts an eth1 block and creates bigtable mutations.
// It transforms the withdrawals contained within a block, extracts the necessary information to create a view and writes that information to bigtable
// It writes withdrawals to table data:
// Row:    <chainID>:W:<reversePaddedNumber>:<rank>:<reversedWithdrawalIndex>
// Family: f
// Column: data
// Cell:   Proto<Eth1WithdrawalIndexed>
// Example scan: "1:W:" returns withdrawals in desc order
// Example scan: "1:W:984886725" returns mainnet withdrawals included after block 15113275 (1000000000 - 984886725)
//
// It indexes withdrawals by:
// Row:    <chainID>:I:W:<Address>:TIME:<reversePaddedBigtableTimestamp>
// Family: f
// Column: <chainID>:W:<reversePaddedNumber>
// Cell:   nil
// Example lookup: "1:I:W:ea674fdde714fd979de3edf0f56aa9716b898ec8:TIME:" returns withdrawals received by ethermine in desc order
func (bigtable *Bigtable) TransformWithdrawals(block *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_withdrawals").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}

	if len(block.Withdrawals) > int(MaxWithdrawalsPerPayload) {
		return nil, nil, fmt.Errorf("unexpected number of withdrawals in block expected at most %v but got: %v", MaxWithdrawalsPerPayload, len(block.Withdrawals))
	}

	for _, withdrawal := range block.Withdrawals {
		iReversed := reversePaddedIndex(int(withdrawal.Index), 9999999999999)

		withdrawalIndexed := types.Eth1WithdrawalIndexed{
			BlockNumber:    block.Number,
			Index:          withdrawal.Index,
			ValidatorIndex: withdrawal.ValidatorIndex,
			Address:        withdrawal.Address,
			Amount:         withdrawal.Amount,
			Time:           block.Time,
			Rank:           block.GetRank(),
		}

		bigtable.markBalanceUpdate(withdrawal.Address, []byte{0x0}, bulkMetadataUpdates, cache)

		// store withdrawals with the key <chainid>:W:<reversePaddedBlockNumber>:<rank>:<reversePaddedWithdrawalIndex>
		key := fmt.Sprintf("%s:W:%s:%02d:%s", bigtable.chainId, reversedPaddedBlockNumber(block.GetNumber()), block.GetRank(), iReversed)
		mut := gcp_bigtable.NewMutation()

		b, err := proto.Marshal(&withdrawalIndexed)
		if err != nil {
			return nil, nil, fmt.Errorf("error marshalling proto object err: %w", err)
		}

		mut.Set(DEFAULT_FAMILY, DATA_COLUMN, gcp_bigtable.Timestamp(0), b)

		bulkData.Keys = append(bulkData.Keys, key)
		bulkData.Muts = append(bulkData.Muts, mut)

		indexes := []string{
			// Index withdrawal by address
			fmt.Sprintf("%s:I:W:%x:TIME:%s:%s", bigtable.chainId, withdrawal.Address, reversePaddedBigtableTimestamp(block.Time), iReversed),
		}

		for _, idx := range indexes {
			mut := gcp_bigtable.NewMutation()
			mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

			bulkData.Keys = append(bulkData.Keys, idx)
			bulkData.Muts = append(bulkData.Muts, mut)
		}
	}

	return bulkData, bulkMetadataUpdates, nil
}

// TransformEnsNameRegistered accepts an eth1 block and creates bigtable mutations for ENS Name events.
// It transforms the logs contained within a block and indexes ens relevant transactions and tags changes (to be verified from the node in a separate process)
// ==================================================
//
// # It indexes transactions
//
// - by hashed ens name
// Row:    <chainID>:ENS:I:H:<nameHash>:<txHash>
// Family: f
// Column: nil
// Cell:   nil
// Example scan: "5:ENS:I:H:4ae569dd0aa2f6e9207e41423c956d0d27cbc376a499ee8d90fe1d84489ae9d1:e627ae94bd16eb1ed8774cd4003fc25625159f13f8a2612cc1c7f8d2ab11b1d7"
//
// - by address
// Row:    <chainID>:ENS:I:A:<address>:<txHash>
// Family: f
// Column: nil
// Cell:   nil
// Example scan: "5:ENS:I:A:05579fadcf7cc6544f7aa018a2726c85251600c5:e627ae94bd16eb1ed8774cd4003fc25625159f13f8a2612cc1c7f8d2ab11b1d7"
//
// ==================================================
//
// Track for later verification via the node ("set dirty")
//
// - by name
// Row:    <chainID>:ENS:V:N:<name>
// Family: f
// Column: nil
// Cell:   nil
// Example scan: "5:ENS:V:N:somename"
//
// - by name hash
// Row:    <chainID>:ENS:V:H:<nameHash>
// Family: f
// Column: nil
// Cell:   nil
// Example scan: "5:ENS:V:H:6f5d9cc23e60abe836401b4fd386ec9280a1f671d47d9bf3ec75dab76380d845"
//
// - by address
// Row:    <chainID>:ENS:V:A:<address>
// Family: f
// Column: nil
// Cell:   nil
// Example scan: "5:ENS:V:A:27234cb8734d5b1fac0521c6f5dc5aebc6e839b6"
//
// ==================================================
func (bigtable *Bigtable) TransformEnsNameRegistered(blk *types.Eth1Block, cache *freecache.Cache) (bulkData *types.BulkMutations, bulkMetadataUpdates *types.BulkMutations, err error) {
	startTime := time.Now()
	defer func() {
		metrics.TaskDuration.WithLabelValues("bt_transform_ens").Observe(time.Since(startTime).Seconds())
	}()

	bulkData = &types.BulkMutations{}
	bulkMetadataUpdates = &types.BulkMutations{}
	var ensCrontractAddresses map[string]string
	switch bigtable.chainId {
	case "1":
		ensCrontractAddresses = ensContracts.ENSCrontractAddressesEthereum
	case "17000":
		ensCrontractAddresses = ensContracts.ENSCrontractAddressesHolesky
	case "11155111":
		ensCrontractAddresses = ensContracts.ENSCrontractAddressesSepolia
	// TODO hoodi
	default:
		return bulkData, bulkMetadataUpdates, nil
	}

	keys := make(map[string]bool)
	ethLog := eth_types.Log{}

	for i, tx := range blk.GetTransactions() {
		if i >= TX_PER_BLOCK_LIMIT {
			return nil, nil, fmt.Errorf("unexpected number of transactions in block expected at most %d but got: %v, tx: %x", TX_PER_BLOCK_LIMIT-1, i, tx.GetHash())
		}
		for j, log := range tx.GetLogs() {
			if j >= ITX_PER_TX_LIMIT {
				return nil, nil, fmt.Errorf("unexpected number of logs in block expected at most %d but got: %v tx: %x", ITX_PER_TX_LIMIT-1, j, tx.GetHash())
			}
			ensContract := ensCrontractAddresses[common.BytesToAddress(log.Address).String()]

			topics := log.GetTopics()
			ethTopics := make([]common.Hash, 0, len(topics))
			for _, t := range topics {
				ethTopics = append(ethTopics, common.BytesToHash(t))
			}

			ethLog.Address = common.BytesToAddress(log.GetAddress())
			ethLog.Data = log.Data
			ethLog.Topics = ethTopics
			ethLog.BlockNumber = blk.GetNumber()
			ethLog.TxHash = common.BytesToHash(tx.GetHash())
			ethLog.TxIndex = uint(i)
			ethLog.BlockHash = common.BytesToHash(blk.GetHash())
			ethLog.Index = uint(j)
			ethLog.Removed = log.GetRemoved()

			for _, lTopic := range topics {
				logFields := logger.Fields{
					"block":       blk.GetNumber(),
					"tx":          tx.GetHash(),
					"logIndex":    j,
					"ensContract": ensContract,
				}

				if ensContract == "Registry" {
					if bytes.Equal(lTopic, ensContracts.ENSRegistryParsedABI.Events["NewResolver"].ID.Bytes()) {
						logFields["event"] = "NewResolver"
						r := &ensContracts.ENSRegistryNewResolver{}
						err = ensContracts.ENSRegistryContract.UnpackLog(r, "NewResolver", ethLog)
						if err != nil {
							bigtable.log.WithFields(logFields).Warnf("error unpacking ens-log: %v", err)
							continue
						}
						keys[fmt.Sprintf("%s:ENS:V:H:%x", bigtable.chainId, r.Node)] = true
					} else if bytes.Equal(lTopic, ensContracts.ENSRegistryParsedABI.Events["NewOwner"].ID.Bytes()) {
						logFields["event"] = "NewOwner"
						r := &ensContracts.ENSRegistryNewOwner{}
						err = ensContracts.ENSRegistryContract.UnpackLog(r, "NewOwner", ethLog)
						if err != nil {
							bigtable.log.WithFields(logFields).Warnf("error unpacking ens-log: %v", err)
							continue
						}
						keys[fmt.Sprintf("%s:ENS:V:A:%x", bigtable.chainId, r.Owner)] = true
					} else if bytes.Equal(lTopic, ensContracts.ENSRegistryParsedABI.Events["NewTTL"].ID.Bytes()) {
						logFields["event"] = "NewTTL"
						r := &ensContracts.ENSRegistryNewTTL{}
						err = ensContracts.ENSRegistryContract.UnpackLog(r, "NewTTL", ethLog)
						if err != nil {
							bigtable.log.WithFields(logFields).Warnf("error unpacking ens-log: %v", err)
							continue
						}
						keys[fmt.Sprintf("%s:ENS:V:H:%x", bigtable.chainId, r.Node)] = true
					}
				} else if ensContract == "ETHRegistrarController" {
					if bytes.Equal(lTopic, ensContracts.ENSETHRegistrarControllerParsedABI.Events["NameRegistered"].ID.Bytes()) {
						logFields["event"] = "NameRegistered"
						r := &ensContracts.ENSETHRegistrarControllerNameRegistered{}
						err = ensContracts.ENSETHRegistrarControllerContract.UnpackLog(r, "NameRegistered", ethLog)
						if err != nil {
							bigtable.log.WithFields(logFields).Warnf("error unpacking ens-log: %v", err)
							continue
						}
						if err = verifyName(r.Name); err != nil {
							bigtable.log.WithFields(logFields).Warnf("error verifying ens-name: %v", err)
							continue
						}
						keys[fmt.Sprintf("%s:ENS:V:N:%s", bigtable.chainId, r.Name)] = true
						keys[fmt.Sprintf("%s:ENS:V:A:%x", bigtable.chainId, r.Owner)] = true
					} else if bytes.Equal(lTopic, ensContracts.ENSETHRegistrarControllerParsedABI.Events["NameRenewed"].ID.Bytes()) {
						logFields["event"] = "NameRenewed"
						r := &ensContracts.ENSETHRegistrarControllerNameRenewed{}
						err = ensContracts.ENSETHRegistrarControllerContract.UnpackLog(r, "NameRenewed", ethLog)
						if err != nil {
							bigtable.log.WithFields(logFields).Warnf("error unpacking ens-log: %v", err)
							continue
						}
						if err = verifyName(r.Name); err != nil {
							bigtable.log.WithFields(logFields).Warnf("error verifying ens-name: %v", err)
							continue
						}
						keys[fmt.Sprintf("%s:ENS:V:N:%s", bigtable.chainId, r.Name)] = true
					}
				} else if ensContract == "OldEnsRegistrarController" {
					if bytes.Equal(lTopic, ensContracts.ENSOldRegistrarControllerParsedABI.Events["NameRegistered"].ID.Bytes()) {
						logFields["event"] = "NameRegistered"
						r := &ensContracts.ENSOldRegistrarControllerNameRegistered{}
						err = ensContracts.ENSOldRegistrarControllerContract.UnpackLog(r, "NameRegistered", ethLog)
						if err != nil {
							bigtable.log.WithFields(logFields).Warnf("error unpacking ens-log: %v", err)
							continue
						}
						if err = verifyName(r.Name); err != nil {
							bigtable.log.WithFields(logFields).Warnf("error verifying ens-name: %v", err)
							continue
						}
						keys[fmt.Sprintf("%s:ENS:V:N:%s", bigtable.chainId, r.Name)] = true
						keys[fmt.Sprintf("%s:ENS:V:A:%x", bigtable.chainId, r.Owner)] = true
					} else if bytes.Equal(lTopic, ensContracts.ENSOldRegistrarControllerParsedABI.Events["NameRenewed"].ID.Bytes()) {
						logFields["event"] = "NameRenewed"
						r := &ensContracts.ENSOldRegistrarControllerNameRenewed{}
						err = ensContracts.ENSOldRegistrarControllerContract.UnpackLog(r, "NameRenewed", ethLog)
						if err != nil {
							bigtable.log.WithFields(logFields).Warnf("error unpacking ens-log: %v", err)
							continue
						}
						if err = verifyName(r.Name); err != nil {
							bigtable.log.WithFields(logFields).Warnf("error verifying ens-name: %v", err)
							continue
						}
						keys[fmt.Sprintf("%s:ENS:V:N:%s", bigtable.chainId, r.Name)] = true
					}
				} else {
					if bytes.Equal(lTopic, ensContracts.ENSPublicResolverParsedABI.Events["NameChanged"].ID.Bytes()) {
						logFields["event"] = "NameChanged"
						r := &ensContracts.ENSPublicResolverNameChanged{}
						err = ensContracts.ENSPublicResolverContract.UnpackLog(r, "NameChanged", ethLog)
						if err != nil {
							bigtable.log.WithFields(logFields).Warnf("error unpacking ens-log: %v", err)
							continue
						}
						if err = verifyName(r.Name); err != nil {
							bigtable.log.WithFields(logFields).Warnf("error verifying ens-name: %v", err)
							continue
						}
						keys[fmt.Sprintf("%s:ENS:V:N:%s", bigtable.chainId, r.Name)] = true
					} else if bytes.Equal(lTopic, ensContracts.ENSPublicResolverParsedABI.Events["AddressChanged"].ID.Bytes()) {
						logFields["event"] = "AddressChanged"
						r := &ensContracts.ENSPublicResolverAddressChanged{}
						err = ensContracts.ENSPublicResolverContract.UnpackLog(r, "AddressChanged", ethLog)
						if err != nil {
							bigtable.log.WithFields(logFields).Warnf("error unpacking ens-log: %v", err)
							continue
						}
						keys[fmt.Sprintf("%s:ENS:V:H:%x", bigtable.chainId, r.Node)] = true
					}
				}
			}
		}
	}
	for key := range keys {
		mut := gcp_bigtable.NewMutation()
		mut.Set(DEFAULT_FAMILY, key, gcp_bigtable.Timestamp(0), nil)

		bulkData.Keys = append(bulkData.Keys, key)
		bulkData.Muts = append(bulkData.Muts, mut)
	}

	return bulkData, bulkMetadataUpdates, nil
}
