package handlers

import (
	"database/sql"
	"fmt"
	"math/big"
	"net/http"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/templates"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	"github.com/ethereum/go-ethereum/consensus/misc/eip4844"
	"github.com/gorilla/mux"
)

var (
	blockTemplate = templates.GetTemplate(
		append(layoutTemplateFiles,
			"slot/slot.html",
			"slot/transactions.html",
			"slot/attestations.html",
			"slot/deposits.html",
			"slot/votes.html",
			"slot/attesterSlashing.html",
			"slot/proposerSlashing.html",
			"slot/exits.html",
			"components/timestamp.html",
			"slot/overview.html",
			"slot/execTransactions.html",
			"slot/blobs.html",
			"slot/withdrawals.html",
		)...)

	preMergeBlockTemplate = templates.GetTemplate(
		append(layoutTemplateFiles,
			"execution/block.html",
			"slot/execTransactions.html",
			"components/timestamp.html",
		)...)

	blockNotFoundTemplate = templates.GetTemplate(
		append(layoutTemplateFiles, "slotnotfound.html")...)
)

// Eth1Block returns an HTTP handler that renders an execution-layer block page.
//
// It supports both pre-Merge (PoW) and post-Merge (PoS) blocks and optionally accepts
// a "rank" parameter to display a specific ranked parallel block.
//
// The handler extracts the block number or hash from the URL, resolves it using the provided
// ExecutionClient, fetches the corresponding execution block data from Bigtable,
// and renders the appropriate HTML template based on the Merge context.
//
// If the block is not found or input is invalid, a 404 Not Found page is rendered.
//
// URL parameters:
//   - {block}: block number (decimal) or hash (64 hex chars)
//   - {rank} (optional): execution block rank (0 to 4)
//
// Parameters:
//   - bt: reference to the Bigtable client
//   - rpc: execution-layer RPC client
func Eth1Block(bt *db.Bigtable, rpc execution.ExecutionClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		vars := mux.Vars(r)

		// Parse block ID from URL
		rank, err := parseRankParam(vars)
		if err != nil {
			renderNotFound(w, r, "blockchain", "/block", err.Error(), blockNotFoundTemplate)
			return
		}
		number, err := parseBlockNumber(vars, rpc)
		if err != nil {
			renderNotFound(w, r, "blockchain", "/block", err.Error(), blockNotFoundTemplate)
			return
		}

		// Fetch execution-layer block data
		eth1BlockPageData, err := getExecutionBlockPageData(number, rank, 10, bt, rpc)
		if err != nil {
			renderNotFound(w, r, "blockchain", "/block", fmt.Sprintf("block %d (rank %d) not found", number, rank), blockNotFoundTemplate)
			return
		}

		if isPostMergeBlock(number, eth1BlockPageData.Ts.Unix(), eth1BlockPageData.Difficulty) {
			handlePostMergeBlock(w, r, number, eth1BlockPageData)
		} else {
			handlePreMergeBlock(w, r, number, eth1BlockPageData)
		}
	}
}

// handlePostMergeBlock handles rendering of a post-Merge (PoS) execution block.
//
// It invokes renderExecutionBlockPostMerge to retrieve the corresponding consensus-layer
// slot data and render the template. If the slot is not found or an unexpected error occurs,
// a 404 Not Found page is rendered.
func handlePostMergeBlock(w http.ResponseWriter, r *http.Request, number uint64, data *types.Eth1BlockPageData) {
	err := renderExecutionBlockPostMerge(w, r, number, data)
	if err != nil {
		if err != sql.ErrNoRows {
			log.Errorf("error retrieving slot page data: %v", err)
		}
		renderNotFound(w, r, "blockchain", "/block", fmt.Sprintf("slot not found for block %d", number), blockNotFoundTemplate)
	}
}

// renderExecutionBlockPostMerge renders a post-Merge execution block using slot data.
//
// It derives the consensus slot from the block timestamp, loads the corresponding slot page data,
// attaches execution-layer data to it, and renders the full block layout.
//
// Returns an error if the slot data cannot be loaded or the template rendering fails.
func renderExecutionBlockPostMerge(
	w http.ResponseWriter,
	r *http.Request,
	number uint64,
	blockData *types.Eth1BlockPageData,
) error {
	blockSlot := uint64(0)
	if !utils.IsPoSBlock0(number, blockData.Ts.Unix()) {
		blockSlot = utils.TimeToSlot(uint64(blockData.Ts.Unix()))
	}

	slotPageData, err := GetSlotPageData(blockSlot)
	if err != nil {
		return err
	}

	slotPageData.ExecutionData = blockData
	slotPageData.ExecutionData.IsValidMev = slotPageData.IsValidMev

	data := InitPageData(w, r, "blockchain", "/block", fmt.Sprintf("Block %d", number), nil)
	data.Data = slotPageData

	return blockTemplate.ExecuteTemplate(w, "layout", data)
}

// handlePreMergeBlock handles rendering of a pre-Merge (PoW) execution block.
//
// It delegates rendering to renderExecutionBlockPreMerge. If rendering fails,
// a 404 Not Found page is shown.
func handlePreMergeBlock(w http.ResponseWriter, r *http.Request, number uint64, data *types.Eth1BlockPageData) {
	err := renderExecutionBlockPreMerge(w, r, data)
	if err != nil {
		renderNotFound(w, r, "blockchain", "/block", fmt.Sprintf("render failed for block %d", number), blockNotFoundTemplate)
	}
}

// renderExecutionBlockPreMerge renders a pre-Merge execution block page.
//
// It initializes the page data and renders the block using the legacy pre-Merge template.
//
// Returns an error if the template rendering fails.
func renderExecutionBlockPreMerge(
	w http.ResponseWriter,
	r *http.Request,
	blockData *types.Eth1BlockPageData,
) error {
	data := InitPageData(w, r, "block", "/block", fmt.Sprintf("Block %d", blockData.Number), nil)
	data.Data = blockData
	return preMergeBlockTemplate.ExecuteTemplate(w, "layout", data)
}

func getExecutionBlockPageData(number uint64, rank uint32, limit int, bt *db.Bigtable, rpc execution.ExecutionClient) (*types.Eth1BlockPageData, error) {
	block, err := bt.GetBlockFromBlocksTable(number, rank)
	if diffToHead := int64(services.LatestEth1BlockNumber()) - int64(number); err != nil && diffToHead < 0 && diffToHead >= -5 {
		block, _, err = rpc.GetBlock(int64(number), "parity/geth", rank)
	}
	if err != nil {
		return nil, err
	}

	// retrieve address names from bigtable
	names := make(map[string]string)
	names[string(block.Coinbase)] = ""
	for _, tx := range block.Transactions {
		names[string(tx.From)] = ""
		names[string(tx.To)] = ""
	}
	for _, uncle := range block.Uncles {
		names[string(uncle.Coinbase)] = ""
	}
	names, _, err = bt.GetAddressesNamesArMetadata(&names, nil)
	if err != nil {
		return nil, err
	}

	// calculate total block reward and set lowest gas price
	txs := []types.Eth1BlockPageTransaction{}
	txFees := new(big.Int)
	lowestGasPrice := big.NewInt(1 << 62)
	blobTxCount := 0
	blobCount := 0

	contractInteractionTypes, err := bt.GetAddressContractInteractionsAtBlock(block)
	if err != nil {
		utils.LogError(err, "error getting contract states", 0)
	}

	for i, tx := range block.Transactions {
		if tx.Type == 3 {
			blobTxCount++
			blobCount += len(tx.BlobVersionedHashes)
		}

		// sum txFees
		txFee := db.CalculateTxFeeFromTransaction(tx, new(big.Int).SetBytes(block.BaseFee))
		txFees.Add(txFees, txFee)

		effectiveGasPrice := big.NewInt(0)
		if gasUsed := new(big.Int).SetUint64(tx.GasUsed); gasUsed.Cmp(big.NewInt(0)) != 0 {
			// calculate effective gas price
			effectiveGasPrice = new(big.Int).Div(txFee, gasUsed)
			if effectiveGasPrice.Cmp(lowestGasPrice) < 0 {
				lowestGasPrice = effectiveGasPrice
			}
		}

		contractCreation := tx.GetTo() == nil
		// set tx to if tx is contract creation
		if contractCreation {
			tx.To = tx.ContractAddress
		}

		var contractInteraction types.ContractInteractionType
		if len(contractInteractionTypes) > i {
			contractInteraction = contractInteractionTypes[i]
		}

		txs = append(txs, types.Eth1BlockPageTransaction{
			Hash:          fmt.Sprintf("%#x", tx.Hash),
			HashFormatted: utils.FormatTransactionHash(tx.Hash, tx.ErrorMsg == ""),
			From:          fmt.Sprintf("%#x", tx.From),
			FromFormatted: utils.FormatAddressWithLimits(tx.From, names[string(tx.From)], false, "address", 15, 20, true),
			To:            fmt.Sprintf("%#x", tx.To),
			ToFormatted:   utils.FormatAddressWithLimits(tx.To, bt.GetAddressLabel(names[string(tx.To)], contractInteraction), contractInteraction != types.CONTRACT_NONE, "address", 15, 20, true),
			Value:         new(big.Int).SetBytes(tx.Value),
			Fee:           txFee,
			GasPrice:      effectiveGasPrice,
			Method:        bt.GetMethodLabel(tx.GetData(), contractInteraction),
		})
	}

	blockReward := utils.Eth1BlockReward(block.Number, block.Difficulty)

	uncleInclusionRewards := new(big.Int)
	uncleInclusionRewards.Div(blockReward, big.NewInt(32)).Mul(uncleInclusionRewards, big.NewInt(int64(len(block.Uncles))))
	uncles := []types.Eth1BlockPageData{}
	for _, uncle := range block.Uncles {
		reward := big.NewInt(int64(uncle.Number - block.Number + 8))
		reward.Mul(reward, blockReward).Div(reward, big.NewInt(8))
		uncles = append(uncles, types.Eth1BlockPageData{
			Number:       uncle.Number,
			MinerAddress: fmt.Sprintf("%#x", uncle.Coinbase),
			//MinerFormatted: utils.FormatAddress(uncle.Coinbase, nil, names[string(uncle.Coinbase)], false, false, false),
			MinerFormatted: utils.FormatAddressWithLimits(uncle.Coinbase, names[string(uncle.Coinbase)], false, "address", 42, 42, true),
			Reward:         reward,
			Extra:          string(uncle.Extra),
		})
	}

	if limit > 0 {
		if len(txs) > limit {
			txs = txs[:limit]
		} else {
			txs = txs[:0]
		}
	}

	blobGasPrice := eip4844.CalcBlobFee(block.ExcessBlobGas, true) // TODO: check this
	burnedTxFees := new(big.Int).Mul(new(big.Int).SetBytes(block.BaseFee), big.NewInt(int64(block.GasUsed)))
	burnedBlobFees := new(big.Int).Mul(blobGasPrice, big.NewInt(int64(block.BlobGasUsed)))
	burnedFees := new(big.Int).Add(burnedTxFees, burnedBlobFees)
	blockReward.Add(blockReward, txFees).Add(blockReward, uncleInclusionRewards).Sub(blockReward, burnedTxFees)
	nextBlock := number + 1
	if nextBlock > services.LatestEth1BlockNumber() {
		nextBlock = 0
	}
	eth1BlockPageData := types.Eth1BlockPageData{
		Number:        number,
		PreviousBlock: number - 1,
		NextBlock:     nextBlock,
		TxCount:       uint64(len(block.Transactions)),
		UncleCount:    uint64(len(block.Uncles)),
		BlobTxCount:   uint64(blobTxCount),
		BlobCount:     uint64(blobCount),
		Hash:          fmt.Sprintf("%#x", block.Hash),
		ParentHash:    fmt.Sprintf("%#x", block.ParentHash),
		MinerAddress:  fmt.Sprintf("%#x", block.Coinbase),
		//MinerFormatted: utils.FormatAddress(block.Coinbase, nil, names[string(block.Coinbase)], false, false, false),
		MinerFormatted: utils.FormatAddressWithLimits(block.Coinbase, names[string(block.Coinbase)], false, "address", 42, 42, true),
		Reward:         blockReward,
		//MevReward:      db.CalculateMevFromBlock(block), // deprecated, don't show this value as mev
		MevReward:      new(big.Int),
		TxFees:         txFees,
		GasUsage:       utils.FormatBlockUsage(block.GasUsed, block.GasLimit),
		GasLimit:       block.GasLimit,
		LowestGasPrice: lowestGasPrice,
		Ts:             block.GetTime().AsTime(),
		Difficulty:     new(big.Int).SetBytes(block.Difficulty),
		BaseFeePerGas:  new(big.Int).SetBytes(block.BaseFee),
		BurnedFees:     burnedFees,
		BurnedTxFees:   burnedTxFees,
		BurnedBlobFees: burnedBlobFees,
		BlobGasUsed:    block.GetBlobGasUsed(),
		ExcessBlobGas:  block.GetExcessBlobGas(),
		BlobGasPrice:   blobGasPrice,
		Extra:          fmt.Sprintf("%#x", block.Extra),
		Txs:            txs,
		Uncles:         uncles,
	}

	var relaysData struct {
		MevRecipient []byte          `db:"proposer_fee_recipient"`
		MevBribe     types.WeiString `db:"value"`
	}
	// try to get mev rewards from relays_blocks table
	err = db.ReaderDb.Get(&relaysData, `SELECT proposer_fee_recipient, value FROM relays_blocks WHERE relays_blocks.exec_block_hash = $1 limit 1`, block.Hash)
	if err == nil {
		eth1BlockPageData.MevBribe = relaysData.MevBribe.BigInt()
		eth1BlockPageData.MevRecipientFormatted = utils.FormatAddressWithLimits(relaysData.MevRecipient, names[string(relaysData.MevRecipient)], false, "address", 42, 42, true)
	}
	return &eth1BlockPageData, nil
}