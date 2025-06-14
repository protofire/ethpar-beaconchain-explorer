package handlers

import (
	"encoding/json"
	"fmt"
	"html/template"
	"math/big"
	"net/http"
	"strconv"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/templates"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	"golang.org/x/sync/errgroup"
)

const (
	visibleDigitsForHash         = 8
	minimumTransactionsPerUpdate = 25
)

func Eth1Transactions(w http.ResponseWriter, r *http.Request) {
	templateFiles := append(layoutTemplateFiles, "execution/transactions.html")
	var eth1TransactionsTemplate = templates.GetTemplate(templateFiles...)

	w.Header().Set("Content-Type", "text/html")

	data := InitPageData(w, r, "blockchain", "/eth1transactions", "Transactions", templateFiles)
	data.Data = getTransactionDataStartingWithPageToken("")

	if handleTemplateError(w, r, "eth1Transactions.go", "Eth1Transactions", "", eth1TransactionsTemplate.ExecuteTemplate(w, "layout", data)) != nil {
		return // an error has occurred and was processed
	}
}

func Eth1TransactionsData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	err := json.NewEncoder(w).Encode(getTransactionDataStartingWithPageToken(r.URL.Query().Get("pageToken")))
	if err != nil {
		logger.Errorf("error enconding json response for %v route: %v", r.URL.String(), err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
	}
}

func getTransactionDataStartingWithPageToken(pageToken string) *types.DataTableResponse {
	pageTokenId := uint64(0)
	{
		if len(pageToken) > 0 {
			v, err := strconv.ParseUint(pageToken, 10, 64)
			if err == nil && v > 0 {
				pageTokenId = v
			}
		}
	}
	if pageTokenId == 0 && pageToken != "0" {
		pageTokenId = services.LatestEth1BlockNumber()
	}

	tableData := make([][]interface{}, 0, minimumTransactionsPerUpdate)
	for len(tableData) < minimumTransactionsPerUpdate && pageTokenId != 0 {
		b, n, err := getEth1BlockAndNext(pageTokenId)
		if err != nil {
			logger.Errorf("error getting transaction from block %v", err)
			return nil
		}
		t := b.GetTransactions()
		contractInteractionTypes, err := db.BigtableClient.GetAddressContractInteractionsAtBlock(b)
		if err != nil {
			utils.LogError(err, "error getting contract states", 0)
		}

		// retrieve metadata
		names := make(map[string]string)
		{
			for _, v := range t {
				names[string(v.GetFrom())] = ""
				names[string(v.GetTo())] = ""
			}
			names, _, err = db.BigtableClient.GetAddressesNamesArMetadata(&names, nil)
			if err != nil {
				logger.Errorf("error getting name for addresses: %v", err)
				return nil
			}
		}

		var wg errgroup.Group
		for i := len(t) - 1; i >= 0; i-- {
			v := t[i]
			wg.Go(func() error {
				if v.GetTo() == nil {
					v.To = v.ContractAddress
				}
				var contractInteraction types.ContractInteractionType
				if len(contractInteractionTypes) > i {
					contractInteraction = contractInteractionTypes[i]
				}
				tableData = append(tableData, []interface{}{
					utils.FormatAddressWithLimits(v.GetHash(), "", false, "tx", visibleDigitsForHash+5, 18, true),
					utils.FormatMethod(db.BigtableClient.GetMethodLabel(v.GetData(), contractInteraction)),
					template.HTML(fmt.Sprintf(`<A href="block/%d">%v</A>`, b.GetNumber(), utils.FormatAddCommas(b.GetNumber()))),
					utils.FormatTimestamp(b.GetTime().AsTime().Unix()),
					utils.FormatAddressWithLimits(v.GetFrom(), names[string(v.GetFrom())], false, "address", visibleDigitsForHash+5, 18, true),
					utils.FormatAddressWithLimits(v.GetTo(), db.BigtableClient.GetAddressLabel(names[string(v.GetTo())], contractInteraction), contractInteraction != types.CONTRACT_NONE, "address", 15, 20, true),
					utils.FormatAmountFormatted(new(big.Int).SetBytes(v.GetValue()), utils.Config.Frontend.ElCurrency, 8, 4, true, true, false),
					utils.FormatAmountFormatted(db.CalculateTxFeeFromTransaction(v, new(big.Int).SetBytes(b.GetBaseFee())), utils.Config.Frontend.ElCurrency, 8, 4, true, true, false),
				})
				return nil
			})
			wg.Wait()
		}

		pageTokenId = n
	}

	return &types.DataTableResponse{
		Data:        tableData,
		PagingToken: fmt.Sprintf("%d", pageTokenId),
	}
}

// Returns the block requested via number and the number of the next block in our bigtable schema (i.e. the block that came chronologically before the requested block)
//
// If nextBlock doesn't exists nil, 0, nil is returned
func getEth1BlockAndNext(number uint64) (*types.Eth1Block, uint64, error) {
	block, err := db.BigtableClient.GetBlockFromBlocksTable(number)
	if err != nil {
		return nil, 0, err
	}
	if block == nil {
		return nil, 0, fmt.Errorf("block %d not found", number)
	}

	if number == 0 {
		return block, 0, nil
	}

	nextBlock := uint64(0)
	{
		blocks, err := db.BigtableClient.GetBlocksDescending(number, 2)
		if err != nil {
			return nil, 0, err
		}
		if len(blocks) > 1 {
			nextBlock = blocks[1].GetNumber()
		}
	}

	return block, nextBlock, nil
}
