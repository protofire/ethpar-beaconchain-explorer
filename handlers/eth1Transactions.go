package handlers

import (
	"strings"
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
)

const (
	visibleDigitsForHash         = 8
	minimumTransactionsPerUpdate = 25
)

var (
	eth1TransactionsTemplate = templates.GetTemplate(
		append(layoutTemplateFiles,
			"execution/transactions.html",
		)...)
)

// Eth1Transactions returns an HTTP handler that renders a list of execution-layer transactions.
//
// The handler initializes standard page metadata and injects a list of transactions
// starting from the first available page token. The result is rendered into the
// "execution/transactions.html" template within the base layout.
//
// Parameters:
//   - bt: reference to the Bigtable client used to retrieve transaction data
//
// TODO: implement pagination
func Eth1Transactions(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")

		data := InitPageData(w, r, "blockchain", "/eth1transactions", "Transactions", nil)
		data.Data = getTransactionsFromCursor("", bt)

		if handleTemplateError(w, r, "eth1Transactions.go", "Eth1Transactions", "", eth1TransactionsTemplate.ExecuteTemplate(w, "layout", data)) != nil {
			return // an error has occurred and was processed
		}
	}
}

// cursor represents a paging position in EthPar.
type cursor struct {
	Block uint64
	Rank  uint32
	Idx   int // next tx index to read in block.Rank (0-based)
}

// parseCursor converts "number:rank:idx" into cursor.
// Empty string or "latest" returns cursor at latest block, rank 4, idx 0.
func parseCursor(s string, latest uint64) (cursor, error) {
	if s == "" || s == "latest" {
		return cursor{Block: latest, Rank: 4, Idx: 0}, nil
	}
	parts := strings.Split(s, ":")
	if len(parts) != 3 {
		return cursor{}, fmt.Errorf("invalid cursor")
	}
	n, err1 := strconv.ParseUint(parts[0], 10, 64)
	r, err2 := strconv.ParseUint(parts[1], 10, 32)
	i, err3 := strconv.Atoi(parts[2])
	if err1 != nil || err2 != nil || err3 != nil || r > 4 || i < 0 {
		return cursor{}, fmt.Errorf("invalid cursor")
	}
	return cursor{Block: n, Rank: uint32(r), Idx: i}, nil
}

// nextCursor moves to the next logical position (rank decreases 4→0, then block-1).
func nextCursor(c cursor) cursor {
	if c.Rank > 0 {
		return cursor{Block: c.Block, Rank: c.Rank - 1, Idx: 0}
	}
	return cursor{Block: c.Block - 1, Rank: 4, Idx: 0}
}

// formatCursor formats a cursor into a string that can be used
// for paging requests.
//
// The result is of the form "block:rank:index" (e.g., "12345:2:17").
func formatCursor(c cursor) string {
	return fmt.Sprintf("%d:%d:%d", c.Block, c.Rank, c.Idx)
}

// getTransactionsFromCursor retrieves a page of execution-layer transactions,
// starting from the specified cursor position.
//
// The cursor string must be in the format "block:rank:index" (e.g., "12345:2:0"),
// where:
//   - block: execution block number,
//   - rank: block rank within the slot (0 = beacon, 4 = latest execution payload),
//   - index: transaction index within that block.
//
// If the cursor is empty or "latest", the iteration starts from the latest block,
// rank 4, index 0. The function traverses blocks and ranks in descending order,
// collecting up to minimumTransactionsPerUpdate transactions.
// It returns a DataTableResponse with the rows and the next paging cursor.
func getTransactionsFromCursor(cursorStr string, bt *db.Bigtable) *types.DataTableResponse {
	const limit = minimumTransactionsPerUpdate

	start, err := parseCursor(cursorStr, services.LatestEth1BlockNumber())
	if err != nil {
		log.Errorf("invalid page cursor: %v", err)
		return nil
	}

	rows := make([][]interface{}, 0, limit)
	cur := start

	for len(rows) < limit && cur.Block > 0 {
		blk, err := bt.GetBlockFromBlocksTable(cur.Block, cur.Rank)
		if err != nil || blk == nil {
			cur = nextCursor(cur)
			continue
		}

		txs := blk.GetTransactions()
		if cur.Idx >= len(txs) {
			cur = nextCursor(cur)
			continue
		}

		interaction, _ := bt.GetAddressContractInteractionsAtBlock(blk)

		names := make(map[string]string)
		for _, tx := range txs {
			names[string(tx.GetFrom())] = ""
			if to := tx.GetTo(); to != nil {
				names[string(to)] = ""
			}
		}
		names, _, _ = bt.GetAddressesNamesArMetadata(&names, nil)

		for ; cur.Idx < len(txs) && len(rows) < limit; cur.Idx++ {
			tx := txs[cur.Idx]
			if tx.GetTo() == nil {
				tx.To = tx.ContractAddress
			}
			ctype := types.CONTRACT_NONE
			if len(interaction) > cur.Idx {
				ctype = interaction[cur.Idx]
			}
			rows = append(rows, []interface{}{
				utils.FormatAddressWithLimits(tx.GetHash(), "", false, "tx", visibleDigitsForHash+5, 18, true),
				utils.FormatMethod(bt.GetMethodLabel(tx.GetData(), ctype)),
				template.HTML(fmt.Sprintf(`<a href="/block/%d/rank/%d">%v</a>`,
					blk.GetNumber(), blk.GetRank(), utils.FormatAddCommas(blk.GetNumber()))),
				utils.FormatTimestamp(blk.GetTime().AsTime().Unix()),
				utils.FormatAddressWithLimits(tx.GetFrom(), names[string(tx.GetFrom())], false,
					"address", visibleDigitsForHash+5, 18, true),
				utils.FormatAddressWithLimits(tx.GetTo(),
					bt.GetAddressLabel(names[string(tx.GetTo())], ctype),
					ctype != types.CONTRACT_NONE, "address", 15, 20, true),
				utils.FormatAmountFormatted(new(big.Int).SetBytes(tx.GetValue()),
					utils.Config.Frontend.ElCurrency, 8, 4, true, true, false),
				utils.FormatAmountFormatted(
					db.CalculateTxFeeFromTransaction(tx, new(big.Int).SetBytes(blk.GetBaseFee())),
					utils.Config.Frontend.ElCurrency, 8, 4, true, true, false),
			})
		}

		if cur.Idx >= len(txs) {
			cur = nextCursor(cur)
		}
	}

	return &types.DataTableResponse{
		Data:        rows,
		PagingToken: formatCursor(cur),
	}
}