package handlers

import (
	"encoding/json"
	"bytes"
	"fmt"
	"html/template"
	"math/big"
	"net/http"
	"strings"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/price"
	"github.com/protofire/ethpar-beaconchain-explorer/templates"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/mux"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"
)

func Eth1Token(bt *db.Bigtable, pg *db.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		templateFiles := append(layoutTemplateFiles, "execution/token.html")
		eth1TokenTemplate := templates.GetTemplate(templateFiles...)

		w.Header().Set("Content-Type", "text/html")
		vars := mux.Vars(r)
		token := common.FromHex(strings.TrimPrefix(vars["token"], "0x"))
		address := common.FromHex(strings.TrimPrefix(r.URL.Query().Get("a"), "0x"))

		g := new(errgroup.Group)
		g.SetLimit(3)

		var (
			rawTxns         []*types.Eth1ERC20Indexed
			pagingToken     string
			metadata        *types.ERC20Metadata
			balance         *types.Eth1AddressBalance
			transfersTable  *types.DataTableResponse
		)

		// 1. Транзакции
		g.Go(func() error {
			var err error
			rawTxns, pagingToken, err = bt.GetERC20TokenTransactions(token, address, "")
			return err
		})

		// 2. Метаданные токена
		g.Go(func() error {
			var err error
			metadata, err = bt.GetERC20MetadataForAddress(token)
			return err
		})

		// 3. Баланс (если задан адрес)
		if len(address) != 0 {
			g.Go(func() error {
				var err error
				balance, err = bt.GetBalanceForAddress(address, token)
				return err
			})
		}

		if err := g.Wait(); err != nil {
			if handleTemplateError(w, r, "eth1Token.go", "Eth1Token", "g.Wait()", err) != nil {
				return
			}
			return
		}

		// --- Подгружаем имена через AddressNamesService ---
		addressSet := make(map[string]string)
		for _, tx := range rawTxns {
			addressSet[string(tx.From)] = ""
			addressSet[string(tx.To)] = ""
		}
		namesSvc := services.AddressNamesService{BT: bt, PG: pg, Ctx: r.Context()}
		names, err := namesSvc.GetNamesForAddresses(addressSet)
		if err != nil {
			log.WithError(err).Warn("failed to resolve address names")
		}

		// --- Форматируем таблицу ---
		formattedRows := make([][]interface{}, len(rawTxns))
		for i, tx := range rawTxns {
			fromName := names[string(tx.From)]
			toName := names[string(tx.To)]

			from := utils.FormatAddress(tx.From, tx.TokenAddress, fromName, false, false, !bytes.Equal(tx.From, address))
			to := utils.FormatAddress(tx.To, tx.TokenAddress, toName, false, false, !bytes.Equal(tx.To, address))

			tb := &types.Eth1AddressBalance{
				Address:  address,
				Balance:  tx.Value,
				Token:    tx.TokenAddress,
				Metadata: metadata,
			}

			formattedRows[i] = []interface{}{
				utils.FormatTransactionHash(tx.ParentHash, true),
				utils.FormatTimestamp(tx.Time.AsTime().Unix()),
				from,
				utils.FormatInOutSelf(address, tx.From, tx.To),
				to,
				utils.FormatTokenValue(tb, false),
			}
		}

		transfersTable = &types.DataTableResponse{
			Data:        formattedRows,
			PagingToken: pagingToken,
		}

		// --- Цены и метрики ---
		ethPriceUsd := decimal.NewFromFloat(price.GetPrice(utils.Config.Frontend.ElCurrency, "USD"))
		tokenDecimals := decimal.NewFromBigInt(new(big.Int).SetBytes(metadata.Decimals), 0)
		tokenDiv := decimal.NewFromInt(10).Pow(tokenDecimals)
		ethDiv := decimal.NewFromInt(utils.Config.Frontend.ElCurrencyDivisor)

		tokenPriceEth := decimal.NewFromBigInt(new(big.Int).SetBytes(metadata.Price), 0).DivRound(ethDiv, 18)
		tokenPriceUsd := ethPriceUsd.Mul(tokenPriceEth).Mul(tokenDiv).DivRound(ethDiv, 18)

		tokenSupply := decimal.NewFromBigInt(new(big.Int).SetBytes(metadata.TotalSupply), 0).DivRound(tokenDiv, 18)
		tokenMarketCapUsd := tokenPriceUsd.Mul(tokenSupply)

		// --- QR ---
		pngStr, pngStrInverse, err := utils.GenerateQRCodeForAddress(token)
		if err != nil {
			log.WithError(err).Errorf("error generating qr code for token %x", token)
		}

		// --- Финальный рендер ---
		data := InitPageData(w, r, "blockchain", "/token", fmt.Sprintf("Token 0x%x", token), templateFiles)
		data.Data = types.Eth1TokenPageData{
			Token:          fmt.Sprintf("%x", token),
			Address:        fmt.Sprintf("%x", address),
			TransfersTable: transfersTable,
			Metadata:       metadata,
			Balance:        balance,
			QRCode:         pngStr,
			QRCodeInverse:  pngStrInverse,
			MarketCap:      template.HTML("$" + utils.FormatThousandsEnglish(tokenMarketCapUsd.StringFixed(2))),
			Supply:         template.HTML(utils.FormatThousandsEnglish(tokenSupply.StringFixed(6))),
			Price:          template.HTML("$" + utils.FormatThousandsEnglish(tokenPriceUsd.StringFixed(6))),
		}

		if handleTemplateError(w, r, "eth1Token.go", "Eth1Token", "Done", eth1TokenTemplate.ExecuteTemplate(w, "layout", data)) != nil {
			return
		}
	}
}


func Eth1TokenTransfers(bt *db.Bigtable, pg *db.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		q := r.URL.Query()
		vars := mux.Vars(r)

		token := common.FromHex(strings.TrimPrefix(vars["token"], "0x"))
		address := common.FromHex(strings.TrimPrefix(q.Get("a"), "0x"))
		pageToken := q.Get("pageToken")

		rawTxs, pagingToken, err := bt.GetERC20TokenTransactions(token, address, pageToken)
		if err != nil {
			utils.LogError(err, "error fetching raw token transactions", 0, map[string]interface{}{
				"route": r.URL.String(),
			})
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		addressSet := make(map[string]string)
		tokenSet := make(map[string]*types.ERC20Metadata)

		for _, t := range rawTxs {
			addressSet[string(t.From)] = ""
			addressSet[string(t.To)] = ""
			tokenSet[string(t.TokenAddress)] = nil
		}

		namesSvc := services.AddressNamesService{BT: bt, PG: pg, Ctx: r.Context()}
		names, err := namesSvc.GetNamesForAddresses(addressSet)
		if err != nil {
			log.WithError(err).Error("failed to resolve address names for token transfers")
		}

		tokens, err := bt.GetERC20MetadataBatch(tokenSet)
		if err != nil {
			log.WithError(err).Error("failed to resolve ERC20 token metadata")
		}

		// Форматирование таблицы
		tableData := make([][]interface{}, len(rawTxs))
		for i, t := range rawTxs {
			fromName := names[string(t.From)]
			toName := names[string(t.To)]

			from := utils.FormatAddress(t.From, t.TokenAddress, fromName, false, false, !bytes.Equal(t.From, address))
			to := utils.FormatAddress(t.To, t.TokenAddress, toName, false, false, !bytes.Equal(t.To, address))

			tb := &types.Eth1AddressBalance{
				Address:  address,
				Balance:  t.Value,
				Token:    t.TokenAddress,
				Metadata: tokens[string(t.TokenAddress)],
			}

			tableData[i] = []interface{}{
				utils.FormatTransactionHash(t.ParentHash, true),
				utils.FormatTimestamp(t.Time.AsTime().Unix()),
				from,
				utils.FormatInOutSelf(address, t.From, t.To),
				to,
				utils.FormatTokenValue(tb, false),
			}
		}

		resp := &types.DataTableResponse{
			Data:        tableData,
			PagingToken: pagingToken,
		}

		// Отправка JSON-ответа
		if err := json.NewEncoder(w).Encode(resp); err != nil {
			utils.LogError(err, "error encoding json response", 0, map[string]interface{}{
				"route": r.URL.String(),
			})
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}
}