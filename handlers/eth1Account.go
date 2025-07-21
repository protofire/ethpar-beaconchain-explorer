package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"html/template"
	"net/http"
	"strings"
	"time"
	"math/big"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/eth1data"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
	"github.com/protofire/ethpar-beaconchain-explorer/templates"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
	"github.com/protofire/ethpar-beaconchain-explorer/services"

	"github.com/ethereum/go-ethereum/common"
	"github.com/gorilla/mux"
	"golang.org/x/sync/errgroup"
)

const (
	digitLimitInAddressPagesTable = 17
	nameLimitInAddressPagesTable  = 0
)

func Eth1Address(bt *db.Bigtable, pg *db.Postgres, rpc execution.ExecutionClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		templateFiles := append(layoutTemplateFiles, "sprites.html", "execution/address.html")
		var eth1AddressTemplate = templates.GetTemplate(templateFiles...)
		w.Header().Set("Content-Type", "text/html")

		vars := mux.Vars(r)
		address := template.HTMLEscapeString(vars["address"])
		ensData, err := GetEnsDomain(address)
		if err != nil && utils.IsValidEnsDomain(address) {
			handleNotFoundHtml(w, r)
			return
		}
		if len(ensData.Address) > 0 {
			address = ensData.Address
		}
		if !utils.IsEth1Address(address) {
			handleNotFoundHtml(w, r)
			return
		}

		address = strings.ToLower(strings.TrimPrefix(address, "0x"))
		addressBytes := common.FromHex(address)
		currency := GetCurrency(r)

		data := InitPageData(w, r, "blockchain", "/address", fmt.Sprintf("Address 0x%x", addressBytes), templateFiles)

		metadata, err := bt.GetMetadataForAddress(addressBytes, 0, db.ECR20TokensPerAddressLimit)
		if err != nil {
			log.Errorf("error retrieving balances for %v route: %v", r.URL.String(), err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		g := new(errgroup.Group)
		g.SetLimit(11)

		var (
			isContract          bool
			rawTxns             []*types.Eth1TransactionIndexed
			contractTypes       []types.ContractInteractionType
			txnsPagingToken     string
			txnsTableData       *types.DataTableResponse
			blobRawTxns 	    []*types.Eth1BlobTransactionIndexed
			blobPagingToken     string
			blobTxnsTableData   *types.DataTableResponse
			internalRawTxns     []*types.Eth1InternalTransactionIndexed
			internalTypes       [][2]types.ContractInteractionType
			internalPagingToken string
			internalTableData   *types.DataTableResponse
			erc20RawTxns        []*types.Eth1ERC20Indexed
			erc20PagingToken    string
			erc20TableData      *types.DataTableResponse
			erc721RawTxns       []*types.Eth1ERC721Indexed
			erc721PagingToken   string
			erc721TableData     *types.DataTableResponse
			erc1155RawTxns      []*types.ETh1ERC1155Indexed
			erc1155PagingToken  string
			erc1155TableData    *types.DataTableResponse
			blocksMined         *types.DataTableResponse
			unclesMined         *types.DataTableResponse
			withdrawals         *types.DataTableResponse
			withdrawalSummary   template.HTML
		)

		g.Go(func() error {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			var err error
			isContract, err = eth1data.IsContract(ctx, rpc, common.BytesToAddress(addressBytes))
			return err
		})

		g.Go(func() error {
			var err error
			rawTxns, _, contractTypes, txnsPagingToken, err = bt.GetAddressTransactions(addressBytes, "")
			return err
		})

		g.Go(func() error {
			var err error
			blobRawTxns, blobPagingToken, err = bt.GetAddressBlobTransactions(addressBytes, "")
			return err
		})

		g.Go(func() error {
			var err error
			internalRawTxns, _, internalTypes, internalPagingToken, err = bt.GetAddressInternalTransactions(addressBytes, "")
			return err
		})

		g.Go(func() error {
			var err error
			erc20RawTxns, erc20PagingToken, err = bt.GetAddressERC20Transactions(addressBytes, "")
			return err
		})

		g.Go(func() error {
			var err error
			erc721RawTxns, erc721PagingToken, err = bt.GetAddressERC721Transactions(addressBytes, "")
			return err
		})

		g.Go(func() error {
			var err error
			erc1155RawTxns, erc1155PagingToken, err = bt.GetAddressERC1155Transactions(addressBytes, "")
			return err
		})

		g.Go(func() error {
			var err error
			blocksMined, err = bt.GetAddressBlocksMinedTableData(address, "")
			return err
		})

		g.Go(func() error {
			var err error
			unclesMined, err = bt.GetAddressUnclesMinedTableData(address, "")
			return err
		})

		g.Go(func() error {
			var err error
			withdrawals, err = pg.GetAddressWithdrawalTableData(addressBytes, "", currency)
			return err
		})

		g.Go(func() error {
			sumWithdrawals, err := pg.GetAddressWithdrawalsTotal(addressBytes)
			if err != nil {
				return fmt.Errorf("GetAddressWithdrawalsTotal: %w", err)
			}
			withdrawalSummary = utils.FormatClCurrency(sumWithdrawals, currency, 6, true, false, false, true)
			return nil
		})

		if err = g.Wait(); err != nil {
			if handleTemplateError(w, r, "eth1Account.go", "Eth1Address", "g.Wait()", err) != nil {
				return // an error has occurred and was processed
			}
			return
		}

		addressSet := make(map[string]string)
		erc20TokenSet := make(map[string]*types.ERC20Metadata)

		for _, t := range rawTxns {
			addressSet[string(t.From)] = ""
			addressSet[string(t.To)] = ""
		}

		for _, t := range blobRawTxns {
			addressSet[string(t.From)] = ""
			addressSet[string(t.To)] = ""
		}
		
		for _, t := range internalRawTxns {
			addressSet[string(t.From)] = ""
			addressSet[string(t.To)] = ""
		}

		for _, t := range erc20RawTxns {
			addressSet[string(t.From)] = ""
			addressSet[string(t.To)] = ""
		}

		for _, t := range erc721RawTxns {
			addressSet[string(t.From)] = ""
			addressSet[string(t.To)] = ""
		}

		for _, t := range erc1155RawTxns {
			addressSet[string(t.From)] = ""
			addressSet[string(t.To)] = ""
		}

		namesSvc := services.AddressNamesService{BT: bt, PG: pg, Ctx: r.Context()}
		addressNames, err := namesSvc.GetNamesForAddresses(addressSet)
		if err != nil {
			log.WithError(err).Error("failed to resolve address names")
		}

		formattedRows := make([][]interface{}, len(rawTxns))
		for i, t := range rawTxns {
			fromName := addressNames[string(t.From)]
			toName := addressNames[string(t.To)]
			var ci types.ContractInteractionType
			if len(contractTypes) > i {
				ci = contractTypes[i]
			}

			formattedRows[i] = []interface{}{
				utils.FormatTransactionHash(t.Hash, t.ErrorMsg == ""),
				utils.FormatMethod(bt.GetMethodLabel(t.MethodId, ci)),
				utils.FormatBlockNumber(t.BlockNumber),
				utils.FormatTimestamp(t.Time.AsTime().Unix()),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.From, fromName, false, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatInOutSelf(addressBytes, t.From, t.To),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.To, bt.GetAddressLabel(toName, ci), ci != types.CONTRACT_NONE, digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatAmount(new(big.Int).SetBytes(t.Value), utils.Config.Frontend.ElCurrency, 6),
			}
		}

		blobRows := make([][]interface{}, len(blobRawTxns))
		for i, t := range blobRawTxns {
			fromName := addressNames[string(t.From)]
			toName := addressNames[string(t.To)]

			blobRows[i] = []interface{}{
				utils.FormatTransactionHash(t.Hash, t.ErrorMsg == ""),
				utils.FormatBlockNumber(t.BlockNumber),
				utils.FormatTimestamp(t.Time.AsTime().Unix()),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.From, fromName, false,
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatInOutSelf(addressBytes, t.From, t.To),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.To, toName, false,
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatBytesAmount(t.BlobGasPrice, "GWei", 6),
				utils.FormatBytesAmount(t.BlobTxFee, "ETH", 6),
				len(t.BlobVersionedHashes),
			}
		}

		internalRows := make([][]interface{}, len(internalRawTxns))
		for i, t := range internalRawTxns {
			fromName := addressNames[string(t.From)]
			toName := addressNames[string(t.To)]

			var fromCI, toCI types.ContractInteractionType
			if len(internalTypes) > i {
				fromCI = internalTypes[i][0]
				toCI = internalTypes[i][1]
			}

			// replace "suicide" → "selfdestruct"
			if t.Type == "suicide" {
				t.Type = "selfdestruct"
			}

			internalRows[i] = []interface{}{
				utils.FormatTransactionHash(t.ParentHash, true),
				utils.FormatBlockNumber(t.BlockNumber),
				utils.FormatTimestamp(t.Time.AsTime().Unix()),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.From, bt.GetAddressLabel(fromName, fromCI), fromCI != types.CONTRACT_NONE,
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatInOutSelf(addressBytes, t.From, t.To),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.To, bt.GetAddressLabel(toName, toCI), toCI != types.CONTRACT_NONE,
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatAmount(new(big.Int).SetBytes(t.Value), utils.Config.Frontend.ElCurrency, 6),
				t.Type,
			}
		}

		for tokenAddr := range erc20TokenSet {
			md, err := bt.GetERC20MetadataForAddress([]byte(tokenAddr))
			if err == nil && md != nil {
				erc20TokenSet[tokenAddr] = md
			}
		}

		erc20Rows := make([][]interface{}, len(erc20RawTxns))
		for i, t := range erc20RawTxns {
			fromName := addressNames[string(t.From)]
			toName := addressNames[string(t.To)]
			tokenMetadata := erc20TokenSet[string(t.TokenAddress)]

			tb := &types.Eth1AddressBalance{
				Address:  addressBytes,
				Balance:  t.Value,
				Token:    t.TokenAddress,
				Metadata: tokenMetadata,
			}

			internalRows[i] = []interface{}{
				utils.FormatTransactionHash(t.ParentHash, true),
				utils.FormatBlockNumber(t.BlockNumber),
				utils.FormatTimestamp(t.Time.AsTime().Unix()),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.From, fromName, false, 
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatInOutSelf(addressBytes, t.From, t.To),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.To, toName, false, 
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatTokenValue(tb, true),
				utils.FormatTokenName(tb),
			}
		}

		erc721Rows := make([][]interface{}, len(erc721RawTxns))
		for i, t := range erc721RawTxns {
			fromName := addressNames[string(t.From)]
			toName := addressNames[string(t.To)]

			internalRows[i] = []interface{}{
				utils.FormatTransactionHash(t.ParentHash, true),
				utils.FormatBlockNumber(t.BlockNumber),
				utils.FormatTimestamp(t.Time.AsTime().Unix()),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.From, fromName, false, 
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.To, toName, false, 
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatAddressAsLink(t.TokenAddress, "", true),
				new(big.Int).SetBytes(t.TokenId).String(),
			}
		}

		erc1155Rows := make([][]interface{}, len(erc1155RawTxns))
		for i, t := range erc1155RawTxns {
			fromName := addressNames[string(t.From)]
			toName := addressNames[string(t.To)]

			internalRows[i] = []interface{}{
				utils.FormatTransactionHash(t.ParentHash, true),
				utils.FormatBlockNumber(t.BlockNumber),
				utils.FormatTimestamp(t.Time.AsTime().Unix()),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.From, fromName, false, 
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.To, toName, false, 
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatAddressAsLink(t.TokenAddress, "", true),
				new(big.Int).SetBytes(t.TokenId).String(),
			}
		}

		txnsTableData = &types.DataTableResponse{
			Data:        formattedRows,
			PagingToken: txnsPagingToken,
		}

		blobTxnsTableData = &types.DataTableResponse{
			Data:        blobRows,
			PagingToken: blobPagingToken,
		}

		internalTableData = &types.DataTableResponse{
			Data:        internalRows,
			PagingToken: internalPagingToken,
		}

		erc20TableData = &types.DataTableResponse{
			Data:        erc20Rows,
			PagingToken: erc20PagingToken,
		}

		erc721TableData = &types.DataTableResponse{
			Data:        erc721Rows,
			PagingToken: erc721PagingToken,
		}

		erc1155TableData = &types.DataTableResponse{
			Data:        erc1155Rows,
			PagingToken: erc1155PagingToken,
		}

		qr, qrInv, err := utils.GenerateQRCodeForAddress(addressBytes)
		if err != nil {
			log.WithError(err).Errorf("error generating QR code for %v", address)
		}

		tabs := buildTabs(blocksMined, unclesMined, withdrawals)

		data.Data = types.Eth1AddressPageData{
			Address:            address,
			EnsName:            ensData.Domain,
			IsContract:         isContract,
			QRCode:             qr,
			QRCodeInverse:      qrInv,
			Metadata:           metadata,
			WithdrawalsSummary: withdrawalSummary,
			TransactionsTable:  txnsTableData,
			BlobTxnsTable:      blobTxnsTableData,
			InternalTxnsTable:  internalTableData,
			Erc20Table:         erc20TableData,
			Erc721Table:        erc721TableData,
			Erc1155Table:       erc1155TableData,
			WithdrawalsTable:   withdrawals,
			BlocksMinedTable:   blocksMined,
			UnclesMinedTable:   unclesMined,
			EtherValue:         utils.FormatPricedValue(utils.WeiBytesToEther(metadata.EthBalance.Balance), utils.Config.Frontend.ElCurrency, currency),
			Tabs:               tabs,
		}

		if handleTemplateError(w, r, "eth1Account.go", "Eth1Address", "Done", eth1AddressTemplate.ExecuteTemplate(w, "layout", data)) != nil {
			return
		}
	}
}

func buildTabs(tabs ...*types.DataTableResponse) []types.Eth1AddressPageTabs {
	var result []types.Eth1AddressPageTabs
	names := []string{"Blob Txns", "Internal Txns", "Erc20 Token Txns", "Erc721 Token Txns", "Erc1155 Token Txns", "Produced Blocks", "Produced Uncles", "Withdrawals"}
	ids := []string{"blobTxns", "internalTxns", "erc20Txns", "erc721Txns", "erc1155Txns", "blocks", "uncles", "withdrawals"}

	for i, tab := range tabs {
		if tab != nil && len(tab.Data) > 0 {
			result = append(result, types.Eth1AddressPageTabs{
				Id:   ids[i],
				Href: "#" + ids[i],
				Text: names[i],
				Data: tab,
			})
		}
	}
	return result
}

func Eth1AddressTransactions(bt *db.Bigtable, pg *db.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		q := r.URL.Query()
		address, err := lowerAddressFromRequest(w, r)
		if err != nil {
			return
		}
		addressBytes := common.FromHex(address)
		pageToken := q.Get("pageToken")

		errFields := map[string]interface{}{
			"route":   r.URL.String(),
			"address": address,
		}

		rawTxns, _, contractTypes, pagingToken, err := bt.GetAddressTransactions(addressBytes, pageToken)
		if err != nil {
			utils.LogError(err, "error getting eth1 tx table data", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		addressSet := make(map[string]string)
		for _, t := range rawTxns {
			addressSet[string(t.From)] = ""
			addressSet[string(t.To)] = ""
		}

		namesSvc := services.AddressNamesService{BT: bt, PG: pg, Ctx: r.Context()}
		addressNames, err := namesSvc.GetNamesForAddresses(addressSet)
		if err != nil {
			utils.LogError(err, "error resolving address names", 0, errFields)
		}

		formatted := make([][]interface{}, len(rawTxns))
		for i, t := range rawTxns {
			fromName := addressNames[string(t.From)]
			toName := addressNames[string(t.To)]
			var ci types.ContractInteractionType
			if len(contractTypes) > i {
				ci = contractTypes[i]
			}

			formatted[i] = []interface{}{
				utils.FormatTransactionHash(t.Hash, t.ErrorMsg == ""),
				utils.FormatMethod(bt.GetMethodLabel(t.MethodId, ci)),
				utils.FormatBlockNumber(t.BlockNumber),
				utils.FormatTimestamp(t.Time.AsTime().Unix()),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.From, fromName, false,
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatInOutSelf(addressBytes, t.From, t.To),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.To,
					bt.GetAddressLabel(toName, ci), ci != types.CONTRACT_NONE,
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatAmount(new(big.Int).SetBytes(t.Value), utils.Config.Frontend.ElCurrency, 6),
			}
		}

		resp := &types.DataTableResponse{
			Data:        formatted,
			PagingToken: pagingToken,
		}

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			utils.LogError(err, "error encoding json response", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}
}

func Eth1AddressBlocksMined(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		q := r.URL.Query()
		address, err := lowerAddressFromRequest(w, r)
		if err != nil {
			return
		}

		errFields := map[string]interface{}{
			"route": r.URL.String()}

		pageToken := q.Get("pageToken")

		data, err := bt.GetAddressBlocksMinedTableData(address, pageToken)
		if err != nil {
			utils.LogError(err, "error getting eth1 blocks mined table data", 0, errFields)
		}

		err = json.NewEncoder(w).Encode(data)
		if err != nil {
			utils.LogError(err, "error enconding json response", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}
}

func Eth1AddressUnclesMined(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		q := r.URL.Query()
		address, err := lowerAddressFromRequest(w, r)
		if err != nil {
			return
		}

		errFields := map[string]interface{}{
			"route": r.URL.String()}

		pageToken := q.Get("pageToken")

		data, err := bt.GetAddressUnclesMinedTableData(address, pageToken)
		if err != nil {
			utils.LogError(err, "error getting eth1 uncles mined data", 0, errFields)
		}

		err = json.NewEncoder(w).Encode(data)
		if err != nil {
			utils.LogError(err, "error enconding json response", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}
}

func Eth1AddressWithdrawals(pg *db.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		currency := GetCurrency(r)
		q := r.URL.Query()
		address, err := lowerAddressFromRequest(w, r)
		if err != nil {
			return
		}

		errFields := map[string]interface{}{
			"route": r.URL.String()}

		data, err := pg.GetAddressWithdrawalTableData(common.HexToAddress(address).Bytes(), q.Get("pageToken"), currency)
		if err != nil {
			utils.LogError(err, "error getting address withdrawals data", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		err = json.NewEncoder(w).Encode(data)
		if err != nil {
			utils.LogError(err, "error enconding json response", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}
}

func Eth1AddressBlobTransactions(bt *db.Bigtable, pg *db.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		q := r.URL.Query()
		address, err := lowerAddressFromRequest(w, r)
		if err != nil {
			return
		}
		addressBytes := common.FromHex(address)
		pageToken := q.Get("pageToken")

		errFields := map[string]interface{}{
			"route":   r.URL.String(),
			"address": address,
		}

		blobTxns, pagingToken, err := bt.GetAddressBlobTransactions(addressBytes, pageToken)
		if err != nil {
			utils.LogError(err, "error getting eth1 blob tx table data", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		addressSet := make(map[string]string)
		for _, t := range blobTxns {
			addressSet[string(t.From)] = ""
			addressSet[string(t.To)] = ""
		}

		namesSvc := services.AddressNamesService{BT: bt, PG: pg, Ctx: r.Context()}
		addressNames, err := namesSvc.GetNamesForAddresses(addressSet)
		if err != nil {
			utils.LogError(err, "error resolving address names", 0, errFields)
		}

		rows := make([][]interface{}, len(blobTxns))
		for i, t := range blobTxns {
			fromName := addressNames[string(t.From)]
			toName := addressNames[string(t.To)]

			rows[i] = []interface{}{
				utils.FormatTransactionHash(t.Hash, t.ErrorMsg == ""),
				utils.FormatBlockNumber(t.BlockNumber),
				utils.FormatTimestamp(t.Time.AsTime().Unix()),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.From, fromName, false,
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatInOutSelf(addressBytes, t.From, t.To),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.To, toName, false,
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatBytesAmount(t.BlobGasPrice, "GWei", 6),
				utils.FormatBytesAmount(t.BlobTxFee, "ETH", 6),
				len(t.BlobVersionedHashes),
			}
		}

		resp := &types.DataTableResponse{
			Data:        rows,
			PagingToken: pagingToken,
		}

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			utils.LogError(err, "error encoding json response", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}
}

func Eth1AddressInternalTransactions(bt *db.Bigtable, pg *db.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		q := r.URL.Query()
		address, err := lowerAddressFromRequest(w, r)
		if err != nil {
			return
		}
		addressBytes := common.FromHex(address)

		errFields := map[string]interface{}{
			"route": r.URL.String(),
		}

		pageToken := q.Get("pageToken")

		rawTxns, _, interactionTypes, pagingToken, err := bt.GetAddressInternalTransactions(addressBytes, pageToken)
		if err != nil {
			utils.LogError(err, "error getting internal txs", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		addressSet := make(map[string]string)
		for _, t := range rawTxns {
			addressSet[string(t.From)] = ""
			addressSet[string(t.To)] = ""
		}

		namesSvc := services.AddressNamesService{BT: bt, PG: pg, Ctx: r.Context()}
		addressNames, err := namesSvc.GetNamesForAddresses(addressSet)
		if err != nil {
			utils.LogError(err, "error resolving internal tx address names", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		formatted := make([][]interface{}, len(rawTxns))
		for i, t := range rawTxns {
			fromName := addressNames[string(t.From)]
			toName := addressNames[string(t.To)]

			var fromCI, toCI types.ContractInteractionType
			if len(interactionTypes) > i {
				fromCI = interactionTypes[i][0]
				toCI = interactionTypes[i][1]
			}

			if t.Type == "suicide" {
				t.Type = "selfdestruct"
			}

			formatted[i] = []interface{}{
				utils.FormatTransactionHash(t.ParentHash, true),
				utils.FormatBlockNumber(t.BlockNumber),
				utils.FormatTimestamp(t.Time.AsTime().Unix()),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.From,
					bt.GetAddressLabel(fromName, fromCI), fromCI != types.CONTRACT_NONE,
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatInOutSelf(addressBytes, t.From, t.To),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.To,
					bt.GetAddressLabel(toName, toCI), toCI != types.CONTRACT_NONE,
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatAmount(new(big.Int).SetBytes(t.Value), utils.Config.Frontend.ElCurrency, 6),
				t.Type,
			}
		}

		resp := &types.DataTableResponse{
			Data:        formatted,
			PagingToken: pagingToken,
		}

		if err := json.NewEncoder(w).Encode(resp); err != nil {
			utils.LogError(err, "error encoding internal tx JSON", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}
}

func Eth1AddressErc20Transactions(bt *db.Bigtable, pg *db.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		q := r.URL.Query()
		address, err := lowerAddressFromRequest(w, r)
		if err != nil {
			return
		}
		addressBytes := common.FromHex(address)
		pageToken := q.Get("pageToken")

		errFields := map[string]interface{}{
			"route": r.URL.String(),
		}

		// Step 1: Get raw ERC20 transactions
		rawTxs, pagingToken, err := bt.GetAddressERC20Transactions(addressBytes, pageToken)
		if err != nil {
			utils.LogError(err, "error getting ERC20 transactions", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Step 2: Collect addresses and token addresses
		addressSet := make(map[string]string)
		tokenSet := make(map[string]*types.ERC20Metadata)
		for _, tx := range rawTxs {
			addressSet[string(tx.From)] = ""
			addressSet[string(tx.To)] = ""
			tokenSet[string(tx.TokenAddress)] = nil
		}

		// Step 3: Resolve names and metadata
		namesSvc := services.AddressNamesService{BT: bt, PG: pg, Ctx: r.Context()}
		addressNames, err := namesSvc.GetNamesForAddresses(addressSet)
		if err != nil {
			utils.LogError(err, "failed to resolve ERC20 address names", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		for token := range tokenSet {
			md, err := bt.GetERC20MetadataForAddress([]byte(token))
			if err == nil && md != nil {
				tokenSet[token] = md
			}
		}

		// Step 4: Format table rows
		rows := make([][]interface{}, len(rawTxs))
		for i, tx := range rawTxs {
			fromName := addressNames[string(tx.From)]
			toName := addressNames[string(tx.To)]
			metadata := tokenSet[string(tx.TokenAddress)]

			balance := &types.Eth1AddressBalance{
				Address:  addressBytes,
				Balance:  tx.Value,
				Token:    tx.TokenAddress,
				Metadata: metadata,
			}

			rows[i] = []interface{}{
				utils.FormatTransactionHash(tx.ParentHash, true),
				utils.FormatBlockNumber(tx.BlockNumber),
				utils.FormatTimestamp(tx.Time.AsTime().Unix()),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, tx.From, fromName, false,
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatInOutSelf(addressBytes, tx.From, tx.To),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, tx.To, toName, false,
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatTokenValue(balance, true),
				utils.FormatTokenName(balance),
			}
		}

		// Step 5: Send response
		response := &types.DataTableResponse{
			Data:        rows,
			PagingToken: pagingToken,
		}

		if err := json.NewEncoder(w).Encode(response); err != nil {
			utils.LogError(err, "error encoding JSON response", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}
}

func Eth1AddressErc721Transactions(bt *db.Bigtable, pg *db.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		q := r.URL.Query()
		address, err := lowerAddressFromRequest(w, r)
		if err != nil {
			return
		}
		addressBytes := common.FromHex(address)
		pageToken := q.Get("pageToken")

		errFields := map[string]interface{}{
			"route": r.URL.String(),
		}

		// Step 1: Get raw ERC721 transactions
		rawTxs, pagingToken, err := bt.GetAddressERC721Transactions(addressBytes, pageToken)
		if err != nil {
			utils.LogError(err, "error getting ERC721 transactions", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Step 2: Collect unique addresses
		addressSet := make(map[string]string)
		for _, tx := range rawTxs {
			addressSet[string(tx.From)] = ""
			addressSet[string(tx.To)] = ""
		}

		// Step 3: Resolve address names
		namesSvc := services.AddressNamesService{BT: bt, PG: pg, Ctx: r.Context()}
		addressNames, err := namesSvc.GetNamesForAddresses(addressSet)
		if err != nil {
			utils.LogError(err, "failed to resolve ERC721 address names", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Step 4: Format response rows
		rows := make([][]interface{}, len(rawTxs))
		for i, t := range rawTxs {
			fromName := addressNames[string(t.From)]
			toName := addressNames[string(t.To)]

			rows[i] = []interface{}{
				utils.FormatTransactionHash(t.ParentHash, true),
				utils.FormatBlockNumber(t.BlockNumber),
				utils.FormatTimestamp(t.Time.AsTime().Unix()),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.From, fromName, false, 
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.To, toName, false, 
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatAddressAsLink(t.TokenAddress, "", true),
				new(big.Int).SetBytes(t.TokenId).String(),
			}
		}

		// Step 5: Encode response
		response := &types.DataTableResponse{
			Data:        rows,
			PagingToken: pagingToken,
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			utils.LogError(err, "error encoding ERC721 JSON response", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}
}

func Eth1AddressErc1155Transactions(bt *db.Bigtable, pg *db.Postgres) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		q := r.URL.Query()
		address, err := lowerAddressFromRequest(w, r)
		if err != nil {
			return
		}
		addressBytes := common.FromHex(address)
		pageToken := q.Get("pageToken")

		errFields := map[string]interface{}{
			"route": r.URL.String(),
		}

		// Step 1: Get raw ERC721 transactions
		rawTxs, pagingToken, err := bt.GetAddressERC1155Transactions(addressBytes, pageToken)
		if err != nil {
			utils.LogError(err, "error getting ERC1155 transactions", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Step 2: Collect unique addresses
		addressSet := make(map[string]string)
		for _, tx := range rawTxs {
			addressSet[string(tx.From)] = ""
			addressSet[string(tx.To)] = ""
		}

		// Step 3: Resolve address names
		namesSvc := services.AddressNamesService{BT: bt, PG: pg, Ctx: r.Context()}
		addressNames, err := namesSvc.GetNamesForAddresses(addressSet)
		if err != nil {
			utils.LogError(err, "failed to resolve ERC1155 address names", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}

		// Step 4: Format response rows
		rows := make([][]interface{}, len(rawTxs))
		for i, t := range rawTxs {
			fromName := addressNames[string(t.From)]
			toName := addressNames[string(t.To)]

			rows[i] = []interface{}{
				utils.FormatTransactionHash(t.ParentHash, true),
				utils.FormatBlockNumber(t.BlockNumber),
				utils.FormatTimestamp(t.Time.AsTime().Unix()),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.From, fromName, false, 
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatAddressWithLimitsInAddressPageTable(addressBytes, t.To, toName, false, 
					digitLimitInAddressPagesTable, nameLimitInAddressPagesTable, true),
				utils.FormatAddressAsLink(t.TokenAddress, "", true),
				new(big.Int).SetBytes(t.TokenId).String(),
			}
		}

		// Step 5: Encode response
		response := &types.DataTableResponse{
			Data:        rows,
			PagingToken: pagingToken,
		}
		if err := json.NewEncoder(w).Encode(response); err != nil {
			utils.LogError(err, "error encoding ERC1155 JSON response", 0, errFields)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
			return
		}
	}
}

// takes the "address" parameter from the request and transforms it to lower case. The ENS name can be used instead of the address
func lowerAddressFromRequest(w http.ResponseWriter, r *http.Request) (string, error) {
	vars := mux.Vars(r)
	address := vars["address"]
	if utils.IsValidEnsDomain(address) {
		ensData, err := GetEnsDomain(address)
		if err != nil {
			handleNotFoundJson(address, w, r, err)
			return "", err
		}
		if len(ensData.Address) > 0 {
			address = ensData.Address
		}
	}
	return strings.ToLower(strings.Replace(address, "0x", "", -1)), nil
}

func handleNotFoundJson(address string, w http.ResponseWriter, r *http.Request, err error) {
	log.Errorf("error getting address for ENS name [%v] not found for %v route: %v", address, r.URL.String(), err)
	http.Error(w, "Invalid ENS name", http.StatusInternalServerError)
}

func handleNotFoundHtml(w http.ResponseWriter, r *http.Request) {
	templateFiles := append(layoutTemplateFiles, "sprites.html", "execution/addressNotFound.html")
	data := InitPageData(w, r, "blockchain", "/address", "not found", templateFiles)

	if handleTemplateError(w, r, "eth1Account.go", "Eth1Address", "not valid", templates.GetTemplate(templateFiles...).ExecuteTemplate(w, "layout", data)) != nil {
		return // an error has occurred and was processed
	}
}