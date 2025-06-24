package eth1indexer

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/erc20"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
	"github.com/protofire/ethpar-beaconchain-explorer/types"

	"github.com/ethereum/go-ethereum/common"
	"github.com/shopspring/decimal"
	"golang.org/x/sync/errgroup"
)

// StartTokenPriceUpdater starts an infinite loop that periodically fetches,
// enriches, and stores ERC-20 token prices in Bigtable.
// It loads token definitions from the given token list file,
// fetches market prices from DeFiLlama, and enriches them with on-chain metadata.
//
// Parameters:
//   - bt: Bigtable instance for storing enriched token prices.
//   - client: RPC client to an execution node used to query token metadata.
//   - tokenListPath: path to the JSON file containing the list of tokens to track.
//   - log: logger for structured logging.
//   - interval: interval between successive updates.
func StartTokenPriceUpdater(bt *db.Bigtable, client execution.ExecutionClient, tokenListPath string, log *logger.Logger, interval *time.Duration) () {
	for {
		if err := updateTokenPrices(bt, client, tokenListPath, log); err != nil {
			log.Errorf("error while updating token prices: %v", err)
			time.Sleep(*interval)
		}
		time.Sleep(*interval)
	}
}

// updateTokenPrices orchestrates a single run of the token price update pipeline.
// It loads tokens from the local list, fetches their market prices, enriches them
// with on-chain metadata, and saves the results in Bigtable.
func updateTokenPrices(bt *db.Bigtable, client execution.ExecutionClient, tokenListPath string, log *logger.Logger) error {
	tokens, err := loadTokenList(tokenListPath)
	if err != nil {
		return fmt.Errorf("load token list: %w", err)
	}
	log.Infof("loaded %d tokens", len(tokens.Tokens))

	prices, err := fetchTokenPrices(tokens)
	if err != nil {
		return fmt.Errorf("fetch token prices: %w", err)
	}
	log.Infof("fetched prices for %d tokens", len(prices))

	if err := enrichTokenPrices(client, prices, log); err != nil {
		return fmt.Errorf("enrich prices with metadata: %w", err)
	}

	if err := bt.SaveERC20TokenPrices(prices); err != nil {
		return fmt.Errorf("saving token prices to Bigtable: %w", err)
	}

	log.Infof("saved %d enriched token prices to Bigtable", len(prices))
	return nil
}

// loadTokenList loads a token list from a given JSON file.
func loadTokenList(path string) (*erc20.ERC20TokenList, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var list erc20.ERC20TokenList
	if err := json.Unmarshal(data, &list); err != nil {
		return nil, err
	}
	return &list, nil
}

// fetchTokenPrices queries DeFiLlama API for prices of tokens.
func fetchTokenPrices(tokens *erc20.ERC20TokenList) ([]*types.ERC20TokenPrice, error) {
	reqBody, err := buildDefiLlamaRequestBody(tokens)
	if err != nil {
		return nil, fmt.Errorf("marshal price request: %w", err)
	}

	httpClient := &http.Client{
		Timeout: 10 * time.Second,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext,
		},
	}

	var resp *http.Response
	for i := 0; i < 3; i++ {
		resp, err = httpClient.Post("https://coins.llama.fi/prices", "application/json", bytes.NewReader(reqBody))
		if err == nil && resp.StatusCode == http.StatusOK {
			break
		}
		time.Sleep(time.Second * 2)
	}
	if err != nil {
		return nil, fmt.Errorf("http request to defillama failed: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read defillama response body: %w", err)
	}

	var parsed struct {
		Coins map[string]struct {
			Price *decimal.Decimal `json:"price"`
		} `json:"coins"`
	}
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, fmt.Errorf("unmarshal defillama response: %w", err)
	}

	prices := make([]*types.ERC20TokenPrice, 0, len(parsed.Coins))
	for key, val := range parsed.Coins {
		address := strings.TrimPrefix(key, "ethereum:")
		if !common.IsHexAddress(address) {
			continue
		}
		prices = append(prices, &types.ERC20TokenPrice{
			Token: common.HexToAddress(address).Bytes(),
			Price: []byte(val.Price.String()),
		})
	}
	return prices, nil
}

// buildDefiLlamaRequestBody builds the JSON payload for DeFiLlama request.
func buildDefiLlamaRequestBody(tokens *erc20.ERC20TokenList) ([]byte, error) {
	coins := make([]string, len(tokens.Tokens))
	for i, t := range tokens.Tokens {
		coins[i] = "ethereum:" + strings.ToLower(t.Address)
	}
	return json.Marshal(struct {
		Coins []string `json:"coins"`
	}{Coins: coins})
}

// enrichTokenPrices queries on-chain metadata (total supply) for tokens.
func enrichTokenPrices(client execution.ExecutionClient, prices []*types.ERC20TokenPrice, log *logger.Logger) error {
	g := new(errgroup.Group)
	g.SetLimit(10)

	for i := range prices {
		i := i // capture loop var
		g.Go(func() error {
			metadata, err := client.GetERC20TokenMetadata(prices[i].Token)
			if err != nil {
				log.Warnf("failed to enrich token %x: %v", prices[i].Token, err)
				return err
			}
			prices[i].TotalSupply = metadata.TotalSupply
			return nil
		})
	}

	return g.Wait()
}