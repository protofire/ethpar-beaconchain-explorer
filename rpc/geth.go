package rpc

import (
	"context"
	"encoding/hex"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/contracts/oneinchoracle"
	"github.com/protofire/ethpar-beaconchain-explorer/erc20"
	"github.com/protofire/ethpar-beaconchain-explorer/types"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	geth_rpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"google.golang.org/protobuf/types/known/timestamppb"

	geth_types "github.com/ethereum/go-ethereum/core/types"
)

type GethClient struct {
	endpoint     string
	rpcClient    *geth_rpc.Client
	ethClient    *ethclient.Client
	chainID      *big.Int
	multiChecker *Balance
}

var CurrentGethClient *GethClient

func NewGethClient(endpoint string) (*GethClient, error) {
	logger.Infof("initializing geth client at %v", endpoint)
	client := &GethClient{
		endpoint: endpoint,
	}

	rpcClient, err := geth_rpc.Dial(client.endpoint)
	if err != nil {
		return nil, fmt.Errorf("error dialing rpc node: %v", err)
	}

	client.rpcClient = rpcClient

	ethClient, err := ethclient.Dial(client.endpoint)
	if err != nil {
		return nil, fmt.Errorf("error dialing rpc node: %v", err)
	}
	client.ethClient = ethClient

	client.multiChecker, err = NewBalance(common.HexToAddress("0xb1F8e55c7f64D203C1400B9D8555d050F94aDF39"), client.ethClient)
	if err != nil {
		return nil, fmt.Errorf("error initiation balance checker contract: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	chainID, err := client.ethClient.ChainID(ctx)
	if err != nil {
		return nil, fmt.Errorf("error getting chainid of rpcclient: %w", err)
	}
	client.chainID = chainID

	return client, nil
}

func (client *GethClient) Close() {
	client.rpcClient.Close()
	client.ethClient.Close()
}

func (client *GethClient) GetChainID() *big.Int {
	return client.chainID
}

func (client *GethClient) GetNativeClient() *ethclient.Client {
	return client.ethClient
}

func (client *GethClient) GetRPCClient() *geth_rpc.Client {
	return client.rpcClient
}

func (client *GethClient) GetBlock(number int64) (*types.Eth1Block, *types.GetBlockTimings, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	start := time.Now()
	timings := &types.GetBlockTimings{}

	block, err := client.ethClient.BlockByNumber(ctx, big.NewInt(int64(number)))
	if err != nil {
		return nil, nil, err
	}

	timings.Headers = time.Since(start)
	start = time.Now()

	c := &types.Eth1Block{
		Hash:         block.Hash().Bytes(),
		ParentHash:   block.ParentHash().Bytes(),
		UncleHash:    block.UncleHash().Bytes(),
		Coinbase:     block.Coinbase().Bytes(),
		Root:         block.Root().Bytes(),
		TxHash:       block.TxHash().Bytes(),
		ReceiptHash:  block.ReceiptHash().Bytes(),
		Difficulty:   block.Difficulty().Bytes(),
		Number:       block.NumberU64(),
		GasLimit:     block.GasLimit(),
		GasUsed:      block.GasUsed(),
		Time:         timestamppb.New(time.Unix(int64(block.Time()), 0)),
		Extra:        block.Extra(),
		MixDigest:    block.MixDigest().Bytes(),
		Bloom:        block.Bloom().Bytes(),
		Uncles:       []*types.Eth1Block{},
		Transactions: []*types.Eth1Transaction{},
	}

	if block.BaseFee() != nil {
		c.BaseFee = block.BaseFee().Bytes()
	}

	for _, uncle := range block.Uncles() {
		pbUncle := &types.Eth1Block{
			Hash:        uncle.Hash().Bytes(),
			ParentHash:  uncle.ParentHash.Bytes(),
			UncleHash:   uncle.UncleHash.Bytes(),
			Coinbase:    uncle.Coinbase.Bytes(),
			Root:        uncle.Root.Bytes(),
			TxHash:      uncle.TxHash.Bytes(),
			ReceiptHash: uncle.ReceiptHash.Bytes(),
			Difficulty:  uncle.Difficulty.Bytes(),
			Number:      uncle.Number.Uint64(),
			GasLimit:    uncle.GasLimit,
			GasUsed:     uncle.GasUsed,
			Time:        timestamppb.New(time.Unix(int64(uncle.Time), 0)),
			Extra:       uncle.Extra,
			MixDigest:   uncle.MixDigest.Bytes(),
			Bloom:       uncle.Bloom.Bytes(),
		}

		c.Uncles = append(c.Uncles, pbUncle)
	}

	receipts := make([]*geth_types.Receipt, len(block.Transactions()))
	reqs := make([]geth_rpc.BatchElem, len(block.Transactions()))

	txs := block.Transactions()

	for _, tx := range txs {

		var from []byte
		sender, err := geth_types.Sender(geth_types.NewPragueSigner(tx.ChainId()), tx)
		if err != nil {
			from, _ = hex.DecodeString("abababababababababababababababababababab")
			logrus.Errorf("error converting tx %v to msg: %v", tx.Hash(), err)
		} else {
			from = sender.Bytes()
		}

		pbTx := &types.Eth1Transaction{
			Type:                 uint32(tx.Type()),
			Nonce:                tx.Nonce(),
			GasPrice:             tx.GasPrice().Bytes(),
			MaxPriorityFeePerGas: tx.GasTipCap().Bytes(),
			MaxFeePerGas:         tx.GasFeeCap().Bytes(),
			Gas:                  tx.Gas(),
			Value:                tx.Value().Bytes(),
			Data:                 tx.Data(),
			From:                 from,
			ChainId:              tx.ChainId().Bytes(),
			AccessList:           []*types.AccessList{},
			Hash:                 tx.Hash().Bytes(),
			Itx:                  []*types.Eth1InternalTransaction{},
		}

		if tx.To() != nil {
			pbTx.To = tx.To().Bytes()
		}
		c.Transactions = append(c.Transactions, pbTx)

	}

	for i := range reqs {
		reqs[i] = geth_rpc.BatchElem{
			Method: "eth_getTransactionReceipt",
			Args:   []interface{}{txs[i].Hash().String()},
			Result: &receipts[i],
		}
	}

	if len(reqs) > 0 {
		if err := client.rpcClient.BatchCallContext(ctx, reqs); err != nil {
			return nil, nil, fmt.Errorf("error retrieving receipts for block %v: %v", block.Number(), err)
		}
	}
	timings.Receipts = time.Since(start)

	for i := range reqs {
		if reqs[i].Error != nil {
			return nil, nil, fmt.Errorf("error retrieving receipt %v for block %v: %v", i, block.Number(), reqs[i].Error)
		}
		if receipts[i] == nil {
			return nil, nil, fmt.Errorf("got null value for receipt %d of block %v", i, block.Number())
		}

		r := receipts[i]
		c.Transactions[i].ContractAddress = r.ContractAddress[:]
		c.Transactions[i].CommulativeGasUsed = r.CumulativeGasUsed
		c.Transactions[i].GasUsed = r.GasUsed
		c.Transactions[i].LogsBloom = r.Bloom[:]
		c.Transactions[i].Logs = make([]*types.Eth1Log, 0, len(r.Logs))

		for _, l := range r.Logs {
			pbLog := &types.Eth1Log{
				Address: l.Address.Bytes(),
				Data:    l.Data,
				Removed: l.Removed,
				Topics:  make([][]byte, 0, len(l.Topics)),
			}

			for _, t := range l.Topics {
				pbLog.Topics = append(pbLog.Topics, t.Bytes())
			}
			c.Transactions[i].Logs = append(c.Transactions[i].Logs, pbLog)
		}
	}

	return c, timings, nil
}

func (client *GethClient) GetLatestEth1BlockNumber() (uint64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	latestBlock, err := client.ethClient.BlockByNumber(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("error getting latest block: %v", err)
	}

	return latestBlock.NumberU64(), nil
}

func (client *GethClient) TraceGeth(blockHash common.Hash) ([]*GethTraceCallResult, error) {
	var res []*GethTraceCallResult

	err := client.rpcClient.Call(&res, "debug_traceBlockByHash", blockHash, gethTracerArg)
	if err != nil {
		return nil, err
	}

	return res, nil
}

func (client *GethClient) GetBalances(pairs []string) ([]*types.Eth1AddressBalance, error) {
	batchElements := make([]geth_rpc.BatchElem, 0, len(pairs))

	ret := make([]*types.Eth1AddressBalance, len(pairs))

	for i, pair := range pairs {
		s := strings.Split(pair, ":")

		if len(s) != 3 {
			logrus.Fatalf("%v has an invalid format", pair)
		}

		if s[0] != "B" {
			logrus.Fatalf("%v has invalid balance update prefix", pair)
		}

		address := s[1]
		token := s[2]
		result := ""

		ret[i] = &types.Eth1AddressBalance{
			Address: common.FromHex(address),
			Token:   common.FromHex(token),
		}

		if token == "00" {
			batchElements = append(batchElements, geth_rpc.BatchElem{
				Method: "eth_getBalance",
				Args:   []interface{}{common.HexToAddress(address), "latest"},
				Result: &result,
			})
		} else {
			to := common.HexToAddress(token)
			msg := ethereum.CallMsg{
				To:   &to,
				Gas:  1000000,
				Data: common.Hex2Bytes("70a08231000000000000000000000000" + address),
			}

			batchElements = append(batchElements, geth_rpc.BatchElem{
				Method: "eth_call",
				Args:   []interface{}{toCallArg(msg), "latest"},
				Result: &result,
			})
		}
	}

	err := client.rpcClient.BatchCall(batchElements)
	if err != nil {
		return nil, fmt.Errorf("error during batch request: %v", err)
	}

	for i, el := range batchElements {
		if el.Error != nil {
			logrus.Warnf("error in batch call: %v", el.Error) // PPR: are smart contracts that pretend to implement the erc20 standard but are somehow buggy
		}

		res := strings.TrimPrefix(*el.Result.(*string), "0x")
		ret[i].Balance = new(big.Int).SetBytes(common.Hex2Bytes(res)).Bytes()
	}

	return ret, nil
}

func (client *GethClient) GetBalancesForAddresse(address string, tokenStr []string) ([]*types.Eth1AddressBalance, error) {
	opts := &bind.CallOpts{
		BlockNumber: nil,
	}

	tokens := make([]common.Address, 0, len(tokenStr))

	for _, token := range tokenStr {
		tokens = append(tokens, common.HexToAddress(token))
	}
	balancesInt, err := client.multiChecker.Balances(opts, []common.Address{common.HexToAddress(address)}, tokens)
	if err != nil {
		return nil, err
	}

	res := make([]*types.Eth1AddressBalance, len(tokenStr))
	for tokenIdx := range tokens {

		res[tokenIdx] = &types.Eth1AddressBalance{
			Address: common.FromHex(address),
			Token:   common.FromHex(string(tokens[tokenIdx].Bytes())),
			Balance: balancesInt[tokenIdx].Bytes(),
		}
	}

	return res, nil
}

func (client *GethClient) GetNativeBalance(address string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	balance, err := client.ethClient.BalanceAt(ctx, common.HexToAddress(address), nil)

	if err != nil {
		return nil, err
	}
	return balance.Bytes(), nil
}

func (client *GethClient) GetERC20TokenBalance(address string, token string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	to := common.HexToAddress(token)
	balance, err := client.ethClient.CallContract(ctx, ethereum.CallMsg{
		To:   &to,
		Gas:  1000000,
		Data: common.Hex2Bytes("70a08231000000000000000000000000" + address),
	}, nil)

	if err != nil && !strings.HasPrefix(err.Error(), "execution reverted") {
		return nil, err
	}
	return balance, nil
}

func (client *GethClient) GetERC20TokenMetadata(token []byte) (*types.ERC20Metadata, error) {
	logger.Infof("retrieving metadata for token %x", token)

	contract, err := erc20.NewErc20(common.BytesToAddress(token), client.ethClient)
	if err != nil {
		return nil, fmt.Errorf("error getting token-contract: erc20.NewErc20: %w", err)
	}

	g := new(errgroup.Group)

	ret := &types.ERC20Metadata{}

	g.Go(func() error {
		symbol, err := contract.Symbol(nil)
		if err != nil {
			if strings.Contains(err.Error(), "abi") {
				ret.Symbol = "UNKNOWN"
				return nil
			}

			return fmt.Errorf("error retrieving token symbol: %w", err)
		}

		ret.Symbol = symbol
		return nil
	})

	g.Go(func() error {
		totalSupply, err := contract.TotalSupply(nil)
		if err != nil {
			return fmt.Errorf("error retrieving token total supply: %w", err)
		}
		ret.TotalSupply = totalSupply.Bytes()
		return nil
	})

	g.Go(func() error {
		decimals, err := contract.Decimals(nil)
		if err != nil {
			return fmt.Errorf("error retrieving token decimals: %w", err)
		}
		ret.Decimals = big.NewInt(int64(decimals)).Bytes()
		return nil
	})

	g.Go(func() error {
		if !oneinchoracle.SupportedChainId(client.GetChainID()) {
			return nil
		}
		oracle, err := oneinchoracle.NewOneInchOracleByChainID(client.GetChainID(), client.ethClient)
		if err != nil {
			return fmt.Errorf("error initializing oneinchoracle.NewOneInchOracleByChainID: %w", err)
		}
		rate, err := oracle.GetRateToEth(nil, common.BytesToAddress(token), false)
		if err != nil {
			return fmt.Errorf("error calling oneinchoracle.GetRateToEth: %w", err)
		}
		ret.Price = rate.Bytes()
		return nil
	})

	err = g.Wait()
	if err != nil {
		return ret, err
	}

	if err == nil && len(ret.Decimals) == 0 && ret.Symbol == "" && len(ret.TotalSupply) == 0 {
		// it's possible that a token contract implements the ERC20 interfaces but does not return any values; we use a backup in this case
		ret = &types.ERC20Metadata{
			Decimals:    []byte{0x0},
			Symbol:      "UNKNOWN",
			TotalSupply: []byte{0x0}}
	}

	return ret, err
}
