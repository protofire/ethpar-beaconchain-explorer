package execution

import (
	"math/big"

	"github.com/protofire/ethpar-beaconchain-explorer/types"

	"github.com/ethereum/go-ethereum/ethclient"
	// geth_rpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/common"
)

type ExecutionClient interface {
	Close()
	GetChainID() *big.Int
	GetNativeClient() *ethclient.Client
	// GetRPCClient() *geth_rpc.Client
	GetBlock(number int64, traceMode string) (*types.Eth1Block, *types.GetBlockTimings, error)
	GetBlockNumberByHash(hash string) (uint64, error)
	GetLatestEth1BlockNumber() (uint64, error)
	TraceGeth(blockHash common.Hash) ([]*GethTraceCallResult, error)
	TraceParity(blockNumber uint64) ([]*ParityTraceResult, error)
	TraceParityTx(txHash string) ([]*ParityTraceResult, error)
	GetBalances(pairs []*types.Eth1AddressBalance, addressIndex, tokenIndex int) ([]*types.Eth1AddressBalance, error)
	GetBalancesForAddresse(address string, tokenStr []string) ([]*types.Eth1AddressBalance, error)
	GetNativeBalance(address string) ([]byte, error)
	GetERC20TokenBalance(address string, token string) ([]byte, error)
	GetERC20TokenMetadata(token []byte) (*types.ERC20Metadata, error)

}