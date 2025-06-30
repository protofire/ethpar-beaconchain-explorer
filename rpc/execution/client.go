package execution

import (
	"math/big"

	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/erigon"
	rpc_types "github.com/protofire/ethpar-beaconchain-explorer/rpc/types"
	"github.com/protofire/ethpar-beaconchain-explorer/types"

	"github.com/ethereum/go-ethereum/ethclient"
	geth_rpc "github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/common"
)

var log = logger.New(nil).WithField("module", "rpc")

type ExecutionClient interface {
	Close()
	GetChainID() *big.Int
	ValidateChainIdFromConfig(confId uint64) bool
	GetNativeClient() *ethclient.Client
	GetRPCClient() *geth_rpc.Client
	GetBlock(number int64, traceMode string) (*types.Eth1Block, *types.GetBlockTimings, error)
	GetBlockNumberByHash(hash string) (uint64, error)
	GetLatestEth1BlockNumber() (uint64, error)
	TraceGeth(blockHash common.Hash) ([]*rpc_types.GethTraceCallResult, error)
	TraceParity(blockNumber uint64) ([]*rpc_types.ParityTraceResult, error)
	TraceParityTx(txHash string) ([]*rpc_types.ParityTraceResult, error)
	GetBalances(pairs []*types.Eth1AddressBalance, addressIndex, tokenIndex int) ([]*types.Eth1AddressBalance, error)
	GetBalancesForAddresse(address string, tokenStr []string) ([]*types.Eth1AddressBalance, error)
	GetNativeBalance(address string) ([]byte, error)
	GetERC20TokenBalance(address string, token string) ([]byte, error)
	GetERC20TokenMetadata(token []byte) (*types.ERC20Metadata, error)
}

func MustInitNewClient(client, endpoint string) ExecutionClient {
	var rpcClient ExecutionClient
	var err error
	
	switch client {
	case "erigon":
		rpcClient, err = erigon.NewErigonClient(endpoint)
		if err != nil {
			log.Fatalf("failed to create a new Erigon client: %v", err)
		}
	case "geth":
		// TODO implement
	default:
		log.Fatalf("unsupported execution client: %s", client)
	}

	return rpcClient
}