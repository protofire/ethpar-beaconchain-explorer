package consensus

import (
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/config"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/teku"
	rpc_types "github.com/protofire/ethpar-beaconchain-explorer/rpc/types"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
)

var log = logger.New(nil).WithField("module", "rpc")

// ConsensusClient provides an interface for RPC clients
type ConsensusClient interface {
	Close()
	GetChainHead() (*types.ChainHead, error)
	GetValidatorQueue() (*types.ValidatorQueue, error)
	GetEpochAssignments(epoch uint64) (*types.EpochAssignments, error)
	GetBlockBySlot(slot uint64) (*types.Block, error)
	GetValidatorInclusion(epoch uint64) (rpc_types.StandardValidatorParticipationResponse, error)
	GetSyncCommittee(stateID string, epoch uint64) (*rpc_types.StandardSyncCommitteeData, error)
	GetBalancesForEpoch(epoch int64) (map[uint64]uint64, error)
	GetValidatorState(epoch uint64) (*rpc_types.StandardValidatorsResponse, error)
	GetBlockHeader(slot uint64) (*rpc_types.StandardBeaconHeaderResponse, error)
}

func MustInitNewClient(client, endpoint string, chainId uint64, chainParams *config.NetworkConfig) ConsensusClient {
	var consClient ConsensusClient
	var err error
	
	switch client {
	case "teku":
		consClient, err = teku.NewTekuClient(endpoint, chainId, chainParams)
		if err != nil {
			log.Fatalf("failed to create a new Teku client: %v", err)
		}
	default:
		log.Fatalf("unsupported consensus client: %s", client)
	}

	return consClient
}