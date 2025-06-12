package consensus

import (
	"github.com/protofire/ethpar-beaconchain-explorer/types"
)

// ConsensusClient provides an interface for RPC clients
type ConsensusClient interface {
	GetChainHead() (*types.ChainHead, error)
	GetEpochData(epoch uint64, skipHistoricBalances bool) (*types.EpochData, error)
	GetValidatorQueue() (*types.ValidatorQueue, error)
	GetEpochAssignments(epoch uint64) (*types.EpochAssignments, error)
	GetBlockBySlot(slot uint64) (*types.Block, error)
	GetValidatorParticipation(epoch uint64) (*types.ValidatorParticipation, error)
	GetNewBlockChan() chan *types.Block
	GetSyncCommittee(stateID string, epoch uint64) (*StandardSyncCommitteeData, error)
	GetBalancesForEpoch(epoch int64) (map[uint64]uint64, error)
	GetValidatorState(epoch uint64) (*StandardValidatorsResponse, error)
	GetBlockHeader(slot uint64) (*StandardBeaconHeaderResponse, error)
	GetPendingDeposits() (*StandardBeaconPendingDepositsResponse, error)
}
