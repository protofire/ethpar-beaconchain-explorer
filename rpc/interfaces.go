package rpc

import (
	"math/big"

	"github.com/protofire/ethpar-beaconchain-explorer/types"

	"github.com/sirupsen/logrus"
)

type Eth1Client interface {
	GetBlock(number uint64) (*types.Eth1Block, *types.GetBlockTimings, error)
	GetLatestEth1BlockNumber() (uint64, error)
	GetChainID() *big.Int
	Close()
}

var logger = logrus.New().WithField("module", "rpc")
