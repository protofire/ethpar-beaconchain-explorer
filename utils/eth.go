package utils

import (
	"crypto/sha256"

	"github.com/ethereum/go-ethereum/common"
	"github.com/sirupsen/logrus"
	e2types "github.com/wealdtech/go-eth2-types/v2"
)

func init() {
	err := e2types.InitBLS()
	if err != nil {
		logrus.Fatalf("error in e2types.InitBLS(): %v", err)
	}
}

func FixAddressCasing(add string) string {
	return common.HexToAddress(add).Hex()
}

func VersionedBlobHash(commitment []byte) common.Hash {
	hasher := sha256.New()
	hasher.Write(commitment[:])
	var vhash common.Hash
	hasher.Sum(vhash[:0])
	vhash[0] = 0x01
	return vhash
}