package eth2indexer

import (
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/utils"
)

// networkLivenessUpdater periodically polls the beacon node's chain head and stores
// liveness-related metadata (head epoch, finalized epoch, justified epoch, and previous
// justified epoch) into the PostgreSQL database.
//
// It runs in a loop synchronized to the beacon chain slot duration and ensures that:
//   - The beacon node is synced before recording data.
//   - Duplicate entries for the same epoch are avoided.
//
// This function is intended to track network progress and consensus stability over time.
func networkLivenessUpdater(p *IndexingParams) {
	prevHeadEpoch, err := p.Database.GetPrevHeadEpoch()
	if err != nil {
		p.Log.Fatalf("getting previous head epoch from db error: %v", err)
	}

	epochDuration := time.Second * time.Duration(p.ChainParams.Time.SecondsPerSlot*p.ChainParams.Time.SlotsPerEpoch)
	slotDuration := time.Second * time.Duration(p.ChainParams.Time.SecondsPerSlot)

	for {
		head, err := p.ConsClient.GetChainHead()
		if err != nil {
			p.Log.Errorf("error getting chainhead when exporting networkliveness: %v", err)
			time.Sleep(slotDuration)
			continue
		}

		if prevHeadEpoch == head.HeadEpoch {
			time.Sleep(slotDuration)
			continue
		}

		// wait for node to be synced
		if time.Now().Add(-epochDuration).After(utils.EpochToTime(head.HeadEpoch)) {
			time.Sleep(slotDuration)
			continue
		}

		if err := p.Database.InsertNetworkLivenessSnapshot(
			head.HeadEpoch, 
			head.FinalizedEpoch, 
			head.JustifiedEpoch, 
			head.PreviousJustifiedEpoch,
		); err != nil {
			p.Log.Errorf("error saving networkliveness: %v", err)
		} else {
			p.Log.Infof("updated networkliveness for epoch %v", head.HeadEpoch)
			prevHeadEpoch = head.HeadEpoch
		}

		time.Sleep(slotDuration)
	}
}