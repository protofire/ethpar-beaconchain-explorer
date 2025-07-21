package cacheupdater

import (
	"sync"
	
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	"github.com/protofire/ethpar-beaconchain-explorer/db"
)

// Init will initialize the services
func Start(client consensus.ConsensusClient, bt *db.Bigtable) {
	ready := &sync.WaitGroup{}
	ready.Add(1)
	go epochUpdater(ready)

	ready.Add(1)
	go slotUpdater(ready)

	ready.Add(1)
	go latestProposedSlotUpdater(ready)

	ready.Add(1)
	go latestBlockUpdater(ready, bt)

	ready.Add(1)
	go headBlockRootHashUpdater(ready)

	ready.Add(1)
	go slotVizUpdater(ready)

	ready.Add(1)
	go indexPageDataUpdater(ready, bt)

	ready.Add(1)
	go statsUpdater(ready)

	ready.Add(1)
	go mempoolUpdater(ready)

	ready.Add(1)
	go gasNowUpdater(ready, bt)

	ready.Add(1)
	go latestExportedStatisticDayUpdater(ready)

	ready.Wait()
}