package eth1indexer

import (
	"fmt"

	"github.com/coocood/freecache"
	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
)

// IndexingParams holds common parameters for any Eth1 indexing operation.
type IndexingParams struct {
	StartBlock         int64
	EndBlock           int64
	StartData	   int64
	EndData            int64
	ConcurrencyBlocks  int64
	ConcurrencyData    int64
	TraceMode          string
	Cache              *freecache.Cache
	Bigtable           *db.Bigtable
	Client             *rpc.ErigonClient
	Log                *logger.Logger
}

// transforms returns the static, canonical list of transformation functions
// used during the indexing of each Eth1 block. These functions convert raw Eth1 block
// data into structured bulk mutations that are persisted in Bigtable.
func (p *IndexingParams) transforms() []func(*types.Eth1Block, *freecache.Cache) (*types.BulkMutations, *types.BulkMutations, error) {
	return []func(*types.Eth1Block, *freecache.Cache) (*types.BulkMutations, *types.BulkMutations, error){
		p.Bigtable.TransformBlock,
		p.Bigtable.TransformTx,
		p.Bigtable.TransformItx,
		p.Bigtable.TransformBlobTx,
		p.Bigtable.TransformERC20,
		p.Bigtable.TransformERC721,
		p.Bigtable.TransformERC1155,
		p.Bigtable.TransformUncle,
		p.Bigtable.TransformWithdrawals,
		p.Bigtable.TransformEnsNameRegistered,
		p.Bigtable.TransformContract,
	}
}

// IndexSingleBlock indexes a single Ethereum 1.0 block by:
//   - fetching it from the execution node and storing it in the Bigtable "blocks" table
//   - transforming the block into associated data entries and storing them in the Bigtable "data" table
//
// This function uses StartBlock from IndexingParams as the target block height
// and ignores EndBlock. It is intended for ad-hoc or isolated block processing.
func IndexSingleBlock(p IndexingParams) error {
	blockTransforms := p.transforms()
	block := p.StartBlock
	p.Log.Infof("indexing single block %v", block)

	if err := IndexFromNode(p.Bigtable, p.Client, block, block, p.ConcurrencyBlocks, p.TraceMode, p.Log); err != nil {
		return err
	}

	if err := p.Bigtable.IndexEventsWithTransformers(block, block, blockTransforms, p.ConcurrencyData, p.Cache); err != nil {
		return err
	}

	p.Cache.Clear()
	p.Log.Infof("indexing of block %v completed", block)
	return nil
}

func IndexBlockRange(p IndexingParams) error {
	blockTransforms := p.transforms()

	// Save specified block range from node to bigtable and exit
	if p.EndBlock != 0 && p.StartBlock < p.EndBlock {
		p.Log.Infof("saving block range from %v to %v to BigTable", p.StartBlock, p.EndBlock)
		if err := IndexFromNode(p.Bigtable, p.Client, p.StartBlock, p.EndBlock, p.ConcurrencyBlocks, p.TraceMode, p.Log); err != nil {
			return fmt.Errorf("index from node [%d - %d]: %w", p.StartBlock, p.EndBlock, err)
		}
	}

	// Transform specified block range from blocks to data bigtable tables and exit
	if p.EndData != 0 && p.StartData < p.EndData {
		p.Log.Infof("transforming data for a block range from %v to %v in BigTable", p.StartData, p.EndData)
		if err := p.Bigtable.IndexEventsWithTransformers(int64(p.StartData), int64(p.EndData), blockTransforms, p.ConcurrencyData, p.Cache); err != nil {
			return fmt.Errorf("error indexing from bigtable: %v", err)
		}
		p.Cache.Clear()
	}

	return nil
}