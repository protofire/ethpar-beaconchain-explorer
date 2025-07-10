package eth1indexer

import (
	"fmt"
	"time"

	"github.com/coocood/freecache"
	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
)

// IndexingParams holds common parameters for any Eth1 indexing operation.
type IndexingParams struct {
	StartBlock        int64
	EndBlock          int64
	BulkBlock         int64
	OffsetBlock       int64
	StartData         int64
	EndData           int64
	BulkData          int64
	OffsetData        int64
	ConcurrencyBlocks int64
	ConcurrencyData   int64
	ReorgDepth        int
	TraceMode         string
	EnsUpdate         bool
	EnsBatch          int64
	BalanceUpdate     bool
	BalanceBatch      int
	BalancePrefix     string
	Cache             *freecache.Cache
	Bigtable          *db.Bigtable
	Client            execution.ExecutionClient
	Log               *logger.Logger
	ReportStatus      bool
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
		// p.Bigtable.TransformUncle,
		p.Bigtable.TransformWithdrawals,
		p.Bigtable.TransformEnsNameRegistered, // TODO: not implemented for EthPar
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
	p.Log.Infof("indexing single block %d", block)

	if err := IndexFromNode(p.Bigtable, p.Client, block, block, p.ConcurrencyBlocks, p.TraceMode, p.Log); err != nil {
		return err
	}

	if err := p.Bigtable.IndexEventsWithTransformers(block, block, blockTransforms, p.ConcurrencyData, p.Cache); err != nil {
		return err
	}

	p.Cache.Clear()
	p.Log.Infof("indexing of block %d completed", block)
	return nil
}

func IndexBlockRange(p IndexingParams) error {
	// Save specified block range from node to bigtable and exit
	if p.EndBlock != 0 && p.StartBlock < p.EndBlock {
		p.Log.Infof("saving block range from %d to %d to BigTable", p.StartBlock, p.EndBlock)
		if err := IndexFromNode(p.Bigtable, p.Client, p.StartBlock, p.EndBlock, p.ConcurrencyBlocks, p.TraceMode, p.Log); err != nil {
			return fmt.Errorf("index from node [%d - %d]: %w", p.StartBlock, p.EndBlock, err)
		}
	}
	return nil
}

func IndexDataRange(p IndexingParams) error {
	// Transform specified block range from blocks to data bigtable tables and exit
	blockTransforms := p.transforms()
	if p.EndData != 0 && p.StartData < p.EndData {
		p.Log.Infof("transforming data for a block range from %d to %d in BigTable", p.StartData, p.EndData)
		if err := p.Bigtable.IndexEventsWithTransformers(int64(p.StartData), int64(p.EndData), blockTransforms, p.ConcurrencyData, p.Cache); err != nil {
			return fmt.Errorf("error indexing from bigtable: %w", err)
		}
		p.Cache.Clear()
	}
	return nil
}

func IndexLive(p IndexingParams) error {
	// Endless cycle if no ranges or single blocks specified
	var lastBlockFromNodeOld uint64
	var lastBlockFromNodeSameCount uint64
	lastSuccessulBlockIndexingTs := time.Now()
	for ; ; time.Sleep(time.Second * 14) {
		err := handleChainReorgs(p.Bigtable, p.Client, p.ReorgDepth, p.Log)
		if err != nil {
			p.Log.Errorf("error handling chain reorgs: %v", err)
			continue
		}

		lastBlockFromNode, err := p.Client.GetLatestEth1BlockNumber()
		if err != nil {
			lastBlockFromNodeSameCount++
			if lastBlockFromNodeSameCount > 20 { // nearly 5 minutes no new block
				p.Log.WithField("lastBlockFromNode", lastBlockFromNodeOld).Fatal("no new block in 20 tries")
			}
			p.Log.Errorf("error retrieving latest eth block number: %v", err)
			continue
		}
		if lastBlockFromNode != lastBlockFromNodeOld {
			lastBlockFromNodeOld = lastBlockFromNode
			lastBlockFromNodeSameCount = 0
		} else {
			lastBlockFromNodeSameCount++
			if lastBlockFromNodeSameCount > 20 { // nearly 5 minutes no new block
				p.Log.WithField("lastBlockFromNode", lastBlockFromNodeOld).Fatal("no new block in 20 tries")
			}
		}

		lastBlockFromBlocksTable, err := p.Bigtable.GetLastBlockInBlocksTable()
		if err != nil {
			p.Log.Errorf("error retrieving last blocks from blocks table: %v", err)
			continue
		}

		lastBlockFromDataTable, err := p.Bigtable.GetLastBlockInDataTable()
		if err != nil {
			p.Log.Errorf("error retrieving last blocks from data table: %v", err)
			continue
		}

		p.Log.WithFields(
			logger.Fields{
				"node":   lastBlockFromNode,
				"blocks": lastBlockFromBlocksTable,
				"data":   lastBlockFromDataTable,
			},
		).Info("last blocks")

		continueAfterError := false
		if lastBlockFromNode > 0 {
			if lastBlockFromBlocksTable < int(lastBlockFromNode) {
				p.Log.Infof("missing blocks %d to %d in blocks table, indexing ...", lastBlockFromBlocksTable+1, lastBlockFromNode)

				startBlock := int64(lastBlockFromBlocksTable+1) - p.OffsetBlock
				if startBlock < 0 {
					startBlock = 0
				}

				if p.BulkBlock <= 0 || p.BulkBlock > int64(lastBlockFromNode)-startBlock+1 {
					p.BulkBlock = int64(lastBlockFromNode) - startBlock + 1
				}

				for startBlock <= int64(lastBlockFromNode) && !continueAfterError {
					endBlock := startBlock + p.BulkBlock - 1
					if endBlock > int64(lastBlockFromNode) {
						endBlock = int64(lastBlockFromNode)
					}

					err = IndexFromNode(p.Bigtable, p.Client, startBlock, endBlock, p.ConcurrencyBlocks, p.TraceMode, p.Log)
					if err != nil {
						fields := logger.Fields{
							"start":       startBlock,
							"end":         endBlock,
							"concurrency": p.ConcurrencyBlocks,
						}
						if time.Since(lastSuccessulBlockIndexingTs) > time.Minute*30 {
							p.Log.WithFields(fields).Fatalf("error indexing from node: %v", err)
						} else {
							p.Log.WithFields(fields).Errorf("error indexing from node: %v", err)
						}
						continueAfterError = true
						continue
					} else {
						lastSuccessulBlockIndexingTs = time.Now()
					}

					startBlock = endBlock + 1
				}
				if continueAfterError {
					continue
				}
			}

			if lastBlockFromDataTable < int(lastBlockFromNode) {
				p.Log.Infof("missing blocks %d to %d in data table, indexing ...", lastBlockFromDataTable+1, lastBlockFromNode)

				startBlock := int64(lastBlockFromDataTable+1) - p.OffsetData
				if startBlock < 0 {
					startBlock = 0
				}

				if p.BulkData <= 0 || p.BulkData > int64(lastBlockFromNode)-startBlock+1 {
					p.BulkData = int64(lastBlockFromNode) - startBlock + 1
				}

				for startBlock <= int64(lastBlockFromNode) && !continueAfterError {
					endBlock := startBlock + p.BulkData - 1
					if endBlock > int64(lastBlockFromNode) {
						endBlock = int64(lastBlockFromNode)
					}

					blockTransforms := p.transforms()
					err = p.Bigtable.IndexEventsWithTransformers(startBlock, endBlock, blockTransforms, p.ConcurrencyData, p.Cache)
					if err != nil {
						fields := logger.Fields{
							"start":       startBlock,
							"end":         endBlock,
							"concurrency": p.ConcurrencyData,
						}
						p.Log.WithFields(fields).Errorf("error indexing from bigtable: %v", err)
						p.Cache.Clear()
						continueAfterError = true
						continue
					}
					p.Cache.Clear()

					startBlock = endBlock + 1
				}
				if continueAfterError {
					continue
				}
			}
		}

		if p.BalanceUpdate {
			ProcessMetadataUpdates(p.Bigtable, p.Client, p.BalancePrefix, p.BalanceBatch, 10, p.Log)
		}

		if p.EnsUpdate {
			err := p.Bigtable.ImportEnsUpdates(p.Client.GetNativeClient(), p.EnsBatch)
			if err != nil {
				p.Log.Errorf("error importing ens updates: %v", err)
				continue
			}
		}

		p.Log.Info("index run completed")
		services.ReportStatus(p.ReportStatus, "eth1indexer", "Running", nil)
	}
}
