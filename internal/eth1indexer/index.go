package eth1indexer

import (
	"fmt"
	"time"

	"github.com/coocood/freecache"
	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/execution"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
)

// IndexingParams holds common parameters for any Eth1 indexing operation.
type IndexingParams struct {
	StartBlock         int64
	EndBlock           int64
	BulkBlock          int64
	OffsetBlock        int64
	StartData	   int64
	EndData            int64
	BulkData           int64
	OffsetData         int64
	ConcurrencyBlocks  int64
	ConcurrencyData    int64
	ReorgDepth         int
	TraceMode          string
	Cache              *freecache.Cache
	Bigtable           *db.Bigtable
	Client             execution.ExecutionClient
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
	// Save specified block range from node to bigtable and exit
	if p.EndBlock != 0 && p.StartBlock < p.EndBlock {
		p.Log.Infof("saving block range from %v to %v to BigTable", p.StartBlock, p.EndBlock)
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
		p.Log.Infof("transforming data for a block range from %v to %v in BigTable", p.StartData, p.EndData)
		if err := p.Bigtable.IndexEventsWithTransformers(int64(p.StartData), int64(p.EndData), blockTransforms, p.ConcurrencyData, p.Cache); err != nil {
			return fmt.Errorf("error indexing from bigtable: %v", err)
		}
		p.Cache.Clear()
	}
	return nil
}

func IndexLive(p IndexingParams, reorgDepth int) error {
	// Endless cycle if no ranges or single blocks specified
	var lastBlockFromNodeOld uint64
	var lastBlockFromNodeSameCount uint64
	lastSuccessulBlockIndexingTs := time.Now()
	for ; ; time.Sleep(time.Second * 14) {
		err := HandleChainReorgs(p.Bigtable, p.Client, reorgDepth, p.Log)
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
		).Infof("last blocks")

		continueAfterError := false
		if lastBlockFromNode > 0 {
			if lastBlockFromBlocksTable < int(lastBlockFromNode) {
				p.Log.Infof("missing blocks %v to %v in blocks table, indexing ...", lastBlockFromBlocksTable+1, lastBlockFromNode)

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

					err = IndexFromNode(bt, rpcClient, startBlock, endBlock, *concurrencyBlocks, *traceMode, mainLogger)
					if err != nil {
						errMsg := "error indexing from node"
						errFields := map[string]interface{}{
							"start":       startBlock,
							"end":         endBlock,
							"concurrency": *concurrencyBlocks}
						if time.Since(lastSuccessulBlockIndexingTs) > time.Minute*30 {
							utils.LogFatal(err, errMsg, 0, errFields)
						} else {
							utils.LogError(err, errMsg, 0, errFields)
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
				mainLogger.Infof("missing blocks %v to %v in data table, indexing ...", lastBlockFromDataTable+1, lastBlockFromNode)

				startBlock := int64(lastBlockFromDataTable+1) - *offsetData
				if startBlock < 0 {
					startBlock = 0
				}

				if *bulkData <= 0 || *bulkData > int64(lastBlockFromNode)-startBlock+1 {
					*bulkData = int64(lastBlockFromNode) - startBlock + 1
				}

				for startBlock <= int64(lastBlockFromNode) && !continueAfterError {
					endBlock := startBlock + *bulkData - 1
					if endBlock > int64(lastBlockFromNode) {
						endBlock = int64(lastBlockFromNode)
					}

					err = bt.IndexEventsWithTransformers(startBlock, endBlock, transforms, *concurrencyData, cache)
					if err != nil {
						utils.LogError(err, "error indexing from bigtable", 0, map[string]interface{}{"start": startBlock, "end": endBlock, "concurrency": *concurrencyData})
						cache.Clear()
						continueAfterError = true
						continue
					}
					cache.Clear()

					startBlock = endBlock + 1
				}
				if continueAfterError {
					continue
				}
			}
		}

		if *enableBalanceUpdater {
			eth1indexer.ProcessMetadataUpdates(bt, rpcClient, balanceUpdaterPrefix, *balanceUpdaterBatchSize, 10, mainLogger)
		}

		if *enableEnsUpdater {
			err := bt.ImportEnsUpdates(rpcClient.GetNativeClient(), *ensBatchSize)
			if err != nil {
				utils.LogError(err, "error importing ens updates", 0, nil)
				continue
			}
		}

		mainLogger.Infof("index run completed")
		services.ReportStatus("eth1indexer", "Running", nil)
	}
}