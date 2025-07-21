package eth2indexer

import (
	"context"
	"math/big"
	"regexp"
	"time"
	"fmt"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	gethTypes "github.com/ethereum/go-ethereum/core/types"
	gethRPC "github.com/ethereum/go-ethereum/rpc"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
	"github.com/prysmaticlabs/prysm/v5/crypto/hash"
	"github.com/prysmaticlabs/prysm/v5/contracts/deposit"
	ethpb "github.com/prysmaticlabs/prysm/v5/proto/prysm/v1alpha1"
	"github.com/prysmaticlabs/prysm/v5/encoding/bytesutil"
)

var eth1LookBack = uint64(100)
var eth1MaxFetch = uint64(1000)
var infuraToMuchResultsErrorRE = regexp.MustCompile("query returned more than [0-9]+ results")
var gethRequestEntityTooLargeRE = regexp.MustCompile("413 Request Entity Too Large")
var eth1DepositEventSignature = hash.Keccak256([]byte("DepositEvent(bytes,bytes,bytes,bytes,bytes)"))

// eth1DepositsExporter continuously fetches and verifies DepositEvent logs
// from the configured Ethereum 1.0 deposit contract.
//
// It connects to an ETH1 RPC endpoint, retrieves logs in a bounded block range,
// unpacks and verifies deposit signatures using Prysm tooling,
// and persists the results into the Postgres eth1_deposits table.
//
// This exporter plays a critical role in reconstructing the genesis validator set
// and tracking new validator entries from the ETH1 chain.
func eth1DepositsExporter(p *IndexingParams) {
	eth1DepositContractAddress := common.HexToAddress(p.ChainParams.Deposit.DepositContractAddress)
	eth1DepositContractFirstBlock := p.Config.Indexing.Eth1DepositContractFirstBlock

	lastFetchedBlock := uint64(0)

	for {
		t0 := time.Now()

		lastDepositBlock, err := p.Database.GetLastDepositBlock()
		if err != nil {
			p.Log.WithError(err).Errorf("error retrieving highest block_number of eth1-deposits from db")
			time.Sleep(time.Second * 5)
			continue
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second*5)
		eth1Client := p.ExecClient.GetNativeClient()
		rpcClient := p.ExecClient.GetRPCClient()
		header, err := eth1Client.HeaderByNumber(ctx, nil)
		if err != nil {
			p.Log.WithError(err).Errorf("error getting header from eth1-client")
			cancel()
			time.Sleep(time.Second * 5)
			continue
		}
		cancel()

		blockHeight := header.Number.Uint64()

		fromBlock := lastDepositBlock + 1
		toBlock := blockHeight

		// start from the first block
		if fromBlock < eth1DepositContractFirstBlock {
			fromBlock = eth1DepositContractFirstBlock
		}
		// make sure we are progressing even if there are no deposits in the last batch
		if fromBlock < lastFetchedBlock+1 {
			fromBlock = lastFetchedBlock + 1
		}
		// if we are not synced to the head yet fetch missing blocks in batches of size 1000
		if toBlock > fromBlock+eth1MaxFetch {
			toBlock = fromBlock + eth1MaxFetch
		}
		if toBlock > blockHeight {
			toBlock = blockHeight
		}
		// if we are synced to the head look at the last 100 blocks
		if toBlock < fromBlock+eth1LookBack {
			if toBlock > eth1LookBack {
				fromBlock = toBlock - eth1LookBack
			} else {
				fromBlock = 0
			}
		}

		depositsToSave, err := fetchEth1Deposits(fromBlock, toBlock, eth1DepositContractAddress, eth1Client, rpcClient)
		if err != nil {
			if infuraToMuchResultsErrorRE.MatchString(err.Error()) || gethRequestEntityTooLargeRE.MatchString(err.Error()) {
				toBlock = fromBlock + 100
				if toBlock > blockHeight {
					toBlock = blockHeight
				}
				p.Log.Infof("limiting block-range to %v-%v when fetching eth1-deposits due to too much results", fromBlock, toBlock)
				depositsToSave, err = fetchEth1Deposits(fromBlock, toBlock, eth1DepositContractAddress, eth1Client, rpcClient)
			}
			if err != nil {
				p.Log.WithError(err).WithField("fromBlock", fromBlock).WithField("toBlock", toBlock).Errorf("error fetching eth1-deposits")
				time.Sleep(time.Second * 5)
				continue
			}
		}

		err = p.Database.SaveEth1Deposits(depositsToSave)
		if err != nil {
			p.Log.WithError(err).Errorf("error saving eth1-deposits")
			time.Sleep(time.Second * 5)
			continue
		}

		// make sure we are progressing even if there are no deposits in the last batch
		lastFetchedBlock = toBlock

		if len(depositsToSave) > 0 {
			p.Log.WithFields(logger.Fields{
				"duration":      time.Since(t0),
				"blockHeight":   blockHeight,
				"fromBlock":     fromBlock,
				"toBlock":       toBlock,
				"depositsSaved": len(depositsToSave),
			}).Info("exported eth1-deposits")
		}

		// progress faster if we are not synced to head yet
		if blockHeight != toBlock {
			time.Sleep(time.Second * 5)
			continue
		}

		time.Sleep(time.Minute)
	}
}

func fetchEth1Deposits(fromBlock, toBlock uint64, address common.Address, client *ethclient.Client, rpcClient *gethRPC.Client) (depositsToSave []*types.Eth1Deposit, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	topic := common.BytesToHash(eth1DepositEventSignature[:])
	qry := ethereum.FilterQuery{
		Addresses: []common.Address{
			address,
		},
		FromBlock: new(big.Int).SetUint64(fromBlock),
		ToBlock:   new(big.Int).SetUint64(toBlock),
		Topics:    [][]common.Hash{{topic}},
	}

	depositLogs, err := client.FilterLogs(ctx, qry)
	if err != nil {
		return depositsToSave, fmt.Errorf("error getting logs from eth1-client: %w", err)
	}

	blocksToFetch := []uint64{}
	txsToFetch := []string{}

	domain, err := utils.GetSigningDomain()
	if err != nil {
		return nil, err
	}

	for _, depositLog := range depositLogs {
		if depositLog.Topics[0] != eth1DepositEventSignature {
			continue
		}
		pubkey, withdrawalCredentials, amount, signature, merkletreeIndex, err := deposit.UnpackDepositLogData(depositLog.Data)
		if err != nil {
			return depositsToSave, fmt.Errorf("error unpacking eth1-deposit-log: %x: %w", depositLog.Data, err)
		}
		err = deposit.VerifyDepositSignature(&ethpb.Deposit_Data{
			PublicKey:             pubkey,
			WithdrawalCredentials: withdrawalCredentials,
			Amount:                bytesutil.FromBytes8(amount),
			Signature:             signature,
		}, domain)
		validSignature := err == nil
		blocksToFetch = append(blocksToFetch, depositLog.BlockNumber)
		txsToFetch = append(txsToFetch, depositLog.TxHash.Hex())
		depositsToSave = append(depositsToSave, &types.Eth1Deposit{
			TxHash:                depositLog.TxHash.Bytes(),
			TxIndex:               uint64(depositLog.TxIndex),
			BlockNumber:           depositLog.BlockNumber,
			PublicKey:             pubkey,
			WithdrawalCredentials: withdrawalCredentials,
			Amount:                bytesutil.FromBytes8(amount),
			Signature:             signature,
			MerkletreeIndex:       merkletreeIndex,
			Removed:               depositLog.Removed,
			ValidSignature:        validSignature,
		})
	}

	headers, txs, err := eth1BatchRequestHeadersAndTxs(blocksToFetch, txsToFetch, rpcClient)
	if err != nil {
		return depositsToSave, fmt.Errorf("error getting eth1-blocks: %w\nblocks to fetch: %v\n tx to fetch: %v", err, blocksToFetch, txsToFetch)
	}

	for _, d := range depositsToSave {
		// get corresponding block (for the tx-time)
		b, exists := headers[d.BlockNumber]
		if !exists {
			return depositsToSave, fmt.Errorf("error getting block for eth1-deposit: block does not exist in fetched map")
		}
		d.BlockTs = int64(b.Time)

		// get corresponding tx (for input and from-address)
		tx, exists := txs[fmt.Sprintf("0x%x", d.TxHash)]
		if !exists {
			return depositsToSave, fmt.Errorf("error getting tx for eth1-deposit: tx does not exist in fetched map")
		}
		d.TxInput = tx.Data()
		chainID := tx.ChainId()
		if chainID == nil {
			return depositsToSave, fmt.Errorf("error getting tx-chainId for eth1-deposit")
		}
		signer := gethTypes.NewPragueSigner(chainID)
		sender, err := signer.Sender(tx)
		if err != nil {
			return depositsToSave, fmt.Errorf("error getting sender for eth1-deposit (txHash: %x, chainID: %v): %w", d.TxHash, chainID, err)
		}
		d.FromAddress = sender.Bytes()
	}

	return depositsToSave, nil
}

// eth1BatchRequestHeadersAndTxs requests the block range specified in the arguments.
// Instead of requesting each block in one call, it batches all requests into a single rpc call.
// This code is shamelessly stolen and adapted from https://github.com/prysmaticlabs/prysm/blob/2eac24c/beacon-chain/powchain/service.go#L473
func eth1BatchRequestHeadersAndTxs(blocksToFetch []uint64, txsToFetch []string, client *gethRPC.Client) (map[uint64]*gethTypes.Header, map[string]*gethTypes.Transaction, error) {
	elems := make([]gethRPC.BatchElem, 0, len(blocksToFetch)+len(txsToFetch))
	headers := make(map[uint64]*gethTypes.Header, len(blocksToFetch))
	txs := make(map[string]*gethTypes.Transaction, len(txsToFetch))
	errors := make([]error, 0, len(blocksToFetch)+len(txsToFetch))

	for _, b := range blocksToFetch {
		header := &gethTypes.Header{}
		err := error(nil)
		elems = append(elems, gethRPC.BatchElem{
			Method: "eth_getBlockByNumber",
			Args:   []interface{}{hexutil.EncodeBig(big.NewInt(int64(b))), false},
			Result: header,
			Error:  err,
		})
		headers[b] = header
		errors = append(errors, err)
	}

	for _, txHashHex := range txsToFetch {
		tx := &gethTypes.Transaction{}
		err := error(nil)
		elems = append(elems, gethRPC.BatchElem{
			Method: "eth_getTransactionByHash",
			Args:   []interface{}{txHashHex},
			Result: tx,
			Error:  err,
		})
		txs[txHashHex] = tx
		errors = append(errors, err)
	}

	lenElems := len(elems)

	if lenElems == 0 {
		return headers, txs, nil
	}

	for i := 0; (i * 100) < lenElems; i++ {
		start := (i * 100)
		end := start + 100

		if end > lenElems {
			end = lenElems
		}

		ioErr := client.BatchCall(elems[start:end])
		if ioErr != nil {
			return nil, nil, ioErr
		}
	}

	for _, e := range errors {
		if e != nil {
			return nil, nil, e
		}
	}

	return headers, txs, nil
}