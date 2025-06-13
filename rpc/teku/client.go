package teku

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	"github.com/donovanhide/eventsource"
	gtypes "github.com/ethereum/go-ethereum/core/types"
	"golang.org/x/sync/errgroup"

	lru "github.com/hashicorp/golang-lru"
)

// TekuClient provides a typed client for interacting with a Teku
// Ethereum Consensus Layer (CL) node. It handles REST API communication,
// in-memory caching for validator assignments and slot metadata, and signing
// logic using the configured chain ID.
type TekuClient struct {
	endpoint            string
	client              *http.Client
	assignmentsCache    *lru.Cache
	assignmentsCacheMux *sync.Mutex
	slotsCache          *lru.Cache
	slotsCacheMux       *sync.Mutex
	signer              gtypes.Signer
}

// NewTekuClient initializes a new TekuClient instance with the given
// endpoint URL and chain ID. It sets up internal HTTP communication,
// LRU-based in-memory caches, and the appropriate signer for use with
// signature-requiring API calls.
//
// Parameters:
//   - endpoint: Base URL of the Teku REST API (e.g., http://localhost:5052).
//   - chainID: Chain ID used to configure the signer.
//
// Returns:
//   - *TekuClient: A ready-to-use client for communicating with the CL node.
//   - error: If cache initialization fails.
func NewTekuClient(endpoint string, chainID *big.Int) (*TekuClient, error) {
	signer := gtypes.NewPragueSigner(chainID)

	// Use transport with keep-alives and connection reuse
	httpTransport := &http.Transport{
		MaxIdleConns:          100,
		MaxConnsPerHost:       100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	client := &TekuClient{
		endpoint:            endpoint,
		signer:              signer,
		assignmentsCacheMux: &sync.Mutex{},
		slotsCacheMux:       &sync.Mutex{},
		client: &http.Client{
			Timeout:   2 * time.Minute,
			Transport: httpTransport,
		},
	}

	var err error
	client.assignmentsCache, err = lru.New(10)
	if err != nil {
		return nil, fmt.Errorf("failed to create assignments cache: %w", err)
	}

	client.slotsCache, err = lru.New(128)
	if err != nil {
		return nil, fmt.Errorf("failed to create slots cache: %w", err)
	}

	return client, nil
}

func (tc *TekuClient) GetNewBlockChan() chan *types.Block {
	blkCh := make(chan *types.Block, 10)
	req, err := http.NewRequest(http.MethodGet, fmt.Sprintf("%s/eth/v1/events?topics=head", tc.endpoint), nil)
	if err != nil {
		logger.Fatal(err, "error initializing event sse request", 0)
	}
	// disable gzip compression for sse
	req.Header.Set("accept-encoding", "identity")

	go func() {
		stream, err := eventsource.SubscribeWithRequest("", req)

		if err != nil {
			utils.LogFatal(err, "getting eventsource stream error", 0)
		}
		defer stream.Close()

		for {
			select {
			// It is important to register to Errors, otherwise the stream does not reconnect if the connection was lost
			case err := <-stream.Errors:
				utils.LogError(err, "Teku connection error (will automatically retry to connect)", 0)
			case e := <-stream.Events:
				// logger.Infof("retrieved %v via event stream", e.Data())
				var parsed consensus.StreamedBlockEventData
				err = json.Unmarshal([]byte(e.Data()), &parsed)
				if err != nil {
					logger.Warnf("failed to decode block event: %v", err)
					continue
				}

				logger.Infof("retrieving data for slot %v", parsed.Slot)
				block, err := tc.GetBlockBySlot(uint64(parsed.Slot))
				if err != nil {
					logger.Warnf("failed to fetch block for slot %d: %v", uint64(parsed.Slot), err)
					continue
				}
				logger.Infof("retrieved block for slot %v", parsed.Slot)
				// logger.Infof("pushing block %v", blk.Slot)
				blkCh <- block
			}
		}
	}()
	return blkCh
}

// GetPendingDeposits retrieves the list of pending validator deposits from the
// Teku REST API at `/eth/v1/beacon/states/head/pending_deposits`.
//
// This endpoint returns deposits that have been observed by the beacon node but
// have not yet been fully processed into the beacon state.
//
// Returns a parsed StandardBeaconPendingDepositsResponse on success, or an error
// if the request fails or the response is invalid.
func (tc *TekuClient) GetPendingDeposits() (*consensus.StandardBeaconPendingDepositsResponse, error) {
	url := fmt.Sprintf("%s/eth/v1/beacon/states/head/pending_deposits", tc.endpoint)

	headResp, err := tc.get(url)
	if err != nil {
		return nil, fmt.Errorf("error retrieving pending deposits: %w", err)
	}

	var parsedHead consensus.StandardBeaconPendingDepositsResponse
	if err := json.Unmarshal(headResp, &parsedHead); err != nil {
		return nil, fmt.Errorf("error parsing pending deposits: %w", err)
	}

	return &parsedHead, nil
}

// GetChainHead retrieves the current chain head from the Teku REST API
// via the `/eth/v1/beacon/headers/head` endpoint. It also attempts to resolve
// finality checkpoint information from `/eth/v1/beacon/states/{state_id}/finality_checkpoints`.
//
// If the node is pruned and finality checkpoint data is unavailable, the function
// returns a valid ChainHead with only head fields populated, while finality-related
// fields are zeroed out (see zeroFinalityFields).
//
// It applies the following logic:
//   - If the head slot is 0 (genesis), the state ID is set to "genesis".
//   - Finalized epoch is adjusted: beacon APIs return the checkpoint epoch,
//     but the actual finalized epoch is the one before it.
//   - Finalized slot is computed as the first slot of the next epoch, unless the
//     finalized root is zero, in which case the slot is set to 0.
//
// Returns a `*types.ChainHead` with head and finality metadata, or an error if
// data cannot be fetched or parsed.
func (tc *TekuClient) GetChainHead() (*types.ChainHead, error) {
	headResp, err := tc.get(fmt.Sprintf("%s/eth/v1/beacon/headers/head", tc.endpoint))
	if err != nil {
		return nil, fmt.Errorf("error retrieving chain head: %w", err)
	}

	var parsedHead consensus.StandardBeaconHeaderResponse
	err = json.Unmarshal(headResp, &parsedHead)
	if err != nil {
		return nil, fmt.Errorf("error parsing chain head: %w", err)
	}

	id := parsedHead.Data.Header.Message.StateRoot
	slot := parsedHead.Data.Header.Message.Slot
	if slot == 0 {
		id = "genesis"
	}
	finalityResp, err := tc.get(fmt.Sprintf("%s/eth/v1/beacon/states/%s/finality_checkpoints", tc.endpoint, id))
	if err != nil {
		// TODO: pruned node workaround
		if isPrunedError(err) {
			logger.Debugf("pruned node: skipping finality_checkpoints for head state root %s (slot %d)", id, slot)
			return zeroFinalityFields(uint64(slot), utils.MustParseHex(parsedHead.Data.Root)), nil
		}
		return nil, fmt.Errorf("error retrieving finality checkpoints of head: %w", err)
	}

	var parsedFinality consensus.StandardFinalityCheckpointsResponse
	err = json.Unmarshal(finalityResp, &parsedFinality)
	if err != nil {
		return nil, fmt.Errorf("error parsing finality checkpoints of head: %w", err)
	}

	// The epoch in the Finalized Object is not the finalized epoch, but the epoch for the checkpoint - the 'real' finalized epoch is the one before
	var finalizedEpoch = uint64(parsedFinality.Data.Finalized.Epoch)
	if finalizedEpoch > 0 {
		finalizedEpoch--
	}

	finalizedSlot := (finalizedEpoch + 1) * utils.Config.Chain.ClConfig.SlotsPerEpoch // The first Slot of the next epoch is finalized.
	if finalizedEpoch == 0 && parsedFinality.Data.Finalized.Root == "0x0000000000000000000000000000000000000000000000000000000000000000" {
		finalizedSlot = 0
	}
	return &types.ChainHead{
		HeadSlot:                   uint64(parsedHead.Data.Header.Message.Slot),
		HeadEpoch:                  uint64(parsedHead.Data.Header.Message.Slot) / utils.Config.Chain.ClConfig.SlotsPerEpoch,
		HeadBlockRoot:              utils.MustParseHex(parsedHead.Data.Root),
		FinalizedSlot:              finalizedSlot,
		FinalizedEpoch:             finalizedEpoch,
		FinalizedBlockRoot:         utils.MustParseHex(parsedFinality.Data.Finalized.Root),
		JustifiedSlot:              uint64(parsedFinality.Data.CurrentJustified.Epoch) * utils.Config.Chain.ClConfig.SlotsPerEpoch,
		JustifiedEpoch:             uint64(parsedFinality.Data.CurrentJustified.Epoch),
		JustifiedBlockRoot:         utils.MustParseHex(parsedFinality.Data.CurrentJustified.Root),
		PreviousJustifiedSlot:      uint64(parsedFinality.Data.PreviousJustified.Epoch) * utils.Config.Chain.ClConfig.SlotsPerEpoch,
		PreviousJustifiedEpoch:     uint64(parsedFinality.Data.PreviousJustified.Epoch),
		PreviousJustifiedBlockRoot: utils.MustParseHex(parsedFinality.Data.PreviousJustified.Root),
	}, nil
}

// GetValidatorQueue retrieves the validator activation and exit queues from the
// current head state using the Teku REST API.
//
// It requests only validators in the following statuses:
//   - `pending_queued`: queued for activation
//   - `active_exiting` or `active_slashed`: active but exiting or slashed
//
// The response is used to compute:
//   - The number of validators queued for activation
//   - The number of validators scheduled to exit
//   - The total effective balance of exiting/slashed validators
//
// Returns a `*types.ValidatorQueue` struct with these metrics, or an error
// if the request or JSON parsing fails.
func (tc *TekuClient) GetValidatorQueue() (*types.ValidatorQueue, error) {
	// Filter only relevant validator statuses to reduce payload and speed up response
	url := fmt.Sprintf("%s/eth/v1/beacon/states/head/validators?status=pending_queued,active_exiting,active_slashed", tc.endpoint)
	validatorsResp, err := tc.get(url)
	if err != nil {
		return nil, fmt.Errorf("error retrieving validator queue from head state: %w", err)
	}

	var parsedValidators consensus.StandardValidatorsResponse
	if err := json.Unmarshal(validatorsResp, &parsedValidators); err != nil {
		return nil, fmt.Errorf("error parsing validator queue response: %w", err)
	}

	statusMap := make(map[string]uint64)
	var exitingBalance uint64

	for _, validator := range parsedValidators.Data {
		statusMap[validator.Status]++
		if validator.Status == "active_exiting" || validator.Status == "active_slashed" {
			exitingBalance += uint64(validator.Validator.EffectiveBalance)
		}
	}

	return &types.ValidatorQueue{
		Activating:     statusMap["pending_queued"],
		Exiting:        statusMap["active_exiting"] + statusMap["active_slashed"],
		ExitingBalance: exitingBalance,
	}, nil
}

// GetEpochAssignments retrieves proposer, attestor, and sync committee assignments
// for a specific epoch from the Teku Consensus Layer (CL) API.
//
// This function aggregates data from several Teku endpoints:
//   - Proposer duties for the epoch
//   - Attestation committees based on a consistent state root
//   - Sync committee membership (for epochs >= Altair fork)
//
// The function handles internal caching of results to avoid redundant remote calls.
// If the result for the given epoch is found in cache, it is returned immediately.
//
// In environments where the Teku node is running in pruned mode, and
// data is no longer available (e.g., proposer duties or committee info for past epochs),
// the function attempts to detect this (via HTTP 503/404) and returns a zeroed-out,
// non-nil *EpochAssignments value instead of failing.
//
// Returns:
//   - A pointer to a fully populated *EpochAssignments if data is available
//   - A valid but empty *EpochAssignments if running against a pruned node and data is missing
//   - An error if any part of the process fails and cannot be gracefully degraded
func (tc *TekuClient) GetEpochAssignments(epoch uint64) (*types.EpochAssignments, error) {
	// Check cache first
	tc.assignmentsCacheMux.Lock()
	if cached, found := tc.assignmentsCache.Get(epoch); found {
		tc.assignmentsCacheMux.Unlock()
		return cached.(*types.EpochAssignments), nil
	}
	tc.assignmentsCacheMux.Unlock()

	assignments := &types.EpochAssignments{
		ProposerAssignments: make(map[uint64]uint64),
		AttestorAssignments: make(map[string]uint64),
		SyncAssignments:     []uint64{},
	}

	// Proposer duties
	proposerResp, err := tc.get(fmt.Sprintf("%s/eth/v1/validator/duties/proposer/%d", tc.endpoint, epoch))
	if err != nil {
		// TODO: pruned node workaround
		if isPrunedError(err) {
			logger.Debugf("pruned mode: proposer duties unavailable for epoch %d", epoch)
		} else {
			return nil, fmt.Errorf("error retrieving proposer duties for epoch %v: %w", epoch, err)
		}
	} else {
		var parsedProposerResponse consensus.StandardProposerDutiesResponse
		if err := json.Unmarshal(proposerResp, &parsedProposerResponse); err != nil {
			return nil, fmt.Errorf("error parsing proposer duties: %w", err)
		}
		for _, duty := range parsedProposerResponse.Data {
			assignments.ProposerAssignments[uint64(duty.Slot)] = uint64(duty.ValidatorIndex)
		}

		// Header for Consistent State Root
		headerResp, err := tc.get(fmt.Sprintf("%s/eth/v1/beacon/headers/%s", tc.endpoint, parsedProposerResponse.DependentRoot))
		if err != nil {
			// TODO: pruned node workaround
			if isPrunedError(err) {
				logger.Debugf("pruned mode: header unavailable for root %s (epoch %d)", parsedProposerResponse.DependentRoot, epoch)
			} else {
				return nil, fmt.Errorf("error retrieving header root %s (epoch %d)", parsedProposerResponse.DependentRoot, epoch)
			}
		} else {
			var parsedHeader consensus.StandardBeaconHeaderResponse
			if err := json.Unmarshal(headerResp, &parsedHeader); err != nil {
				return nil, fmt.Errorf("error parsing chain header: %w", err)
			}
			depStateRoot := parsedHeader.Data.Header.Message.StateRoot

			// Committees
			committeesResp, err := tc.get(fmt.Sprintf("%s/eth/v1/beacon/states/%s/committees?epoch=%d", tc.endpoint, depStateRoot, epoch))
			if err != nil {
				// TODO: pruned node workaround
				if isPrunedError(err) {
					logger.Debugf("pruned mode: committees unavailable for root %s (epoch %d)", depStateRoot, epoch)
				}
				return nil, fmt.Errorf("error retrieving committees for root %s (epoch %d)", depStateRoot, epoch)
			} else {
				var parsedCommittees consensus.StandardCommitteesResponse
				if err := json.Unmarshal(committeesResp, &parsedCommittees); err != nil {
					return nil, fmt.Errorf("error parsing committees data: %w", err)
				}
				for _, committee := range parsedCommittees.Data {
					for i, valIndex := range committee.Validators {
						valIndexU64, err := strconv.ParseUint(valIndex, 10, 64)
						if err != nil {
							return nil, fmt.Errorf("epoch %d committee %d index %d has bad validator index %q", epoch, committee.Index, i, valIndex)
						}
						k := utils.FormatAttestorAssignmentKey(uint64(committee.Slot), uint64(committee.Index), uint64(i))
						assignments.AttestorAssignments[k] = valIndexU64
					}
				}
			}
		}
	}

	// Sync Committee (Altair and later)
	if epoch >= utils.Config.Chain.ClConfig.AltairForkEpoch {
		syncState := fmt.Sprintf("%d", epoch*utils.Config.Chain.ClConfig.SlotsPerEpoch)
		if epoch > utils.Config.Chain.ClConfig.AltairForkEpoch && len(assignments.ProposerAssignments) > 0 {
			// use DependentRoot state only if proposer and header worked
			for slot := range assignments.ProposerAssignments {
				syncState = fmt.Sprintf("%d", slot)
				break
			}
		}

		syncCommittee, err := tc.GetSyncCommittee(syncState, epoch)
		if err != nil {
			// TODO: pruned node workaround
			if isPrunedError(err) {
				logger.Debugf("pruned mode: sync committee unavailable for state %s (epoch %d)", syncState, epoch)
			} else {
				return nil, fmt.Errorf("failed to get sync committee for state %s (epoch %d)", syncState, epoch)
			}
		} else {
			assignments.SyncAssignments = make([]uint64, len(syncCommittee.Validators))
			for i, valIndexStr := range syncCommittee.Validators {
				valIndexU64, err := strconv.ParseUint(valIndexStr, 10, 64)
				if err != nil {
					return nil, fmt.Errorf("in sync_committee for epoch %d validator %d has bad index: %q", epoch, i, valIndexStr)
				}
				assignments.SyncAssignments[i] = valIndexU64
			}
		}
	}

	// Cache result
	tc.assignmentsCacheMux.Lock()
	tc.assignmentsCache.Add(epoch, assignments)
	tc.assignmentsCacheMux.Unlock()

	return assignments, nil
}

// GetEpochProposerAssignments retrieves the proposer duties for a given epoch
// from the Teku Consensus Layer (CL) API.
//
// It returns a fully populated StandardProposerDutiesResponse containing the
// dependent root and a list of proposer assignments. In pruned node mode, where
// historical data may be unavailable, it returns an empty-but-valid response
// (with Data = [] and DependentRoot = "") instead of nil.
//
// This allows downstream consumers to operate without additional nil checks or
// conditional logic.
//
// Returns:
//   - *StandardProposerDutiesResponse: Always non-nil
//   - error: Only if the response fails to decode or is otherwise invalid
func (tc *TekuClient) GetEpochProposerAssignments(epoch uint64) (*consensus.StandardProposerDutiesResponse, error) {
	url := fmt.Sprintf("%s/eth/v1/validator/duties/proposer/%d", tc.endpoint, epoch)

	proposerResp, err := tc.get(url)
	if err != nil {
		// TODO: pruned node workaround
		if isPrunedError(err) {
			logger.Debugf("pruned mode: proposer duties unavailable for epoch %d", epoch)
			return &consensus.StandardProposerDutiesResponse{
				DependentRoot: "",
				Data:          []consensus.BeaconProposerDutyData{},
			}, nil
		}
		return nil, fmt.Errorf("error retrieving proposer duties for epoch %v: %w", epoch, err)
	}

	var result consensus.StandardProposerDutiesResponse
	if err := json.Unmarshal(proposerResp, &result); err != nil {
		return nil, fmt.Errorf("error parsing proposer duties: %w", err)
	}

	return &result, nil
}

// GetValidatorState retrieves the full validator state for a given epoch from
// the Teku Consensus Layer (CL) API. It queries the validator list at the
// slot corresponding to the start of the epoch.
//
// If the node is pruned and data for the given epoch is no longer available,
// the function returns a non-nil, empty StandardValidatorsResponse to signal that
// the result is intentionally unavailable but not an error.
//
// This allows downstream consumers to avoid nil-checks and handle empty results
// gracefully.
//
// Returns:
//   - *StandardValidatorsResponse: Non-nil, with .Data containing zero or more validator entries
//   - error: Only if the HTTP request or JSON decoding fails and is not pruned-related
func (tc *TekuClient) GetValidatorState(epoch uint64) (*consensus.StandardValidatorsResponse, error) {
	slot := epoch * utils.Config.Chain.ClConfig.SlotsPerEpoch
	url := fmt.Sprintf("%s/eth/v1/beacon/states/%d/validators", tc.endpoint, slot)

	validatorsResp, err := tc.get(url)
	if err != nil {
		// TODO: pruned node workaround
		if isPrunedError(err) {
			logger.Debugf("pruned mode: validator state unavailable for epoch %d (slot %d)", epoch, slot)
			return &consensus.StandardValidatorsResponse{
				Data: []consensus.StandardValidatorEntry{},
			}, nil
		}
		return nil, fmt.Errorf("error retrieving validators for epoch %d: %w", epoch, err)
	}

	var result consensus.StandardValidatorsResponse
	if err := json.Unmarshal(validatorsResp, &result); err != nil {
		return nil, fmt.Errorf("error parsing validators for epoch %d: %w", epoch, err)
	}

	return &result, nil
}

func (tc *TekuClient) GetEpochData(epoch uint64, skipHistoricBalances bool) (*types.EpochData, error) {
	head, err := tc.GetChainHead()
	if err != nil {
		return nil, fmt.Errorf("error retrieving chain head: %w", err)
	}

	data := &types.EpochData{
		Epoch:             epoch,
		SyncDuties:        make(map[types.Slot]map[types.ValidatorIndex]bool),
		AttestationDuties: make(map[types.Slot]map[types.ValidatorIndex][]types.Slot),
		Blocks:            make(map[uint64]map[string]*types.Block),
		FutureBlocks:      make(map[uint64]map[string]*types.Block),
	}

	if head.FinalizedEpoch >= epoch {
		data.Finalized = true
	}
	if head.FinalizedEpoch == 0 && epoch == 0 {
		data.Finalized = false
	}

	validators, err := tc.GetValidatorState(epoch)
	if err != nil {
		return nil, err
	}

	for _, validator := range validators.Data {
		data.Validators = append(data.Validators, &types.Validator{
			Index:                      uint64(validator.Index),
			PublicKey:                  utils.MustParseHex(validator.Validator.Pubkey),
			WithdrawalCredentials:      utils.MustParseHex(validator.Validator.WithdrawalCredentials),
			Balance:                    uint64(validator.Balance),
			EffectiveBalance:           uint64(validator.Validator.EffectiveBalance),
			Slashed:                    validator.Validator.Slashed,
			ActivationEligibilityEpoch: uint64(validator.Validator.ActivationEligibilityEpoch),
			ActivationEpoch:            uint64(validator.Validator.ActivationEpoch),
			ExitEpoch:                  uint64(validator.Validator.ExitEpoch),
			WithdrawableEpoch:          uint64(validator.Validator.WithdrawableEpoch),
			Status:                     validator.Status,
		})
	}
	logger.Infof("retrieved %v validators for epoch %v", len(data.Validators), epoch)

	var wg errgroup.Group
	var mux sync.Mutex

	// Assignments
	wg.Go(func() error {
		assignments, err := tc.GetEpochAssignments(epoch)
		if err != nil {
			return fmt.Errorf("error retrieving assignments for epoch %d: %w", epoch, err)
		}

		data.ValidatorAssignmentes = assignments

		for slot := epoch * utils.Config.Chain.ClConfig.SlotsPerEpoch; slot <= (epoch+1)*utils.Config.Chain.ClConfig.SlotsPerEpoch-1; slot++ {
			data.SyncDuties[types.Slot(slot)] = make(map[types.ValidatorIndex]bool)
			for _, vIdx := range assignments.SyncAssignments {
				data.SyncDuties[types.Slot(slot)][types.ValidatorIndex(vIdx)] = false
			}
		}

		for key, vIdx := range assignments.AttestorAssignments {
			parts := strings.Split(key, "-")
			attestedSlot, err := strconv.ParseUint(parts[0], 10, 64)
			if err != nil {
				return fmt.Errorf("error parsing attestation key %q: %w", key, err)
			}
			if data.AttestationDuties[types.Slot(attestedSlot)] == nil {
				data.AttestationDuties[types.Slot(attestedSlot)] = make(map[types.ValidatorIndex][]types.Slot)
			}
			data.AttestationDuties[types.Slot(attestedSlot)][types.ValidatorIndex(vIdx)] = []types.Slot{}
		}

		logger.Infof("retrieved assignment data for epoch %v", epoch)
		return nil
	})

	// Participation
	if epoch < head.HeadEpoch {
		wg.Go(func() error {
			stats, err := tc.GetValidatorParticipation(epoch)
			if err != nil {
				// TODO: pruned node workaround
				if isPrunedError(err) {
					logger.Warnf("epoch %d: participation stats unavailable (pruned)", epoch)
					data.EpochParticipationStats = &types.ValidatorParticipation{Epoch: epoch}
					data.PrunedPartial = true
					return nil
				}
				return fmt.Errorf("error retrieving participation stats for epoch %d: %w", epoch, err)
			}
			data.EpochParticipationStats = stats
			return nil
		})
	} else {
		data.EpochParticipationStats = &types.ValidatorParticipation{Epoch: epoch}
	}

	// Blocks
	wg.Go(func() error {
		for slot := epoch * utils.Config.Chain.ClConfig.SlotsPerEpoch; slot <= (epoch+1)*utils.Config.Chain.ClConfig.SlotsPerEpoch-1; slot++ {
			if slot > head.HeadSlot {
				continue
			}
			block, err := tc.GetBlockBySlot(slot)
			if err != nil {
				return fmt.Errorf("error retrieving block for slot %d: %w", slot, err)
			}

			mux.Lock()
			if data.Blocks[block.Slot] == nil {
				data.Blocks[block.Slot] = make(map[string]*types.Block)
			}
			data.Blocks[block.Slot][fmt.Sprintf("%x", block.BlockRoot)] = block

			for vIdx, duty := range block.SyncDuties {
				data.SyncDuties[types.Slot(block.Slot)][types.ValidatorIndex(vIdx)] = duty
			}
			for vIdx, attestedSlots := range block.AttestationDuties {
				for _, attestedSlot := range attestedSlots {
					if data.AttestationDuties[types.Slot(attestedSlot)] == nil {
						data.AttestationDuties[types.Slot(attestedSlot)] = make(map[types.ValidatorIndex][]types.Slot)
					}
					data.AttestationDuties[types.Slot(attestedSlot)][types.ValidatorIndex(vIdx)] = append(
						data.AttestationDuties[types.Slot(attestedSlot)][types.ValidatorIndex(vIdx)],
						types.Slot(block.Slot),
					)
				}
			}
			mux.Unlock()
		}
		return nil
	})

	// Future Blocks
	wg.Go(func() error {
		for slot := (epoch + 1) * utils.Config.Chain.ClConfig.SlotsPerEpoch; slot <= (epoch+2)*utils.Config.Chain.ClConfig.SlotsPerEpoch-1; slot++ {
			if slot > head.HeadSlot {
				continue
			}
			block, err := tc.GetBlockBySlot(slot)
			if err != nil {
				return fmt.Errorf("error retrieving future block for slot %d: %w", slot, err)
			}

			mux.Lock()
			if data.FutureBlocks[block.Slot] == nil {
				data.FutureBlocks[block.Slot] = make(map[string]*types.Block)
			}
			data.FutureBlocks[block.Slot][fmt.Sprintf("%x", block.BlockRoot)] = block

			for vIdx, attestedSlots := range block.AttestationDuties {
				for _, attestedSlot := range attestedSlots {
					if attestedSlot < types.Slot((epoch+1)*utils.Config.Chain.ClConfig.SlotsPerEpoch) {
						data.AttestationDuties[types.Slot(attestedSlot)][types.ValidatorIndex(vIdx)] = append(
							data.AttestationDuties[types.Slot(attestedSlot)][types.ValidatorIndex(vIdx)],
							types.Slot(block.Slot),
						)
					}
				}
			}
			mux.Unlock()
		}
		return nil
	})

	if err := wg.Wait(); err != nil {
		return nil, err
	}

	// missed/scheduled blocks
	if data.ValidatorAssignmentes != nil {
		for slot, proposer := range data.ValidatorAssignmentes.ProposerAssignments {
			if _, found := data.Blocks[slot]; !found {
				status := types.BlockStatusMissed
				blockRoot := []byte{0x1}
				if utils.SlotToTime(slot).After(time.Now().Add(-4 * time.Second)) {
					status = types.BlockStatusScheduled
					blockRoot = []byte{0x0}
				}
				data.Blocks[slot] = map[string]*types.Block{
					"0x0": {
						Status:    status,
						Proposer:  int64(proposer),
						BlockRoot: blockRoot,
						Slot:      slot,
					},
				}
			}
		}
	}

	logger.Infof("retrieved epoch data for epoch %d (prunedPartial = %v)", epoch, data.PrunedPartial)

	return data, nil
}

// GetBalancesForEpoch fetches the validator balances at the given epoch
// from the Teku Consensus Layer (CL) API.
//
// It resolves the corresponding slot for the epoch and queries the validator_balances
// endpoint for that slot. If the requested epoch is 0 and the slot is unavailable,
// it attempts to fall back to the "genesis" slot. In pruned mode, if balances are
// not available, it logs the condition and returns an empty (but non-nil) map.
//
// Returns:
//   - map[uint64]uint64: Mapping from validator index to balance
//   - error: If the request or response parsing fails for a non-pruned node
func (tc *TekuClient) GetBalancesForEpoch(epoch int64) (map[uint64]uint64, error) {
	if epoch < 0 {
		epoch = 0
	}

	slot := epoch * int64(utils.Config.Chain.ClConfig.SlotsPerEpoch)
	validatorBalances := make(map[uint64]uint64)

	url := fmt.Sprintf("%s/eth/v1/beacon/states/%d/validator_balances", tc.endpoint, slot)
	resp, err := tc.get(url)

	// Fallback for genesis
	if err != nil && epoch == 0 {
		logger.Warnf("slot 0 validator balances unavailable, falling back to 'genesis'")
		resp, err = tc.get(fmt.Sprintf("%s/eth/v1/beacon/states/genesis/validator_balances", tc.endpoint))
		if err != nil {
			logger.Debug("pruned mode: validator balances for genesis unavailable")
			return validatorBalances, nil
		}
	} else if err != nil {
		// TODO: pruned node workaround
		if isPrunedError(err) {
			logger.Debugf("pruned mode: validator balances for epoch %d unavailable", epoch)
			return validatorBalances, nil
		}
		return nil, fmt.Errorf("failed to get validator balances for epoch %d: %w", epoch, err)
	}

	var parsedResponse consensus.StandardValidatorBalancesResponse
	if err := json.Unmarshal(resp, &parsedResponse); err != nil {
		return nil, fmt.Errorf("error parsing response for validator_balances")
	}

	for _, b := range parsedResponse.Data {
		validatorBalances[uint64(b.Index)] = uint64(b.Balance)
	}

	return validatorBalances, nil
}

func (tc *TekuClient) GetBlockByBlockroot(blockroot []byte) (*types.Block, error) {
	resHeaders, err := tc.get(fmt.Sprintf("%s/eth/v1/beacon/headers/0x%x", tc.endpoint, blockroot))
	if err != nil {
		if err == errNotFound {
			// no block found
			return &types.Block{}, nil
		}
		return nil, fmt.Errorf("error retrieving headers for blockroot 0x%x: %w", blockroot, err)
	}
	var parsedHeaders consensus.StandardBeaconHeaderResponse
	err = json.Unmarshal(resHeaders, &parsedHeaders)
	if err != nil {
		return nil, fmt.Errorf("error parsing header-response for blockroot 0x%x: %w", blockroot, err)
	}

	slot := uint64(parsedHeaders.Data.Header.Message.Slot)

	resp, err := tc.get(fmt.Sprintf("%s/eth/v2/beacon/blocks/%s", tc.endpoint, parsedHeaders.Data.Root))
	if err != nil {
		return nil, fmt.Errorf("error retrieving block data at slot %v: %w", slot, err)
	}

	var parsedResponse consensus.StandardV2BlockResponse
	err = json.Unmarshal(resp, &parsedResponse)
	if err != nil {
		logger.Errorf("error parsing block data at slot %v: %v", parsedHeaders.Data.Header.Message.Slot, err)
		return nil, fmt.Errorf("error parsing block-response at slot %v: %w", slot, err)
	}

	return tc.blockFromResponse(&parsedHeaders, &parsedResponse)
}

// GetBlockHeader will get the block header by slot from Teku RPC api
func (tc *TekuClient) GetBlockHeader(slot uint64) (*consensus.StandardBeaconHeaderResponse, error) {
	var parsedHeaders *consensus.StandardBeaconHeaderResponse

	resHeaders, err := tc.get(fmt.Sprintf("%s/eth/v1/beacon/headers/%d", tc.endpoint, slot))
	if err != nil && slot == 0 {
		headResp, err := tc.get(fmt.Sprintf("%s/eth/v1/beacon/headers", tc.endpoint))
		if err != nil {
			return nil, fmt.Errorf("error retrieving chain head for slot %v: %w", slot, err)
		}

		var parsedHeader consensus.StandardBeaconHeadersResponse
		err = json.Unmarshal(headResp, &parsedHeader)
		if err != nil {
			return nil, fmt.Errorf("error parsing chain head for slot %v: %w", slot, err)
		}

		if len(parsedHeader.Data) == 0 {
			return nil, fmt.Errorf("error no headers available for slot %v", slot)
		}

		parsedHeaders = &consensus.StandardBeaconHeaderResponse{
			Data: parsedHeader.Data[len(parsedHeader.Data)-1],
		}

	} else if err != nil {
		if err == errNotFound { // return dummy block for missed slots
			// no block found
			return nil, nil
		}
		return nil, fmt.Errorf("error retrieving headers at slot %v: %w", slot, err)
	}

	if parsedHeaders == nil {
		err = json.Unmarshal(resHeaders, &parsedHeaders)
		if err != nil {
			return nil, fmt.Errorf("error parsing header-response at slot %v: %w", slot, err)
		}
	}

	return parsedHeaders, nil
}

// GetBlockBySlot retrieves the Beacon block for a given slot from the Teku CL API.
//
// If the block was missed (i.e., no proposal), it builds a synthetic block with proposer info,
// zero block root, and enriches it with epoch-level data (assignments, validators) if it's the first slot of an epoch.
//
// Cached blocks are reused if available.
func (tc *TekuClient) GetBlockBySlot(slot uint64) (*types.Block, error) {
	epoch := slot / utils.Config.Chain.ClConfig.SlotsPerEpoch
	isFirstSlotOfEpoch := slot%utils.Config.Chain.ClConfig.SlotsPerEpoch == 0

	headerURL := fmt.Sprintf("%s/eth/v1/beacon/headers/%d", tc.endpoint, slot)
	headerResp, err := tc.get(headerURL)

	var parsedHeader consensus.StandardBeaconHeaderResponse
	isMissedSlot := false

	switch {
	case err == nil:
		if err := json.Unmarshal(headerResp, &parsedHeader); err != nil {
			return nil, fmt.Errorf("failed to parse header at slot %d: %w", slot, err)
		}
	case errors.Is(err, errNotFound):
		isMissedSlot = true
	default:
		return nil, fmt.Errorf("failed to retrieve header at slot %d: %w", slot, err)
	}

	if isMissedSlot {
		logger.Infof("slot %d: no block proposed (missed slot)", slot)
		return tc.buildMissedSlotBlock(slot, epoch, isFirstSlotOfEpoch)
	}

	// Check cache first
	tc.slotsCacheMux.Lock()
	if cached, ok := tc.slotsCache.Get(parsedHeader.Data.Root); ok {
		tc.slotsCacheMux.Unlock()
		if block, ok := cached.(*types.Block); ok {
			logger.Infof("slot %d (0x%x) retrieved from cache", block.Slot, block.BlockRoot)
			return block, nil
		}
		logger.Errorf("invalid cached block for slot %d", slot)
	} else {
		tc.slotsCacheMux.Unlock()
	}

	// Fetch full block
	blockURL := fmt.Sprintf("%s/eth/v2/beacon/blocks/%s", tc.endpoint, parsedHeader.Data.Root)
	blockResp, err := tc.get(blockURL)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve full block at slot %d: %w", slot, err)
	}

	var parsedBlock consensus.StandardV2BlockResponse
	if err := json.Unmarshal(blockResp, &parsedBlock); err != nil {
		return nil, fmt.Errorf("failed to parse block at slot %d: %w", slot, err)
	}

	block, err := tc.blockFromResponse(&parsedHeader, &parsedBlock)
	if err != nil {
		return nil, fmt.Errorf("failed to construct block at slot %d: %w", slot, err)
	}

	if isFirstSlotOfEpoch {
		tc.enrichBlockWithEpochData(block, epoch)
	}

	// Cache result
	tc.slotsCacheMux.Lock()
	tc.slotsCache.Add(parsedHeader.Data.Root, block)
	tc.slotsCacheMux.Unlock()

	return block, nil
}

// blockFromResponse constructs a types.Block from Teku's StandardBeaconHeaderResponse
// and StandardV2BlockResponse, enriching it with epoch assignments and blob sidecars.
// It gracefully handles pruned node behavior and missing data where applicable.
func (tc *TekuClient) blockFromResponse(
	parsedHeaders *consensus.StandardBeaconHeaderResponse,
	parsedResponse *consensus.StandardV2BlockResponse,
) (*types.Block, error) {

	parsedBlock := parsedResponse.Data
	slot := uint64(parsedHeaders.Data.Header.Message.Slot)
	block := buildBlockSkeleton(&parsedBlock, parsedHeaders, slot)

	// Validate BlobKZG commitments against blob sidecars
	if err := tc.validateBlobSidecars(block, &parsedBlock); err != nil {
		return nil, err
	}

	epoch := slot / utils.Config.Chain.ClConfig.SlotsPerEpoch
	epochAssignments, err := tc.GetEpochAssignments(epoch)
	if err != nil {
		logger.Warnf("epoch assignments unavailable at epoch %d: %v", epoch, err)
		epochAssignments = &types.EpochAssignments{
			ProposerAssignments: map[uint64]uint64{},
			AttestorAssignments: map[string]uint64{},
			SyncAssignments:     []uint64{},
		}
	}

	// SyncAggregate
	if err := tc.buildSyncAggregate(block, &parsedBlock, epochAssignments); err != nil {
		return nil, err
	}

	// ExecutionPayload
	if err := tc.buildExecutionPayload(block, &parsedBlock); err != nil {
		return nil, err
	}

	block.Eth1Data.DepositCount = sanitizeDepositCount(block.Eth1Data.DepositCount)

	// Other major structures
	buildProposerSlashings(block, &parsedBlock)
	buildAttesterSlashings(block, &parsedBlock)
	buildAttestations(tc, block, &parsedBlock)
	buildDeposits(block, &parsedBlock)
	buildVoluntaryExits(block, &parsedBlock)
	buildBLSChanges(block, &parsedBlock)

	return block, nil
}

// GetValidatorParticipation fetches validator participation metrics for a given epoch
// from the Teku Beacon Node REST API. It returns a ValidatorParticipation struct containing
// participation rate, eligible/voted ETH, and finalization status.
//
// The function:
//   1. Retrieves the current chain head.
//   2. Verifies that the requested epoch is not in the future or ongoing.
//   3. Adjusts the request epoch to account for Teku's API calculating participation stats
//      at the *end* of an epoch.
//   4. Handles pruned data by returning a valid empty object if expected (404s due to non-archival node).
//   5. Parses response and constructs a ValidatorParticipation object with appropriate values.
//
// Returns an empty ValidatorParticipation object with zero values and no error if data is pruned.
func (tc *TekuClient) GetValidatorParticipation(epoch uint64) (*types.ValidatorParticipation, error) {
	head, err := tc.GetChainHead()
	if err != nil {
		return nil, fmt.Errorf("failed to get chain head: %w", err)
	}

	if epoch > head.HeadEpoch {
		return nil, fmt.Errorf("epoch %v is newer than the latest head %v", epoch, TekuLatestHeadEpoch)
	}

	if epoch == head.HeadEpoch {
		// Participation stats are not available for an ongoing epoch
		return nil, fmt.Errorf("epoch %v is ongoing and cannot be queried", epoch)
	}

	requestEpoch := epoch
	if epoch+1 < head.HeadEpoch {
		// Teku provides finalized data one epoch after
		requestEpoch += 1
	}

	logger.Infof("requesting validator inclusion data for epoch %v", requestEpoch)

	resp, err := tc.get(fmt.Sprintf("%s/teku/validator_inclusion/%d/global", tc.endpoint, requestEpoch))
	if err != nil {
		// TODO: pruned node workaround
		if isPrunedError(err) {
			logger.Debugf("pruned mode: epoch %v pruned from node history; returning empty participation", requestEpoch)
			return &types.ValidatorParticipation{
				Epoch:                   epoch,
				GlobalParticipationRate: 0,
				VotedEther:              0,
				EligibleEther:           0,
				Finalized:               false,
			}, nil
		}
		return nil, fmt.Errorf("error retrieving validator participation data for epoch %v: %w", requestEpoch, err)
	}

	var parsedResponse consensus.StandardValidatorParticipationResponse
	if err := json.Unmarshal(resp, &parsedResponse); err != nil {
		return nil, fmt.Errorf("error parsing validator participation data for epoch %v: %w", epoch, err)
	}

	var res *types.ValidatorParticipation
	
	if epoch < requestEpoch {
		// Teku reports stats under 'previous' when requesting +1 epoch
		prevEpochActiveGwei := parsedResponse.Data.PreviousEpochActiveGwei
		
		// Sometimes PreviousEpochActiveGwei is 0 (rare case), try to fallback
		if prevEpochActiveGwei == 0 {
			prevResp, err := tc.get(fmt.Sprintf("%s/teku/validator_inclusion/%d/global", tc.endpoint, requestEpoch-1))
			if err != nil {
				// TODO: pruned node workaround
				if isPrunedError(err) {
					logger.Debugf("pruned mode: prevEpoch %v data pruned; returning empty participation", requestEpoch-1)
					return &types.ValidatorParticipation{
						Epoch:                   epoch,
						GlobalParticipationRate: 0,
						VotedEther:              0,
						EligibleEther:           0,
						Finalized:               false,
					}, nil
				}
				return nil, fmt.Errorf("error retrieving validator participation data for prevEpoch %v: %w", requestEpoch-1, err)
			}

			var parsedPrevResponse consensus.StandardValidatorParticipationResponse
			if err := json.Unmarshal(prevResp, &parsedPrevResponse); err != nil {
				return nil, fmt.Errorf("error parsing validator participation data for prevEpoch %v: %w", epoch, err)
			}
			prevEpochActiveGwei = parsedPrevResponse.Data.CurrentEpochActiveGwei
		}

		res = &types.ValidatorParticipation{
			Epoch:                   epoch,
			GlobalParticipationRate: utils.SafeDivideFloat(parsedResponse.Data.PreviousEpochTargetAttestingGwei, prevEpochActiveGwei),
			VotedEther:              uint64(parsedResponse.Data.PreviousEpochTargetAttestingGwei),
			EligibleEther:           uint64(prevEpochActiveGwei),
			Finalized:               epoch <= head.FinalizedEpoch && head.JustifiedEpoch > 0,
		}
	} else {
		res = &types.ValidatorParticipation{
			Epoch:                   epoch,
			GlobalParticipationRate: utils.SafeDivideFloat(parsedResponse.Data.CurrentEpochTargetAttestingGwei, parsedResponse.Data.CurrentEpochActiveGwei),
			VotedEther:              uint64(parsedResponse.Data.CurrentEpochTargetAttestingGwei),
			EligibleEther:           uint64(parsedResponse.Data.CurrentEpochActiveGwei),
			Finalized:               epoch <= head.FinalizedEpoch && head.JustifiedEpoch > 0,
		}
	}
	return res, nil
}

// GetSyncCommittee fetches sync committee information for a given beacon state and epoch
// from a Teku beacon node via the standard REST API.
//
// Sync committees are part of Ethereum's consensus mechanism used in light client sync.
// This method queries the `/eth/v1/beacon/states/{state_id}/sync_committees` endpoint
// with a specific epoch as a query parameter.
//
// Parameters:
//   - stateID: The state identifier (usually a block root, block ID like "head", "finalized", etc.).
//   - epoch: The target epoch for which to fetch sync committee data.
//
// Returns:
//   - *StandardSyncCommitteeData: A pointer to a struct containing validators and their aggregates.
//   - error: An error if the request or decoding fails.
//
// Behavior:
//   - If the beacon node has pruned historical state (non-archival), the method gracefully handles
//     404 or similar errors by returning an empty sync committee object and no error.
//   - This ensures compatibility with non-archival nodes while allowing the caller to distinguish
//     between actual data and missing data due to pruning.
func (tc *TekuClient) GetSyncCommittee(stateID string, epoch uint64) (*consensus.StandardSyncCommitteeData, error) {
	syncCommitteesResp, err := tc.get(fmt.Sprintf("%s/eth/v1/beacon/states/%s/sync_committees?epoch=%d", tc.endpoint, stateID, epoch))
	if err != nil {
		// TODO: pruned node workaround
		if isPrunedError(err) {
			logger.Debugf("pruned mode: sync committee unavailable for epoch %s", epoch)
			return &consensus.StandardSyncCommitteeData{
				Validators:          []string{},
				ValidatorAggregates: [][]string{},
			}, nil
		}
		return nil, fmt.Errorf("error retrieving sync_committees for epoch %v (state: %v): %w", epoch, stateID, err)
	}

	var parsedSyncCommittees consensus.StandardSyncCommitteesResponse
	err = json.Unmarshal(syncCommitteesResp, &parsedSyncCommittees)
	if err != nil {
		return nil, fmt.Errorf("error parsing sync_committees data for epoch %v (state: %v): %w", epoch, stateID, err)
	}

	return &parsedSyncCommittees.Data, nil
}

// GetBlobSidecars retrieves the blob sidecars associated with the given state ID
// using the Teku REST API via the `/eth/v1/beacon/blob_sidecars/{state_id}` endpoint.
//
// Blob sidecars contain KZG commitments and actual blob data introduced in the
// Deneb upgrade (EIP-4844) for data availability sampling.
//
// The `stateID` can be a slot, block root, or one of the predefined identifiers:
//   - "head"
//   - "genesis"
//   - "finalized"
//
// Returns a parsed *consensus.StandardBlobSidecarsResponse or an error if retrieval or decoding fails.
func (tc *TekuClient) GetBlobSidecars(stateID string) (*consensus.StandardBlobSidecarsResponse, error) {
	url := fmt.Sprintf("%s/eth/v1/beacon/blob_sidecars/%s", tc.endpoint, stateID)
	res, err := tc.get(url)
	if err != nil {
		return nil, fmt.Errorf("error retrieving blob_sidecars for %v: %w", stateID, err)
	}
	var parsed consensus.StandardBlobSidecarsResponse
	if err := json.Unmarshal(res, &parsed); err != nil {
		return nil, fmt.Errorf("error parsing blob_sidecars for %v: %w", stateID, err)
	}
	return &parsed, nil
}

// get performs a GET request to the given URL using the Teku's shared HTTP client.
// It reads and returns the response body on success (HTTP 200), and returns semantic errors
// for known edge cases including:
//
//   - HTTP 404: treated as errNotFound (commonly returned by pruned nodes)
//   - HTTP 503: treated as errUnavailable (may also indicate pruned or syncing state)
//   - Other non-200 codes result in a formatted error with response body snippet
//
// Parameters:
//   - url: full URL to call (should include base endpoint)
//
// Returns:
//   - []byte: raw response body, if the status is 200 OK
//   - error: semantic error (e.g., errNotFound, errUnavailable) or wrapped response error
func (tc *TekuClient) get(url string) ([]byte, error) {
	resp, err := tc.client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("failed to GET %s: %w", url, err)
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read body from %s: %w", url, err)
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return data, nil
	case http.StatusNotFound:
		return nil, errNotFound
	case http.StatusServiceUnavailable:
		return nil, errUnavailable
	default:
		const maxSnippetLen = 1024
		snippet := data
		if len(snippet) > maxSnippetLen {
			snippet = append(snippet[:maxSnippetLen], []byte("... [truncated]")...)
		}
		return nil, fmt.Errorf("unexpected status %d from %s: %s", resp.StatusCode, url, string(snippet))
	}
}

// enrichBlockWithEpochData fills in validator state and epoch assignments.
func (tc *TekuClient) enrichBlockWithEpochData(block *types.Block, epoch uint64) {
	assignments, err := tc.GetEpochAssignments(epoch)
	if err != nil {
		logger.Warnf("slot %d: assignments unavailable: %v", block.Slot, err)
	} else {
		block.EpochAssignments = assignments
	}

	validators, err := tc.GetValidatorState(epoch)
	if err != nil {
		logger.Warnf("slot %d: validators unavailable: %v", block.Slot, err)
		return
	}

	block.Validators = make([]*types.Validator, 0, len(validators.Data))
	for _, v := range validators.Data {
		block.Validators = append(block.Validators, &types.Validator{
			Index:                      uint64(v.Index),
			PublicKey:                  utils.MustParseHex(v.Validator.Pubkey),
			WithdrawalCredentials:      utils.MustParseHex(v.Validator.WithdrawalCredentials),
			Balance:                    uint64(v.Balance),
			EffectiveBalance:           uint64(v.Validator.EffectiveBalance),
			Slashed:                    v.Validator.Slashed,
			ActivationEligibilityEpoch: uint64(v.Validator.ActivationEligibilityEpoch),
			ActivationEpoch:            uint64(v.Validator.ActivationEpoch),
			ExitEpoch:                  uint64(v.Validator.ExitEpoch),
			WithdrawableEpoch:          uint64(v.Validator.WithdrawableEpoch),
			Status:                     v.Status,
		})
	}
}

func (tc *TekuClient) buildMissedSlotBlock(slot, epoch uint64, enrich bool) (*types.Block, error) {
	proposer := int64(-1)
	if proposerResp, err := tc.GetEpochProposerAssignments(epoch); err == nil {
		for _, pa := range proposerResp.Data {
			if uint64(pa.Slot) == slot {
				proposer = int64(pa.ValidatorIndex)
				break
			}
		}
	} else {
		logger.Warnf("slot %d: failed to get proposer assignments: %v", slot, err)
	}

	block := buildEmptyBlock(slot, proposer)

	if enrich {
		tc.enrichBlockWithEpochData(block, epoch)
	}

	return block, nil
}

func (tc *TekuClient) validateBlobSidecars(
	block *types.Block,
	parsedBlock *consensus.AnySignedBlock,
) error {
	commitments := parsedBlock.Message.Body.BlobKZGCommitments
	if len(commitments) == 0 {
		return nil
	}

	res, err := tc.GetBlobSidecars(fmt.Sprintf("%#x", block.BlockRoot))
	if err != nil {
		return err
	}
	if len(res.Data) != len(commitments) {
		return fmt.Errorf("len(blob_sidecars) != len(blob_kzg_commitments): %v != %v", len(res.Data), len(commitments))
	}
	for i, d := range res.Data {
		if !bytes.Equal(d.KzgCommitment, commitments[i]) {
			return fmt.Errorf("mismatched KZG commitment at index %d: %#x != %#x", i, d.KzgCommitment, commitments[i])
		}
		block.BlobKZGCommitments[i] = commitments[i]
		block.BlobKZGProofs[i] = d.KzgProof
	}
	return nil
}

func (tc *TekuClient) buildSyncAggregate(
	block *types.Block,
	parsedBlock *consensus.AnySignedBlock,
	assignments *types.EpochAssignments,
) error {
	agg := parsedBlock.Message.Body.SyncAggregate
	if agg == nil {
		return nil // phase0 or altair block
	}

	bits := utils.MustParseHex(agg.SyncCommitteeBits)
	expectedSize := int(utils.Config.Chain.ClConfig.SyncCommitteeSize)
	if len(bits)*8 != expectedSize {
		return fmt.Errorf("sync-aggregate bit size mismatch: %d != %d", len(bits)*8, expectedSize)
	}

	block.SyncAggregate = &types.SyncAggregate{
		SyncCommitteeValidators:    assignments.SyncAssignments,
		SyncCommitteeBits:          bits,
		SyncAggregateParticipation: syncCommitteeParticipation(bits, expectedSize),
		SyncCommitteeSignature:     utils.MustParseHex(agg.SyncCommitteeSignature),
	}

	for i, valIndex := range assignments.SyncAssignments {
		if i >= len(bits)*8 {
			break
		}
		block.SyncDuties[types.ValidatorIndex(valIndex)] = utils.BitAtVector(bits, i)
	}
	return nil
}

func (tc *TekuClient) buildExecutionPayload(
	block *types.Block,
	parsedBlock *consensus.AnySignedBlock,
) error {
	payload := parsedBlock.Message.Body.ExecutionPayload
	if payload == nil || bytes.Equal(payload.ParentHash, make([]byte, 32)) {
		return nil // no payload in early forks
	}

	txs := make([]*types.Transaction, 0, len(payload.Transactions))
	for i, rawTx := range payload.Transactions {
		tx := &types.Transaction{Raw: rawTx}
		var decTx gtypes.Transaction
		if err := decTx.UnmarshalBinary(rawTx); err != nil {
			return fmt.Errorf("error parsing tx %d: %w", i, err)
		}
		h := decTx.Hash()
		tx.TxHash = h[:]
		tx.AccountNonce = decTx.Nonce()
		tx.Price = decTx.GasPrice().Bytes()
		tx.GasLimit = decTx.Gas()
		sender, err := tc.signer.Sender(&decTx)
		if err != nil {
			return fmt.Errorf("invalid sender for tx %d: %w", i, err)
		}
		tx.Sender = sender.Bytes()
		if v := decTx.To(); v != nil {
			tx.Recipient = v.Bytes()
		}
		tx.Amount = decTx.Value().Bytes()
		tx.Payload = decTx.Data()
		tx.MaxPriorityFeePerGas = decTx.GasTipCap().Uint64()
		tx.MaxFeePerGas = decTx.GasFeeCap().Uint64()
		if decTx.BlobGasFeeCap() != nil {
			tx.MaxFeePerBlobGas = decTx.BlobGasFeeCap().Uint64()
		}
		for _, h := range decTx.BlobHashes() {
			tx.BlobVersionedHashes = append(tx.BlobVersionedHashes, h.Bytes())
		}
		txs = append(txs, tx)
	}

	withdrawals := make([]*types.Withdrawals, len(payload.Withdrawals))
	for i, w := range payload.Withdrawals {
		withdrawals[i] = &types.Withdrawals{
			Index:          int64(w.Index),
			ValidatorIndex: uint64(w.ValidatorIndex),
			Address:        w.Address,
			Amount:         uint64(w.Amount),
		}
	}

	block.ExecutionPayload = &types.ExecutionPayload{
		ParentHash:    payload.ParentHash,
		FeeRecipient:  payload.FeeRecipient,
		StateRoot:     payload.StateRoot,
		ReceiptsRoot:  payload.ReceiptsRoot,
		LogsBloom:     payload.LogsBloom,
		Random:        payload.PrevRandao,
		BlockNumber:   uint64(payload.BlockNumber),
		GasLimit:      uint64(payload.GasLimit),
		GasUsed:       uint64(payload.GasUsed),
		Timestamp:     uint64(payload.Timestamp),
		ExtraData:     payload.ExtraData,
		BaseFeePerGas: uint64(payload.BaseFeePerGas),
		BlockHash:     payload.BlockHash,
		Transactions:  txs,
		Withdrawals:   withdrawals,
		BlobGasUsed:   uint64(payload.BlobGasUsed),
		ExcessBlobGas: uint64(payload.ExcessBlobGas),
	}

	return nil
}
