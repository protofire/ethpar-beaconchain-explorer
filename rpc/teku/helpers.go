package teku

import (
	"errors"

	"github.com/protofire/ethpar-beaconchain-explorer/codec"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	"github.com/prysmaticlabs/go-bitfield"
	"github.com/sirupsen/logrus"
)

// errNotFound indicates that the requested resource was not found (HTTP 404).
// This often occurs when querying a pruned node for historical state or finality data.
var errNotFound = errors.New("resource not found (404)")

// errUnavailable indicates the service is temporarily unavailable (HTTP 503).
// Teku may return this when data is pruned, not yet available, or the node is syncing.
var errUnavailable = errors.New("service unavailable or pruned data (503)")

var logger = logrus.New().WithField("module", "rpc")

// TekuLatestHeadEpoch is used to cache the latest head epoch for participation requests
var TekuLatestHeadEpoch uint64 = 0

func uint64List(li []codec.Uint64Str) []uint64 {
	out := make([]uint64, len(li))
	for i, v := range li {
		out[i] = uint64(v)
	}
	return out
}

// syncCommitteeParticipation calculates the participation rate of the sync committee
// based on the provided bitfield.
//
// The bitfield `bits` represents the committee participation bitmap, where each bit
// at position `i` indicates whether the `i`-th validator in the sync committee has
// participated in the current sync committee period.
//
// The function iterates through the configured sync committee size (as per
// `utils.Config.Chain.ClConfig.SyncCommitteeSize`), counts the number of set bits,
// and returns the fraction of participating validators as a float between 0.0 and 1.0.
//
// Parameters:
//   - bits: a byte slice representing the sync committee participation bitfield.
//
// Returns:
//   - A float64 value representing the participation ratio.
func syncCommitteeParticipation(bits []byte, committeeSize int) float64 {
	if committeeSize == 0 || len(bits)*8 < committeeSize {
		return 0.0
	}

	participating := 0
	for i := 0; i < committeeSize; i++ {
		if utils.BitAtVector(bits, i) {
			participating++
		}
	}
	return float64(participating) / float64(committeeSize)
}

// zeroFinalityFields constructs a *types.ChainHead with only the head slot and
// block root populated. All finality-related fields are set to their zero values.
//
// This is primarily used when interacting with pruned Beacon nodes that do not expose
// finality checkpoint data. It enables graceful degradation of chain head data
// without failing the request.
//
// Parameters:
//   - headSlot: the slot number of the head block.
//   - headRoot: the root hash of the head block (expected to be 32 bytes).
//
// Returns:
//   - A pointer to a types.ChainHead with finality fields zeroed out.
func zeroFinalityFields(headSlot uint64, headRoot []byte) *types.ChainHead {
	return &types.ChainHead{
		HeadSlot:      headSlot,
		HeadEpoch:     headSlot / utils.Config.Chain.ClConfig.SlotsPerEpoch,
		HeadBlockRoot: headRoot,

		FinalizedSlot:              0,
		FinalizedEpoch:             0,
		FinalizedBlockRoot:         make([]byte, 32),
		JustifiedSlot:              0,
		JustifiedEpoch:             0,
		JustifiedBlockRoot:         make([]byte, 32),
		PreviousJustifiedSlot:      0,
		PreviousJustifiedEpoch:     0,
		PreviousJustifiedBlockRoot: make([]byte, 32),
	}
}

// isPrunedError determines whether the given error represents a data unavailability
// condition caused by interacting with a pruned Consensus Layer (CL) node.
//
// It checks if the error is either:
//   - errUnavailable (HTTP 503), often returned when a Teku node has pruned
//     historical data or is still syncing.
//   - errNotFound (HTTP 404), returned when data is no longer available.
//
// The check also ensures that the client is explicitly running in "pruned" mode
// as configured in utils.Config.Indexer.Node.Mode.
//
// This function is used to gracefully handle partial or missing responses from
// Teku APIs without treating them as hard failures.
func isPrunedError(err error) bool {
	return (errors.Is(err, errUnavailable) || errors.Is(err, errNotFound)) &&
		utils.Config.Indexer.Node.Mode == "pruned"
}

// buildEmptyBlock creates a placeholder block for a missed slot.
func buildEmptyBlock(slot uint64, proposer int64) *types.Block {
	return &types.Block{
		Status:            0,
		Proposer:          proposer,
		BlockRoot:         make([]byte, 32),
		Slot:              slot,
		ParentRoot:        make([]byte, 32),
		StateRoot:         make([]byte, 32),
		Signature:         make([]byte, 96),
		RandaoReveal:      make([]byte, 96),
		Graffiti:          make([]byte, 32),
		BodyRoot:          make([]byte, 32),
		Eth1Data:          &types.Eth1Data{},
		ProposerSlashings: []*types.ProposerSlashing{},
		AttesterSlashings: []*types.AttesterSlashing{},
		Attestations:      []*types.Attestation{},
		Deposits:          []*types.Deposit{},
		VoluntaryExits:    []*types.VoluntaryExit{},
		SyncAggregate:     nil,
	}
}

// buildBlockSkeleton constructs the foundational structure of a types.Block from
// the consensus block header and body data.
//
// It populates the basic metadata (slot, proposer, roots, signatures, etc.), initializes
// empty slices and maps for all possible block operations (attestations, slashings, deposits, etc.),
// and embeds Eth1Data into the block.
//
// This function does not include payload validation (e.g., blobs or sync aggregates).
// It is typically called early in block parsing to ensure the block has a usable, initialized shape.
//
// Parameters:
//   - parsedBlock: The signed beacon block (with header and body) extracted from the consensus response.
//   - parsedHeaders: The consensus header response that includes the finalized status and block root.
//   - slot: The uint64 slot number extracted from the header.
//
// Returns:
//   - A pointer to a partially populated types.Block, ready for enrichment by later helpers.
func buildBlockSkeleton(
	parsedBlock *consensus.AnySignedBlock,
	parsedHeaders *consensus.StandardBeaconHeaderResponse,
	slot uint64,
) *types.Block {
	return &types.Block{
		Status:       1,
		Finalized:    parsedHeaders.Finalized,
		Proposer:     int64(parsedBlock.Message.ProposerIndex),
		BlockRoot:    utils.MustParseHex(parsedHeaders.Data.Root),
		Slot:         slot,
		ParentRoot:   utils.MustParseHex(parsedBlock.Message.ParentRoot),
		StateRoot:    utils.MustParseHex(parsedBlock.Message.StateRoot),
		Signature:    parsedBlock.Signature,
		RandaoReveal: utils.MustParseHex(parsedBlock.Message.Body.RandaoReveal),
		Graffiti:     utils.MustParseHex(parsedBlock.Message.Body.Graffiti),
		Eth1Data: &types.Eth1Data{
			DepositRoot:  utils.MustParseHex(parsedBlock.Message.Body.Eth1Data.DepositRoot),
			DepositCount: uint64(parsedBlock.Message.Body.Eth1Data.DepositCount),
			BlockHash:    utils.MustParseHex(parsedBlock.Message.Body.Eth1Data.BlockHash),
		},
		ProposerSlashings:          make([]*types.ProposerSlashing, len(parsedBlock.Message.Body.ProposerSlashings)),
		AttesterSlashings:          make([]*types.AttesterSlashing, len(parsedBlock.Message.Body.AttesterSlashings)),
		Attestations:               make([]*types.Attestation, len(parsedBlock.Message.Body.Attestations)),
		Deposits:                   make([]*types.Deposit, len(parsedBlock.Message.Body.Deposits)),
		VoluntaryExits:             make([]*types.VoluntaryExit, len(parsedBlock.Message.Body.VoluntaryExits)),
		SignedBLSToExecutionChange: make([]*types.SignedBLSToExecutionChange, len(parsedBlock.Message.Body.SignedBLSToExecutionChange)),
		BlobKZGCommitments:         make([][]byte, len(parsedBlock.Message.Body.BlobKZGCommitments)),
		BlobKZGProofs:              make([][]byte, len(parsedBlock.Message.Body.BlobKZGCommitments)),
		AttestationDuties:          make(map[types.ValidatorIndex][]types.Slot),
		SyncDuties:                 make(map[types.ValidatorIndex]bool),
	}
}

const maxInt32 = 2147483647

// sanitizeDepositCount ensures the Eth1 deposit count is within a valid range.
// Some older Teku nodes have been observed returning bogus int64-overflow values.
// If the count exceeds the maximum int32 value, it is reset to 0.
//
// Parameters:
//   - count: Raw deposit count from the block.
//
// Returns:
//   - Sanitized uint64 value, or 0 if the input exceeds maxInt32.
func sanitizeDepositCount(count uint64) uint64 {
	if count > maxInt32 {
		return 0 // ignore bogus value from some older Teku nodes
	}
	return count
}

// buildProposerSlashings populates the block's ProposerSlashings field from the parsed block data.
//
// It creates structured slashing evidence for each slashing, including both headers
// involved in the slashing proof.
//
// Parameters:
//   - block: Pointer to the target block being constructed.
//   - parsedBlock: Consensus block from which to extract proposer slashing data.
func buildProposerSlashings(
	block *types.Block,
	parsedBlock *consensus.AnySignedBlock,
) {
	for i, slashing := range parsedBlock.Message.Body.ProposerSlashings {
		block.ProposerSlashings[i] = &types.ProposerSlashing{
			ProposerIndex: uint64(slashing.SignedHeader1.Message.ProposerIndex),
			Header1: &types.Block{
				Slot:       uint64(slashing.SignedHeader1.Message.Slot),
				ParentRoot: utils.MustParseHex(slashing.SignedHeader1.Message.ParentRoot),
				StateRoot:  utils.MustParseHex(slashing.SignedHeader1.Message.StateRoot),
				Signature:  utils.MustParseHex(slashing.SignedHeader1.Signature),
				BodyRoot:   utils.MustParseHex(slashing.SignedHeader1.Message.BodyRoot),
			},
			Header2: &types.Block{
				Slot:       uint64(slashing.SignedHeader2.Message.Slot),
				ParentRoot: utils.MustParseHex(slashing.SignedHeader2.Message.ParentRoot),
				StateRoot:  utils.MustParseHex(slashing.SignedHeader2.Message.StateRoot),
				Signature:  utils.MustParseHex(slashing.SignedHeader2.Signature),
				BodyRoot:   utils.MustParseHex(slashing.SignedHeader2.Message.BodyRoot),
			},
		}
	}
}

// buildAttesterSlashings constructs attester slashing data and assigns it to the block.
//
// Each attester slashing includes two conflicting attestations (attestation_1 and attestation_2),
// each of which includes validator indices, committee info, and signature.
//
// Parameters:
//   - block: Target block receiving attester slashing evidence.
//   - parsedBlock: Consensus block containing raw attester slashing input.
func buildAttesterSlashings(
	block *types.Block,
	parsedBlock *consensus.AnySignedBlock,
) {
	for i, slashing := range parsedBlock.Message.Body.AttesterSlashings {
		block.AttesterSlashings[i] = &types.AttesterSlashing{
			Attestation1: &types.IndexedAttestation{
				Data: &types.AttestationData{
					Slot:            uint64(slashing.Attestation1.Data.Slot),
					CommitteeIndex:  uint64(slashing.Attestation1.Data.Index),
					BeaconBlockRoot: utils.MustParseHex(slashing.Attestation1.Data.BeaconBlockRoot),
					Source: &types.Checkpoint{
						Epoch: uint64(slashing.Attestation1.Data.Source.Epoch),
						Root:  utils.MustParseHex(slashing.Attestation1.Data.Source.Root),
					},
					Target: &types.Checkpoint{
						Epoch: uint64(slashing.Attestation1.Data.Target.Epoch),
						Root:  utils.MustParseHex(slashing.Attestation1.Data.Target.Root),
					},
				},
				Signature:        utils.MustParseHex(slashing.Attestation1.Signature),
				AttestingIndices: uint64List(slashing.Attestation1.AttestingIndices),
			},
			Attestation2: &types.IndexedAttestation{
				Data: &types.AttestationData{
					Slot:            uint64(slashing.Attestation2.Data.Slot),
					CommitteeIndex:  uint64(slashing.Attestation2.Data.Index),
					BeaconBlockRoot: utils.MustParseHex(slashing.Attestation2.Data.BeaconBlockRoot),
					Source: &types.Checkpoint{
						Epoch: uint64(slashing.Attestation2.Data.Source.Epoch),
						Root:  utils.MustParseHex(slashing.Attestation2.Data.Source.Root),
					},
					Target: &types.Checkpoint{
						Epoch: uint64(slashing.Attestation2.Data.Target.Epoch),
						Root:  utils.MustParseHex(slashing.Attestation2.Data.Target.Root),
					},
				},
				Signature:        utils.MustParseHex(slashing.Attestation2.Signature),
				AttestingIndices: uint64List(slashing.Attestation2.AttestingIndices),
			},
		}
	}
}

// buildAttestations processes the block's attestations and reconstructs validator duty mappings.
//
// It also cross-references the provided aggregation bits with epoch assignments to
// map committee positions to validator indices. Handles missing epoch assignment data gracefully.
//
// Parameters:
//   - tc: TekuClient instance (used for fetching epoch assignments).
//   - block: The target block object to populate.
//   - parsedBlock: Consensus block data containing attestation information.
func buildAttestations(
	tc *TekuClient,
	block *types.Block,
	parsedBlock *consensus.AnySignedBlock,
) {
	for i, att := range parsedBlock.Message.Body.Attestations {
		a := &types.Attestation{
			AggregationBits: utils.MustParseHex(att.AggregationBits),
			Attesters:       []uint64{},
			Data: &types.AttestationData{
				Slot:            uint64(att.Data.Slot),
				CommitteeIndex:  uint64(att.Data.Index),
				BeaconBlockRoot: utils.MustParseHex(att.Data.BeaconBlockRoot),
				Source: &types.Checkpoint{
					Epoch: uint64(att.Data.Source.Epoch),
					Root:  utils.MustParseHex(att.Data.Source.Root),
				},
				Target: &types.Checkpoint{
					Epoch: uint64(att.Data.Target.Epoch),
					Root:  utils.MustParseHex(att.Data.Target.Root),
				},
			},
			Signature: utils.MustParseHex(att.Signature),
		}

		assignments, err := tc.GetEpochAssignments(a.Data.Slot / utils.Config.Chain.ClConfig.SlotsPerEpoch)
		if err != nil || assignments == nil {
			logger.Warnf("missing epoch assignment for attestation at slot %d: %v", a.Data.Slot, err)
			block.Attestations[i] = a
			continue
		}

		aggBits := bitfield.Bitlist(a.AggregationBits)
		for j := uint64(0); j < aggBits.Len(); j++ {
			if !aggBits.BitAt(j) {
				continue
			}
			key := utils.FormatAttestorAssignmentKey(a.Data.Slot, a.Data.CommitteeIndex, j)
			valIdx, found := assignments.AttestorAssignments[key]
			if !found {
				logger.Errorf("unknown attestor: slot %d, committee %d, index %d", a.Data.Slot, a.Data.CommitteeIndex, j)
				valIdx = 0
			}
			a.Attesters = append(a.Attesters, valIdx)
			block.AttestationDuties[types.ValidatorIndex(valIdx)] = append(
				block.AttestationDuties[types.ValidatorIndex(valIdx)],
				types.Slot(a.Data.Slot),
			)
		}

		block.Attestations[i] = a
	}
}

// buildDeposits populates the block's deposit list from the consensus block.
//
// Each deposit includes public key, withdrawal credentials, signature, and amount.
//
// Parameters:
//   - block: Target block being constructed.
//   - parsedBlock: Consensus block from which to extract deposit data.
func buildDeposits(
	block *types.Block,
	parsedBlock *consensus.AnySignedBlock,
) {
	for i, d := range parsedBlock.Message.Body.Deposits {
		block.Deposits[i] = &types.Deposit{
			PublicKey:             utils.MustParseHex(d.Data.Pubkey),
			WithdrawalCredentials: utils.MustParseHex(d.Data.WithdrawalCredentials),
			Amount:                uint64(d.Data.Amount),
			Signature:             utils.MustParseHex(d.Data.Signature),
		}
	}
}

// buildVoluntaryExits extracts and assigns validator voluntary exit messages to the block.
//
// Each voluntary exit contains the epoch and validator index, along with the signature.
//
// Parameters:
//   - block: The target block to which exits will be assigned.
//   - parsedBlock: Source consensus block containing the exit data.
func buildVoluntaryExits(
	block *types.Block,
	parsedBlock *consensus.AnySignedBlock,
) {
	for i, v := range parsedBlock.Message.Body.VoluntaryExits {
		block.VoluntaryExits[i] = &types.VoluntaryExit{
			Epoch:          uint64(v.Message.Epoch),
			ValidatorIndex: uint64(v.Message.ValidatorIndex),
			Signature:      utils.MustParseHex(v.Signature),
		}
	}
}

// buildBLSChanges populates BLS-to-execution address change operations in the block.
//
// These changes are introduced in post-Merge forks and are optional, but critical
// for validator execution credential transitions.
//
// Parameters:
//   - block: Target block that holds validator change operations.
//   - parsedBlock: Consensus block containing the signed BLS changes.
func buildBLSChanges(
	block *types.Block,
	parsedBlock *consensus.AnySignedBlock,
) {
	for i, c := range parsedBlock.Message.Body.SignedBLSToExecutionChange {
		block.SignedBLSToExecutionChange[i] = &types.SignedBLSToExecutionChange{
			Message: types.BLSToExecutionChange{
				Validatorindex: uint64(c.Message.ValidatorIndex),
				BlsPubkey:      c.Message.FromBlsPubkey,
				Address:        c.Message.ToExecutionAddress,
			},
			Signature: c.Signature,
		}
	}
}
