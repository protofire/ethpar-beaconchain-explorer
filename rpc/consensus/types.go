package consensus

import (
	"github.com/protofire/ethpar-beaconchain-explorer/codec"
)

type StandardBeaconHeaderResponse struct {
	Data      BeaconHeaderData `json:"data"`
	Finalized bool             `json:"finalized"`
}

type StandardBeaconHeadersResponse struct {
	Data      []BeaconHeaderData
	Finalized bool `json:"finalized"`
}

type BeaconHeaderData struct {
	Root   string `json:"root"`
	Header struct {
		Message struct {
			Slot          codec.Uint64Str `json:"slot"`
			ProposerIndex codec.Uint64Str `json:"proposer_index"`
			ParentRoot    string          `json:"parent_root"`
			StateRoot     string          `json:"state_root"`
			BodyRoot      string          `json:"body_root"`
		} `json:"message"`
		Signature string `json:"signature"`
	} `json:"header"`
}

type StandardBeaconPendingDepositsResponse struct {
	ExecutionOptimistic bool                        `json:"execution_optimistic"`
	Finalized           bool                        `json:"finalized"`
	Data                []BeaconPendingDepositsData `json:"data"`
}

type BeaconPendingDepositsData struct {
	Pubkey                codec.BytesHexStr `json:"pubkey"`
	WithdrawalCredentials codec.BytesHexStr `json:"withdrawal_credentials"`
	Amount                uint64            `json:"amount,string"`
	Signature             codec.BytesHexStr `json:"signature"`
	Slot                  uint64            `json:"slot,string"`
}

type StandardFinalityCheckpointsResponse struct {
	Data BeaconFinalityCheckpointData `json:"data"`
}

type BeaconFinalityCheckpointData struct {
	PreviousJustified struct {
		Epoch codec.Uint64Str `json:"epoch"`
		Root  string          `json:"root"`
	} `json:"previous_justified"`
	CurrentJustified struct {
		Epoch codec.Uint64Str `json:"epoch"`
		Root  string          `json:"root"`
	} `json:"current_justified"`
	Finalized struct {
		Epoch codec.Uint64Str `json:"epoch"`
		Root  string          `json:"root"`
	} `json:"finalized"`
}

type StreamedBlockEventData struct {
	Slot                codec.Uint64Str `json:"slot"`
	Block               string          `json:"block"`
	ExecutionOptimistic bool            `json:"execution_optimistic"`
}

type StandardProposerDutiesResponse struct {
	DependentRoot string                   `json:"dependent_root"`
	Data          []BeaconProposerDutyData `json:"data"`
}

type BeaconProposerDutyData struct {
	Pubkey         string          `json:"pubkey"`
	ValidatorIndex codec.Uint64Str `json:"validator_index"`
	Slot           codec.Uint64Str `json:"slot"`
}

type StandardCommitteeData struct {
	Index      codec.Uint64Str `json:"index"`
	Slot       codec.Uint64Str `json:"slot"`
	Validators []string        `json:"validators"`
}

type StandardCommitteesResponse struct {
	Data []StandardCommitteeData `json:"data"`
}

type StandardSyncCommitteeData struct {
	Validators          []string   `json:"validators"`
	ValidatorAggregates [][]string `json:"validator_aggregates"`
}

type StandardSyncCommitteesResponse struct {
	Data StandardSyncCommitteeData `json:"data"`
}

type StandardValidatorParticipationResponse struct {
	Data ValidatorParticipationData `json:"data"`
}

type ValidatorParticipationData struct {
	CurrentEpochActiveGwei           codec.Uint64Str `json:"current_epoch_active_gwei"`
	PreviousEpochActiveGwei          codec.Uint64Str `json:"previous_epoch_active_gwei"`
	CurrentEpochTargetAttestingGwei  codec.Uint64Str `json:"current_epoch_target_attesting_gwei"`
	PreviousEpochTargetAttestingGwei codec.Uint64Str `json:"previous_epoch_target_attesting_gwei"`
	PreviousEpochHeadAttestingGwei   codec.Uint64Str `json:"previous_epoch_head_attesting_gwei"`
}

type ProposerSlashing struct {
	SignedHeader1 struct {
		Message struct {
			Slot          codec.Uint64Str `json:"slot"`
			ProposerIndex codec.Uint64Str `json:"proposer_index"`
			ParentRoot    string          `json:"parent_root"`
			StateRoot     string          `json:"state_root"`
			BodyRoot      string          `json:"body_root"`
		} `json:"message"`
		Signature string `json:"signature"`
	} `json:"signed_header_1"`
	SignedHeader2 struct {
		Message struct {
			Slot          codec.Uint64Str `json:"slot"`
			ProposerIndex codec.Uint64Str `json:"proposer_index"`
			ParentRoot    string          `json:"parent_root"`
			StateRoot     string          `json:"state_root"`
			BodyRoot      string          `json:"body_root"`
		} `json:"message"`
		Signature string `json:"signature"`
	} `json:"signed_header_2"`
}

type AttesterSlashing struct {
	Attestation1 struct {
		AttestingIndices []codec.Uint64Str `json:"attesting_indices"`
		Signature        string            `json:"signature"`
		Data             struct {
			Slot            codec.Uint64Str `json:"slot"`
			Index           codec.Uint64Str `json:"index"`
			BeaconBlockRoot string          `json:"beacon_block_root"`
			Source          struct {
				Epoch codec.Uint64Str `json:"epoch"`
				Root  string          `json:"root"`
			} `json:"source"`
			Target struct {
				Epoch codec.Uint64Str `json:"epoch"`
				Root  string          `json:"root"`
			} `json:"target"`
		} `json:"data"`
	} `json:"attestation_1"`
	Attestation2 struct {
		AttestingIndices []codec.Uint64Str `json:"attesting_indices"`
		Signature        string            `json:"signature"`
		Data             struct {
			Slot            codec.Uint64Str `json:"slot"`
			Index           codec.Uint64Str `json:"index"`
			BeaconBlockRoot string          `json:"beacon_block_root"`
			Source          struct {
				Epoch codec.Uint64Str `json:"epoch"`
				Root  string          `json:"root"`
			} `json:"source"`
			Target struct {
				Epoch codec.Uint64Str `json:"epoch"`
				Root  string          `json:"root"`
			} `json:"target"`
		} `json:"data"`
	} `json:"attestation_2"`
}

type Attestation struct {
	AggregationBits string `json:"aggregation_bits"`
	Signature       string `json:"signature"`
	Data            struct {
		Slot            codec.Uint64Str `json:"slot"`
		Index           codec.Uint64Str `json:"index"`
		BeaconBlockRoot string          `json:"beacon_block_root"`
		Source          struct {
			Epoch codec.Uint64Str `json:"epoch"`
			Root  string          `json:"root"`
		} `json:"source"`
		Target struct {
			Epoch codec.Uint64Str `json:"epoch"`
			Root  string          `json:"root"`
		} `json:"target"`
	} `json:"data"`
}

type Deposit struct {
	Proof []string `json:"proof"`
	Data  struct {
		Pubkey                string          `json:"pubkey"`
		WithdrawalCredentials string          `json:"withdrawal_credentials"`
		Amount                codec.Uint64Str `json:"amount"`
		Signature             string          `json:"signature"`
	} `json:"data"`
}

type VoluntaryExit struct {
	Message struct {
		Epoch          codec.Uint64Str `json:"epoch"`
		ValidatorIndex codec.Uint64Str `json:"validator_index"`
	} `json:"message"`
	Signature string `json:"signature"`
}

type Eth1Data struct {
	DepositRoot  string          `json:"deposit_root"`
	DepositCount codec.Uint64Str `json:"deposit_count"`
	BlockHash    string          `json:"block_hash"`
}

type SyncAggregate struct {
	SyncCommitteeBits      string `json:"sync_committee_bits"`
	SyncCommitteeSignature string `json:"sync_committee_signature"`
}

// https://ethereum.github.io/beacon-APIs/#/Beacon/getBlockV2
// https://github.com/ethereum/consensus-specs/blob/v1.1.9/specs/bellatrix/beacon-chain.md#executionpayload
type ExecutionPayload struct {
	ParentHash    codec.BytesHexStr   `json:"parent_hash"`
	FeeRecipient  codec.BytesHexStr   `json:"fee_recipient"`
	StateRoot     codec.BytesHexStr   `json:"state_root"`
	ReceiptsRoot  codec.BytesHexStr   `json:"receipts_root"`
	LogsBloom     codec.BytesHexStr   `json:"logs_bloom"`
	PrevRandao    codec.BytesHexStr   `json:"prev_randao"`
	BlockNumber   codec.Uint64Str     `json:"block_number"`
	GasLimit      codec.Uint64Str     `json:"gas_limit"`
	GasUsed       codec.Uint64Str     `json:"gas_used"`
	Timestamp     codec.Uint64Str     `json:"timestamp"`
	ExtraData     codec.BytesHexStr   `json:"extra_data"`
	BaseFeePerGas codec.Uint64Str     `json:"base_fee_per_gas"`
	BlockHash     codec.BytesHexStr   `json:"block_hash"`
	Transactions  []codec.BytesHexStr `json:"transactions"`
	// present only after capella
	Withdrawals []WithdrawalPayload `json:"withdrawals"`
	// present only after deneb
	BlobGasUsed   codec.Uint64Str `json:"blob_gas_used"`
	ExcessBlobGas codec.Uint64Str `json:"excess_blob_gas"`
}

type WithdrawalPayload struct {
	Index          codec.Uint64Str   `json:"index"`
	ValidatorIndex codec.Uint64Str   `json:"validator_index"`
	Address        codec.BytesHexStr `json:"address"`
	Amount         codec.Uint64Str   `json:"amount"`
}

type SignedBLSToExecutionChange struct {
	Message struct {
		ValidatorIndex     codec.Uint64Str   `json:"validator_index"`
		FromBlsPubkey      codec.BytesHexStr `json:"from_bls_pubkey"`
		ToExecutionAddress codec.BytesHexStr `json:"to_execution_address"`
	} `json:"message"`
	Signature codec.BytesHexStr `json:"signature"`
}

type AnySignedBlock struct {
	Message struct {
		Slot          codec.Uint64Str `json:"slot"`
		ProposerIndex codec.Uint64Str `json:"proposer_index"`
		ParentRoot    string          `json:"parent_root"`
		StateRoot     string          `json:"state_root"`
		Body          struct {
			RandaoReveal      string             `json:"randao_reveal"`
			Eth1Data          Eth1Data           `json:"eth1_data"`
			Graffiti          string             `json:"graffiti"`
			ProposerSlashings []ProposerSlashing `json:"proposer_slashings"`
			AttesterSlashings []AttesterSlashing `json:"attester_slashings"`
			Attestations      []Attestation      `json:"attestations"`
			Deposits          []Deposit          `json:"deposits"`
			VoluntaryExits    []VoluntaryExit    `json:"voluntary_exits"`

			// not present in phase0 blocks
			SyncAggregate *SyncAggregate `json:"sync_aggregate,omitempty"`

			// not present in phase0/altair blocks
			ExecutionPayload *ExecutionPayload `json:"execution_payload"`

			// present only after capella
			SignedBLSToExecutionChange []*SignedBLSToExecutionChange `json:"bls_to_execution_changes"`

			// present only after deneb
			BlobKZGCommitments []codec.BytesHexStr `json:"blob_kzg_commitments"`
		} `json:"body"`
	} `json:"message"`
	Signature codec.BytesHexStr `json:"signature"`
}

type StandardV2BlockResponse struct {
	Version             string         `json:"version"`
	ExecutionOptimistic bool           `json:"execution_optimistic"`
	Finalized           bool           `json:"finalized"`
	Data                AnySignedBlock `json:"data"`
}

type StandardV1BlockRootResponse struct {
	Data struct {
		Root string `json:"root"`
	} `json:"data"`
}

type StandardValidatorEntry struct {
	Index     codec.Uint64Str `json:"index"`
	Balance   codec.Uint64Str `json:"balance"`
	Status    string          `json:"status"`
	Validator struct {
		Pubkey                     string          `json:"pubkey"`
		WithdrawalCredentials      string          `json:"withdrawal_credentials"`
		EffectiveBalance           codec.Uint64Str `json:"effective_balance"`
		Slashed                    bool            `json:"slashed"`
		ActivationEligibilityEpoch codec.Uint64Str `json:"activation_eligibility_epoch"`
		ActivationEpoch            codec.Uint64Str `json:"activation_epoch"`
		ExitEpoch                  codec.Uint64Str `json:"exit_epoch"`
		WithdrawableEpoch          codec.Uint64Str `json:"withdrawable_epoch"`
	} `json:"validator"`
}

type StandardValidatorsResponse struct {
	Data []StandardValidatorEntry `json:"data"`
}

type StandardSyncingResponse struct {
	Data struct {
		IsSyncing    bool            `json:"is_syncing"`
		HeadSlot     codec.Uint64Str `json:"head_slot"`
		SyncDistance codec.Uint64Str `json:"sync_distance"`
	} `json:"data"`
}

type StandardValidatorBalancesResponse struct {
	Data []struct {
		Index   codec.Uint64Str `json:"index"`
		Balance codec.Uint64Str `json:"balance"`
	} `json:"data"`
}

type StandardBlobSidecarsResponse struct {
	Data []struct {
		BlockRoot       codec.BytesHexStr `json:"block_root"`
		Index           codec.Uint64Str   `json:"index"`
		Slot            codec.Uint64Str   `json:"slot"`
		BlockParentRoot codec.BytesHexStr `json:"block_parent_root"`
		ProposerIndex   codec.Uint64Str   `json:"proposer_index"`
		KzgCommitment   codec.BytesHexStr `json:"kzg_commitment"`
		KzgProof        codec.BytesHexStr `json:"kzg_proof"`
		// Blob            string `json:"blob"`
	}
}
