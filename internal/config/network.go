package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// NOTE: These global variables are loaded once via RegisterGlobalChainParams().
// Do not modify them at runtime.
var (
	NetworkName                       string
	
	MinGenesisActiveValidatorCount    uint64
	MinGenesisTime                    uint64
	GenesisForkVersion                string
	GenesisDelay                      uint64
	
	AltairForkVersion                 string
	AltairForkEpoch                   uint64
	BellatrixForkVersion              string
	BellatrixForkEpoch                uint64
	TerminalTotalDifficulty           uint64
	TerminalBlockHash                 string
	TerminalBlockHashActivationEpoch  uint64
	CapellaForkVersion                string
	CapellaForkEpoch                  uint64
	DenebForkVersion                  string
	DenebForkEpoch                    uint64

	ProposerScoreBoost 		  uint64

	SecondsPerSlot                    uint64
	SecondsPerEth1Block               uint64
	MinValidatorWithdrawabilityDelay  uint64
	ShardCommitteePeriod              uint64
	Eth1FollowDistance                uint64
	MinAttestationInclusionDelay      uint64
	SlotsPerEpoch                     uint64
	MinSeedLookahead                  uint64
	MaxSeedLookahead                  uint64
	EpochsPerEth1VotingPeriod         uint64
	SlotsPerHistoricalRoot            uint64
	MinEpochsToInactivityPenalty      uint64

	EjectionBalance                   uint64
	MinPerEpochChurnLimit             uint64
	ChurnLimitQuotient                uint64
	InactivityScoreBias               uint64
	InactivityScoreRecoveryRate       uint64
	MaxPerEpochActivationChurnLimit   uint64

	DepositChainID                    uint64
	DepositNetworkID                  uint64
	DepositContractAddress            string

	SyncCommitteeSize                 uint64
	EpochsPerSyncCommitteePeriod      uint64

	GossipMaxSize                     uint64
	MaxRequestBlocks                  uint64
	EpochsPerSubnetSubscription       uint64
	MinEpochsForBlockRequest          uint64
	MaxChunkSize                      uint64
	TTFBTimeout                       uint64
	RespTimeout                       uint64
	AttestationPropagationSlotRange   uint64
	MaximumGossipClockDisparity       uint64
	MessageDomainInvalidSnappy        string
	MessageDomainValidSnappy          string
	SubnetsPerNode                    uint64
	AttestationSubnetCount            uint64
        AttestationSubnetExtraBits        uint64
	AttestationSubnetPrefixBits       uint64

	MaxRequestBlocksDeneb             uint64
	MaxRequestBlobSidecars            uint64
	MinEpochsForBlobSidecarsRequests  uint64
	BlobSidecarSubnetCount            uint64
	MaxBlobsPerBlock                  uint64
	MaxWithdrawalsPerPayload          uint64
)

func RegisterGlobalChainParams(params *NetworkConfig) {
	NetworkName = params.ConfigName

	MinGenesisActiveValidatorCount = params.Genesis.MinGenesisActiveValidatorCount
	MinGenesisTime = params.Genesis.MinGenesisTime
	GenesisForkVersion = params.Genesis.GenesisForkVersion
	GenesisDelay = params.Genesis.GenesisDelay

	AltairForkVersion = params.Forks.AltairForkVersion
	AltairForkEpoch = params.Forks.AltairForkEpoch
	BellatrixForkVersion = params.Forks.BellatrixForkVersion
	BellatrixForkEpoch = params.Forks.BellatrixForkEpoch
	TerminalTotalDifficulty = params.Forks.TerminalTotalDifficulty
	TerminalBlockHash = params.Forks.TerminalBlockHash
	TerminalBlockHashActivationEpoch = params.Forks.TerminalBlockHashActivationEpoch
	CapellaForkVersion = params.Forks.CapellaForkVersion
	CapellaForkEpoch = params.Forks.CapellaForkEpoch
	DenebForkVersion = params.Forks.DenebForkVersion
	DenebForkEpoch = params.Forks.DenebForkEpoch

	ProposerScoreBoost = params.ForkChoise.ProposerScoreBoost

	SecondsPerSlot = params.Time.SecondsPerSlot
	SecondsPerEth1Block = params.Time.SecondsPerEth1Block
	MinValidatorWithdrawabilityDelay = params.Time.MinValidatorWithdrawabilityDelay
	ShardCommitteePeriod = params.Time.ShardCommitteePeriod
	Eth1FollowDistance = params.Time.Eth1FollowDistance
	MinAttestationInclusionDelay = params.Time.MinAttestationInclusionDelay
	SlotsPerEpoch = params.Time.SlotsPerEpoch
	MinSeedLookahead = params.Time.MinSeedLookahead
	MaxSeedLookahead = params.Time.MaxSeedLookahead
	EpochsPerEth1VotingPeriod = params.Time.EpochsPerEth1VotingPeriod
	SlotsPerHistoricalRoot = params.Time.SlotsPerHistoricalRoot
	MinEpochsToInactivityPenalty = params.Time.MinEpochsToInactivityPenalty

	EjectionBalance = params.Validator.EjectionBalance
	MinPerEpochChurnLimit = params.Validator.MinPerEpochChurnLimit
	ChurnLimitQuotient = params.Validator.ChurnLimitQuotient
	InactivityScoreBias = params.Validator.InactivityScoreBias
	InactivityScoreRecoveryRate = params.Validator.InactivityScoreRecoveryRate
	MaxPerEpochActivationChurnLimit = params.Validator.MaxPerEpochActivationChurnLimit

	DepositChainID = params.Deposit.DepositChainID
	DepositNetworkID = params.Deposit.DepositNetworkID
	DepositContractAddress = params.Deposit.DepositContractAddress

	SyncCommitteeSize = params.SyncCommittee.SyncCommitteeSize
	EpochsPerSyncCommitteePeriod = params.SyncCommittee.EpochsPerSyncCommitteePeriod

	GossipMaxSize = params.Networking.GossipMaxSize
	MaxRequestBlocks = params.Networking.MaxRequestBlocks
	EpochsPerSubnetSubscription = params.Networking.EpochsPerSubnetSubscription
	MinEpochsForBlockRequest = params.Networking.MinEpochsForBlockRequest
	MaxChunkSize = params.Networking.MaxChunkSize
	TTFBTimeout = params.Networking.TTFBTimeout
	RespTimeout = params.Networking.RespTimeout
	AttestationPropagationSlotRange = params.Networking.AttestationPropagationSlotRange
	MaximumGossipClockDisparity = params.Networking.MaximumGossipClockDisparity
	MessageDomainInvalidSnappy = params.Networking.MessageDomainInvalidSnappy
	MessageDomainValidSnappy = params.Networking.MessageDomainValidSnappy
	SubnetsPerNode = params.Networking.SubnetsPerNode
	AttestationSubnetCount = params.Networking.AttestationSubnetCount
        AttestationSubnetExtraBits = params.Networking.AttestationSubnetExtraBits
	AttestationSubnetPrefixBits = params.Networking.AttestationSubnetPrefixBits

	MaxRequestBlocksDeneb = params.Deneb.MaxRequestBlocksDeneb
	MaxRequestBlobSidecars = params.Deneb.MaxRequestBlobSidecars
	MinEpochsForBlobSidecarsRequests = params.Deneb.MinEpochsForBlobSidecarsRequests
	BlobSidecarSubnetCount = params.Deneb.BlobSidecarSubnetCount
	MaxBlobsPerBlock = params.Deneb.MaxBlobsPerBlock
	MaxWithdrawalsPerPayload = params.Deneb.MaxWithdrawalsPerPayload
}

// NetworkConfig represents the full configuration for a Beacon Chain network,
// loaded from a YAML file with preset.
type NetworkConfig struct {
	PresetBase string `yaml:"PRESET_BASE"`
	ConfigName string `yaml:"CONFIG_NAME"`

	Genesis  GenesisConfig  `yaml:",inline"`
	Forks    ForksConfig    `yaml:",inline"`
	ForkChoise ForkChoiseConfig `yaml:",inline"`
	Time     TimeConfig     `yaml:",inline"`
	Deposit  DepositConfig  `yaml:",inline"`
	Deneb    DenebConfig    `yaml:",inline"`
	Networking NetworkingConfig `yaml:",inline"`
	Validator ValidatorConfig   `yaml:",inline"`
	SyncCommittee SyncCommitteeConfig `yaml:",inline"`
}

// GenesisConfig defines genesis-time settings.
type GenesisConfig struct {
	MinGenesisActiveValidatorCount uint64 `yaml:"MIN_GENESIS_ACTIVE_VALIDATOR_COUNT"`
	MinGenesisTime                 uint64 `yaml:"MIN_GENESIS_TIME"`
	GenesisForkVersion             string `yaml:"GENESIS_FORK_VERSION"`
	GenesisDelay                   uint64 `yaml:"GENESIS_DELAY"`
}

// ForksConfig contains versioning and fork epoch settings.
type ForksConfig struct {
	AltairForkVersion                 string `yaml:"ALTAIR_FORK_VERSION"`
	AltairForkEpoch                   uint64 `yaml:"ALTAIR_FORK_EPOCH"`
	BellatrixForkVersion              string `yaml:"BELLATRIX_FORK_VERSION"`
	BellatrixForkEpoch                uint64 `yaml:"BELLATRIX_FORK_EPOCH"`
	TerminalTotalDifficulty           uint64 `yaml:"TERMINAL_TOTAL_DIFFICULTY"`
	TerminalBlockHash                 string `yaml:"TERMINAL_BLOCK_HASH"`
	TerminalBlockHashActivationEpoch uint64 `yaml:"TERMINAL_BLOCK_HASH_ACTIVATION_EPOCH"`
	CapellaForkVersion                string `yaml:"CAPELLA_FORK_VERSION"`
	CapellaForkEpoch                  uint64 `yaml:"CAPELLA_FORK_EPOCH"`
	DenebForkVersion                  string `yaml:"DENEB_FORK_VERSION"`
	DenebForkEpoch                    uint64 `yaml:"DENEB_FORK_EPOCH"`
}

// TimeConfig holds slot and epoch timing values.
type TimeConfig struct {
	SecondsPerSlot                   uint64 `yaml:"SECONDS_PER_SLOT"`
	SecondsPerEth1Block              uint64 `yaml:"SECONDS_PER_ETH1_BLOCK"`
	MinValidatorWithdrawabilityDelay uint64 `yaml:"MIN_VALIDATOR_WITHDRAWABILITY_DELAY"`
	ShardCommitteePeriod             uint64 `yaml:"SHARD_COMMITTEE_PERIOD"`
	Eth1FollowDistance               uint64 `yaml:"ETH1_FOLLOW_DISTANCE"`
	MinAttestationInclusionDelay     uint64 `yaml:"MIN_ATTESTATION_INCLUSION_DELAY"`
	SlotsPerEpoch                    uint64 `yaml:"SLOTS_PER_EPOCH"`
	MinSeedLookahead                 uint64 `yaml:"MIN_SEED_LOOKAHEAD"`
	MaxSeedLookahead                 uint64 `yaml:"MAX_SEED_LOOKAHEAD"`
	EpochsPerEth1VotingPeriod        uint64 `yaml:"EPOCHS_PER_ETH1_VOTING_PERIOD"`
	SlotsPerHistoricalRoot           uint64 `yaml:"SLOTS_PER_HISTORICAL_ROOT"`
	MinEpochsToInactivityPenalty     uint64 `yaml:"MIN_EPOCHS_TO_INACTIVITY_PENALTY"`
}

// ValidatorConfig includes validator churn and penalty settings.
type ValidatorConfig struct {
	EjectionBalance                 uint64 `yaml:"EJECTION_BALANCE"`
	MinPerEpochChurnLimit           uint64 `yaml:"MIN_PER_EPOCH_CHURN_LIMIT"`
	ChurnLimitQuotient              uint64 `yaml:"CHURN_LIMIT_QUOTIENT"`
	InactivityScoreBias             uint64 `yaml:"INACTIVITY_SCORE_BIAS"`
	InactivityScoreRecoveryRate     uint64 `yaml:"INACTIVITY_SCORE_RECOVERY_RATE"`
	MaxPerEpochActivationChurnLimit uint64 `yaml:"MAX_PER_EPOCH_ACTIVATION_CHURN_LIMIT"`
}

type ForkChoiseConfig struct {
	ProposerScoreBoost uint64 `yaml:"PROPOSER_SCORE_BOOST"`
}

// DepositConfig contains data related to the deposit contract.
type DepositConfig struct {
	DepositChainID         uint64 `yaml:"DEPOSIT_CHAIN_ID"`
	DepositNetworkID       uint64 `yaml:"DEPOSIT_NETWORK_ID"`
	DepositContractAddress string `yaml:"DEPOSIT_CONTRACT_ADDRESS"`
}

type SyncCommitteeConfig struct {
	SyncCommitteeSize            uint64 `yaml:"SYNC_COMMITTEE_SIZE"`
	EpochsPerSyncCommitteePeriod uint64 `yaml:"EPOCHS_PER_SYNC_COMMITTEE_PERIOD"`
}

// NetworkingConfig defines p2p-related networking constraints.
type NetworkingConfig struct {
	GossipMaxSize                   uint64 `yaml:"GOSSIP_MAX_SIZE"`
	MaxRequestBlocks                uint64 `yaml:"MAX_REQUEST_BLOCKS"`
	EpochsPerSubnetSubscription     uint64 `yaml:"EPOCHS_PER_SUBNET_SUBSCRIPTION"`
	MinEpochsForBlockRequest        uint64 `yaml:"MIN_EPOCHS_FOR_BLOCK_REQUESTS"`
	MaxChunkSize                    uint64 `yaml:"MAX_CHUNK_SIZE"`
	TTFBTimeout                     uint64 `yaml:"TTFB_TIMEOUT"`
	RespTimeout                     uint64 `yaml:"RESP_TIMEOUT"`
	AttestationPropagationSlotRange uint64 `yaml:"ATTESTATION_PROPAGATION_SLOT_RANGE"`
	MaximumGossipClockDisparity     uint64 `yaml:"MAXIMUM_GOSSIP_CLOCK_DISPARITY"`
	MessageDomainInvalidSnappy      string `yaml:"MESSAGE_DOMAIN_INVALID_SNAPPY"`
	MessageDomainValidSnappy        string `yaml:"MESSAGE_DOMAIN_VALID_SNAPPY"`
	SubnetsPerNode                  uint64 `yaml:"SUBNETS_PER_NODE"`
	AttestationSubnetCount          uint64 `yaml:"ATTESTATION_SUBNET_COUNT"`
        AttestationSubnetExtraBits      uint64 `yaml:"ATTESTATION_SUBNET_EXTRA_BITS"`
	AttestationSubnetPrefixBits     uint64 `yaml:"ATTESTATION_SUBNET_PREFIX_BITS"`
}

// DenebConfig includes EIP-4844-related parameters.
type DenebConfig struct {
	MaxRequestBlocksDeneb             uint64 `yaml:"MAX_REQUEST_BLOCKS_DENEB"`
	MaxRequestBlobSidecars            uint64 `yaml:"MAX_REQUEST_BLOB_SIDECARS"`
	MinEpochsForBlobSidecarsRequests  uint64 `yaml:"MIN_EPOCHS_FOR_BLOB_SIDECARS_REQUESTS"`
	BlobSidecarSubnetCount            uint64 `yaml:"BLOB_SIDECAR_SUBNET_COUNT"`
	MaxBlobsPerBlock                  uint64 `yaml:"MAX_BLOBS_PER_BLOCK"`
	MaxWithdrawalsPerPayload          uint64 `yaml:"MAX_WITHDRAWALS_PER_PAYLOAD"`
}

// LoadNetworkConfig reads a YAML file and returns a parsed NetworkConfig struct.
func LoadNetworkConfig(path string) (*NetworkConfig, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("open network config: %w", err)
	}
	defer file.Close()

	var cfg NetworkConfig
	decoder := yaml.NewDecoder(file)
	if err := decoder.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("decode network config: %w", err)
	}
	return &cfg, nil
}
