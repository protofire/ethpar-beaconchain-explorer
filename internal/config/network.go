package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

// NetworkConfig represents the full configuration for a Beacon Chain network,
// loaded from a YAML file with preset.
type NetworkConfig struct {
	PresetBase string `yaml:"PRESET_BASE"`
	ConfigName string `yaml:"CONFIG_NAME"`

	Genesis  GenesisConfig  `yaml:",inline"`
	Forks    ForksConfig    `yaml:",inline"`
	Time     TimeConfig     `yaml:",inline"`
	Deposit  DepositConfig  `yaml:",inline"`
	Deneb    DenebConfig    `yaml:",inline"`
	Networking NetworkingConfig `yaml:",inline"`
	Validator ValidatorConfig   `yaml:",inline"`
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
	SlotsPerEpoch                    uint64 `yaml:"SLOTS_PER_EPOCH"`
	MinAttestationInclusionDelay     uint64 `yaml:"MIN_ATTESTATION_INCLUSION_DELAY"`
	EpochsPerEth1VotingPeriod        uint64 `yaml:"EPOCHS_PER_ETH1_VOTING_PERIOD"`
	SlotsPerHistoricalRoot           uint64 `yaml:"SLOTS_PER_HISTORICAL_ROOT"`
	MinEpochsToInactivityPenalty     uint64 `yaml:"MIN_EPOCHS_TO_INACTIVITY_PENALTY"`
}

// DepositConfig contains data related to the deposit contract.
type DepositConfig struct {
	DepositChainID         uint64 `yaml:"DEPOSIT_CHAIN_ID"`
	DepositNetworkID       uint64 `yaml:"DEPOSIT_NETWORK_ID"`
	DepositContractAddress string `yaml:"DEPOSIT_CONTRACT_ADDRESS"`
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

// NetworkingConfig defines p2p-related networking constraints.
type NetworkingConfig struct {
	GossipMaxSize                  uint64 `yaml:"GOSSIP_MAX_SIZE"`
	MaxRequestBlocks               uint64 `yaml:"MAX_REQUEST_BLOCKS"`
	EpochsPerSubnetSubscription    uint64 `yaml:"EPOCHS_PER_SUBNET_SUBSCRIPTION"`
	TTFBTimeout                    uint64 `yaml:"TTFB_TIMEOUT"`
	ResponseTimeout                uint64 `yaml:"RESP_TIMEOUT"`
	SubnetsPerNode                 uint64 `yaml:"SUBNETS_PER_NODE"`
	AttestationSubnetCount         uint64 `yaml:"ATTESTATION_SUBNET_COUNT"`
}

// ValidatorConfig includes validator churn and penalty settings.
type ValidatorConfig struct {
	EjectionBalance            uint64 `yaml:"EJECTION_BALANCE"`
	MinPerEpochChurnLimit      uint64 `yaml:"MIN_PER_EPOCH_CHURN_LIMIT"`
	ChurnLimitQuotient         uint64 `yaml:"CHURN_LIMIT_QUOTIENT"`
	InactivityScoreBias        uint64 `yaml:"INACTIVITY_SCORE_BIAS"`
	InactivityScoreRecoveryRate uint64 `yaml:"INACTIVITY_SCORE_RECOVERY_RATE"`
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
