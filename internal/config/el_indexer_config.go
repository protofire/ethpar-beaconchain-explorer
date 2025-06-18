package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Constants for trace modes
const (
	ParityTraceMode = "parity"
	GethTraceMode   = "geth"
	DualTraceMode   = "parity/geth"
)

// Eth1IndexerConfig holds all configuration for the Execution Layer Indexer
type Eth1IndexerConfig struct {
	ConfigPath  string `mapstructure:"config"`
	VersionFlag bool   `mapstructure:"version"`

	Execution struct {
		JsonRpc string `mapstructure:"jsonrpc"`
	} `mapstructure:"execution"`

	Consensus struct {
		ConfigName     string `mapstructure:"config"`
		DepositChainId uint64 `mapstructure:"deposit_chain_id"`
	} `mapstructure:"consensus"`

	Bigtable struct {
		Project  string `mapstructure:"project"`
		Instance string `mapstructure:"instance"`
		Emulated bool   `mapstructure:"emulated"`
	} `mapstructure:"bigtable"`

	Redis struct {
		Address string `mapstructure:"address"`
		Port    uint   `mapstructure:"port"`
	} `mapstructure:"redis"`

	Metrics struct {
		Enabled bool   `mapstructure:"enabled"`
		Address string `mapstructure:"address"`
	} `mapstructure:"metrics"`

	Pprof struct {
		Enabled bool   `mapstructure:"enabled"`
		Port    string `mapstructure:"port"`
	} `mapstructure:"pprof"`

	Block      int64 `mapstructure:"block"`
	ReorgDepth int   `mapstructure:"reorg_depth"`

	Blocks struct {
		Concurrency       int64 `mapstructure:"concurrency"`
		Start             int64 `mapstructure:"start"`
		End               int64 `mapstructure:"end"`
		Bulk              int64 `mapstructure:"bulk"`
		Offset            int64 `mapstructure:"offset"`
		CheckGaps         bool  `mapstructure:"check_gaps"`
		CheckGapsLookback int   `mapstructure:"check_gaps_lookback"`
	} `mapstructure:"blocks"`

	TraceMode string `mapstructure:"trace_mode"`

	Data struct {
		Concurrency       int64 `mapstructure:"concurrency"`
		Start             int64 `mapstructure:"start"`
		End               int64 `mapstructure:"end"`
		Bulk              int64 `mapstructure:"bulk"`
		Offset            int64 `mapstructure:"offset"`
		CheckGaps         bool  `mapstructure:"check_gaps"`
		CheckGapsLookback int   `mapstructure:"check_gaps_lookback"`
	} `mapstructure:"data"`

	BalanceUpdater struct {
		Enable    bool `mapstructure:"enable"`
		Full      bool `mapstructure:"full"`
		BatchSize int  `mapstructure:"batch_size"`
	} `mapstructure:"balance_updater"`

	TokenPrice struct {
		Export          bool          `mapstructure:"export"`
		ExportList      string        `mapstructure:"export_list"`
		ExportFrequency time.Duration `mapstructure:"export_frequency"`
	} `mapstructure:"token_price"`

	EnsUpdater struct {
		Enable    bool  `mapstructure:"enable"`
		BatchSize int64 `mapstructure:"batch_size"`
	} `mapstructure:"ens_updater"`
}

// LoadEth1IndexerConfig loads the configuration using flags, env, and config file with default fallbacks.
func LoadEth1IndexerConfig() (*Eth1IndexerConfig, error) {
	v := viper.New()
	flags := pflag.NewFlagSet("eth1indexer", pflag.ExitOnError)
	cfg := &Eth1IndexerConfig{}

	// Core Flags
	flags.String("config", "", "Path to config file")
	flags.Bool("version", false, "Print version and exit")
	flags.String("trace_mode", DualTraceMode, "Trace mode: parity, geth, or parity/geth")

	// Execution Layer
	flags.String("execution.jsonrpc", "", "Execution layer JSON-RPC endpoint")

	// Consensus Layer
	flags.String("consensus.config", "", "Consensus config name")
	flags.Uint64("consensus.deposit_chain_id", 1, "Consensus deposit chain ID")

	// Bigtable
	flags.String("bigtable.project", "", "Bigtable project ID")
	flags.String("bigtable.instance", "", "Bigtable instance ID")
	flags.Bool("bigtable.emulated", false, "Use BigTable emulator")

	// Redis
	flags.String("redis.address", "", "Redis address")
	flags.Uint("redis.port", 6379, "Redis port")

	// Metrics & Pprof
	flags.Bool("metrics.enabled", false, "Enable metrics")
	flags.String("metrics.address", "", "Metrics bind address")
	flags.Bool("pprof.enabled", false, "Enable pprof")
	flags.String("pprof.port", "", "Pprof bind port")

	// Block
	flags.Int64("block", 0, "Index a specific block")
	flags.Int("reorg_depth", 20, "Reorg lookback depth")

	// Blocks
	flags.Int64("blocks.concurrency", 30, "Block indexing concurrency")
	flags.Int64("blocks.start", 0, "Block start")
	flags.Int64("blocks.end", 0, "Block end")
	flags.Int64("blocks.bulk", 8000, "Block bulk size")
	flags.Int64("blocks.offset", 100, "Block offset")
	flags.Bool("blocks.check_gaps", false, "Check block gaps")
	flags.Int("blocks.check_gaps_lookback", 1000000, "Lookback for block gap checks")

	// Data
	flags.Int64("data.concurrency", 30, "Data indexing concurrency")
	flags.Int64("data.start", 0, "Data start")
	flags.Int64("data.end", 0, "Data end")
	flags.Int64("data.bulk", 8000, "Data bulk size")
	flags.Int64("data.offset", 1000, "Data offset")
	flags.Bool("data.check_gaps", false, "Check data gaps")
	flags.Int("data.check_gaps_lookback", 1000000, "Lookback for data gap checks")

	// Balance Updater
	flags.Bool("balance_updater.enable", false, "Enable balance updater")
	flags.Bool("balance_updater.full", false, "Enable full balance update")
	flags.Int("balance_updater.batch_size", 1000, "Balance update batch size")

	// Token Price
	flags.Bool("token_price.export", false, "Enable token price export")
	flags.String("token_price.export_list", "", "Path to token list")
	flags.Duration("token_price.export_frequency", time.Hour, "Export frequency")

	// ENS Updater
	flags.Bool("ens_updater.enable", false, "Enable ENS updater")
	flags.Int64("ens_updater.batch_size", 200, "ENS batch size")

	// Parse & Bind
	if err := flags.Parse(os.Args[1:]); err != nil {
		return nil, fmt.Errorf("flag parsing failed: %w", err)
	}
	if err := v.BindPFlags(flags); err != nil {
		return nil, fmt.Errorf("viper flag binding failed: %w", err)
	}
	v.SetEnvPrefix("ETH1INDEXER")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Defaults
	v.SetDefault("metrics.address", "0.0.0.0:9090")
	v.SetDefault("pprof.port", "6060")
	v.SetDefault("blocks.bulk", 8000)
	v.SetDefault("blocks.offset", 100)
	v.SetDefault("data.bulk", 8000)
	v.SetDefault("data.offset", 1000)
	v.SetDefault("reorg_depth", 20)
	v.SetDefault("trace_mode", DualTraceMode)
	v.SetDefault("balance_updater.batch_size", 1000)
	v.SetDefault("ens_updater.batch_size", 200)
	v.SetDefault("token_price.export_frequency", time.Hour)

	// Config File
	configFile := v.GetString("config")
	if configFile != "" {
		v.SetConfigFile(configFile)
		if err := v.ReadInConfig(); err != nil {
			return nil, fmt.Errorf("failed to read config file: %w", err)
		}
	}

	// Unmarshal
	if err := v.Unmarshal(cfg); err != nil {
		return nil, fmt.Errorf("failed to unmarshal config: %w", err)
	}

	cfg.ConfigPath = configFile
	cfg.VersionFlag = v.GetBool("version")

	// Validate
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}
	
	return cfg, nil
}

// TODO: implement validators
// Validate validates the configuration values.
func (cfg *Eth1IndexerConfig) Validate() error {
	validTraceModes := map[string]bool{
		ParityTraceMode: true,
		GethTraceMode:   true,
		DualTraceMode:   true,
	}
	if !validTraceModes[cfg.TraceMode] {
		return fmt.Errorf("invalid trace mode: %s (expected: %s, %s, %s)",
			cfg.TraceMode, ParityTraceMode, GethTraceMode, DualTraceMode)
	}

	if cfg.Blocks.Start != 0 && cfg.Blocks.Start >= cfg.Blocks.End {
		return fmt.Errorf("invalid block range: start (%d) must be less than end (%d)",
			cfg.Blocks.Start, cfg.Blocks.End)
	}
	if cfg.Data.Start != 0 && cfg.Data.Start >= cfg.Data.End {
		return fmt.Errorf("invalid data range: start (%d) must be less than end (%d)",
			cfg.Data.Start, cfg.Data.End)
	}

	if cfg.Blocks.Concurrency <= 0 {
		return fmt.Errorf("blocks concurrency must be > 0")
	}
	if cfg.Data.Concurrency <= 0 {
		return fmt.Errorf("data concurrency must be > 0")
	}
	if cfg.Blocks.Bulk <= 0 {
		return fmt.Errorf("blocks bulk size must be > 0")
	}
	if cfg.Data.Bulk <= 0 {
		return fmt.Errorf("data bulk size must be > 0")
	}
	if cfg.BalanceUpdater.BatchSize <= 0 {
		return fmt.Errorf("balance updater batch size must be > 0")
	}
	if cfg.Blocks.CheckGaps && cfg.Blocks.CheckGapsLookback <= 0 {
		return fmt.Errorf("block gaps lookback must be > 0")
	}
	if cfg.Data.CheckGaps && cfg.Data.CheckGapsLookback <= 0 {
		return fmt.Errorf("data gaps lookback must be > 0")
	}
	if cfg.EnsUpdater.Enable && cfg.EnsUpdater.BatchSize <= 0 {
		return fmt.Errorf("ENS batch size must be > 0")
	}
	if cfg.TokenPrice.Export && cfg.TokenPrice.ExportList == "" {
		return fmt.Errorf("token price export enabled but no export list provided")
	}
	return nil
}

func (cfg *Eth1IndexerConfig) IsSingleBlockMode() bool {
	return cfg.Block != 0
}

func (cfg *Eth1IndexerConfig) IsBlockRangeMode() bool {
	return cfg.Blocks.End != 0 && cfg.Blocks.Start < cfg.Blocks.End
}

func (cfg *Eth1IndexerConfig) IsDataRangeMode() bool {
	return cfg.Data.End != 0 && cfg.Data.Start < cfg.Data.End
}

func (cfg *Eth1IndexerConfig) IsGapCheckMode() bool {
	return cfg.Blocks.CheckGaps || cfg.Data.CheckGaps
}

func (cfg *Eth1IndexerConfig) IsFullBalanceUpdateMode() bool {
	return cfg.BalanceUpdater.Enable
}

func (cfg *Eth1IndexerConfig) IsContinuousMode() bool {
	return !cfg.IsSingleBlockMode() &&
		!cfg.IsBlockRangeMode() &&
		!cfg.IsDataRangeMode() &&
		!cfg.IsGapCheckMode() &&
		!cfg.IsFullBalanceUpdateMode()
}

func (cfg *Eth1IndexerConfig) GetBlockRange() (int64, int64) {
	if cfg.IsSingleBlockMode() {
		return cfg.Block, cfg.Block
	}
	if cfg.IsBlockRangeMode() {
		return cfg.Blocks.Start, cfg.Blocks.End
	}
	return 0, 0
}

func (cfg *Eth1IndexerConfig) GetDataRange() (int64, int64) {
	if cfg.IsDataRangeMode() {
		return cfg.Data.Start, cfg.Data.End
	}
	return 0, 0
}

func (cfg *Eth1IndexerConfig) GetGapCheckLookback() int {
	if cfg.Blocks.CheckGaps {
		return cfg.Blocks.CheckGapsLookback
	}
	if cfg.Data.CheckGaps {
		return cfg.Data.CheckGapsLookback
	}
	return 0
}

func (cfg *Eth1IndexerConfig) GetBalanceUpdaterPrefix() string {
	return fmt.Sprintf("%d:B:", cfg.Consensus.DepositChainId)
}

func (cfg *Eth1IndexerConfig) LogSummary() {
	fmt.Printf("Config loaded:\n")
	fmt.Printf("  Chain config:      %s\n", cfg.Consensus.ConfigName)
	fmt.Printf("  Trace mode:        %s\n", cfg.TraceMode)
	fmt.Printf("  Single block:      %v\n", cfg.IsSingleBlockMode())
	fmt.Printf("  Block range:       %d - %d\n", cfg.Blocks.Start, cfg.Blocks.End)
	fmt.Printf("  Data range:        %d - %d\n", cfg.Data.Start, cfg.Data.End)
	fmt.Printf("  Balance update:    %v (full: %v)\n", cfg.BalanceUpdater.Enable, cfg.BalanceUpdater.Full)
	fmt.Printf("  ENS update:        %v\n", cfg.EnsUpdater.Enable)
	fmt.Printf("  Token export:      %v\n", cfg.TokenPrice.Export)
}
