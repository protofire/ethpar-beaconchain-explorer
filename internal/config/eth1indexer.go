package config

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-playground/locales/en"
	ut "github.com/go-playground/universal-translator"
	"github.com/go-playground/validator/v10"
	enTranslations "github.com/go-playground/validator/v10/translations/en"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Trace-mode constants recognised by the indexer.
const (
	ParityTraceMode = "parity"
	GethTraceMode   = "geth"
	DualTraceMode   = "dual"
)

// Range groups block/data range parameters reused by several indexer modes.
type Range struct {
	Start        int64 `mapstructure:"start,omitempty" validate:"omitempty,gte=0"`
	End          int64 `mapstructure:"end,omitempty"   validate:"omitempty,gtefield=Start"`
	Bulk         int64 `mapstructure:"bulk,omitempty"  validate:"omitempty,gte=1"`
	Offset       int64 `mapstructure:"offset,omitempty" validate:"omitempty,gte=1"`
	Concurrency  int64 `mapstructure:"concurrency,omitempty" validate:"omitempty,gte=1"`
	CheckGaps    bool  `mapstructure:"checkgaps,omitempty"`
	GapsLoopback int   `mapstructure:"gapsloopback,omitempty" validate:"required_if=CheckGaps true,omitempty,gte=1"`
}

// Eth1IndexerConfig is the root configuration object consumed by the
// indexer. It can be populated from CLI flags, environment variables and/or
// YAML/TOML/JSON files via Viper, and then validated.
type Eth1IndexerConfig struct {
	JsonRpc struct {
		Client   string `mapstructure:"client" validate:"required,oneof=erigon geth"`
		Endpoint string `mapstructure:"endpoint" validate:"required,url"`
	} `mapstructure:"jsonrpc"`
	Chain struct {
		Id                       uint64 `mapstructure:"id" validate:"required,gte=1"`
		MaxWithdrawalsPerPayload uint64 `mapstructure:"maxwithdrawalsperpayload" validate:"required,gte=1"`
		// Currency 		 string `mapstructure:"currency"`
	} `mapstructure:"chain"`
	Indexing struct {
		Mode   string `mapstructure:"mode" validate:"required,oneof=single blockrange datarange live"`
		Block  int64  `mapstructure:"block,omitempty"`
		Blocks Range `mapstructure:"blocks,omitempty"`
		Data Range `mapstructure:"data,omitempty"`
		BalanceUpdater struct {
			Enabled bool `mapstructure:"enabled,omitempty"`
			Full    bool `mapstructure:"full,omitempty"`
			Batch   int  `mapstructure:"batch,omitempty" validate:"omitempty,gte=1"`
		} `mapstructure:"balanceupdater,omitempty"`
		TokenPriceExporter struct {
			Enabled   bool           `mapstructure:"enabled,omitempty"`
			List      string         `mapstructure:"list,omitempty" validate:"omitempty,file"`
			Frequency *time.Duration `mapstructure:"frequency,omitempty"`
		} `mapstructure:"tokenpriceexporter,omitempty"`
		EnsUpdater struct {
			Enabled bool  `mapstructure:"enabled,omitempty"`
			Batch   int64 `mapstructure:"batch,omitempty" validate:"omitempty,gte=1"`
		} `mapstructure:"ensupdater,omitempty"`
		ReorgDepth int    `mapstructure:"reorgdepth,omitempty" validate:"required_if=Mode live,omitempty,gte=1"`
		TraceMode  string `mapstructure:"tracemode" validate:"required,oneof=geth parity dual"`
	} `mapstructure:"indexing" validate:"required"`
	BigTable struct {
		Project      string `mapstructure:"project" validate:"required"`
		Instance     string `mapstructure:"instance" validate:"required"`
		Emulated     bool   `mapstructure:"emulated,omitempty"`
		EmulatorHost string `mapstructure:"emulatorhost,omitempty" validate:"required_if=Emulated true,omitempty,hostname|ip"`
		EmulatorPort uint16 `mapstructure:"emulatorport,omitempty" validate:"required_if=Emulated true,omitempty,gte=1,lte=65535"`
	} `mapstructure:"bigtable" validate:"required"`
	Cache struct {
		Endpoint string `mapstructure:"endpoint" validate:"required,hostname_port"`
	} `mapstructure:"cache" validate:"required"`
	Metrics struct {
		Enabled bool   `mapstructure:"enabled,omitempty"`
		Address string `mapstructure:"address,omitempty" validate:"required_if=Enabled true,omitempty,hostname_port"`
	} `mapstructure:"metrics,omitempty"`
	Pprof struct {
		Enabled bool   `mapstructure:"enabled,omitempty"`
		Address string `mapstructure:"address,omitempty" validate:"required_if=Enabled true,omitempty,hostname|ip"`
		Port    uint16 `mapstructure:"port,omitempty" validate:"required_if=Enabled true,omitempty,gte=1,lte=65535"`
	} `mapstructure:"pprof,omitempty"`
	ReportStatus bool `mapstructure:"report,omitempty"`
	Config       string `mapstructure:"config,omitempty" validate:"omitempty,file"`
	Version      bool   `mapstructure:"version,omitempty"`
}

var (
	validate   *validator.Validate
	trans      ut.Translator
)

func init() {
	// Set up validator.
	validate = validator.New()

	// Register struct-level checks once.
	validate.RegisterStructValidation(eth1IndexerStructValidation, Eth1IndexerConfig{})

	// Set up English translator (default/fallback).
	enLocale := en.New()
	uni      := ut.New(enLocale, enLocale)

	var ok bool
	trans, ok = uni.GetTranslator("en")
	if !ok {
		panic("could not load English translator")
	}
	if err := enTranslations.RegisterDefaultTranslations(validate, trans); err != nil {
		panic(err)
	}

	// Register custom-tag translations here if needed.
	registerCustomTranslations()
}

// registerCustomTranslations wires human-readable messages
// for tags the stock set does not cover (file, required_if_blockrange, …).
func registerCustomTranslations() {
	custom := map[string]string{
		"file":                    "'{0}' must be a readable file path",
		"required_if_blockrange":  "'{0}' is required when indexing.mode = blockrange",
		"required_if_datarange":   "'{0}' is required when indexing.mode = datarange",
	}
	for tag, msg := range custom {
		_ = validate.RegisterTranslation(tag, trans,
			func(ut ut.Translator) error { return ut.Add(tag, msg, true) },
			func(ut ut.Translator, fe validator.FieldError) string {
				t, _ := ut.T(tag, fe.Field())
				return t
			},
		)
	}
}

// LoadEth1IndexerConfig parses CLI flags, environment variables and an
// optional config file, applies defaults, unmarshals the result into
// Eth1IndexerConfig, validates it and returns the populated struct.
func LoadEth1IndexerConfig(args []string) (*Eth1IndexerConfig, error) {
	v := viper.New()
	flags := pflag.NewFlagSet("eth1indexer", pflag.ExitOnError)
	cfg := &Eth1IndexerConfig{}

	// Parse CLI flags
	flags.String("jsonrpc.client", "", "Execution client, can be 'erigon', 'geth'")
	flags.String("jsonrpc.endpoint", "", "Execution client JSON-RPC enpoint")

	flags.Uint64("chain.id", 0, "Indexed chain ID")
	flags.Uint64("chain.maxwithdrawalsperpayload", 0, "Max withdrawals per payload")
	
	flags.String("indexing.mode", "live", "Indexer mode, can be 'single', 'blockrange', 'datarange' or 'live'")

	flags.Int64("indexing.block", 0, "Index a specific block")

	flags.Int64("indexing.blocks.start", 0, "Block to start indexing")
	flags.Int64("indexing.blocks.end", 0, "Block to finish indexing")
	flags.Int64("indexing.blocks.bulk", 8000, "Maximum number of blocks to be processed before saving")
	flags.Int64("indexing.blocks.offset", 100, "Blocks offset")
	flags.Int64("indexing.blocks.concurrency", 30, "Concurrency to use when indexing blocks from erigon")
	flags.Bool("indexing.blocks.checkgaps", false, "Check for gaps in the blocks table")
	flags.Int("indexing.blocks.gapslookback", 1000000, "Lookback for gaps check of the blocks table")

	flags.Int64("indexing.data.start", 0, "Block to start indexing")
	flags.Int64("indexing.data.end", 0, "Block to finish indexing")
	flags.Int64("indexing.data.bulk", 8000, "Maximum number of blocks to be processed before saving")
	flags.Int64("indexing.data.offset", 1000, "Data offset")
	flags.Int64("indexing.data.concurrency", 30, "Concurrency to use when indexing data from bigtable")
	flags.Bool("indexing.data.checkgaps", false, "Check for gaps in the data table")
	flags.Int("indexing.data.gapslookback", 1000000, "Lookback for gaps check of the blocks table")

	flags.Bool("indexing.balanceupdater.enabled", false, "Enable balance update process")
	flags.Bool("indexing.balanceupdater.full", false, "Enable full balance update process")
	flags.Int("indexing.balanceupdater.batch", 1000, "Batch size for balance updates")

	flags.Bool("indexing.tokenpriceexporter.enabled", false, "Enable token export process")
	flags.String("indexing.tokenpriceexporter.list", "", "Tokenlist path to use for the token price export")
	flags.Duration("indexing.tokenpriceexporter.frequency", time.Hour, "Token price export interval")

	flags.Bool("indexing.ensupdater.enabled", false, "Enable ens update process")
	flags.Int64("indexing.ensupdater.batch", 200, "Batch size for ens updates")

	flags.Int("indexing.reorgdepth", 20, "Lookback to check and handle chain reorgs")
	flags.String("indexing.tracemode", DualTraceMode, "Trace mode to use, can be either 'parity', 'geth' or 'dual' for both")

	flags.String("bigtable.project", "emulated", "BigTable project")
	flags.String("bigtable.instance", "emulated", "BigTable instance")
	flags.Bool("bigtable.emulated", false, "Use BigTable emulator")
	flags.String("bigtable.emulatorhost", "localhost", "BigTable emulator address")
	flags.Uint16("bigtable.emulatorport", 8080, "BigTable emulator port")

	flags.String("cache.endpoint", "", "Cache service endpoint in the address:port format")

	flags.Bool("metrics.enabled", false, "Enable Prometheus metrics")
	flags.String("metrics.address", "localhost:9090", "Address to expose Prometheus metrics on")

	flags.Bool("pprof.enabled", false, "Enable profiling")
	flags.String("pprof.address", "localhost", "Address to expose profilig endpoints on")
	flags.Uint16("pprof.port", 6060, "Port to expose profiling endpoints on")

	flags.Bool("report", false, "Report service status")
	flags.String("config", "", "Path to the config file, if empty string defaults will be used")
	flags.Bool("version", false, "Print version and exit")

	// Parse & Bind
	if err := flags.Parse(args); err != nil {
		return nil, fmt.Errorf("flag parsing failed: %w", err)
	}

	if err := v.BindPFlags(flags); err != nil {
		return nil, fmt.Errorf("viper flag binding failed: %w", err)
	}

	v.SetEnvPrefix("ETH1")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Defaults
	v.SetDefault("indexing.mode", "live")
	v.SetDefault("indexing.block", 0)
	v.SetDefault("indexing.blocks.start", 0)
	v.SetDefault("indexing.blocks.end", 0)
	v.SetDefault("indexing.blocks.bulk", 8000)
	v.SetDefault("indexing.blocks.offset", 100)
	v.SetDefault("indexing.blocks.concurrency", 30)
	v.SetDefault("indexing.blocks.checkgaps", false)
	v.SetDefault("indexing.blocks.gapslookback", 1000000)
	v.SetDefault("indexing.data.start", 0)
	v.SetDefault("indexing.data.end", 0)
	v.SetDefault("indexing.data.bulk", 8000)
	v.SetDefault("indexing.data.offset", 1000)
	v.SetDefault("indexing.data.concurrency", 30)
	v.SetDefault("indexing.data.checkgaps", false)
	v.SetDefault("indexing.data.gapslookback", 1000000)
	v.SetDefault("indexing.balanceupdater.enabled", false)
	v.SetDefault("indexing.balanceupdater.full", false)
	v.SetDefault("indexing.balanceupdater.batch", 1000)
	v.SetDefault("indexing.tokenpriceexporter.enabled", false)
	v.SetDefault("indexing.tokenpriceexporter.frequency", time.Hour)
	v.SetDefault("indexing.ensupdater.enabled", false)
	v.SetDefault("indexing.ensupdater.batch", 200)
	v.SetDefault("indexing.reorgdepth", 20)
	v.SetDefault("indexing.tracemode", DualTraceMode)
	v.SetDefault("bigtable.project", "emulated")
	v.SetDefault("bigtable.instance", "emulated")
	v.SetDefault("bigtable.emulated", false)
	v.SetDefault("bigtable.emulatorhost", "localhost")
	v.SetDefault("bigtable.emulatorport", 8080)
	v.SetDefault("metrics.enabled", false)
	v.SetDefault("metrics.address", "localhost:9090")
	v.SetDefault("pprof.enabled", false)
	v.SetDefault("pprof.address", "localhost")
	v.SetDefault("pprof.port", 6060)
	v.SetDefault("version", false)

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

	// Validate
	if err := cfg.Validate(); err != nil {
		return nil, err
	}

	return cfg, nil
}

// Validate runs field-level and struct-level validation rules.
// It returns a validator.ValidationErrors error on failure, wrapped
// with a human-readable English message.
func (cfg *Eth1IndexerConfig) Validate() error {
	if err := validate.Struct(cfg); err != nil {
		// Collect translated messages.
		if verrs, ok := err.(validator.ValidationErrors); ok {
			var sb strings.Builder
			for _, fe := range verrs {
				sb.WriteString(fe.Translate(trans))
				sb.WriteByte('\n')
			}
			return fmt.Errorf("configuration invalid:\n%s", sb.String())
		}
		return fmt.Errorf("validation error: %w", err)
	}
	return nil
}

// eth1IndexerStructValidation adds cross-field rules for range modes.
func eth1IndexerStructValidation(sl validator.StructLevel) {
	cfg := sl.Current().Interface().(Eth1IndexerConfig)

	switch cfg.Indexing.Mode {
	case "blockrange":
		reportRangeSection(sl, "Indexing.Blocks",
			"required_if_blockrange", cfg.Indexing.Blocks)
	case "datarange":
		reportRangeSection(sl, "Indexing.Data",
			"required_if_datarange", cfg.Indexing.Data)
	}
}

// reportRangeSection emits errors for missing mandatory fields inside
// a Range section when the surrounding mode makes them required.
func reportRangeSection(sl validator.StructLevel, ns, tag string, s Range) {
	if s.Bulk == 0 {
		sl.ReportError(s.Bulk, ns+".Bulk", "Bulk", tag, "")
	}
	if s.Offset == 0 {
		sl.ReportError(s.Offset, ns+".Offset", "Offset", tag, "")
	}
	if s.Concurrency == 0 {
		sl.ReportError(s.Concurrency, ns+".Concurrency", "Concurrency", tag, "")
	}
	if s.CheckGaps && s.GapsLoopback == 0 {
		sl.ReportError(s.GapsLoopback, ns+".GapsLoopback", "GapsLoopback", tag, "")
	}
}