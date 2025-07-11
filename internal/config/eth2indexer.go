package config

import (
	"fmt"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// Eth2IndexerConfig is the root configuration object consumed by the consensus layer
// indexer. It can be populated from CLI flags, environment variables and/or
// YAML/TOML/JSON files via Viper, and then validated.
type Eth2IndexerConfig struct {
	Chain struct {
		Id   uint64 `mapstructure:"id" validate:"required,gte=1"`
		Name string `mapstructure:"name" validate:"required,oneof=ethpar"`
	} `mapstructure:"chain"`
	Execution ExecutionClientConfig `mapstructure:"execution"`
	Consensus ConsensusClientConfig `mapstructure:"consensus"`
	BigTable  BigTableConfig        `mapstructure:"bigtable" validate:"required"`
	Database  SqlDatabaseConfig     `mapstructure:"database" validate:"required"`
	Indexing struct {
		HistoricalPrice             bool `mapstructure:"historicalprice,omitempty"` 
		PubKeyTagsExporter          bool `mapstructure:"pubkeytagsexporter,omitempty"` 
		SyncCommitteesExporter      bool `mapstructure:"synccommitteesexporter,omitempty"` 
		SyncCommitteesCountExporter bool `mapstructure:"synccommitteescountexporter,omitempty"` 
		PendingQueueIndexer         bool `mapstructure:"pendingqueueindexer,omitempty"` 
		SsvExporter                 bool `mapstructure:"ssvexporter,omitempty"`
		RocketPoolExporter          bool `mapstructure:"rocketpoolexporter,omitempty"`
		MevBoostRelayExporter       bool `mapstructure:"mevboostrelayexporter,omitempty"`   
		EnsTransformer struct {
			ValidRegistrarContracts []string `mapstructure:"validRegistrarContracts,omitempty"`
		} `mapstructure:"ensTransformer,omitempty"`
	} `mapstructure:"indexing,omitempty"`
	Cache         CacheConfig   `mapstructure:"cache" validate:"required"`
	Metrics       MetricsConfig `mapstructure:"metrics,omitempty"`
	Pprof         PprofConfig   `mapstructure:"pprof,omitempty"`
	ReportStatus  bool          `mapstructure:"report,omitempty"`
	Config        string        `mapstructure:"config,omitempty" validate:"omitempty,file"`
	NetworkParams string        `mapstructure:"networkparams" validate:"required,file"`
	Version       bool          `mapstructure:"version,omitempty"`
}

// LoadEth2IndexerConfig parses CLI flags, environment variables and an
// optional config file, applies defaults, unmarshals the result into
// Eth2IndexerConfig, validates it and returns the populated struct.
func LoadEth2IndexerConfig(args []string) (*Eth2IndexerConfig, error) {
	v := viper.New()
	flags := pflag.NewFlagSet("eth2indexer", pflag.ExitOnError)
	cfg := &Eth2IndexerConfig{}

	// Parse CLI flags
	flags.Uint64("chain.id", 0, "Indexed chain ID")
	flags.String("chain.name", "", "Indexed blockchain name, can be 'ethpar'. Other chains have not been implemented yet")
	
	flags.String("execution.client", "", "Execution client, can be 'besu'. Other clients have not benn implemented yet")
	flags.String("execution.endpoint", "", "Execution client JSON-RPC enpoint")

	flags.String("consensus.client", "", "Consensus client, can be 'teku'. Other clients have not benn implemented yet")
	flags.String("consensus.endpoint", "", "Consensus client REST API enpoint")
	
	flags.String("bigtable.project", "emulated", "BigTable project")
	flags.String("bigtable.instance", "emulated", "BigTable instance")
	flags.Bool("bigtable.emulated", false, "Use BigTable emulator")
	flags.String("bigtable.emulatorhost", "localhost", "BigTable emulator address")
	flags.Uint16("bigtable.emulatorport", 8080, "BigTable emulator port")

	flags.String("database.host", "", "Postgres database host")
	flags.Uint16("database.port", 5432, "Postgres databse port")
	flags.String("database.dbname", "", "Database name")
	flags.String("database.username", "", "Database user with write permissions")
	flags.String("database.password", "", "Database user's password")
	flags.Int("database.maxopenconns", 50, "Maximum number of open connections to the database")
	flags.Int("database.maxidleconns", 10, "Maximum number of idle connections to the database")
	flags.Bool("database.ssl", false, "Database SSL mode")
	
	flags.Bool("indexing.historicalprice", false, "Enable Historical Price service")
	flags.Bool("indexing.pubkeytagsexporter", false, "Enable Public Keys Tags exporter")
	flags.Bool("indexing.synccommitteesexporter", false, "Enable Sync Committees exporter")
	flags.Bool("indexing.synccommitteescountexporter", false, "Enable Sync Committees Count exporter")
	flags.Bool("indexing.pendingqueueindexer", false, "Enable Pending Queue indexer")
	flags.Bool("indexing.ssvexporter", false, "Enable SSV exporter")
	flags.Bool("indexing.rocketpoolexporter", false, "Enable Rocket Pool exporter")
	flags.Bool("indexing.mevboostrelayexporter", false, "Enable MEV Boost Relay exporter")
	flags.StringSlice("indexing.enstransformer.validregistrarcontracts", []string{}, "Comma-separated list of registrar contract addresses")

	flags.String("cache.endpoint", "", "Cache service endpoint in the address:port format")

	flags.Bool("metrics.enabled", false, "Enable Prometheus metrics")
	flags.String("metrics.address", "localhost:9090", "Address to expose Prometheus metrics on")

	flags.Bool("pprof.enabled", false, "Enable profiling")
	flags.String("pprof.address", "localhost", "Address to expose profilig endpoints on")
	flags.Uint16("pprof.port", 6060, "Port to expose profiling endpoints on")

	flags.Bool("report", false, "Report service status")
	flags.String("config", "", "Path to the config file, if empty string defaults will be used")
	flags.String("networkparams", "", "Path to a file with Beacon Chain network parameters")
	flags.Bool("version", false, "Print version and exit")

	// Parse & Bind
	if err := flags.Parse(args); err != nil {
		return nil, fmt.Errorf("flag parsing failed: %w", err)
	}

	if err := v.BindPFlags(flags); err != nil {
		return nil, fmt.Errorf("viper flag binding failed: %w", err)
	}

	v.SetEnvPrefix("ETH2")
	v.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	v.AutomaticEnv()

	// Defaults
	v.SetDefault("bigtable.project", "emulated")
	v.SetDefault("bigtable.instance", "emulated")
	v.SetDefault("bigtable.emulated", false)
	v.SetDefault("bigtable.emulatorhost", "localhost")
	v.SetDefault("bigtable.emulatorport", 8080)
	v.SetDefault("database.port", 5432)
	v.SetDefault("database.maxopenconns", 50)
	v.SetDefault("database.maxidleconns", 10)
	v.SetDefault("database.ssl", false)
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
	validate := validator.New()
	if err := validate.Struct(cfg); err != nil {
		return nil, err
	}

	return cfg, nil
}