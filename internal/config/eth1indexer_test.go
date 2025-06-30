package config

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// validBaseCfg returns a minimal, fully valid configuration that passes
// Validate(). Individual test-cases mutate a copy of this struct.
func validBaseCfg() *Eth1IndexerConfig {
	cfg := &Eth1IndexerConfig{}

	// JSON-RPC section
	cfg.JsonRpc.Client   = "erigon"
	cfg.JsonRpc.Endpoint = "localhost:8545"
	cfg.JsonRpc.ChainId  = 1

	// Indexing (default live mode)
	cfg.Indexing.Mode       = "live"
	cfg.Indexing.ReorgDepth = 10
	cfg.Indexing.TraceMode  = DualTraceMode

	// BigTable: no emulator
	cfg.BigTable.Project  = "p"
	cfg.BigTable.Instance = "i"
	cfg.BigTable.Emulated = false

	// Cache
	cfg.Cache.Endpoint = "redis:6379"

	// Features disabled by default
	cfg.Metrics.Enabled = false
	cfg.Pprof.Enabled   = false

	// Just to satisfy file-validator in demo
	cfg.Config = "eth1indexer_test.go"

	return cfg
}

/* ------------------------------------------------------------------ */
/* happy-path                                                          */
/* ------------------------------------------------------------------ */

func TestValidateOK(t *testing.T) {
	require.NoError(t, validBaseCfg().Validate())
}

/* ------------------------------------------------------------------ */
/* field-level rules                                                  */
/* ------------------------------------------------------------------ */

func TestValidateClientEnum(t *testing.T) {
	cfg := validBaseCfg()
	cfg.JsonRpc.Client = "bad"
	require.Error(t, cfg.Validate())
}

func TestValidateEndpointFormat(t *testing.T) {
	cfg := validBaseCfg()
	cfg.JsonRpc.Endpoint = "not_a_host"
	require.Error(t, cfg.Validate())
}

func TestValidateChainId(t *testing.T) {
	cfg := validBaseCfg()
	cfg.JsonRpc.ChainId = 0            // gte=1 should fail
	require.Error(t, cfg.Validate())

	cfg.JsonRpc.ChainId = 10_000       // valid
	require.NoError(t, cfg.Validate())
}

/* ------------------------------------------------------------------ */
/* mode-specific rules (struct-level)                                 */
/* ------------------------------------------------------------------ */

func TestValidateSingleModeNoBlockRequired(t *testing.T) {
	cfg := validBaseCfg()
	cfg.Indexing.Mode  = "single"
	// Block left at zero – no rule enforces it
	require.NoError(t, cfg.Validate())
}

func TestValidateBlockrangeRequiresBlocks(t *testing.T) {
	cfg := validBaseCfg()
	cfg.Indexing.Mode = "blockrange"
	require.Error(t, cfg.Validate()) // missing Bulk/Offset/Concurrency

	// Populate mandatory fields
	cfg.Indexing.Blocks = Range{
		Start:       10,
		End:         20,
		Bulk:        100,
		Offset:      10,
		Concurrency: 5,
	}
	require.NoError(t, cfg.Validate())
}

/* ------------------------------------------------------------------ */
/* feature toggles                                                    */
/* ------------------------------------------------------------------ */

func TestValidateBigtableEmulator(t *testing.T) {
	cfg := validBaseCfg()
	cfg.BigTable.Emulated = true
	require.Error(t, cfg.Validate()) // host/port missing

	cfg.BigTable.EmulatorHost = "127.0.0.1"
	cfg.BigTable.EmulatorPort = uint16(9000)
	require.NoError(t, cfg.Validate())
}

func TestValidatePprofEnabled(t *testing.T) {
	cfg := validBaseCfg()
	cfg.Pprof.Enabled = true
	require.Error(t, cfg.Validate()) // address/port missing

	cfg.Pprof.Address = "localhost"
	cfg.Pprof.Port    = 6060
	require.NoError(t, cfg.Validate())
}

func TestValidateMetricsEnabled(t *testing.T) {
	cfg := validBaseCfg()
	cfg.Metrics.Enabled = true
	require.Error(t, cfg.Validate()) // address missing

	cfg.Metrics.Address = "0.0.0.0:9090"
	require.NoError(t, cfg.Validate())
}

/* ------------------------------------------------------------------ */
/* token-price exporter frequency (pointer duration demo)             */
/* ------------------------------------------------------------------ */

func TestValidateTokenPriceExporterFrequency(t *testing.T) {
	cfg := validBaseCfg()
	dur := 30 * time.Second
	cfg.Indexing.TokenPriceExporter.Enabled   = true
	cfg.Indexing.TokenPriceExporter.Frequency = &dur
	require.NoError(t, cfg.Validate())
}
