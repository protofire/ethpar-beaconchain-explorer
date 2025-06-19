package services

import (
	"encoding/json"
	"os"
	"time"
	"sync"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
	"github.com/protofire/ethpar-beaconchain-explorer/version"
)

var lastStatusUpdate sync.Map // key: string, value: time.Time

// ReportStatus records the current status of a named service in the database,
// together with metadata such as the process ID, executable path, and build version.
//
// Reporting is rate-limited to at most one update per minute per service name to
// prevent excessive writes. If service-status reporting is disabled via
// `utils.Config.ReportServiceStatus`, the call returns immediately.
//
// This function is safe for concurrent use: it employs an internal `sync.Map`
// to track per-service throttling timestamps.
//
// Parameters:
//   - name:    Unique identifier for the service.
//   - status:  Human-readable status string.
//   - metadata: Optional JSON blob providing additional context.
//
// Any error encountered while writing to the database is logged via
// `utils.LogError` but does not cause a panic or return an error.
func ReportStatus(name, status string, metadata *json.RawMessage) {
	if !utils.Config.ReportServiceStatus {
		return
	}

	if val, ok := lastStatusUpdate.Load(name); ok {
		if t, ok := val.(time.Time); ok && time.Since(t) < time.Minute {
			return
		}
	}

	// Metadata capture
	pid := os.Getpid()
	execName, err := os.Executable()
	if err != nil {
		execName = "Unknown"
	}
	version := version.Version

	// Database write
	_, err = db.WriterDb.Exec(`
		INSERT INTO service_status (name, executable_name, version, pid, status, metadata, last_update) VALUES ($1, $2, $3, $4, $5, $6, NOW()) 
		ON CONFLICT (name, executable_name, version, pid) DO UPDATE SET
		status = excluded.status,
		metadata = excluded.metadata,
		last_update = excluded.last_update
	`, name, execName, version, pid, status, metadata)

	if err != nil {
		utils.LogError(err, "error reporting service status", 0, map[string]interface{}{"name": name, "status": status})
	}

	lastStatusUpdate.Store(name, time.Now())
}
