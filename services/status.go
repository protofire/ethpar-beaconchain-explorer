package services

import (
	"encoding/json"
	"os"
	"sync"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/internal/logger"
	"github.com/protofire/ethpar-beaconchain-explorer/version"
)

var lastStatusUpdate sync.Map // key: string, value: time.Time

// ReportStatus logs the current status of a service to the database.
//
// It rate-limits updates to once per minute per service name. If `report` is false,
// the call returns immediately. Safe for concurrent use.
//
// Inputs:
//   - pg:       Postgres client for writing.
//   - report:   Whether to perform the write (feature toggle).
//   - name:     Unique service identifier.
//   - status:   Human-readable status message.
//   - metadata: Optional JSON blob with context (nullable).
func ReportStatus(pg *db.Postgres, report bool, name, status string, metadata *json.RawMessage) {
	if !report {
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

	if err := pg.LogServiceStatus(name, execName, version, status, pid, metadata); err != nil {
		log.WithFields(logger.Fields{
			"name": name, 
			"status": status,
		}).Errorf("error reporting service status: %v", err)
	}

	lastStatusUpdate.Store(name, time.Now())
}