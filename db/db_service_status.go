package db

import "encoding/json"

// LogServiceStatus inserts or updates the status of a running service instance
// into the `service_status` table. It uniquely identifies a service by the combination
// of name, executable name, version, and PID.
//
// If a record with the same identity already exists, the status, metadata, and last_update
// fields will be updated. This function is typically used by background processes or
// health checkers to report service liveness or diagnostic info.
//
// Parameters:
//   - name: Human-readable service name.
//   - execName: Executable or binary name.
//   - version: Service version string.
//   - status: Arbitrary status string.
//   - pid: Process ID of the service instance.
//   - metadata: Optional JSON blob with diagnostic info (nullable).
//
// Returns:
//   - error: Any error from the underlying database operation.
func (pg *Postgres) LogServiceStatus(
	name, execName, version, status string,
	pid int,
	metadata *json.RawMessage,
) error {
	const query = `
		INSERT INTO service_status (
			name,
			executable_name,
			version,
			pid,
			status,
			metadata,
			last_update
		)
		VALUES ($1, $2, $3, $4, $5, $6, NOW())
		ON CONFLICT (name, executable_name, version, pid)
		DO UPDATE SET
			status = EXCLUDED.status,
			metadata = EXCLUDED.metadata,
			last_update = EXCLUDED.last_update
	`

	_, err := pg.Db.Exec(query, name, execName, version, pid, status, metadata)
	return err
}