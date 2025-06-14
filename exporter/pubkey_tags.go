package exporter

import (
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/metrics"
)

// UpdatePubkeyTag periodically updates the validator_tags table by associating
// validator public keys with known stake pools.
//
// It scans the eth1_deposits table and joins it with the stake_pools_stats table
// using the depositor address. For each match (excluding Rocket Pool entries),
// it inserts a tag in the format "pool:<pool_name>" for the corresponding public key.
//
// The function runs in an infinite loop with a 10-minute interval between updates.
// It also logs execution duration to a Prometheus metrics histogram under the
// "validator_pubkey_tag_updater" label.
//
// This is useful for tagging validators associated with centralized or known staking pools.
func UpdatePubkeyTag() {
	logger.Infoln("Started Pubkey Tags Updater")
	for {
		start := time.Now()

		tx, err := db.WriterDb.Beginx()
		if err != nil {
			logger.WithError(err).Error("Error connecting to DB")
			// return err
		}
		_, err = tx.Exec(`INSERT INTO validator_tags (publickey, tag)
		SELECT publickey, FORMAT('pool:%s', sps.name) tag
		FROM eth1_deposits
		inner join stake_pools_stats as sps on ENCODE(from_address::bytea, 'hex')=sps.address
		WHERE sps.name NOT LIKE '%Rocketpool -%'
		ON CONFLICT (publickey, tag) DO NOTHING;`)
		if err != nil {
			logger.WithError(err).Error("Error updating validator_tags")
			// return err
		}

		err = tx.Commit()
		if err != nil {
			logger.WithError(err).Error("Error committing transaction")
		}
		tx.Rollback()

		logger.Infof("Updating Pubkey Tags took %v sec.", time.Since(start).Seconds())
		metrics.TaskDuration.WithLabelValues("validator_pubkey_tag_updater").Observe(time.Since(start).Seconds())

		time.Sleep(time.Minute * 10)
	}
}
