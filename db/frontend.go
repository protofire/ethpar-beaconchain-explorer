package db

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
)

// AddSubscription adds a new subscription to the database.
func (pg *Postgres) AddSubscription(userID uint64, network string, eventName types.EventName, eventFilter string, eventThreshold float64) error {
	now := time.Now()
	nowTs := now.Unix()
	nowEpoch := utils.TimeToEpoch(now)

	var onConflictDo = "NOTHING"
	if strings.HasPrefix(string(eventName), "monitoring_") || eventName == types.RocketpoolCollateralMaxReached || eventName == types.RocketpoolCollateralMinReached || eventName == types.ValidatorIsOfflineEventName {
		onConflictDo = "UPDATE SET event_threshold = $6"
	}

	name := string(eventName)
	if network != "" {
		name = strings.ToLower(network) + ":" + string(eventName)
	}
	_, err := pg.Db.Exec("INSERT INTO users_subscriptions (user_id, event_name, event_filter, created_ts, created_epoch, event_threshold) VALUES ($1, $2, $3, TO_TIMESTAMP($4), $5, $6) ON CONFLICT (user_id, event_name, event_filter) DO "+onConflictDo, userID, name, eventFilter, nowTs, nowEpoch, eventThreshold)
	return err
}

// DeleteSubscription removes a subscription from the database.
func (pg *Postgres) DeleteSubscription(userID uint64, network string, eventName types.EventName, eventFilter string) error {
	name := string(eventName)
	if network != "" && !types.IsUserIndexed(eventName) {
		name = strings.ToLower(network) + ":" + string(eventName)
	}

	_, err := pg.Db.Exec("DELETE FROM users_subscriptions WHERE user_id = $1 AND event_name = $2 AND event_filter = $3", userID, name, eventFilter)
	return err
}

func (pg *Postgres) GetAllAppSubscriptions() ([]*types.PremiumData, error) {
	data := []*types.PremiumData{}

	err := pg.Db.Select(&data,
		"SELECT id, receipt, store, active, expires_at, product_id, user_id, validate_remotely from users_app_subscriptions WHERE validate_remotely = true order by id desc",
	)

	return data, err
}

func (pg *Postgres) UpdateUserSubscription(tx *sql.Tx, id uint64, valid bool, expiration int64, rejectReason string) error {
	now := time.Now()
	nowTs := now.Unix()
	var err error
	if tx == nil {
		_, err = pg.Db.Exec("UPDATE users_app_subscriptions SET active = $1, updated_at = TO_TIMESTAMP($2), expires_at = TO_TIMESTAMP($3), reject_reason = $4 WHERE id = $5;",
			valid, nowTs, expiration, rejectReason, id,
		)
	} else {
		_, err = tx.Exec("UPDATE users_app_subscriptions SET active = $1, updated_at = TO_TIMESTAMP($2), expires_at = TO_TIMESTAMP($3), reject_reason = $4 WHERE id = $5;",
			valid, nowTs, expiration, rejectReason, id,
		)
	}

	return err
}

func (pg *Postgres) UpdateUserSubscriptionProduct(tx *sql.Tx, id uint64, productID string) error {
	var err error
	if tx == nil {
		_, err = pg.Db.Exec("UPDATE users_app_subscriptions SET product_id = $1 WHERE id = $2;",
			productID, id,
		)
	} else {
		_, err = tx.Exec("UPDATE users_app_subscriptions SET product_id = $1 WHERE id = $2",
			productID, id,
		)
	}

	return err
}

func (pg *Postgres) SetSubscriptionToExpired(tx *sql.Tx, id uint64) error {
	var err error
	query := "UPDATE users_app_subscriptions SET validate_remotely = false, reject_reason = 'expired' WHERE id = $1;"
	if tx == nil {
		_, err = pg.Db.Exec(query,
			id,
		)
	} else {
		_, err = tx.Exec(query,
			id,
		)
	}

	return err
}

// TODO: move to helpers
func getMachineStatsGap(resultCount uint64) int {
	if resultCount > 20160 { // more than 14 (31)
		return 8
	}
	if resultCount > 10080 { // more than 7 (14)
		return 7
	}
	if resultCount > 2880 { // more than 2 (7)
		return 5
	}
	if resultCount > 1440 { // more than 1 (2)
		return 4
	}
	if resultCount > 770 { // more than 12h
		return 2
	}
	return 1
}

func (pg *Postgres) GetHistoricalPrice(chainId uint64, currency string, day uint64) (float64, error) {
	if chainId != 1 && chainId != 100 {
		// Don't show a historical price for testnets
		return 0.0, nil
	}
	if currency == utils.Config.Frontend.ClCurrency {
		currency = "USD"
	}
	currency = strings.ToLower(currency)

	if currency != "eur" && currency != "usd" && currency != "rub" && currency != "cny" && currency != "cad" && currency != "jpy" && currency != "gbp" && currency != "aud" {
		return 0.0, fmt.Errorf("currency %v not supported", currency)
	}

	// Convert day to ts
	genesisTime := time.Unix(int64(utils.Config.Chain.GenesisTimestamp), 0)
	dayStartGenesisTime := time.Date(genesisTime.Year(), genesisTime.Month(), genesisTime.Day(), 0, 0, 0, 0, time.UTC)
	ts := dayStartGenesisTime.Add(utils.Day * time.Duration(day))

	var value float64
	err := pg.Db.Get(&value, fmt.Sprintf("SELECT %s FROM price WHERE ts = $1", currency), ts)
	if err != nil {
		return 0.0, err
	}
	return value, nil
}

// SaveDataTableState saves the state of the current datatable state update
func (pg *Postgres) SaveDataTableState(user uint64, key string, state types.DataTableSaveState) error {
	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	// check how many table states are stored
	count := 0
	err := pg.Db.GetContext(ctx, &count, `
		SELECT count(*)
		FROM users_datatable
		WHERE user_id = $1
	`, user)
	if err != nil {
		return err
	}

	// only store the most recent 100 table states across all networks
	if count > 100 {
		_, err := pg.Db.ExecContext(ctx, `
			DELETE FROM users_datatable 
			WHERE user_id = $1 
			ORDER by updated_at asc 
			LIMIT 10
		`)
		if err != nil {
			return err
		}
	}
	// append network prefix
	key = utils.GetNetwork() + ":" + key

	_, err = pg.Db.ExecContext(ctx, `
		INSERT INTO 
			users_datatable (user_id, key, state) 
		VALUES ($1, $2, $3) 
		ON CONFLICT (user_id, key) DO UPDATE SET state = $3, updated_at = now()
	`, user, key, state)

	return err
}

// GetDataTablesState retrieves the state for a given user and table
func (pg *Postgres) GetDataTablesState(user uint64, key string) (*types.DataTableSaveState, error) {
	var state types.DataTableSaveState

	// append network prefix
	key = utils.GetNetwork() + ":" + key

	ctx, done := context.WithTimeout(context.Background(), time.Second*30)
	defer done()

	err := pg.Db.GetContext(ctx, &state, `
		SELECT state 
		FROM users_datatable
		WHERE user_id = $1 and key = $2
	`, user, key)

	return &state, err
}
