// Copyright (C) 2025 Bitfly GmbH
//
// This file is part of Beaconchain Dashboard.
//
// Beaconchain Dashboard is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Beaconchain Dashboard is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with Beaconchain Dashboard.  If not, see <https://www.gnu.org/licenses/>.

package exporter

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/jmoiron/sqlx"
	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/rpc/consensus"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
	"github.com/protofire/ethpar-beaconchain-explorer/version"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type PendingQueueIndexer struct {
	running   bool
	runningMu *sync.Mutex
	cc        consensus.ConsensusClient
	db        *sqlx.DB
}

func NewPendingQueueIndexer(client consensus.ConsensusClient) *PendingQueueIndexer {
	indexer := &PendingQueueIndexer{
		running:   false,
		cc:        client,
		db:        db.WriterDb,
		runningMu: &sync.Mutex{},
	}
	return indexer
}

func (qi *PendingQueueIndexer) Start() {
	qi.runningMu.Lock()
	if qi.running {
		qi.runningMu.Unlock()
		return
	}
	qi.running = true
	qi.runningMu.Unlock()

	logrus.WithFields(logrus.Fields{"version": version.Version}).Infof("starting pending queue indexer")
	for {
		err := qi.Index()
		if err != nil {
			logrus.WithFields(logrus.Fields{"error": err}).Errorf("failed indexing pending queue")
		}
		logrus.Infof("pending queue indexer finished indexing, sleeping for 10 minutes")
		time.Sleep(time.Minute * 10) // interval MUST be longer than one epoch
		// Background: A freshly exported validator will have an eligible epoch of max uint64, by keeping the pending deposits
		// a bit longer in the db, we can rely on the pending deposits table to still get us an estimate for eligibility
	}
}

func (qi *PendingQueueIndexer) Index() error {
	head, err := qi.cc.GetChainHead()
	if err != nil {
		return errors.Wrap(err, "failed to get chain head")
	}
	epoch := head.HeadEpoch

	deposits, err := qi.cc.GetPendingDeposits()
	if err != nil {
		return errors.Wrap(err, "failed to get pending deposits")
	}

	validators, err := qi.cc.GetValidatorState(epoch)
	if err != nil {
		return errors.Wrap(err, "failed to get validator state")
	}

	type MiniState struct {
		Index             uint64
		ExitEpoch         uint64
		WithdrawableEpoch uint64
	}

	totalActiveEffectiveBalance := uint64(0)
	pubkeyToIndexMap := make(map[string]*MiniState)

	for _, v := range validators.Data {
		pubkeyToIndexMap[v.Validator.Pubkey] = &MiniState{
			Index:             uint64(v.Index),
			ExitEpoch:         uint64(v.Validator.ExitEpoch),
			WithdrawableEpoch: uint64(v.Validator.WithdrawableEpoch),
		}
		if epoch >= uint64(v.Validator.ActivationEpoch) && epoch < uint64(v.Validator.ExitEpoch) {
			totalActiveEffectiveBalance += uint64(v.Validator.EffectiveBalance)
		}
	}

	etherChurnByEpoch := utils.GetActivationExitChurnLimit(totalActiveEffectiveBalance)
	count := 0
	balanceAhead := uint64(0)
	clearEpoch := head.HeadEpoch + 1

	// transition period
	// pre electra system will keep going for follow distance until every deposit of the last system is converted to the new system
	// before the new system starts
	electraQueueDelay := uint64(utils.Config.ClConfig.Eth1FollowDistance/utils.Config.ClConfig.SlotsPerEpoch + utils.Config.ClConfig.EpochsPerEth1VotingPeriod)
	if clearEpoch < utils.Config.ClConfig.ElectraForkEpoch+electraQueueDelay {
		clearEpoch = utils.Config.ClConfig.ElectraForkEpoch + electraQueueDelay
	}

	depositsList := make([]types.PendingDeposit, 0)

	// spec vars (in snake_case)
	nextDepositIndex := uint64(0)
	maxPendingDepositsPerEpoch := utils.Config.ClConfig.MaxPendingDepositsPerEpoch
	if maxPendingDepositsPerEpoch == 0 { // eth mainnet spec default
		maxPendingDepositsPerEpoch = uint64(16)
	}
	processedAmount := uint64(0)
	stateDepositBalanceToConsume := uint64(0)

	pendingDeposits := deposits.Data
	depositsToPostpone := []types.PendingDeposit{} // est differently than the spec as we just set these to the same clearEpoch as the "normal" last entry. Not snake case to highlight the different handling to spec

	// emulate spec based on current view in time (approx estimation)
	// https://github.com/ethereum/consensus-specs/blob/dev/specs/electra/beacon-chain.md#new-process_pendingDeposits
	for {
		nextEpoch := clearEpoch + 1
		availableForProcessing := stateDepositBalanceToConsume + etherChurnByEpoch
		processedAmount = 0
		nextDepositIndex = 0

		isChurnLimitReached := false
		finalizedSlot := nextEpoch * utils.Config.ClConfig.SlotsPerEpoch // first slot of next epoch is finalized
		// potential improvement: utils.GetActivationExitChurnLimit(totalActiveEffectiveBalance + balanceAhead - withdrawalsAhead)

		for _, deposit := range pendingDeposits {
			if deposit.Slot > finalizedSlot {
				break
			}

			if nextDepositIndex >= maxPendingDepositsPerEpoch {
				break
			}

			miniState, found := pubkeyToIndexMap[string(deposit.Pubkey)]
			var isValidatorExited bool
			var isValidatorWithdrawn bool

			if found {
				isValidatorExited = miniState.ExitEpoch < 100_000_000_000
				isValidatorWithdrawn = miniState.WithdrawableEpoch < nextEpoch
			}

			getPendingDeposit := func() types.PendingDeposit {
				pendingDeposit := types.PendingDeposit{
					ID:                    count,
					Pubkey:                deposit.Pubkey,
					WithdrawalCredentials: deposit.WithdrawalCredentials,
					Amount:                deposit.Amount,
					Signature:             deposit.Signature,
					Slot:                  deposit.Slot,
					ValidatorIndex:        sql.NullInt64{},
					QueuedBalanceAhead:    balanceAhead,
					EstClearEpoch:         clearEpoch,
				}

				if found {
					pendingDeposit.ValidatorIndex = sql.NullInt64{
						Int64: int64(miniState.Index),
						Valid: true,
					}
				}
				return pendingDeposit
			}

			if isValidatorWithdrawn { // do not consume churn
				depositsList = append(depositsList, getPendingDeposit())
			} else if isValidatorExited { // do not consume churn
				depositsToPostpone = append(depositsToPostpone, getPendingDeposit())
			} else {
				isChurnLimitReached = processedAmount+deposit.Amount > availableForProcessing
				if isChurnLimitReached {
					break
				}
				processedAmount += deposit.Amount
				depositsList = append(depositsList, getPendingDeposit())
			}

			nextDepositIndex++

			// out of spec
			balanceAhead += deposit.Amount
			count++
		}

		pendingDeposits = pendingDeposits[nextDepositIndex:]

		if len(pendingDeposits) == 0 {
			break
		}

		if isChurnLimitReached {
			stateDepositBalanceToConsume = availableForProcessing - processedAmount
		} else {
			stateDepositBalanceToConsume = 0
		}

		clearEpoch++
	}

	// treat postpones deposits differently, set to last epoch of "normal" deposits
	// since we can't accurately predict them anyway if they are that far out where there are no "normal" deposits with current state
	if len(depositsList) > 0 {
		lastEntry := depositsList[len(depositsList)-1]
		for i := range depositsToPostpone {
			depositsToPostpone[i].EstClearEpoch = lastEntry.EstClearEpoch
			depositsToPostpone[i].QueuedBalanceAhead = lastEntry.QueuedBalanceAhead
		}
		depositsList = append(depositsList, depositsToPostpone...)
	}

	return qi.save(depositsList)
}

func (*PendingQueueIndexer) matchDepositRequests(tx *sql.Tx) error {
	// matching will be wrong for postponed system-deposits
	// but likelihood to ever occur for one pubkey, amount, slot combo is effectively 0
	query := `
	WITH pdq_ranked AS (
		SELECT *, ROW_NUMBER() OVER (
			PARTITION BY pubkey, amount, slot ORDER BY id
		) AS rn
		FROM pendingDeposits_queue
	),
	bdr_ranked AS (
		SELECT *, ROW_NUMBER() OVER (
			PARTITION BY pubkey, amount, slot_queued ORDER BY index_queued ASC
		) AS rn
		FROM blocks_deposit_requests_v2
		WHERE status = 'queued' OR status = 'postponed'
	),
	matches AS (
		SELECT pdq.id AS pdq_id, bdr.id AS bdr_id
		FROM pdq_ranked pdq
		JOIN bdr_ranked bdr
			ON pdq.pubkey = bdr.pubkey
			AND pdq.amount = bdr.amount
			AND (
				pdq.slot = bdr.slot_queued AND pdq.rn = bdr.rn OR
				(pdq.slot = 0 AND bdr.index_queued < 0)
			)
	)
	UPDATE pendingDeposits_queue
	SET request_id = matches.bdr_id
	FROM matches
	WHERE pendingDeposits_queue.id = matches.pdq_id;`

	_, err := tx.Exec(query)
	if err != nil && err != sql.ErrNoRows {
		return err
	}
	return nil
}

func (qi *PendingQueueIndexer) save(pendingDeposits []types.PendingDeposit) error {
	tx, err := qi.db.Begin()
	if err != nil {
		return errors.Wrap(err, "failed to start db transaction")
	}

	defer tx.Rollback()

	// prepare data for bulk insert
	dat := make([][]interface{}, len(pendingDeposits))
	for i, r := range pendingDeposits {
		dat[i] = []interface{}{r.ID, r.ValidatorIndex, encodeToHex(r.Pubkey), encodeToHex(r.WithdrawalCredentials), r.Amount, encodeToHex(r.Signature), r.Slot, r.QueuedBalanceAhead, r.EstClearEpoch}
	}

	err = db.ClearAndCopyToTable(qi.db, "pendingDeposits_queue", []string{"id", "validator_index", "pubkey", "withdrawal_credentials", "amount", "signature", "slot", "queued_balance_ahead", "est_clear_epoch"}, dat)
	if err != nil {
		return fmt.Errorf("error copying data to pendingDeposits_queue table: %w", err)
	}

	err = qi.matchDepositRequests(tx)
	if err != nil {
		return fmt.Errorf("error matching data with blocks_deposit_requests_v2 table: %w", err)
	}

	if err := tx.Commit(); err != nil {
		return errors.Wrap(err, "failed to commit transaction")
	}

	return nil
}

func encodeToHex(data []byte) string {
	return fmt.Sprintf("\\x%x", data)
}
