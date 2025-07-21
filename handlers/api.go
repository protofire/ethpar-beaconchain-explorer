package handlers

import (
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"

	"github.com/ethereum/go-ethereum/common"
	gorillacontext "github.com/gorilla/context"
	"github.com/gorilla/mux"
	"github.com/jmoiron/sqlx"
	"github.com/lib/pq"
	utilMath "github.com/protolambda/zrnt/eth2/util/math"
	"golang.org/x/sync/errgroup"
)

// @title beaconcha.in Ethereum API Documentation
// @version 1.1
// @description High performance API for querying information about Ethereum
// @description The API is currently free to use. A fair use policy applies. Calls are rate limited to
// @description 10 requests / 1 minute / IP. All API results are cached for 1 minute.
// @description If you required a higher usage plan please checkout https://beaconcha.in/pricing.
// @description The API key can be provided in the Header or as a query string parameter.
// @description
// @description Key as a query string parameter: `curl https://beaconcha.in/api/v1/slot/1?apikey=<your_key>`
// @description
// @description Key in a request header:  `curl -H 'apikey: <your_key>' https://beaconcha.in/api/v1/slot/1`
// @tag.name Epoch
// @tag.description Consensus layer information about epochs
// @tag.docs.url https://example.com
// @tag.name Slot
// @tag.description Consensus layer information about slots
// @tag.name Validator
// @tag.description Consensus layer information about validators
// @tag.name SyncCommittee
// @tag.name Execution
// @tag.description layer information about addresses, blocks and transactions
// @tag.name ETH.STORE®
// @tag.description is the transparent Ethereum staking reward reference rate.
// @tag.docs.url https://staking.ethermine.org/statistics
// @tag.docs.description More info
// @tag.name Rocketpool
// @tag.description validator statistics
// @tag.docs.url https://rocketpool.net
// @tag.docs.description More info
// @tag.name Misc
// @tag.name User
// @tag.description provided for Oauth applications (public OAuth support is a work in progress).
// @securitydefinitions.oauth2.accessCode OAuthAccessCode
// @tokenurl https://beaconcha.in/user/token
// @authorizationurl https://beaconcha.in/user/authorize
// @securitydefinitions.apikey ApiKeyAuth
// @in header
// @name Authorization

// ApiHealthz godoc
// @Summary Health of the explorer
// @Tags Misc
// @Description Health endpoint for monitoring if the explorer is in sync
// @Produce  text/plain
// @Success 200 {object} types.ApiResponse
// @Router /api/healthz [get]
func ApiHealthz(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "text/plain")

	modules := []string{
		"monitoring_app",
		"monitoring_el_data",
		"monitoring_services",
		"monitoring_cl_data",
		"monitoring_api",
		"monitoring_redis",
	}

	res := []struct {
		Name   string
		Status string
	}{}
	err := db.WriterDb.Select(&res, "SELECT name, status FROM service_status WHERE name = ANY($1) AND last_update > NOW() - INTERVAL '5 MINUTES' ORDER BY last_update DESC", pq.Array(modules))

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			http.Error(w, "No monitoring data available", http.StatusNotFound)
		} else {
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}

	modulesMap := make(map[string]string)
	for _, module := range modules {
		modulesMap[module] = ""
	}

	hasError := false
	response := strings.Builder{}
	for _, status := range res {

		if modulesMap[status.Name] == "" {
			modulesMap[status.Name] = status.Status

			if status.Status != "OK" {
				hasError = true
			}

			response.WriteString(fmt.Sprintf("module %s: %s\n", status.Name, status.Status))
		}
	}

	for _, module := range modules {
		if modulesMap[module] == "" {
			hasError = true
			response.WriteString(fmt.Sprintf("module %s: %s\n", module, "No monitoring data available"))
		}
	}

	if !hasError {
		_, err = fmt.Fprint(w, response.String())

		if err != nil {
			log.Debugf("error writing status: %v", err)
		}
	} else {
		http.Error(w, response.String(), http.StatusInternalServerError)
		return
	}
}

// ApiHealthzLoadbalancer godoc
// @Summary Health of the explorer-api regarding having a healthy connection to the database
// @Tags Misc
// @Description Health endpoint for montitoring if the explorer-api
// @Produce  text/plain
// @Success 200 {object} types.ApiResponse
// @Router /api/healthz-loadbalancer [get]
func ApiHealthzLoadbalancer(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "text/plain")

	lastEpoch, err := db.GetLatestEpoch()

	if err != nil {
		http.Error(w, "Internal server error: could not retrieve latest epoch from the db", http.StatusInternalServerError)
		return
	}

	if utils.Config.Chain.GenesisTimestamp == 18446744073709551615 {
		fmt.Fprint(w, "OK. No GENESIS_TIMESTAMP defined yet")
		return
	}

	genesisTime := time.Unix(int64(utils.Config.Chain.GenesisTimestamp), 0)
	if genesisTime.After(time.Now()) {
		fmt.Fprintf(w, "OK. Genesis in %v (%v)", time.Until(genesisTime), genesisTime)
		return
	}

	fmt.Fprintf(w, "OK. Last epoch is from %v ago", time.Since(utils.EpochToTime(lastEpoch)))
}

// ApiLatestState godoc
// @Summary Get the latest state of the network
// @Tags Network
// @Description Returns information on the current state of the network
// @Produce  json
// @Failure 400 {object} types.ApiResponse "Failure"
// @Failure 500 {object} types.ApiResponse "Server Error"
// @Router /api/v1/latestState [get]
func ApiLatestState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", fmt.Sprintf("public, max-age=%d", utils.Config.Chain.ClConfig.SecondsPerSlot)) // set local cache to the seconds per slot interval

	data := services.LatestState()
	data.Rates = services.GetRates(GetCurrency(r))
	userAgent := r.Header.Get("User-Agent")
	userAgent = strings.ToLower(userAgent)
	if strings.Contains(userAgent, "android") || strings.Contains(userAgent, "iphone") || strings.Contains(userAgent, "windows phone") {
		data.Rates.MainCurrencyPriceFormatted = utils.KFormatterEthPrice(uint64(data.Rates.MainCurrencyPrice))
	}

	err := json.NewEncoder(w).Encode(data)
	if err != nil {
		log.Errorf("error sending latest index page data: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
}

// ApiEpoch godoc
// @Summary Get epoch by number, latest, finalized
// @Tags Epoch
// @Description Returns information for a specified epoch by the epoch number or an epoch tag (can be latest or finalized)
// @Produce  json
// @Param  epoch path string true "Epoch number, the string latest or the string finalized"
// @Success 200 {object} types.ApiResponse{data=types.APIEpochResponse} "Success"
// @Failure 400 {object} types.ApiResponse "Failure"
// @Failure 500 {object} types.ApiResponse "Server Error"
// @Router /api/v1/epoch/{epoch} [get]
func ApiEpoch(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)

	epoch, err := strconv.ParseInt(vars["epoch"], 10, 64)
	if err != nil && vars["epoch"] != "latest" && vars["epoch"] != "finalized" {
		SendBadRequestResponse(w, r.URL.String(), "invalid epoch provided")
		return
	}

	if vars["epoch"] == "latest" {
		epoch = int64(services.LatestEpoch())
	}

	latestFinalizedEpoch := services.LatestFinalizedEpoch()
	if vars["epoch"] == "finalized" {
		epoch = int64(services.LatestFinalizedEpoch())
	}

	if epoch > int64(services.LatestEpoch()) {
		SendBadRequestResponse(w, r.URL.String(), fmt.Sprintf("epoch is in the future. The latest epoch is %v", services.LatestEpoch()))
		return
	}

	if epoch < 0 {
		SendBadRequestResponse(w, r.URL.String(), "epoch must be a positive number")
		return
	}

	rows, err := db.ReaderDb.Query(`SELECT attestationscount, attesterslashingscount, averagevalidatorbalance, blockscount, depositscount, eligibleether, epoch, (epoch <= $2) AS finalized, globalparticipationrate, proposerslashingscount, rewards_exported, totalvalidatorbalance, validatorscount, voluntaryexitscount, votedether, COALESCE(withdrawalcount,0) as withdrawalcount, 
		(SELECT COUNT(*) FROM blocks WHERE epoch = $1 AND status = '0') as scheduledblocks,
		(SELECT COUNT(*) FROM blocks WHERE epoch = $1 AND status = '1') as proposedblocks,
		(SELECT COUNT(*) FROM blocks WHERE epoch = $1 AND status = '2') as missedblocks,
		(SELECT COUNT(*) FROM blocks WHERE epoch = $1 AND status = '3') as orphanedblocks
		FROM epochs WHERE epoch = $1`, epoch, latestFinalizedEpoch)
	if err != nil {
		log.WithError(err).Error("error retrieving epoch data")
		sendServerErrorResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	addEpochTime := func(dataEntryMap map[string]interface{}) error {
		dataEntryMap["ts"] = utils.EpochToTime(uint64(epoch))
		return nil
	}

	returnQueryResults(rows, w, r, addEpochTime)
}

// ApiEpochSlots godoc
// @Summary Get epoch blocks by epoch number, latest or finalized
// @Tags Epoch
// @Description Returns all slots for a specified epoch
// @Produce  json
// @Param  epoch path string true "Epoch number, the string latest or string finalized"
// @Success 200 {object} types.ApiResponse{data=[]types.APISlotResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/epoch/{epoch}/slots [get]
func ApiEpochSlots(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)

	epoch, err := strconv.ParseInt(vars["epoch"], 10, 64)
	if err != nil && vars["epoch"] != "latest" && vars["epoch"] != "finalized" {
		SendBadRequestResponse(w, r.URL.String(), "invalid epoch provided")
		return
	}

	if vars["epoch"] == "latest" {
		epoch = int64(services.LatestEpoch())
	}

	if vars["epoch"] == "finalized" {
		epoch = int64(services.LatestFinalizedEpoch())
	}

	if epoch > int64(services.LatestEpoch()) {
		SendBadRequestResponse(w, r.URL.String(), fmt.Sprintf("epoch is in the future. The latest epoch is %v", services.LatestEpoch()))
		return
	}

	if epoch < 0 {
		SendBadRequestResponse(w, r.URL.String(), "epoch must be a positive number")
		return
	}

	rows, err := db.ReaderDb.Query("SELECT attestationscount, attesterslashingscount, blockroot, depositscount, epoch, eth1data_blockhash, eth1data_depositcount, eth1data_depositroot, exec_base_fee_per_gas, exec_block_hash, exec_block_number, exec_extra_data, exec_fee_recipient, exec_gas_limit, exec_gas_used, exec_logs_bloom, exec_parent_hash, exec_random, exec_receipts_root, exec_state_root, exec_timestamp, COALESCE(exec_transactions_count,0) as exec_transactions_count, graffiti, graffiti_text, parentroot, proposer, proposerslashingscount, randaoreveal, signature, slot, stateroot, status, syncaggregate_bits, syncaggregate_participation, syncaggregate_signature, voluntaryexitscount, COALESCE(withdrawalcount,0) as withdrawalcount FROM blocks WHERE epoch = $1 ORDER BY slot", epoch)
	if err != nil {
		sendServerErrorResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	returnQueryResultsAsArray(rows, w, r)
}

// ApiSlots godoc
// @Summary Get a slot by its slot number or root hash. Alternatively get the latest slot or the slot containing the head block.
// @Tags Slot
// @Description Returns a slot by its slot number or root hash, the latest slot with string latest or the slot containing the head block with string head
// @Produce  json
// @Param  slotOrHash path string true "Slot or root hash or the string latest or head"
// @Success 200 {object} types.ApiResponse{data=types.APISlotResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/slot/{slotOrHash} [get]
func ApiSlots(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	slotOrHash := strings.Replace(vars["slotOrHash"], "0x", "", -1)

	blockSlot := int64(-1)
	blockRootHash := []byte{}

	if slotOrHash == "latest" {
		// simply check the latest slot (might be empty which causes an error)
		blockSlot = int64(services.LatestSlot())
	} else if slotOrHash == "head" {
		// retrieve the slot containing the head block of the chain
		blockRootHash = services.Eth1HeadBlockRootHash()
		if len(blockRootHash) != 32 {
			SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
			return
		}
	} else {
		var err error
		blockRootHash, err = hex.DecodeString(slotOrHash)
		if err != nil || len(slotOrHash) != 64 {
			// not a valid root hash, try to parse as slot number instead
			blockRootHash = []byte{}
			blockSlot, err = strconv.ParseInt(vars["slotOrHash"], 10, 64)
			if err != nil {
				SendBadRequestResponse(w, r.URL.String(), "could not parse slot number")
				return
			}
		}
	}

	if len(blockRootHash) != 32 {
		err := db.ReaderDb.Get(&blockRootHash, `SELECT blockroot FROM blocks WHERE slot = $1`, blockSlot)

		if err != nil || len(blockRootHash) != 32 {
			SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
			return
		}
	}

	rows, err := db.ReaderDb.Query(`
	SELECT
		blocks.epoch,
		blocks.slot,
		blocks.blockroot,
		blocks.parentroot,
		blocks.stateroot,
		blocks.signature,
		blocks.randaoreveal,
		blocks.graffiti,
		blocks.graffiti_text,
		blocks.eth1data_depositroot,
		blocks.eth1data_depositcount,
		blocks.eth1data_blockhash,
		blocks.proposerslashingscount,
		blocks.attesterslashingscount,
		blocks.attestationscount,
		blocks.depositscount,
		COALESCE(withdrawalcount,0) as withdrawalcount, 
		blocks.voluntaryexitscount,
		blocks.proposer,
		blocks.status,
		blocks.syncaggregate_bits,
		blocks.syncaggregate_signature,
		blocks.syncaggregate_participation,
		blocks.exec_parent_hash,
		blocks.exec_fee_recipient,
		blocks.exec_state_root,
		blocks.exec_receipts_root,
		blocks.exec_logs_bloom,
		blocks.exec_random,
		blocks.exec_block_number,
		blocks.exec_gas_limit,
		blocks.exec_gas_used,
		blocks.exec_timestamp,
		blocks.exec_extra_data,
		blocks.exec_base_fee_per_gas,
		blocks.exec_block_hash,     
		blocks.exec_transactions_count,
		ba.votes
	FROM
		blocks
	LEFT JOIN
		(SELECT beaconblockroot, sum(array_length(validators, 1)) AS votes FROM blocks_attestations GROUP BY beaconblockroot) ba ON (blocks.blockroot = ba.beaconblockroot)
	WHERE
		blocks.blockroot = $1`, blockRootHash)

	if err != nil {
		log.WithError(err).Error("could not retrieve db results")
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	returnQueryResults(rows, w, r)
}

// ApiSlotAttestations godoc
// @Summary Get the attestations included in a specific slot
// @Tags Slot
// @Description Returns the attestations included in a specific slot
// @Produce  json
// @Param  slot path string true "Slot"
// @Success 200 {object} types.ApiResponse{data=[]types.APIAttestationResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/slot/{slot}/attestations [get]
func ApiSlotAttestations(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)

	slot, err := strconv.ParseInt(vars["slot"], 10, 64)
	if err != nil && vars["slot"] != "latest" {
		SendBadRequestResponse(w, r.URL.String(), "invalid block slot provided")
		return
	}

	if vars["slot"] == "latest" {
		slot = int64(services.LatestSlot())
	}

	if slot > int64(services.LatestSlot()) {
		SendBadRequestResponse(w, r.URL.String(), fmt.Sprintf("slot is in the future. The latest slot is %v", services.LatestSlot()))
		return
	}

	if slot < 0 {
		SendBadRequestResponse(w, r.URL.String(), "slot must be a positive number")
		return
	}

	rows, err := db.ReaderDb.Query("SELECT aggregationbits, beaconblockroot, block_index, block_root, block_slot, committeeindex, signature, slot, source_epoch, source_root, target_epoch, target_root, validators FROM blocks_attestations WHERE block_slot = $1 ORDER BY block_index", slot)
	if err != nil {
		log.WithError(err).Error("could not retrieve db results")
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	returnQueryResultsAsArray(rows, w, r)
}

// ApiSlotAttesterSlashings godoc
// @Summary Get the attester slashings included in a specific slot
// @Tags Slot
// @Description Returns the attester slashings included in a specific slot
// @Produce  json
// @Param  slot path string true "Slot"
// @Success 200 {object} types.ApiResponse{data=[]types.APIAttesterSlashingResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/slot/{slot}/attesterslashings [get]
func ApiSlotAttesterSlashings(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)

	slot, err := strconv.ParseInt(vars["slot"], 10, 64)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "invalid block slot provided")
		return
	}

	rows, err := db.ReaderDb.Query("SELECT attestation1_beaconblockroot, attestation1_index, attestation1_indices, attestation1_signature, attestation1_slot, attestation1_source_epoch, attestation1_source_root, attestation1_target_epoch, attestation1_target_root, attestation2_beaconblockroot, attestation2_index, attestation2_indices, attestation2_signature, attestation2_slot, attestation2_source_epoch, attestation2_source_root, attestation2_target_epoch, attestation2_target_root, block_index, block_root, block_slot FROM blocks_attesterslashings WHERE block_slot = $1 ORDER BY block_index DESC", slot)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	returnQueryResultsAsArray(rows, w, r)
}

// ApiSlotDeposits godoc
// @Summary Get the deposits included in a specific block
// @Tags Slot
// @Description Returns the deposits included in a specific block
// @Produce  json
// @Param  slot path string true "Block slot"
// @Param  limit query string false "Limit the number of results"
// @Param offset query string false "Offset the number of results"
// @Success 200 {object} types.ApiResponse{[]APIAttestationResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/slot/{slot}/deposits [get]
func ApiSlotDeposits(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	q := r.URL.Query()

	limitQuery := q.Get("limit")
	offsetQuery := q.Get("offset")

	offset, err := strconv.ParseInt(offsetQuery, 10, 64)
	if err != nil {
		offset = 0
	}

	limit, err := strconv.ParseInt(limitQuery, 10, 64)
	if err != nil {
		limit = 100 + offset
	}

	if offset < 0 {
		offset = 0
	}

	if limit > (100+offset) || limit <= 0 || limit <= offset {
		limit = 100 + offset
	}

	slot, err := strconv.ParseInt(vars["slot"], 10, 64)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "invalid block slot provided")
		return
	}

	rows, err := db.ReaderDb.Query("SELECT amount, block_index, block_root, block_slot, proof, publickey, signature, withdrawalcredentials FROM blocks_deposits WHERE block_slot = $1 ORDER BY block_index DESC limit $2 offset $3", slot, limit, offset)
	if err != nil {
		log.WithError(err).Error("could not retrieve db results")
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	returnQueryResultsAsArray(rows, w, r)
}

// ApiSlotProposerSlashings godoc
// @Summary Get the proposer slashings included in a specific slot
// @Tags Slot
// @Description Returns the proposer slashings included in a specific slot
// @Produce  json
// @Param  slot path string true "Slot"
// @Success 200 {object} types.ApiResponse{data=[]types.APIProposerSlashingResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/slot/{slot}/proposerslashings [get]
func ApiSlotProposerSlashings(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)

	slot, err := strconv.ParseInt(vars["slot"], 10, 64)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "invalid block slot provided")
		return
	}

	rows, err := db.ReaderDb.Query("SELECT block_index, block_root, block_slot, header1_bodyroot, header1_parentroot, header1_signature, header1_slot, header1_stateroot, header2_bodyroot, header2_parentroot, header2_signature, header2_slot, header2_stateroot, proposerindex FROM blocks_proposerslashings WHERE block_slot = $1 ORDER BY block_index DESC", slot)
	if err != nil {
		log.WithError(err).Error("could not retrieve db results")
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	returnQueryResultsAsArray(rows, w, r)
}

// ApiSlotVoluntaryExits godoc
// @Summary Get the voluntary exits included in a specific slot
// @Tags Slot
// @Description Returns the voluntary exits included in a specific slot
// @Produce  json
// @Param  slot path string true "Slot"
// @Success 200 {object} types.ApiResponse{data=[]types.APIVoluntaryExitResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/slot/{slot}/voluntaryexits [get]
func ApiSlotVoluntaryExits(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)

	slot, err := strconv.ParseInt(vars["slot"], 10, 64)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "invalid block slot provided")
		return
	}

	rows, err := db.ReaderDb.Query("SELECT block_slot, block_index, block_root, epoch, validatorindex, signature FROM blocks_voluntaryexits WHERE block_slot = $1 ORDER BY block_index DESC", slot)
	if err != nil {
		log.WithError(err).Error("could not retrieve db results")
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	returnQueryResultsAsArray(rows, w, r)
}

// ApiSlotWithdrawals godoc
// @Summary Get the withdrawals included in a specific slot
// @Tags Slot
// @Description Returns the withdrawals included in a specific slot
// @Produce json
// @Param slot path string true "Block slot"
// @Success 200 {object} types.ApiResponse
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/slot/{slot}/withdrawals [get]
func ApiSlotWithdrawals(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	vars := mux.Vars(r)

	slot, err := strconv.ParseInt(vars["slot"], 10, 64)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "invalid block slot provided")
		return
	}

	rows, err := db.ReaderDb.Query("SELECT block_slot, withdrawalindex, validatorindex, address, amount FROM blocks_withdrawals WHERE block_slot = $1 ORDER BY withdrawalindex", slot)
	if err != nil {
		log.WithError(err).Error("error getting blocks_withdrawals")
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()
	returnQueryResults(rows, w, r)
}

// ApiBlockVoluntaryExits godoc
// ApiSyncCommittee godoc
// @Summary Get the sync-committee for a sync-period
// @Tags SyncCommittee
// @Description Returns the sync-committee for a sync-period. Validators are sorted by sync-committee-index.
// @Description Sync committees where introduced in the Altair hardfork. Peroids before the hardfork do not contain sync-committees.
// @Description For mainnet sync-committes first started after epoch 74240 (period 290) and each sync-committee is active for 256 epochs.
// @Produce json
// @Param period path string true "Period ('latest' for latest period or 'next' for next period in the future)"
// @Success 200 {object} types.ApiResponse{data=types.APISyncCommitteeResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/sync_committee/{period} [get]
func ApiSyncCommittee(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)

	period, err := strconv.ParseUint(vars["period"], 10, 64)
	if err != nil && vars["period"] != "latest" && vars["period"] != "next" {
		SendBadRequestResponse(w, r.URL.String(), "invalid epoch provided")
		return
	}

	if vars["period"] == "latest" {
		period = utils.SyncPeriodOfEpoch(services.LatestEpoch())
	} else if vars["period"] == "next" {
		period = utils.SyncPeriodOfEpoch(services.LatestEpoch()) + 1
	}

	// Beware that we do not deduplicate here since a validator can be part multiple times of the same sync committee period
	// and the order of the committeeindex is important, deduplicating it would mess up the order
	rows, err := db.ReaderDb.Query(`SELECT period, GREATEST(period*$2, $3) AS start_epoch, ((period+1)*$2)-1 AS end_epoch, ARRAY_AGG(validatorindex ORDER BY committeeindex) AS validators FROM sync_committees WHERE period = $1 GROUP BY period`, period, utils.Config.Chain.ClConfig.EpochsPerSyncCommitteePeriod, utils.Config.Chain.ClConfig.AltairForkEpoch)
	if err != nil {
		log.WithError(err).WithField("url", r.URL.String()).Errorf("error querying db")
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	returnQueryResults(rows, w, r)
}

// ApiValidatorQueue godoc
// @Summary Get the current validator queue
// @Tags Validator
// @Description Returns the current number of validators entering and exiting the beacon chain
// @Produce  json
// @Success 200 {object} types.ApiResponse{data=types.ApiValidatorQueueResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validators/queue [get]
func ApiValidatorQueue(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	rows, err := db.ReaderDb.Query("SELECT e.validatorscount, q.entering_validators_count as beaconchain_entering, q.exiting_validators_count as beaconchain_exiting FROM epochs e, queue q ORDER BY e.epoch DESC, q.ts DESC LIMIT 1")
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	returnQueryResults(rows, w, r)
}

// TODO: move to proper file
func getExpectedSyncCommitteeSlots(validators []uint64, epoch uint64) (expectedSlots uint64, err error) {
	if epoch < utils.Config.Chain.ClConfig.AltairForkEpoch {
		// no sync committee duties before altair fork
		return 0, nil
	}

	lastFinalizedEpoch := services.LatestFinalizedEpoch()
	if epoch > lastFinalizedEpoch {
		epoch = lastFinalizedEpoch
	}

	// retrieve activation and exit epochs from database per validator
	type ValidatorInfo struct {
		Id                         int64  `db:"validatorindex"`
		ActivationEpoch            uint64 `db:"activationepoch"`
		ExitEpoch                  uint64 `db:"exitepoch"`
		FirstPossibleSyncCommittee uint64 // calculated
		LastPossibleSyncCommittee  uint64 // calculated
	}

	var validatorsInfoFromDb = []ValidatorInfo{}
	query, args, err := sqlx.In(`SELECT validatorindex, activationepoch, exitepoch FROM validators WHERE validatorindex IN (?) ORDER BY validatorindex ASC`, validators)
	if err != nil {
		return 0, err
	}

	err = db.ReaderDb.Select(&validatorsInfoFromDb, db.ReaderDb.Rebind(query), args...)
	if err != nil {
		return 0, err
	}

	// only check validators that are/have been active and that did not exit before altair
	const noEpoch = uint64(9223372036854775807)
	var validatorsInfo = make([]ValidatorInfo, 0, len(validatorsInfoFromDb))
	for _, v := range validatorsInfoFromDb {
		if v.ActivationEpoch != noEpoch && v.ActivationEpoch < epoch && (v.ExitEpoch == noEpoch || v.ExitEpoch >= utils.Config.Chain.ClConfig.AltairForkEpoch) {
			validatorsInfo = append(validatorsInfo, v)
		}
	}

	if len(validatorsInfo) == 0 {
		// no validators relevant for sync duties
		return 0, nil
	}

	// we need all related and unique timeframes (activation and exit sync period) for all validators
	uniquePeriods := make(map[uint64]bool)
	for i := range validatorsInfo {
		// first epoch (activation epoch or Altair if Altair was later as there were no sync committees pre Altair)
		firstSyncEpoch := validatorsInfo[i].ActivationEpoch
		if validatorsInfo[i].ActivationEpoch < utils.Config.Chain.ClConfig.AltairForkEpoch {
			firstSyncEpoch = utils.Config.Chain.ClConfig.AltairForkEpoch
		}
		validatorsInfo[i].FirstPossibleSyncCommittee = utils.SyncPeriodOfEpoch(firstSyncEpoch)
		uniquePeriods[validatorsInfo[i].FirstPossibleSyncCommittee] = true

		// last epoch (exit epoch or current epoch if not exited yet)
		lastSyncEpoch := epoch
		if validatorsInfo[i].ExitEpoch != noEpoch && validatorsInfo[i].ExitEpoch <= epoch {
			lastSyncEpoch = validatorsInfo[i].ExitEpoch
		}
		validatorsInfo[i].LastPossibleSyncCommittee = utils.SyncPeriodOfEpoch(lastSyncEpoch)
		uniquePeriods[validatorsInfo[i].LastPossibleSyncCommittee] = true
	}

	// transform map to slice; this will be used to query sync_committees_count_per_validator
	periodSlice := make([]uint64, 0, len(uniquePeriods))
	for period := range uniquePeriods {
		periodSlice = append(periodSlice, period)
	}

	// get aggregated count for all relevant committees from sync_committees_count_per_validator
	var countStatistics []struct {
		Period     uint64  `db:"period"`
		CountSoFar float64 `db:"count_so_far"`
	}

	query, args, errs := sqlx.In(`SELECT period, count_so_far FROM sync_committees_count_per_validator WHERE period IN (?) ORDER BY period ASC`, periodSlice)
	if errs != nil {
		return 0, errs
	}
	err = db.ReaderDb.Select(&countStatistics, db.ReaderDb.Rebind(query), args...)
	if err != nil {
		return 0, err
	}
	if len(countStatistics) != len(periodSlice) {
		return 0, fmt.Errorf("unable to retrieve all sync committee count statistics, required %v entries but got %v entries (epoch: %v)", len(periodSlice), len(countStatistics), epoch)
	}

	// transform query result to map for easy access
	periodInfoMap := make(map[uint64]float64)
	for _, pl := range countStatistics {
		periodInfoMap[pl.Period] = pl.CountSoFar
	}

	// calculate expected committies for every single validator and aggregate them
	expectedCommitties := 0.0
	for _, vi := range validatorsInfo {
		expectedCommitties += periodInfoMap[vi.LastPossibleSyncCommittee] - periodInfoMap[vi.FirstPossibleSyncCommittee]
	}

	// transform committees to slots
	expectedSlots = uint64(expectedCommitties * float64(utils.SlotsPerSyncCommittee()))

	return expectedSlots, nil
}

type Cached struct {
	Data interface{}
	Ts   int64
}

func getValidatorEffectiveness(epoch uint64, indices []uint64, bt *db.Bigtable) ([]*types.ValidatorEffectiveness, error) {
	data, err := bt.GetValidatorEffectiveness(indices, epoch)
	if err != nil {
		return nil, fmt.Errorf("error getting validator effectiveness from bigtable: %w", err)
	}
	for i := 0; i < len(data); i++ {
		// convert value to old api schema
		data[i].AttestationEfficiency = 1 + (1 - data[i].AttestationEfficiency/100)
	}
	return data, nil
}

// ApiValidator godoc
// @Summary Get up to 100 validators
// @Tags Validator
// @Description Searching for too many validators based on their pubkeys will lead to a "URI too long" error
// @Produce  json
// @Param  indexOrPubkey path string true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Success 200 {object} types.ApiResponse{data=[]types.APIValidatorResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/{indexOrPubkey} [get]
func ApiValidatorGet(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		getApiValidator(bt)(w, r)
	}
}

// ApiValidator godoc
// @Summary Get up to 100 validators
// @Tags Validator
// @Description This POST endpoint exists because the GET endpoint can lead to a "URI too long" error when searching for too many validators based on their pubkeys.
// @Produce  json
// @Param  indexOrPubkey body types.DashboardRequest true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Success 200 {object} types.ApiResponse{data=[]types.APIValidatorResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator [post]
func ApiValidatorPost(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		getApiValidator(bt)(w, r)
	}
}

// This endpoint supports both GET and POST but requires different swagger descriptions based on the type
func getApiValidator(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		vars := mux.Vars(r)

		maxValidators := getUserPremium(r).MaxValidators

		var param string
		if r.Method == http.MethodGet {
			// Get the validators from the URL
			param = vars["indexOrPubkey"]
		} else {
			// Get the validators from the request body
			decoder := json.NewDecoder(r.Body)
			req := &types.DashboardRequest{}

			err := decoder.Decode(req)
			if err != nil {
				SendBadRequestResponse(w, r.URL.String(), "error decoding request body")
				return
			}
			param = req.IndicesOrPubKey
		}

		queryIndices, err := parseApiValidatorParamToIndices(param, maxValidators)

		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), err.Error())
			return
		}

		lastExportedDay, err := services.LatestExportedStatisticDay()
		if err != nil {
			sendServerErrorResponse(w, r.URL.String(), "error retrieving data, please try again later")
			return
		}
		_, lastEpochOfDay := utils.GetFirstAndLastEpochForDay(lastExportedDay)
		cutoffSlot := (lastEpochOfDay * utils.Config.Chain.ClConfig.SlotsPerEpoch) + 1

		data := make([]*ApiValidatorResponse, 0)

		err = db.ReaderDb.Select(&data, `
			WITH today AS (
				SELECT
					w.validatorindex,
					COALESCE(SUM(w.amount), 0) as amount
				FROM blocks_withdrawals w
				INNER JOIN blocks b ON b.blockroot = w.block_root AND b.status = '1'
				WHERE w.validatorindex = ANY($1) AND w.block_slot >= $2
				GROUP BY w.validatorindex
			),
			stats AS (
				SELECT
					vs.validatorindex,
					COALESCE(vs.withdrawals_amount_total, 0) as amount
				FROM validator_stats vs
				WHERE vs.validatorindex = ANY($1) AND vs.day = $3
			),
			withdrawals_summary AS (
				SELECT
					COALESCE(t.validatorindex, s.validatorindex) as validatorindex,
					COALESCE(t.amount, 0) + COALESCE(s.amount, 0) as total
				FROM today t
				FULL JOIN stats s ON t.validatorindex = s.validatorindex
			)
			SELECT
				v.validatorindex, '0x' || encode(pubkey, 'hex') as  pubkey, withdrawableepoch,
				'0x' || encode(withdrawalcredentials, 'hex') as withdrawalcredentials,
				slashed,
				activationeligibilityepoch,
				activationepoch,
				exitepoch,
				status,
				COALESCE(n.name, '') AS name,
				COALESCE(ws.total, 0) as total_withdrawals
			FROM validators v
			LEFT JOIN validator_names n ON n.publickey = v.pubkey
			LEFT JOIN withdrawals_summary ws ON ws.validatorindex = v.validatorindex
			WHERE v.validatorindex = ANY($1)
			ORDER BY v.validatorindex
		`, pq.Array(queryIndices), cutoffSlot, lastExportedDay)
		if err != nil {
			log.Warnf("error retrieving validator data from db: %v", err)
			SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
			return
		}

		balances, err := bt.GetValidatorBalanceHistory(queryIndices, services.LatestEpoch(), services.LatestEpoch())
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "could not retrieve validator balance data")
			return
		}

		for _, validator := range data {
			for balanceIndex, balance := range balances {
				if len(balance) == 0 {
					continue
				}
				if validator.Validatorindex == int64(balanceIndex) {
					validator.Balance = int64(balance[0].Balance)
					validator.Effectivebalance = int64(balance[0].EffectiveBalance)
				}
			}
		}

		lastAttestationSlots, err := bt.GetLastAttestationSlots(queryIndices)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), fmt.Sprintf("error getting validator last attestation slots from bigtable: %v", err))
			return
		}

		for _, validator := range data {
			validator.Lastattestationslot = int64(lastAttestationSlots[uint64(validator.Validatorindex)])
		}

		j := json.NewEncoder(w)
		response := &types.ApiResponse{}
		response.Status = "OK"

		if len(data) == 1 {
			response.Data = data[0]
		} else {
			response.Data = data
		}
		err = j.Encode(response)

		if err != nil {
			sendServerErrorResponse(w, r.URL.String(), "could not serialize data results")
			log.Errorf("error serializing json data for API %v route: %v", r.URL, err)
		}
	}
}

type ApiValidatorResponse struct {
	Activationeligibilityepoch int64  `json:"activationeligibilityepoch"`
	Activationepoch            int64  `json:"activationepoch"`
	Balance                    int64  `json:"balance"`
	Effectivebalance           int64  `json:"effectivebalance"`
	Exitepoch                  int64  `json:"exitepoch"`
	Lastattestationslot        int64  `json:"lastattestationslot"`
	Name                       string `json:"name"`
	Pubkey                     string `json:"pubkey"`
	Slashed                    bool   `json:"slashed"`
	Status                     string `json:"status"`
	Validatorindex             int64  `json:"validatorindex"`
	Withdrawableepoch          int64  `json:"withdrawableepoch"`
	Withdrawalcredentials      string `json:"withdrawalcredentials"`
	TotalWithdrawals           uint64 `json:"total_withdrawals" db:"total_withdrawals"`
}

// ApiValidatorDailyStats godoc
// @Summary Get the daily validator stats by the validator index
// @Tags Validator
// @Produce  json
// @Param  index path string true "Validator index"
// @Param  end_day query string false "End day (default: latest day)"
// @Param  start_day query string false "Start day (default: 0)"
// @Success 200 {object} types.ApiResponse{data=[]types.ApiValidatorDailyStatsResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/stats/{index} [get]
func ApiValidatorDailyStats(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	q := r.URL.Query()

	latestEpoch := services.LatestEpoch()

	latestDay := latestEpoch / utils.EpochsPerDay()

	startDay := int64(-1)
	endDay := int64(latestDay)

	if q.Get("end_day") != "" {
		end, err := strconv.ParseInt(q.Get("end_day"), 10, 64)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "invalid end_day parameter")
			return
		}
		if end < endDay {
			endDay = end
		}
	}

	if q.Get("start_day") != "" {
		start, err := strconv.ParseInt(q.Get("start_day"), 10, 64)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "invalid start_day parameter")
			return
		}
		if start > endDay {
			SendBadRequestResponse(w, r.URL.String(), "start_day must be less than end_day")
			return
		}
		if start > startDay {
			startDay = start
		}
	}

	index, err := strconv.ParseUint(vars["index"], 10, 64)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "invalid validator index")
		return
	}

	rows, err := db.ReaderDb.Query(`
		SELECT 
		validatorindex,
		day,
		start_balance,
		end_balance,
		min_balance,
		max_balance,
		start_effective_balance,
		end_effective_balance,
		min_effective_balance,
		max_effective_balance,
		COALESCE(missed_attestations, 0) AS missed_attestations,
		0 AS orphaned_attestations,
		COALESCE(proposed_blocks, 0) AS proposed_blocks,
		COALESCE(missed_blocks, 0) AS missed_blocks,
		COALESCE(orphaned_blocks, 0) AS orphaned_blocks,
		COALESCE(attester_slashings, 0) AS attester_slashings,
		COALESCE(proposer_slashings, 0) AS proposer_slashings,
		COALESCE(deposits, 0) AS deposits,
		COALESCE(deposits_amount, 0) AS deposits_amount,
		COALESCE(withdrawals, 0) AS withdrawals,
		COALESCE(withdrawals_amount, 0) AS withdrawals_amount,
		COALESCE(participated_sync, 0) AS participated_sync,
		COALESCE(missed_sync, 0) AS missed_sync,
		COALESCE(orphaned_sync, 0) AS orphaned_sync
	FROM validator_stats WHERE validatorindex = $1 and day <= $2 and day >= $3 ORDER BY day DESC`, index, endDay, startDay)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	addDayTime := func(dataEntryMap map[string]interface{}) error {
		day, ok := dataEntryMap["day"].(int64)
		if !ok {
			return fmt.Errorf("error type asserting day as an int")
		} else {
			dataEntryMap["day_start"] = utils.DayToTime(day)
			dataEntryMap["day_end"] = utils.DayToTime(day + 1)
		}
		return nil
	}

	returnQueryResultsAsArray(rows, w, r, addDayTime)
}

// ApiValidatorByEth1Address godoc
// @Summary Get all validators that belong to an eth1 address
// @Tags Validator
// @Produce  json
// @Param  eth1address path string true "Eth1 address from which the validator deposits were sent". It can also be a valid ENS name.
// @Param limit query string false "Limit the number of results (default: 2000)"
// @Param offset query string false "Offset the results (default: 0)"
// @Success 200 {object} types.ApiResponse{data=[]types.ApiValidatorEth1Response}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/eth1/{eth1address} [get]
func ApiValidatorByEth1Address(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")
	q := r.URL.Query()
	limitQuery := q.Get("limit")
	offsetQuery := q.Get("offset")

	limit, err := strconv.ParseInt(limitQuery, 10, 64)
	if err != nil {
		limit = 2000
	}

	offset, err := strconv.ParseInt(offsetQuery, 10, 64)
	if err != nil {
		offset = 0
	}

	if offset < 0 {
		offset = 0
	}

	if limit > (2000+offset) || limit <= 0 || limit <= offset {
		limit = 2000 + offset
	}

	vars := mux.Vars(r)
	search := ReplaceEnsNameWithAddress(vars["address"])
	eth1Address, err := hex.DecodeString(strings.Replace(search, "0x", "", -1))
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "invalid eth1 address provided")
		return
	}

	rows, err := db.ReaderDb.Query("SELECT publickey, validatorindex, valid_signature FROM eth1_deposits LEFT JOIN validators ON eth1_deposits.publickey = validators.pubkey WHERE from_address = $1 GROUP BY publickey, validatorindex, valid_signature ORDER BY validatorindex OFFSET $2 LIMIT $3;", eth1Address, offset, limit)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	returnQueryResultsAsArray(rows, w, r)
}

// ApiValidator godoc
// @Summary Get the income detail history of up to 100 validators
// @Tags Validator
// @Produce  json
// @Param  indexOrPubkey path string true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Param  latest_epoch query int false "The latest epoch to consider in the query"
// @Param  offset query int false "Number of items to skip"
// @Param  limit query int false "Maximum number of items to return, up to 100"
// @Success 200 {object} types.ApiResponse{data=[]types.ApiValidatorIncomeHistoryResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/{indexOrPubkey}/incomedetailhistory [get]
func ApiValidatorIncomeDetailsHistory(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json")

		j := json.NewEncoder(w)
		vars := mux.Vars(r)
		maxValidators := getUserPremium(r).MaxValidators

		latestEpoch, limit, err := getIncomeDetailsHistoryQueryParameters(r.URL.Query())
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), err.Error())
			return
		}

		queryIndices, err := parseApiValidatorParamToIndices(vars["indexOrPubkey"], maxValidators)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), err.Error())
			return
		}

		if len(queryIndices) == 0 {
			SendBadRequestResponse(w, r.URL.String(), "no validators provided")
			return
		}

		history, err := bt.GetValidatorIncomeDetailsHistory(queryIndices, latestEpoch-(limit-1), latestEpoch)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
			return
		}

		responseData := make([]*types.ApiValidatorIncomeHistoryResponse, 0, uint64(len(history))*limit)

		epochsPerWeek := utils.EpochsPerDay() * 7
		for validatorIndex, epochs := range history {
			for epoch, income := range epochs {
				epochAtStartOfTheWeek := (epoch / epochsPerWeek) * epochsPerWeek

				txFeeRewardWei := ""
				if len(income.TxFeeRewardWei) > 0 {
					txFeeRewardWei = new(big.Int).SetBytes(income.TxFeeRewardWei).String()
				}

				responseIncome := &types.ApiValidatorIncomeHistory{
					AttestationSourceReward:            income.AttestationSourceReward,
					AttestationSourcePenalty:           income.AttestationSourcePenalty,
					AttestationTargetReward:            income.AttestationTargetReward,
					AttestationTargetPenalty:           income.AttestationTargetPenalty,
					AttestationHeadReward:              income.AttestationHeadReward,
					FinalityDelayPenalty:               income.FinalityDelayPenalty,
					ProposerSlashingInclusionReward:    income.ProposerSlashingInclusionReward,
					ProposerAttestationInclusionReward: income.ProposerAttestationInclusionReward,
					ProposerSyncInclusionReward:        income.ProposerSyncInclusionReward,
					SyncCommitteeReward:                income.SyncCommitteeReward,
					SyncCommitteePenalty:               income.SyncCommitteePenalty,
					SlashingReward:                     income.SlashingReward,
					SlashingPenalty:                    income.SlashingPenalty,
					TxFeeRewardWei:                     txFeeRewardWei,
					ProposalsMissed:                    income.ProposalsMissed}

				responseData = append(responseData, &types.ApiValidatorIncomeHistoryResponse{
					Income:         responseIncome,
					Epoch:          epoch,
					ValidatorIndex: validatorIndex,
					Week:           epoch / epochsPerWeek,
					WeekStart:      utils.EpochToTime(epochAtStartOfTheWeek),
					WeekEnd:        utils.EpochToTime(epochAtStartOfTheWeek + epochsPerWeek),
				})
			}
		}

		sort.Slice(responseData, func(i, j int) bool {
			if responseData[i].Epoch != responseData[j].Epoch {
				return responseData[i].Epoch > responseData[j].Epoch
			}
			return responseData[i].ValidatorIndex < responseData[j].ValidatorIndex
		})

		response := &types.ApiResponse{}
		response.Status = "OK"

		response.Data = responseData

		err = j.Encode(response)

		if err != nil {
			sendServerErrorResponse(w, r.URL.String(), "could not serialize data results")
			return
		}
	}
}

func getIncomeDetailsHistoryQueryParameters(q url.Values) (uint64, uint64, error) {
	onChainLatestEpoch := services.LatestFinalizedEpoch()
	defaultLimit := uint64(100)

	latestEpoch := onChainLatestEpoch
	if q.Has("latest_epoch") {
		var err error
		latestEpoch, err = strconv.ParseUint(q.Get("latest_epoch"), 10, 64)
		if err != nil || latestEpoch > onChainLatestEpoch {
			return 0, 0, fmt.Errorf("invalid latest epoch parameter")
		}
	}

	if q.Has("offset") {
		offset, err := strconv.ParseUint(q.Get("offset"), 10, 64)
		if err != nil || offset > latestEpoch {
			return 0, 0, fmt.Errorf("invalid offset parameter")
		}
		latestEpoch -= offset
	}

	limit := defaultLimit
	if q.Has("limit") {
		var err error
		limit, err = strconv.ParseUint(q.Get("limit"), 10, 64)
		if err != nil || limit > defaultLimit || limit < 1 {
			return 0, 0, fmt.Errorf("invalid limit parameter")
		}
	}

	return latestEpoch, limit, nil
}

// ApiValidatorWithdrawals godoc
// @Summary Get the withdrawal history of up to 100 validators for the last 100 epochs. To receive older withdrawals modify the epoch paraum
// @Tags Validator
// @Produce  json
// @Param  indexOrPubkey path string true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Param  epoch query int false "the start epoch for the withdrawal history (default: latest epoch)"
// @Success 200 {object} types.ApiResponse{data=[]types.ApiValidatorWithdrawalResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/{indexOrPubkey}/withdrawals [get]
func ApiValidatorWithdrawals(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	maxValidators := getUserPremium(r).MaxValidators

	queryIndices, err := parseApiValidatorParamToIndices(vars["indexOrPubkey"], maxValidators)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), err.Error())
		return
	}

	if len(queryIndices) == 0 {
		SendBadRequestResponse(w, r.URL.String(), "no or invalid validator indicies provided")
	}

	q := r.URL.Query()

	epoch, err := strconv.ParseUint(q.Get("epoch"), 10, 64)
	if err != nil {
		epoch = services.LatestEpoch()
	}

	// startEpoch and endEpoch are both inclusive, so substracting 99 here will result in a limit of 100 epochs
	endEpoch := epoch - 99
	if epoch < 99 {
		endEpoch = 0
	}

	data, err := db.GetValidatorsWithdrawals(queryIndices, endEpoch, epoch)
	if err != nil {
		log.Errorf("error retrieving withdrawals for %v route: %v", r.URL.String(), err)
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}

	dataFormatted := make([]*types.ApiValidatorWithdrawalResponse, 0, len(data))
	for _, w := range data {
		dataFormatted = append(dataFormatted, &types.ApiValidatorWithdrawalResponse{
			Epoch:          w.Slot / utils.Config.Chain.ClConfig.SlotsPerEpoch,
			Slot:           w.Slot,
			Index:          w.Index,
			ValidatorIndex: w.ValidatorIndex,
			Amount:         w.Amount,
			BlockRoot:      fmt.Sprintf("0x%x", w.BlockRoot),
			Address:        fmt.Sprintf("0x%x", w.Address),
		})
	}

	response := &types.ApiResponse{}
	response.Status = "OK"

	response.Data = dataFormatted

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		sendServerErrorResponse(w, r.URL.String(), "could not serialize data results")
		return
	}
}

// ApiValidatorBlsChange godoc
// @Summary Gets the BLS withdrawal address change for up to 100 validators
// @Tags Validator
// @Produce  json
// @Param  indexOrPubkey path string true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Success 200 {object} types.ApiResponse{data=[]types.ApiValidatorBlsChangeResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/{indexOrPubkey}/blsChange [get]
func ApiValidatorBlsChange(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	maxValidators := getUserPremium(r).MaxValidators

	queryIndices, err := parseApiValidatorParamToIndices(vars["indexOrPubkey"], maxValidators)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), err.Error())
		return
	}

	if len(queryIndices) == 0 {
		SendBadRequestResponse(w, r.URL.String(), "no or invalid validator indicies provided")
	}

	data, err := db.GetValidatorsBLSChange(queryIndices)
	if err != nil {
		log.Errorf("error retrieving validators bls change for %v route: %v", r.URL.String(), err)
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}

	dataFormatted := make([]*types.ApiValidatorBlsChangeResponse, 0, len(data))

	for _, d := range data {
		dataFormatted = append(dataFormatted, &types.ApiValidatorBlsChangeResponse{
			Epoch:                    d.Slot / utils.Config.Chain.ClConfig.SlotsPerEpoch,
			Slot:                     d.Slot,
			BlockRoot:                fmt.Sprintf("0x%x", d.BlockRoot),
			Validatorindex:           d.Validatorindex,
			BlsPubkey:                fmt.Sprintf("0x%x", d.BlsPubkey),
			Address:                  fmt.Sprintf("0x%x", d.Address),
			Signature:                fmt.Sprintf("0x%x", d.Signature),
			WithdrawalCredentialsOld: fmt.Sprintf("0x%x", d.WithdrawalCredentialsOld),
			WithdrawalCredentialsNew: fmt.Sprintf("0x"+utils.BeginningOfSetWithdrawalCredentials+"%x", d.Address),
		})
	}

	response := &types.ApiResponse{}
	response.Status = "OK"

	response.Data = dataFormatted

	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		sendServerErrorResponse(w, r.URL.String(), "could not serialize data results")
		return
	}
}

// ApiValidator godoc
// @Summary Get the balance history of up to 100 validators
// @Tags Validator
// @Produce  json
// @Param  indexOrPubkey path string true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Param  latest_epoch query int false "The latest epoch to consider in the query"
// @Param  offset query int false "Number of items to skip"
// @Param  limit query int false "Maximum number of items to return, up to 100"
// @Success 200 {object} types.ApiResponse{data=[]types.ApiValidatorBalanceHistoryResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/{indexOrPubkey}/balancehistory [get]
func ApiValidatorBalanceHistory(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json")

		j := json.NewEncoder(w)
		vars := mux.Vars(r)
		maxValidators := getUserPremium(r).MaxValidators

		latestEpoch, limit, err := getBalanceHistoryQueryParameters(r.URL.Query())
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), err.Error())
			return
		}

		queryIndices, err := parseApiValidatorParamToIndices(vars["indexOrPubkey"], maxValidators)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), err.Error())
			return
		}

		if len(queryIndices) == 0 {
			SendBadRequestResponse(w, r.URL.String(), "no or invalid validator indicies provided")
		}

		history, err := bt.GetValidatorBalanceHistory(queryIndices, latestEpoch-(limit-1), latestEpoch)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
			return
		}

		responseData := make([]*types.ApiValidatorBalanceHistoryResponse, 0, len(history)*101)

		epochsPerWeek := utils.EpochsPerDay() * 7
		for validatorIndex, balances := range history {
			for _, balance := range balances {
				epochAtStartOfTheWeek := (balance.Epoch / epochsPerWeek) * epochsPerWeek
				responseData = append(responseData, &types.ApiValidatorBalanceHistoryResponse{
					Balance:          balance.Balance,
					EffectiveBalance: balance.EffectiveBalance,
					Epoch:            balance.Epoch,
					Validatorindex:   validatorIndex,
					Week:             balance.Epoch / epochsPerWeek,
					WeekStart:        utils.EpochToTime(epochAtStartOfTheWeek),
					WeekEnd:          utils.EpochToTime(epochAtStartOfTheWeek + epochsPerWeek),
				})
			}
		}

		sort.Slice(responseData, func(i, j int) bool {
			if responseData[i].Epoch != responseData[j].Epoch {
				return responseData[i].Epoch > responseData[j].Epoch
			}
			return responseData[i].Validatorindex < responseData[j].Validatorindex
		})

		response := &types.ApiResponse{}
		response.Status = "OK"

		response.Data = responseData

		err = j.Encode(response)

		if err != nil {
			sendServerErrorResponse(w, r.URL.String(), "could not serialize data results")
			return
		}
	}
}

func getBalanceHistoryQueryParameters(q url.Values) (uint64, uint64, error) {
	onChainLatestEpoch := services.LatestEpoch()
	defaultLimit := uint64(100)

	latestEpoch := onChainLatestEpoch
	if q.Has("latest_epoch") {
		var err error
		latestEpoch, err = strconv.ParseUint(q.Get("latest_epoch"), 10, 64)
		if err != nil || latestEpoch > onChainLatestEpoch {
			return 0, 0, fmt.Errorf("invalid latest epoch parameter")
		}
	}

	if q.Has("offset") {
		offset, err := strconv.ParseUint(q.Get("offset"), 10, 64)
		if err != nil || offset > latestEpoch {
			return 0, 0, fmt.Errorf("invalid offset parameter")
		}
		latestEpoch -= offset
	}

	limit := defaultLimit
	if q.Has("limit") {
		var err error
		limit, err = strconv.ParseUint(q.Get("limit"), 10, 64)
		if err != nil || limit > defaultLimit || limit < 1 {
			return 0, 0, fmt.Errorf("invalid limit parameter")
		}
	}

	return latestEpoch, limit, nil
}

// ApiValidatorPerformance godoc
// @Summary Get the current consensus reward performance of up to 100 validators
// @Tags Validator
// @Produce  json
// @Param  indexOrPubkey path string true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Success 200 {object} types.ApiResponse{data=[]types.ApiValidatorPerformanceResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/{indexOrPubkey}/performance [get]
func ApiValidatorPerformance(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json")

		vars := mux.Vars(r)
		maxValidators := getUserPremium(r).MaxValidators

		queryIndices, err := parseApiValidatorParamToIndices(vars["indexOrPubkey"], maxValidators)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), err.Error())
			return
		}

		rows, err := db.ReaderDb.Query(`
		SELECT 
			validators.validatorindex, 
			COALESCE(validator_performance.cl_performance_1d, 0) AS performance1d, 
			COALESCE(validator_performance.cl_performance_7d, 0) AS performance7d, 
			COALESCE(validator_performance.cl_performance_31d, 0) AS performance31d, 
			COALESCE(validator_performance.cl_performance_365d, 0) AS performance365d, 
			COALESCE(validator_performance.cl_performance_total, 0) AS performanceTotal, 
			COALESCE(validator_performance.rank7d, 0) AS rank7d
		FROM validators 
		LEFT JOIN validator_performance ON 
			validators.validatorindex = validator_performance.validatorindex 
		WHERE validators.validatorindex = ANY($1) 
		ORDER BY validatorindex`, pq.Array(queryIndices))
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
			return
		}
		defer rows.Close()

		data, err := utils.SqlRowsToJSON(rows)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "could not parse db results")
			return
		}

		currentDayIncome, err := db.GetCurrentDayClIncome(queryIndices, bt)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "error retrieving current day income")
			return
		}

		latestEpoch := int64(services.LatestFinalizedEpoch())
		latestBalances, err := bt.GetValidatorBalanceHistory(queryIndices, uint64(latestEpoch), uint64(latestEpoch))
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "error retrieving balances")
			return
		}

		// create a map to easily check if a validator is part of data
		validatorIndexMap := make(map[uint64]bool)
		for _, entry := range data {
			eMap, ok := entry.(map[string]interface{})
			if !ok {
				log.Errorf("error converting validator data to map[string]interface{}")
				continue
			}

			validatorIndex, ok := eMap["validatorindex"].(int64)
			if !ok {
				log.Errorf("error converting validatorindex to int64")
				continue
			}

			validatorIndexMap[uint64(validatorIndex)] = true
		}

		// check for recently activated validators that have no performance data yet but already generate income
		for incomeValidatorIndex := range currentDayIncome {
			_, ok := validatorIndexMap[incomeValidatorIndex]
			if !ok {
				// validator not found in data, add minimum set of data
				data = append(data, map[string]interface{}{
					"validatorindex":   int64(incomeValidatorIndex),
					"performancetotal": int64(0), // has to exist and will be updated below
				})
			}
		}

		for _, entry := range data {
			eMap, ok := entry.(map[string]interface{})
			if !ok {
				log.Errorf("error converting validator data to map[string]interface{}")
				continue
			}

			validatorIndex, ok := eMap["validatorindex"].(int64)
			if !ok {
				log.Errorf("error converting validatorindex to int64")
				continue
			}

			eMap["balance"] = latestBalances[uint64(validatorIndex)][0].Balance
			eMap["performancetoday"] = currentDayIncome[uint64(validatorIndex)]
			eMap["performancetotal"] = eMap["performancetotal"].(int64) + currentDayIncome[uint64(validatorIndex)]
		}

		j := json.NewEncoder(w)
		SendOKResponse(j, r.URL.String(), []any{data})
	}
}

// ApiValidatorExecutionPerformance godoc
// @Summary Get the current execution reward performance of up to 100 validators. If block was produced via mev relayer, this endpoint will use the relayer data as block reward instead of the normal block reward.
// @Tags Validator
// @Produce  json
// @Param  indexOrPubkey path string true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Success 200 {object} types.ApiResponse{data=[]types.ApiValidatorExecutionPerformanceResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/{indexOrPubkey}/execution/performance [get]
func ApiValidatorExecutionPerformance(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		j := json.NewEncoder(w)
		vars := mux.Vars(r)
		maxValidators := getUserPremium(r).MaxValidators

		queryIndices, err := parseApiValidatorParamToIndices(vars["indexOrPubkey"], maxValidators)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), err.Error())
			return
		}

		result, err := getValidatorExecutionPerformance(queryIndices, bt)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), err.Error())
			log.WithError(err).Error("can not getValidatorExecutionPerformance")
			return
		}

		SendOKResponse(j, r.URL.String(), []any{result})
	}
}

// ApiValidatorAttestationEffectiveness godoc
// @Summary DEPRECIATED - USE /attestationefficiency (Get the current performance of up to 100 validators)
// @Tags Validator
// @Produce  json
// @Param  indexOrPubkey path string true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Success 200 {object} types.ApiResponse
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/{indexOrPubkey}/attestationeffectiveness [get]
func ApiValidatorAttestationEffectiveness(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json")

		j := json.NewEncoder(w)
		vars := mux.Vars(r)

		maxValidators := getUserPremium(r).MaxValidators

		queryIndices, err := parseApiValidatorParamToIndices(vars["indexOrPubkey"], maxValidators)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), err.Error())
			return
		}

		data, err := getValidatorEffectiveness(services.LatestEpoch()-1, queryIndices, bt)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
			return
		}

		response := &types.ApiResponse{}
		response.Status = "OK"

		response.Data = data

		err = j.Encode(response)

		if err != nil {
			sendServerErrorResponse(w, r.URL.String(), "could not serialize data results")
			return
		}
	}
}

// ApiValidatorAttestationEfficiency godoc
// @Summary Get the current performance of up to 100 validators
// @Tags Validator
// @Produce  json
// @Param  indexOrPubkey path string true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Success 200 {object} types.ApiResponse
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/{indexOrPubkey}/attestationefficiency [get]
func ApiValidatorAttestationEfficiency(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json")

		j := json.NewEncoder(w)
		vars := mux.Vars(r)

		maxValidators := getUserPremium(r).MaxValidators

		queryIndices, err := parseApiValidatorParamToIndices(vars["indexOrPubkey"], maxValidators)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), err.Error())
			return
		}

		data, err := getValidatorEffectiveness(services.LatestEpoch()-1, queryIndices, bt)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
			return
		}

		response := &types.ApiResponse{}
		response.Status = "OK"

		response.Data = data

		err = j.Encode(response)

		if err != nil {
			sendServerErrorResponse(w, r.URL.String(), "could not serialize data results")
			return
		}
	}
}

// ApiValidatorLeaderboard godoc
// @Summary Get the current top 100 performing validators (using the income over the last 7 days)
// @Tags Validator
// @Produce  json
// @Success 200 {object} types.ApiResponse{data=[]types.ApiValidatorPerformanceResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/leaderboard [get]
func ApiValidatorLeaderboard(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")

	rows, err := db.ReaderDb.Query(`
			SELECT 
				balance, 
				COALESCE(validator_performance.cl_performance_1d, 0) AS performance1d, 
				COALESCE(validator_performance.cl_performance_7d, 0) AS performance7d, 
				COALESCE(validator_performance.cl_performance_31d, 0) AS performance31d, 
				COALESCE(validator_performance.cl_performance_365d, 0) AS performance365d, 
				COALESCE(validator_performance.cl_performance_total, 0) AS performanceTotal, 
				rank7d, 
				validatorindex
			FROM validator_performance 
			ORDER BY rank7d ASC LIMIT 100`)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	returnQueryResultsAsArray(rows, w, r)
}

// ApiValidatorDeposits godoc
// @Summary Get all eth1 deposits for up to 100 validators
// @Tags Validator
// @Produce  json
// @Param  indexOrPubkey path string true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Success 200 {object} types.ApiResponse{data=[]types.ApiValidatorDepositsResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/{indexOrPubkey}/deposits [get]
func ApiValidatorDeposits(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	maxValidators := getUserPremium(r).MaxValidators

	pubkeys, err := parseApiValidatorParamToPubkeys(vars["indexOrPubkey"], maxValidators)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), err.Error())
		return
	}

	rows, err := db.ReaderDb.Query(
		`SELECT amount, block_number, block_ts, from_address, merkletree_index, publickey, removed, signature, tx_hash, tx_index, tx_input, valid_signature, withdrawal_credentials FROM eth1_deposits 
		WHERE publickey = ANY($1)`, pubkeys,
	)
	if err != nil {
		log.WithError(err).Error("could not retrieve db results")
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}
	defer rows.Close()

	returnQueryResultsAsArray(rows, w, r)
}

// ApiValidatorAttestations godoc
// @Summary Get all attestations during the last 100 epochs for up to 100 validators
// @Tags Validator
// @Produce  json
// @Param  indexOrPubkey path string true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Success 200 {object} types.ApiResponse{[]types.ApiValidatorAttestationsResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/{indexOrPubkey}/attestations [get]
func ApiValidatorAttestations(bt *db.Bigtable) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {

		w.Header().Set("Content-Type", "application/json")

		j := json.NewEncoder(w)
		vars := mux.Vars(r)
		maxValidators := getUserPremium(r).MaxValidators

		queryIndices, err := parseApiValidatorParamToIndices(vars["indexOrPubkey"], maxValidators)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), err.Error())
			return
		}

		history, err := bt.GetValidatorAttestationHistory(queryIndices, services.LatestEpoch()-99, services.LatestEpoch())
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
			return
		}

		responseData := make([]*types.ApiValidatorAttestationsResponse, 0, len(history)*100)

		epochsPerWeek := utils.EpochsPerDay() * 7
		for validatorIndex, balances := range history {
			for _, attestation := range balances {
				epochAtStartOfTheWeek := (attestation.Epoch / epochsPerWeek) * epochsPerWeek
				responseData = append(responseData, &types.ApiValidatorAttestationsResponse{
					AttesterSlot:   attestation.AttesterSlot,
					CommitteeIndex: 0,
					Epoch:          attestation.Epoch,
					InclusionSlot:  attestation.InclusionSlot,
					Status:         attestation.Status,
					ValidatorIndex: validatorIndex,
					Week:           attestation.Epoch / epochsPerWeek,
					WeekStart:      utils.EpochToTime(epochAtStartOfTheWeek),
					WeekEnd:        utils.EpochToTime(epochAtStartOfTheWeek + epochsPerWeek),
				})
			}
		}

		sort.Slice(responseData, func(i, j int) bool {
			if responseData[i].Epoch != responseData[j].Epoch {
				return responseData[i].Epoch > responseData[j].Epoch
			}
			return responseData[i].ValidatorIndex < responseData[j].ValidatorIndex
		})

		response := &types.ApiResponse{}
		response.Status = "OK"

		response.Data = responseData

		err = j.Encode(response)

		if err != nil {
			sendServerErrorResponse(w, r.URL.String(), "could not serialize data results")
			return
		}
	}
}

// ApiValidatorProposals godoc
// @Summary Get all proposed blocks during the last 100 epochs for up to 100 validators. Optionally set the epoch query parameter to look back further.
// @Tags Validator
// @Produce  json
// @Param  indexOrPubkey path string true "Up to 100 validator indicesOrPubkeys, comma separated"
// @Param  epoch query string false "Page the result by epoch"
// @Success 200 {object} types.ApiResponse{data=[]types.ApiValidatorProposalsResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/{indexOrPubkey}/proposals [get]
func ApiValidatorProposals(w http.ResponseWriter, r *http.Request) {

	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	maxValidators := getUserPremium(r).MaxValidators
	q := r.URL.Query()

	epochQuery := uint64(0)
	if q.Get("epoch") == "" {
		epochQuery = services.LatestEpoch()
	} else {
		var err error
		epochQuery, err = strconv.ParseUint(q.Get("epoch"), 10, 64)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), err.Error())
			return
		}
	}

	queryIndices, err := parseApiValidatorParamToIndices(vars["indexOrPubkey"], maxValidators)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), err.Error())
		return
	}
	if epochQuery < 100 {
		epochQuery = 100
	}

	rows, err := db.ReaderDb.Query(`
	SELECT 
		b.epoch,
		b.slot,
		b.blockroot,
		b.parentroot,
		b.stateroot,
		b.signature,
		b.attestationscount,
		b.attesterslashingscount,
		b.depositscount,
		b.eth1data_blockhash,
		b.eth1data_depositcount,
		b.eth1data_depositroot,
		b.exec_base_fee_per_gas,
		b.exec_block_hash,
		b.exec_block_number,
		b.exec_extra_data,
		b.exec_fee_recipient,
		b.exec_gas_limit,
		b.exec_gas_used,
		b.exec_logs_bloom,
		b.exec_parent_hash,
		b.exec_random,
		b.exec_receipts_root,
		b.exec_state_root,
		b.exec_timestamp,
		b.exec_transactions_count,
		b.graffiti,
		b.graffiti_text,
		b.proposer,
		b.proposerslashingscount,
		b.randaoreveal,
		b.status,
		b.syncaggregate_bits,
		b.syncaggregate_participation,
		b.syncaggregate_signature,
		b.voluntaryexitscount
	FROM blocks as b 
	LEFT JOIN validators ON validators.validatorindex = b.proposer 
	WHERE (proposer = ANY($1)) and epoch <= $2 AND epoch >= $3 
	ORDER BY proposer, epoch desc, slot desc`, pq.Array(queryIndices), epochQuery, epochQuery-100)
	if err != nil {
		log.Errorf("could not retrieve db results: %v", err)
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}

	returnQueryResultsAsArray(rows, w, r)
}


type PremiumUser struct {
	Package                string
	MaxValidators          int
	MaxStats               uint64
	MaxNodes               uint64
	WidgetSupport          bool
	NotificationThresholds bool
	NoAds                  bool
}

func getUserPremium(r *http.Request) PremiumUser {
	var pkg string = ""

	if strings.HasPrefix(r.URL.Path, "/api/") {
		claims := getAuthClaims(r)
		if claims != nil {
			pkg = claims.Package
		}
	} else {
		sessionUser := getUser(r)
		if sessionUser.Authenticated {
			pkg = sessionUser.Subscription
		}
	}

	return GetUserPremiumByPackage(pkg)
}

func GetUserPremiumByPackage(pkg string) PremiumUser {
	result := PremiumUser{
		Package:                "standard",
		MaxValidators:          100,
		MaxStats:               180,
		MaxNodes:               1,
		WidgetSupport:          false,
		NotificationThresholds: false,
		NoAds:                  false,
	}

	pkg = utils.MapProductV2ToV1(pkg)

	if pkg == "" || pkg == "standard" {
		return result
	}

	result.Package = pkg
	result.MaxStats = 43200
	result.NotificationThresholds = true
	result.NoAds = true

	if result.Package != "plankton" {
		result.WidgetSupport = true
	}

	if result.Package == "goldfish" {
		result.MaxNodes = 2
	}
	if result.Package == "whale" {
		result.MaxValidators = 300
		result.MaxNodes = 10
	}

	return result
}

func parseUintWithDefault(input string, defaultValue uint64) uint64 {
	result, error := strconv.ParseUint(input, 10, 64)
	if error != nil {
		return defaultValue
	}
	return result
}

// ApiWithdrawalCredentialsValidators godoc
// @Summary Get validator indexes and pubkeys of a withdrawal credential or eth1 address
// @Tags Validator
// @Description Returns the validator indexes and pubkeys of a withdrawal credential or eth1 address
// @Produce json
// @Param withdrawalCredentialsOrEth1address path string true "Provide a withdrawal credential or an eth1 address with an optional 0x prefix". It can also be a valid ENS name.
// @Param  limit query int false "Limit the number of results, maximum: 200" default(10)
// @Param offset query int false "Offset the number of results" default(0)
// @Success 200 {object} types.ApiResponse{data=[]types.ApiWithdrawalCredentialsResponse}
// @Failure 400 {object} types.ApiResponse
// @Router /api/v1/validator/withdrawalCredentials/{withdrawalCredentialsOrEth1address} [get]
func ApiWithdrawalCredentialsValidators(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")

	vars := mux.Vars(r)
	q := r.URL.Query()

	credentialsOrAddressString := ReplaceEnsNameWithAddress(vars["withdrawalCredentialsOrEth1address"])
	credentialsOrAddressString = strings.ToLower(credentialsOrAddressString)

	if !utils.IsValidEth1Address(credentialsOrAddressString) &&
		!utils.IsValidWithdrawalCredentials(credentialsOrAddressString) {
		SendBadRequestResponse(w, r.URL.String(), "invalid withdrawal credentials or eth1 address provided")
		return
	}

	credentialsOrAddress := common.FromHex(credentialsOrAddressString)

	credentials, err := utils.AddressToWithdrawalCredentials(credentialsOrAddress)
	if err != nil {
		// Input is not an address so it must already be withdrawal credentials
		credentials = [][]byte{credentialsOrAddress}
	}

	limitQuery := q.Get("limit")
	offsetQuery := q.Get("offset")

	offset := parseUintWithDefault(offsetQuery, 0)
	limit := parseUintWithDefault(limitQuery, 10)

	// We set a max limit to limit the request call time.
	var maxLimit uint64 = utilMath.MaxU64(200, uint64(getUserPremium(r).MaxValidators))

	limit = utilMath.MinU64(limit, maxLimit)

	result := []struct {
		Index  uint64 `db:"validatorindex"`
		Pubkey []byte `db:"pubkey"`
	}{}

	err = db.ReaderDb.Select(&result, `
	SELECT
		validatorindex,
		pubkey
	FROM validators
	WHERE withdrawalcredentials = $1
	ORDER BY validatorindex ASC
	LIMIT $2
	OFFSET $3
	`, credentials, limit, offset)

	if err != nil {
		log.Warnf("error retrieving validator data from db: %v", err)
		SendBadRequestResponse(w, r.URL.String(), "could not retrieve db results")
		return
	}

	response := make([]*types.ApiWithdrawalCredentialsResponse, 0, len(result))
	for _, validator := range result {
		response = append(response, &types.ApiWithdrawalCredentialsResponse{
			Publickey:      fmt.Sprintf("%#x", validator.Pubkey),
			ValidatorIndex: validator.Index,
		})
	}

	SendOKResponse(json.NewEncoder(w), r.URL.String(), []interface{}{response})
}

// ApiProposalLuck godoc
// @Summary Get the proposal luck of a validator or a list of validators
// @Tags Validator
// @Description Returns the proposal luck of a validator or a list of validators
// @Produce json
// @Param validators query string true "Provide a comma separated list of validator indices or pubkeys"
// @Success 200 {object} types.ApiResponse{data=[]types.ApiProposalLuckResponse}
// @Failure 400 {object} types.ApiResponse
// @Failure 500 {object} types.ApiResponse
// @Router /api/v1/validators/proposalLuck [get]
func ApiProposalLuck(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	q := r.URL.Query()
	response := &types.ApiResponse{}
	response.Status = "OK"

	indices, pubkeys, err := parseValidatorsFromQueryString(q.Get("validators"), 100)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "could not parse validators")
		return
	}
	if len(pubkeys) > 0 {
		indicesFromPubkeys, err := resolveIndices(pubkeys)
		if err != nil {
			SendBadRequestResponse(w, r.URL.String(), "could not resolve pubkeys to indices")
			return
		}
		indices = append(indices, indicesFromPubkeys...)
	}

	if len(indices) == 0 {
		SendBadRequestResponse(w, r.URL.String(), "no validators provided")
		return
	}

	// dedup indices
	allKeys := make(map[uint64]bool)
	list := []uint64{}
	for _, item := range indices {
		if _, ok := allKeys[item]; !ok {
			allKeys[item] = true
			list = append(list, item)
		}
	}
	indices = list
	data, err := getProposalLuckStats(indices)
	if err != nil {
		sendServerErrorResponse(w, r.URL.String(), "error processing request, please try again later")
		utils.LogError(err, "error retrieving data from db for proposal luck", 0, map[string]interface{}{"request": r.Method + " " + r.URL.String()})
	}

	response.Data = data
	err = json.NewEncoder(w).Encode(response)
	if err != nil {
		sendServerErrorResponse(w, r.URL.String(), "could not serialize data results")
		utils.LogError(err, "error serializing json data for API", 0, map[string]interface{}{"request": r.Method + " " + r.URL.String()})
	}
}

func getProposalLuckStats(indices []uint64) (*types.ApiProposalLuckResponse, error) {
	data := types.ApiProposalLuckResponse{}
	g := errgroup.Group{}

	var firstActivationEpoch uint64
	g.Go(func() error {
		return db.GetFirstActivationEpoch(indices, &firstActivationEpoch)
	})

	var slots []uint64
	g.Go(func() error {
		return db.ReaderDb.Select(&slots, `
			SELECT
				slot
			FROM blocks
			WHERE proposer = ANY($1)
			AND exec_block_number IS NOT NULL
			ORDER BY slot ASC`, pq.Array(indices))
	})

	err := g.Wait()
	if err != nil {
		return nil, err
	}

	proposalLuck, proposalTimeFrame := getProposalLuck(slots, len(indices), firstActivationEpoch)
	if proposalLuck > 0 {
		data.ProposalLuck = &proposalLuck
		timeframeName := getProposalTimeframeName(proposalTimeFrame)
		data.TimeFrameName = &timeframeName
	}

	avgProposalInterval := getAvgSlotInterval(len(indices))
	data.AverageProposalInterval = avgProposalInterval

	var estimateLowerBoundSlot *uint64
	if len(slots) > 0 {
		estimateLowerBoundSlot = &slots[len(slots)-1]
	} else if len(indices) == 1 {
		activationSlot := firstActivationEpoch * utils.Config.Chain.ClConfig.SlotsPerEpoch
		estimateLowerBoundSlot = &activationSlot
	}

	if estimateLowerBoundSlot != nil {
		nextProposalEstimate := utils.SlotToTime(*estimateLowerBoundSlot + uint64(avgProposalInterval)).Unix()
		data.NextProposalEstimateTs = &nextProposalEstimate
	}
	return &data, nil
}

func getAuthClaims(r *http.Request) *utils.CustomClaims {
	middleWare := gorillacontext.Get(r, utils.MobileAuthorizedKey)
	if middleWare == nil {
		return utils.GetAuthorizationClaims(r)
	}

	claims := gorillacontext.Get(r, utils.ClaimsContextKey)
	if claims == nil {
		return nil
	}
	return claims.(*utils.CustomClaims)
}

// Saves the result of a query converted to JSON in the response writer.
// An arbitrary amount of functions adjustQueryEntriesFuncs can be added to adjust the JSON response.
func returnQueryResults(rows *sql.Rows, w http.ResponseWriter, r *http.Request, adjustQueryEntriesFuncs ...func(map[string]interface{}) error) {
	j := json.NewEncoder(w)
	data, err := utils.SqlRowsToJSON(rows)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "could not parse db results")
		return
	}

	err = adjustQueryResults(data, adjustQueryEntriesFuncs...)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "could not adjust query results")
		return
	}

	SendOKResponse(j, r.URL.String(), data)
}

// Saves the result of a query converted to JSON in the response writer as an array.
// An arbitrary amount of functions adjustQueryEntriesFuncs can be added to adjust the JSON response.
func returnQueryResultsAsArray(rows *sql.Rows, w http.ResponseWriter, r *http.Request, adjustQueryEntriesFuncs ...func(map[string]interface{}) error) {
	data, err := utils.SqlRowsToJSON(rows)

	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "could not parse db results")
		return
	}

	err = adjustQueryResults(data, adjustQueryEntriesFuncs...)
	if err != nil {
		SendBadRequestResponse(w, r.URL.String(), "could not adjust query results")
		return
	}

	response := &types.ApiResponse{
		Status: "OK",
		Data:   data,
	}

	err = json.NewEncoder(w).Encode(response)

	if err != nil {
		sendServerErrorResponse(w, r.URL.String(), "could not serialize data results")
		log.Errorf("error serializing json data for API %v route: %v", r.URL.String(), err)
	}
}

func adjustQueryResults(data []interface{}, adjustQueryEntriesFuncs ...func(map[string]interface{}) error) error {
	for _, dataEntry := range data {
		dataEntryMap, ok := dataEntry.(map[string]interface{})
		if !ok {
			return fmt.Errorf("error type asserting query results as a map")
		} else {
			for _, f := range adjustQueryEntriesFuncs {
				if err := f(dataEntryMap); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func SendBadRequestResponse(w http.ResponseWriter, route, message string) {
	sendErrorWithCodeResponse(w, route, message, http.StatusBadRequest)
}

func sendServerErrorResponse(w http.ResponseWriter, route, message string) {
	sendErrorWithCodeResponse(w, route, message, http.StatusInternalServerError)
}

func sendErrorWithCodeResponse(w http.ResponseWriter, route, message string, errorcode int) {
	w.WriteHeader(errorcode)
	j := json.NewEncoder(w)
	response := &types.ApiResponse{}
	response.Status = "ERROR: " + message
	err := j.Encode(response)

	if err != nil {
		log.Errorf("error serializing json error for API %v route: %v", route, err)
	}
}

func SendOKResponse(j *json.Encoder, route string, data []interface{}) {
	response := &types.ApiResponse{}
	response.Status = "OK"

	if len(data) == 1 {
		response.Data = data[0]
	} else {
		response.Data = data
	}
	err := j.Encode(response)

	if err != nil {
		log.Errorf("error serializing json data for API %v route: %v", route, err)
	}
}

func parseApiValidatorParamToIndices(origParam string, limit int) (indices []uint64, err error) {
	var pubkeys pq.ByteaArray
	params := strings.Split(origParam, ",")
	if len(params) > limit {
		return nil, fmt.Errorf("only a maximum of %d query parameters are allowed", limit)
	}
	for _, param := range params {
		if strings.Contains(param, "0x") || len(param) == 96 {
			pubkey, err := hex.DecodeString(strings.Replace(param, "0x", "", -1))
			if err != nil {
				return nil, fmt.Errorf("invalid validator-parameter")
			}
			pubkeys = append(pubkeys, pubkey)
		} else {
			index, err := strconv.ParseUint(param, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid validator-parameter: %v", param)
			}
			if index < db.MaxSqlInteger {
				indices = append(indices, index)
			}
		}
	}

	var queryIndicesDeduped []uint64
	queryIndicesDeduped = append(queryIndicesDeduped, indices...)
	if len(pubkeys) != 0 {
		indicesFromPubkeys := []uint64{}
		err = db.ReaderDb.Select(&indicesFromPubkeys, "SELECT validatorindex FROM validators WHERE pubkey = ANY($1)", pubkeys)

		if err != nil {
			return nil, err
		}

		indices = append(indices, indicesFromPubkeys...)

		m := make(map[uint64]uint64)
		for _, x := range indices {
			m[x] = x
		}
		for x := range m {
			queryIndicesDeduped = append(queryIndicesDeduped, x)
		}
	}

	if len(queryIndicesDeduped) == 0 {
		return nil, fmt.Errorf("invalid validator argument, pubkey(s) did not resolve to a validator index")
	}

	return queryIndicesDeduped, nil
}

func parseApiValidatorParamToPubkeys(origParam string, limit int) (pubkeys pq.ByteaArray, err error) {
	var indices pq.Int64Array
	params := strings.Split(origParam, ",")
	if len(params) > limit {
		return nil, fmt.Errorf("only a maximum of 100 query parameters are allowed")
	}
	for _, param := range params {
		if strings.Contains(param, "0x") || len(param) == 96 {
			pubkey, err := hex.DecodeString(strings.Replace(param, "0x", "", -1))
			if err != nil {
				return nil, fmt.Errorf("invalid validator-parameter")
			}
			pubkeys = append(pubkeys, pubkey)
		} else {
			index, err := strconv.ParseUint(param, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid validator-parameter: %v", param)
			}
			indices = append(indices, int64(index))
		}
	}

	var queryIndicesDeduped pq.ByteaArray
	queryIndicesDeduped = append(queryIndicesDeduped, pubkeys...)
	if len(indices) != 0 {
		var pubkeysFromIndices pq.ByteaArray
		err = db.ReaderDb.Select(&pubkeysFromIndices, "SELECT pubkey FROM validators WHERE validatorindex = ANY($1)", indices)

		if err != nil {
			return nil, err
		}

		pubkeys = append(pubkeys, pubkeysFromIndices...)

		m := make(map[string][]byte)
		for _, x := range pubkeys {
			m[string(x)] = x
		}
		for _, x := range m {
			queryIndicesDeduped = append(queryIndicesDeduped, x)
		}
	}

	if len(queryIndicesDeduped) == 0 {
		return nil, fmt.Errorf("invalid validator argument, pubkey(s) did not resolve to a validator index")
	}

	return queryIndicesDeduped, nil
}