package handlers

import (
	"net/http"

	"github.com/protofire/ethpar-beaconchain-explorer/db"
	"github.com/protofire/ethpar-beaconchain-explorer/templates"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	"github.com/protofire/ethpar-beaconchain-explorer/utils"
)

// StakingCalculator renders stakingCalculatorTemplate
func StakingCalculator(w http.ResponseWriter, r *http.Request) {
	templateFiles := append(layoutTemplateFiles, "calculator.html")
	var stakingCalculatorTemplate = templates.GetTemplate(templateFiles...)

	calculatorPageData := types.StakingCalculatorPageData{}

	total, err := db.GetTotalEligibleEther()
	if err != nil {
		logger.WithError(err).Error("error getting total staked ether")
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	calculatorPageData.TotalStaked = total
	calculatorPageData.EtherscanApiBaseUrl = utils.GetEtherscanAPIBaseUrl(true)

	w.Header().Set("Content-Type", "text/html")

	data := InitPageData(w, r, "stats", "/calculator", "Staking calculator", templateFiles)
	data.Data = calculatorPageData

	if handleTemplateError(w, r, "calculator.go", "StakingCalculator", "", stakingCalculatorTemplate.ExecuteTemplate(w, "layout", data)) != nil {
		return // an error has occurred and was processed
	}
}
