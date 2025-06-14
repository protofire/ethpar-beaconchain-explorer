package handlers

import (
	// "github.com/protofire/ethpar-beaconchain-explorer/db"

	"fmt"
	"net/http"

	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/templates"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
	// "strings"
)

func Pools(w http.ResponseWriter, r *http.Request) {
	templateFiles := append(layoutTemplateFiles,
		"pools/pools.html",
		"pools/loadingSvg.html",
		"pools/charts.html")
	var poolsServicesTemplate = templates.GetTemplate(templateFiles...)

	w.Header().Set("Content-Type", "text/html")

	data := InitPageData(w, r, "services", "/pools", "Staking Pools Services Overview", templateFiles)

	poolsData, err := poolsPageData()
	if err != nil {
		logger.Errorf("unable to retrieve data for %v route", r.URL.String())
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}
	data.Data = poolsData

	if handleTemplateError(w, r, "pools.go", "Pools", "Done", poolsServicesTemplate.ExecuteTemplate(w, "layout", data)) != nil {
		return // an error has occurred and was processed
	}
}

func poolsPageData() (*types.PoolsData, error) {
	data := &types.PoolsData{}

	cpd := services.LatestChartsPageData()
	var distributionData *types.GenericChartData
	var performanceData *types.GenericChartData

	for _, chart := range cpd {
		if chart.Path == "pools_distribution" {
			distributionData = chart.Data
		}
		if chart.Path == "historic_pool_performance" {
			performanceData = chart.Data
		}
	}

	if distributionData == nil || performanceData == nil {
		return nil, fmt.Errorf("unable to retrieve distribution or performance data")
	}

	data.PoolsResp = services.LatestPoolsPageData()

	data.PoolsDistribution.Data = distributionData
	data.PoolsDistribution.Height = 500
	data.PoolsDistribution.Path = "pools_distribution"

	data.HistoricPoolPerformance.Data = performanceData
	data.HistoricPoolPerformance.Height = 500
	data.HistoricPoolPerformance.Path = "historic_pool_performance"

	data.Disclaimer = services.EthStoreDisclaimer()

	return data, nil
}
