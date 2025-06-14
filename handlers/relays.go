package handlers

import (
	"net/http"

	"github.com/protofire/ethpar-beaconchain-explorer/services"
	"github.com/protofire/ethpar-beaconchain-explorer/templates"
	"github.com/protofire/ethpar-beaconchain-explorer/types"
)

func Relays(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html")
	templateFiles := append(layoutTemplateFiles, "relays.html")
	var relaysServicesTemplate = templates.GetTemplate(templateFiles...)

	data := InitPageData(w, r, "services", "/relays", "Relay Overview", templateFiles)

	relayData := services.LatestRelaysPageData()

	if relayData == nil {
		data.Data = types.RelaysResp{} //need to add a dummy data to prevent template to crash
	} else {
		data.Data = relayData
	}

	if handleTemplateError(w, r, "relays.go", "Relays", "", relaysServicesTemplate.ExecuteTemplate(w, "layout", data)) != nil {
		return // an error has occurred and was processed
	}
}
