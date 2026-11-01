// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"net/http"
	"strconv"
)

// getScopeActivity godoc
// @Summary Recent scope evidence, receiver areas and repeat activity
// @Description Counts cover non-TRACE FLOOD/TRANSPORT_FLOOD observations; direct traffic and traces are excluded. Window is the current partial hour plus prior hourly buckets. uniqueSamples deduplicates equal packet/path/receiving-area/hour samples across observers, not RF transmissions. Missing hours have no evidence. Country is the MQTT receiving area's country, not origin or border direction. Existing ingest geographic restrictions apply.
// @Tags Scopes
// @Produce json
// @Param hours query int false "Hourly window" Enums(1,6,24)
// @Param iatas query string false "Comma-separated receiving IATAs; repeater counts use nodes heard in those IATAs"
// @Success 200 {object} api.ScopeActivity
// @Failure 400 {object} handlers.APIError
// @Failure 500 {object} handlers.APIError
// @Router /scopes/activity [get]
func getScopeActivity(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		hours := 24
		if raw := r.URL.Query().Get("hours"); raw != "" {
			v, err := strconv.Atoi(raw)
			if err != nil || (v != 1 && v != 6 && v != 24) {
				respondError(w, http.StatusBadRequest, "hours must be 1, 6 or 24")
				return
			}
			hours = v
		}
		rows, err := reader.GetScopeActivity(r.Context(), parseIATAs(r), hours)
		if err != nil {
			respondReaderError(w, err, "failed to load scope activity")
			return
		}
		respond(w, http.StatusOK, rows)
	}
}
