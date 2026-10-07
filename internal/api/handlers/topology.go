// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package handlers

import (
	"context"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"net/http"
	"time"
)

// getTopologyLinks godoc
// @Summary Unique adjacent route links for a topology snapshot
// @Description Returns up to 100000 undirected node-ID pairs, deduplicated across known routes. Minute-aligned window includes the current minute. Existing rate limits apply; no per-node lookup or route pagination is needed. Capped is true when more links exist.
// @Tags Routes
// @Produce json
// @Param window query string false "History window: 15m (default), 1h, or 24h"
// @Param iatas query string false "Comma-separated reception IATA codes"
// @Success 200 {object} api.TopologyLinks
// @Failure 400 {object} handlers.APIError
// @Failure 503 {object} handlers.APIError
// @Router /routes/topology [get]
func getTopologyLinks(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		window := r.URL.Query().Get("window")
		if window == "" {
			window = "15m"
		}
		duration, ok := map[string]time.Duration{"15m": 15 * time.Minute, "1h": time.Hour, "24h": 24 * time.Hour}[window]
		if !ok {
			respondError(w, http.StatusBadRequest, "window must be 15m, 1h or 24h")
			return
		}
		until := time.Now().UTC().Truncate(time.Minute).Add(time.Minute)
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		links, err := reader.GetTopologyLinks(ctx, parseIATAs(r), until.Add(-duration), until)
		if err != nil {
			respondError(w, http.StatusServiceUnavailable, "route snapshot unavailable; try again shortly")
			return
		}
		respond(w, http.StatusOK, links)
	}
}
