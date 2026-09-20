// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

// getPathStats godoc
//
// @Summary Received path-entry and hash-width distributions
// @Description Counts stored receptions in [since, until), at most 30 days. Hash widths count only validated nonempty ordinary header paths (1/2/3 bytes). Empty paths do not vote for a width. TRACE header paths contain signal readings and are separate. Missing payload type or inconsistent/unsupported path metadata is unclassified. The four categories partition total receptions. Flood paths accumulate entries; direct paths contain remaining entries, so counts do not measure distance or a complete traversed route. Both boundaries round down to UTC hours and the response reports that effective window. Reads use materialized snapshots refreshed by background.view_refresh; the current partial hour is excluded and missing hours are omitted.
// @Tags Stats
// @Produce json
// @Param since query int true "Inclusive start, epoch milliseconds (0 through 253402300799999)"
// @Param until query int true "Exclusive end, epoch milliseconds; at most 30 days after since"
// @Param iatas query string false "Comma-separated reception IATA codes"
// @Param regionId query int false "Region ID, expands to member IATAs"
// @Param region query string false "Region slug, expands to member IATAs"
// @Success 200 {object} api.PathStats
// @Failure 400 {object} handlers.APIError
// @Failure 500 {object} handlers.APIError
// @Failure 503 {object} handlers.APIError
// @Router /stats/paths [get]
func getPathStats(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		since, until, err := parseStatsWindow(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		iatas := parseIATAs(r)
		if q.Get("regionId") != "" || q.Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(ctx, q.Get("regionId"), q.Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, "region not found")
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""}
			}
		}
		stats, err := reader.GetPathStats(ctx, since, until, iatas)
		switch {
		case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
			respondError(w, http.StatusServiceUnavailable, "path query timed out; try a shorter time period")
		case err != nil:
			if r.Context().Err() != nil {
				return
			}
			slog.Error("path stats failed", "error", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
		default:
			respond(w, http.StatusOK, stats)
		}
	}
}
