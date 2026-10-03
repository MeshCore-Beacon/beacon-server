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

const defaultSeriesWindow = 90 * 24 * time.Hour

// getStatsSeries godoc
//
// @Summary Hourly network activity from the analytics rollups
// @Description One entry per UTC hour in [since, until) plus a summary over the complete hours. Both bounds round down to UTC hours and since is clamped to the rollup retention; the response reports the effective window. An hour is rolled about 95 minutes after it closes, so recent hours are "missing" until then; "partial" hours lost raw rows before they could be rolled and never get values. Counts (observations, packets) sum across hours, with a packet counted once in each hour it was heard; observers, IATAs and scopes are distinct across the whole window; averages are sum / samples. revision changes whenever any rolled hour changes.
// @Tags Stats
// @Produce json
// @Param since query int true "Inclusive start, epoch milliseconds"
// @Param until query int true "Exclusive end, epoch milliseconds; the window is at most the rollup retention (default 90 days)"
// @Param iatas query string false "Comma-separated reception IATA codes"
// @Param regionId query int false "Region ID, expands to member IATAs"
// @Param region query string false "Region slug, expands to member IATAs"
// @Success 200 {object} api.StatsSeries
// @Failure 400 {object} handlers.APIError
// @Failure 500 {object} handlers.APIError
// @Failure 503 {object} handlers.APIError
// @Router /stats/series [get]
func getStatsSeries(reader api.Reader, maxWindow time.Duration) http.HandlerFunc {
	if maxWindow <= 0 {
		maxWindow = defaultSeriesWindow
	}
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		since, until, err := parseStatsWindow(r, maxWindow)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		if oldest := time.Now().UTC().Add(-maxWindow).Truncate(time.Hour); since.Before(oldest) {
			since = oldest
		}
		if !since.Before(until) {
			since = until
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		iatas := parseIATAs(r)
		if q.Get("regionId") != "" || q.Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(ctx, q.Get("regionId"), q.Get("region"), reader)
			if err != nil {
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""} // an empty region matches nothing, not everything
			}
		}
		series, err := reader.GetStatsSeries(ctx, since, until, iatas)
		switch {
		case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
			respondError(w, http.StatusServiceUnavailable, "series query timed out; try a shorter time period")
		case err != nil:
			if r.Context().Err() != nil {
				return
			}
			slog.Error("stats series failed", "error", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
		default:
			respond(w, http.StatusOK, series)
		}
	}
}
