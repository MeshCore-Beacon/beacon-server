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

// getSignalStats godoc
//
// @Summary Reception signal distributions and hourly trends
// @Description Aggregates stored observer receptions in [since, until), at most 30 days. SNR is dB; RSSI is dBm. Null/non-finite readings and the zero/zero unavailable sentinel are excluded per metric; actual zero SNR with nonzero RSSI remains valid. Averages are null without samples. Histogram bounds are lower-inclusive/upper-exclusive, with null for unbounded ends. Both boundaries round down to UTC hours and the response reports that effective window. Reads the hourly signal rollup; an hour appears only after it is rolled, about 95 minutes after it ends, and hours without data are omitted. These last-hop readings do not measure end-to-end quality or packet loss.
// @Tags Stats
// @Produce json
// @Param since query int true "Inclusive start, epoch milliseconds (0 through 253402300799999)"
// @Param until query int true "Exclusive end, epoch milliseconds; at most 30 days after since"
// @Param iatas query string false "Comma-separated reception IATA codes"
// @Param regionId query int false "Region ID, expands to member IATAs"
// @Param region query string false "Region slug, expands to member IATAs"
// @Success 200 {object} api.SignalStats
// @Failure 400 {object} handlers.APIError
// @Failure 500 {object} handlers.APIError
// @Failure 503 {object} handlers.APIError
// @Router /stats/signal [get]
func getSignalStats(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		since, until, err := parseStatsWindow(r, distributionWindow)
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
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""}
			}
		}
		stats, err := reader.GetSignalStats(ctx, since, until, iatas)
		switch {
		case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
			respondError(w, http.StatusServiceUnavailable, "signal query timed out; try a shorter time period")
		case err != nil:
			if r.Context().Err() != nil {
				return
			}
			slog.Error("signal stats failed", "error", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
		default:
			respond(w, http.StatusOK, stats)
		}
	}
}
