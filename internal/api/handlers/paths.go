// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

// getPathStats godoc
//
// @Summary Received path-entry and hash-width distributions
// @Description Counts stored receptions in [since, until), at most 30 days. Hash widths count only validated nonempty ordinary header paths (1/2/3 bytes). Empty paths do not vote for a width. TRACE header paths contain signal readings and are separate. Missing payload type or inconsistent/unsupported path metadata is unclassified. The four categories partition total receptions. Flood paths accumulate entries; direct paths contain remaining entries, so counts do not measure distance or a complete traversed route. UTC hourly buckets are clipped to the window; missing hours are omitted.
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
		for _, key := range []string{"since", "until"} {
			if len(q[key]) != 1 || q.Get(key) == "" {
				respondError(w, http.StatusBadRequest, key+" must be supplied exactly once")
				return
			}
		}
		since, errSince := strconv.ParseInt(q.Get("since"), 10, 64)
		until, errUntil := strconv.ParseInt(q.Get("until"), 10, 64)
		if errSince != nil || errUntil != nil || since < 0 || until <= since || until > 253402300799999 || until-since > int64((30*24*time.Hour)/time.Millisecond) {
			respondError(w, http.StatusBadRequest, "since and until must be epoch milliseconds between 0 and 253402300799999, with a positive window of at most 30 days")
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
		stats, err := reader.GetPathStats(ctx, time.UnixMilli(since), time.UnixMilli(until), iatas)
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
