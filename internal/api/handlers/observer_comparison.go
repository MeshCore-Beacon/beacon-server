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
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// getObserverComparison godoc
//
// @Summary Compare flood packets reported by two observers
// @Description Counts distinct retained packet hashes with route type 0 or 1, using reception time in [since, until). onlyA, onlyB and both partition totalPackets (the union). Repeated receptions count once. These are reported receptions, not a measure of radio packet loss or continuous observer coverage.
// @Tags Stats
// @Produce json
// @Param observerA query string true "First observer UUID"
// @Param observerB query string true "Second, distinct observer UUID"
// @Param since query int true "Inclusive start, epoch milliseconds (0 through 253402300799999)"
// @Param until query int true "Exclusive end, epoch milliseconds; later than since"
// @Param iatas query string false "Comma-separated reception IATA codes"
// @Param regionId query int false "Region ID, expands to member IATAs"
// @Param region query string false "Region slug, expands to member IATAs"
// @Success 200 {object} api.ObserverComparison
// @Failure 400 {object} handlers.APIError
// @Failure 404 {object} handlers.APIError
// @Failure 500 {object} handlers.APIError
// @Failure 503 {object} handlers.APIError
// @Router /stats/observer-comparison [get]
func getObserverComparison(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		for _, key := range []string{"observerA", "observerB", "since", "until"} {
			if len(q[key]) != 1 || q.Get(key) == "" {
				respondError(w, http.StatusBadRequest, key+" must be supplied exactly once")
				return
			}
		}
		a, errA := uuid.Parse(q.Get("observerA"))
		b, errB := uuid.Parse(q.Get("observerB"))
		if errA != nil || errB != nil || a == uuid.Nil || b == uuid.Nil || a == b {
			respondError(w, http.StatusBadRequest, "observerA and observerB must be distinct, nonzero UUIDs")
			return
		}
		since, errSince := strconv.ParseInt(q.Get("since"), 10, 64)
		until, errUntil := strconv.ParseInt(q.Get("until"), 10, 64)
		if errSince != nil || errUntil != nil || since < 0 || until <= since || until > 253402300799999 {
			respondError(w, http.StatusBadRequest, "since and until must be epoch milliseconds between 0 and 253402300799999, with until later than since")
			return
		}
		// Bound database work even for arbitrary historical windows. Cancellation
		// also propagates when a user changes filters or leaves the page.
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
				// An empty selected region must not silently become a global query.
				iatas = []string{""}
			}
		}
		comparison, err := reader.GetObserverComparison(ctx, a, b, time.UnixMilli(since), time.UnixMilli(until), iatas)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			respondError(w, http.StatusNotFound, "observer not found")
		case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
			respondError(w, http.StatusServiceUnavailable, "comparison timed out; try a shorter time period")
		case err != nil:
			if r.Context().Err() != nil {
				return
			}
			slog.Error("observer comparison failed", "error", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
		default:
			respond(w, http.StatusOK, comparison)
		}
	}
}
