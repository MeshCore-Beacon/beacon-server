// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// DBStatementsReader reads statement statistics grouped by sqlc name.
type DBStatementsReader interface {
	DBStatements(context.Context) ([]api.DBStatement, error)
}

// PlanguardSource holds the daily plan regression check.
type PlanguardSource interface {
	Last() (api.PlanguardReport, error) // api.ErrPlanguardNotRun before the first run; api.ErrPlanguardDisabled when off
	RunNow(ctx context.Context) (api.PlanguardReport, error)
}

// DBAdminRouter serves database diagnostics under the admin router. With pg nil, /planguard is not served.
func DBAdminRouter(reader DBStatementsReader, pg PlanguardSource) http.Handler {
	r := chi.NewRouter()
	r.Get("/statements", getDBStatements(reader))
	if pg != nil {
		r.Get("/planguard", getPlanguard(pg))
	}
	return r
}

// getPlanguard godoc
//
// @Summary Last daily plan regression check
// @Description Plans every corpus statement in custom and generic mode against this database, read-only and without executing it (plain EXPLAIN, 5 s per plan, 1 s lock timeout), and checks each plan against the corpus. Off unless planguard.daily_check is set (404 "planguard daily check is disabled"); when on, the check runs about an hour after boot and then daily, and until its first run this returns 404. fresh=true runs it now, shared with any run in progress and cancelled at server shutdown; within 5 minutes of the last run's end it starts nothing and returns the last successful report (started_at gives its age), or 500 when no run has succeeded yet. A read-only server cannot enable the check (configuration refuses planguard.daily_check with server.read_only), so there this returns 404 "planguard daily check is disabled".
// @Tags Admin
// @Produce json
// @Security AdminKey
// @Param fresh query bool false "Run the check now instead of returning the last report"
// @Success 200 {object} api.PlanguardReport
// @Failure 400,401,404,500 {object} map[string]APIError
// @Router /admin/db/planguard [get]
func getPlanguard(pg PlanguardSource) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fresh := false
		if v := r.URL.Query().Get("fresh"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				respondError(w, http.StatusBadRequest, "invalid fresh value")
				return
			}
			fresh = b
		}
		if fresh {
			rep, err := pg.RunNow(r.Context())
			if errors.Is(err, api.ErrPlanguardDisabled) {
				respondError(w, http.StatusNotFound, err.Error())
				return
			}
			if err != nil {
				slog.Error("api: plan check", "component", "api", "error", err)
				respondError(w, http.StatusInternalServerError, "cannot run the plan check")
				return
			}
			respond(w, http.StatusOK, rep)
			return
		}
		rep, err := pg.Last()
		if errors.Is(err, api.ErrPlanguardNotRun) || errors.Is(err, api.ErrPlanguardDisabled) {
			respondError(w, http.StatusNotFound, err.Error())
			return
		}
		if err != nil {
			slog.Error("api: read plan check", "component", "api", "error", err)
			respondError(w, http.StatusInternalServerError, "cannot read the plan check")
			return
		}
		respond(w, http.StatusOK, rep)
	}
}

// getDBStatements godoc
//
// @Summary Statement statistics by sqlc name
// @Description Sums pg_stat_statements for this database by sqlc statement name, highest total execution time first. Statements without a name are summed as "(unnamed)". Times are milliseconds rounded to microseconds; plan_ms stays 0 unless pg_stat_statements.track_planning is on. Counters run from the last pg_stat_statements reset.
// @Tags Admin
// @Produce json
// @Security AdminKey
// @Success 200 {object} api.DBStatementsResponse
// @Failure 401,500,503 {object} map[string]APIError
// @Router /admin/db/statements [get]
func getDBStatements(reader DBStatementsReader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		items, err := reader.DBStatements(r.Context())
		if errors.Is(err, api.ErrStatementsUnavailable) {
			respondError(w, http.StatusServiceUnavailable, "pg_stat_statements is not available: create the extension and add it to shared_preload_libraries")
			return
		}
		if err != nil {
			respondReaderError(w, err, "cannot read statement statistics")
			return
		}
		if items == nil {
			items = []api.DBStatement{}
		}
		respond(w, http.StatusOK, api.DBStatementsResponse{Items: items})
	}
}
