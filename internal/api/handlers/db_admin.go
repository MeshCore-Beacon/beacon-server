// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// DBStatementsReader reads statement statistics grouped by sqlc name.
type DBStatementsReader interface {
	DBStatements(context.Context) ([]api.DBStatement, error)
}

// DBAdminRouter serves database diagnostics under the admin router.
func DBAdminRouter(reader DBStatementsReader) http.Handler {
	r := chi.NewRouter()
	r.Get("/statements", getDBStatements(reader))
	return r
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
			slog.Error("api: read statement statistics", "component", "api", "error", err)
			respondError(w, http.StatusInternalServerError, "cannot read statement statistics")
			return
		}
		if items == nil {
			items = []api.DBStatement{}
		}
		respond(w, http.StatusOK, api.DBStatementsResponse{Items: items})
	}
}
