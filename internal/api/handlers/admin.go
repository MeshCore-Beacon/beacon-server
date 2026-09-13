// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// AdminRouter mounts read-only operator endpoints. Its caller must wrap the
// entire subrouter with BearerAuth, including unknown paths and methods.
func AdminRouter(snapshot api.AdminConfig) http.Handler {
	r := chi.NewRouter()
	r.Get("/config", getAdminConfig(snapshot))
	return r
}

// getAdminConfig godoc
//
// @Summary Inspect selected startup configuration
// @Description Returns CORS startup options with Beacon defaults, auth configuration status and configured broker count. Credential fields and other configuration are excluded. Changes require restart; this endpoint is read-only.
// @Tags Admin
// @Produce json
// @Security AdminKey
// @Success 200 {object} api.AdminConfig
// @Failure 401 {object} map[string]APIError
// @Failure 503 {object} map[string]APIError
// @Router /admin/config [get]
func getAdminConfig(snapshot api.AdminConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, snapshot)
	}
}
