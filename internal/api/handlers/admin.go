// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/guards"
	"github.com/go-chi/chi/v5"
)

// AdminRouter mounts operator endpoints. Its caller must wrap the
// entire subrouter with BearerAuth, including unknown paths and methods.
func AdminRouter(runtime *mw.RuntimeConfig, routes map[string]http.Handler) http.Handler {
	r := chi.NewRouter()
	r.Get("/config", getAdminConfig(runtime))
	r.Put("/config", updateAdminConfig(runtime))
	for path, handler := range routes {
		r.Mount(path, handler)
	}
	return r
}

// getAdminConfig godoc
//
// @Summary Inspect selected running configuration
// @Description Returns current CORS options, auth configuration status, configured broker count and, when database guards are configured, their state. Credential fields and other configuration are excluded. Runtime origin updates are lost on restart. PUT also replaces guards.enforce at runtime; the configuration file is the boot default.
// @Tags Admin
// @Produce json
// @Security AdminKey
// @Success 200 {object} api.AdminConfig
// @Failure 401 {object} map[string]APIError
// @Failure 503 {object} map[string]APIError
// @Router /admin/config [get]
func getAdminConfig(runtime *mw.RuntimeConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, runtime.Snapshot())
	}
}

// updateAdminConfig godoc
// @Summary Update runtime CORS origins and enforced guard families
// @Description Replaces cors.allowed_origins, guards.enforce or both immediately; at least one is required. Both are validated before either is applied. An empty guards.enforce list makes every family observe. Updates are serialized; concurrent valid updates are applied one at a time. Already-running requests may finish with the previous policy. Nothing is persisted; restart reloads saved configuration. PUT also replaces guards.enforce at runtime; the configuration file is the boot default.
// @Tags Admin
// @Accept json
// @Produce json
// @Security AdminKey
// @Param config body api.UpdateAdminConfigRequest true "cors.allowed_origins: 1-32 ASCII HTTP(S) origins, at most 512 bytes each; one hostname wildcard is supported, or a sole *. Empty/null lists and unsupported fields are rejected. guards.enforce: guard family names, possibly empty. Body at most 16 KiB."
// @Success 200 {object} api.UpdateAdminConfigResponse
// @Failure 400,401,413,415,503 {object} map[string]APIError
// @Router /admin/config [put]
func updateAdminConfig(runtime *mw.RuntimeConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		media, _, err := mime.ParseMediaType(r.Header.Get("Content-Type"))
		if err != nil || media != "application/json" {
			respondError(w, 415, "Content-Type must be application/json")
			return
		}
		r.Body = http.MaxBytesReader(w, r.Body, 16*1024)
		decoder := json.NewDecoder(r.Body)
		decoder.DisallowUnknownFields()
		var input api.UpdateAdminConfigRequest
		err = decoder.Decode(&input)
		if err == nil {
			var extra any
			err = decoder.Decode(&extra)
			if errors.Is(err, io.EOF) {
				err = nil
			} else if err == nil {
				err = errors.New("multiple JSON values")
			}
		}
		if err != nil {
			var sizeError *http.MaxBytesError
			if errors.As(err, &sizeError) {
				respondError(w, 413, "request body exceeds 16 KiB")
			} else {
				respondError(w, 400, "invalid configuration JSON")
			}
			return
		}
		var origins, enforce *[]string
		switch {
		case input.CORS == nil && input.Guards == nil:
			respondError(w, 400, "cors.allowed_origins or guards.enforce is required")
			return
		case input.CORS != nil && input.CORS.AllowedOrigins == nil:
			respondError(w, 400, "cors.allowed_origins is required")
			return
		case input.Guards != nil && input.Guards.Enforce == nil:
			respondError(w, 400, "guards.enforce is required")
			return
		}
		if input.CORS != nil {
			origins = input.CORS.AllowedOrigins
		}
		if input.Guards != nil {
			enforce = input.Guards.Enforce
		}
		config, err := runtime.Update(origins, enforce)
		switch {
		case errors.Is(err, mw.ErrInvalidOrigins):
			respondError(w, 400, mw.ErrInvalidOrigins.Error())
			return
		case errors.Is(err, guards.ErrUnknownFamily):
			respondError(w, 400, guards.ErrUnknownFamily.Error())
			return
		case errors.Is(err, mw.ErrGuardsNotConfigured):
			respondError(w, 400, mw.ErrGuardsNotConfigured.Error())
			return
		case err != nil:
			respondError(w, 400, "invalid configuration")
			return
		}
		respond(w, 200, api.UpdateAdminConfigResponse{Config: config, Persisted: false, RequiresRestart: false})
	}
}
