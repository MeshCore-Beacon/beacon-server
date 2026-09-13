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
	"github.com/go-chi/chi/v5"
)

// AdminRouter mounts operator endpoints. Its caller must wrap the
// entire subrouter with BearerAuth, including unknown paths and methods.
func AdminRouter(runtime *mw.RuntimeConfig) http.Handler {
	r := chi.NewRouter()
	r.Get("/config", getAdminConfig(runtime))
	r.Put("/config", updateAdminConfig(runtime))
	return r
}

// getAdminConfig godoc
//
// @Summary Inspect selected running configuration
// @Description Returns current CORS options, auth configuration status and configured broker count. Credential fields and other configuration are excluded. Runtime origin updates are lost on restart.
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
// @Summary Update runtime CORS origins
// @Description Replaces only cors.allowed_origins immediately. Updates are serialized; concurrent valid updates are applied one at a time. Already-running requests may finish with the previous policy. Nothing is persisted; restart reloads saved configuration.
// @Tags Admin
// @Accept json
// @Produce json
// @Security AdminKey
// @Param config body api.UpdateAdminConfigRequest true "1-32 ASCII HTTP(S) origins, at most 512 bytes each; one hostname wildcard is supported, or a sole *. Empty/null lists and unsupported fields are rejected. Body at most 16 KiB."
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
		if input.CORS == nil || input.CORS.AllowedOrigins == nil {
			respondError(w, 400, "cors.allowed_origins is required")
			return
		}
		config, err := runtime.UpdateOrigins(*input.CORS.AllowedOrigins)
		if err != nil {
			respondError(w, 400, mw.ErrInvalidOrigins.Error())
			return
		}
		respond(w, 200, api.UpdateAdminConfigResponse{Config: config, Persisted: false, RequiresRestart: false})
	}
}
