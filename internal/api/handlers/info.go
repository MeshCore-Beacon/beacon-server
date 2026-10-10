// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// InfoRouter mounts GET /info. Clients treat a 404 as "no requirement",
// so this route must stay mounted once shipped.
func InfoRouter(info api.Info) http.Handler {
	r := chi.NewRouter()
	r.Get("/", getInfo(info))
	return r
}

// getInfo godoc
//
//	@Summary		Server info and minimum client versions
//	@Description	minAppVersion (BEACON Mobile) and minWebVersion (Beacon Web) are null when the
//	@Description	server sets no requirement. Each is the higher of the built-in floor and config.
//	@Tags			Info
//	@Produce		json
//	@Success		200	{object}	api.Info
//	@Router			/info [get]
func getInfo(info api.Info) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-cache")
		respond(w, http.StatusOK, info)
	}
}
