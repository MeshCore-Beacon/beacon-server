// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// MetaRouter mounts GET /meta, which serves fixed deployment settings.
func MetaRouter(meta api.Meta) http.Handler {
	r := chi.NewRouter()
	r.Get("/", getMeta(meta))
	return r
}

// getMeta godoc
//
//	@Summary		Get deployment metadata
//	@Description	Retention windows in seconds, so clients can bound time-range pickers.
//	@Tags			Meta
//	@Produce		json
//	@Success		200	{object}	api.Meta
//	@Router			/meta [get]
func getMeta(meta api.Meta) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		respond(w, http.StatusOK, meta)
	}
}
