// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	mw "github.com/MeshCore-Beacon/beacon-server/internal/api/middleware"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

// AtlasStore is the storage behind /me/atlas.
type AtlasStore interface {
	UserAtlas(ctx context.Context, userID uuid.UUID) (api.Atlas, error)
	ObserverKeysByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]byte, error)
	// ReplaceUserAtlas reports ok=false when base is not the stored revision.
	// On success it returns the document it committed, built in the same transaction.
	ReplaceUserAtlas(ctx context.Context, userID uuid.UUID, base int64, rng string, pins []api.StoredPin) (doc api.Atlas, ok bool, err error)
}

// A full Atlas with long names is a few KiB more than the usual body limit.
const maxAtlasBody = 32 << 10

// AtlasRoutes mounts GET and PUT /atlas on the /me router, holding at most limit pins. A nil store mounts nothing.
func AtlasRoutes(store AtlasStore, limit int) MeMount {
	return func(r chi.Router, writes func(http.Handler) http.Handler) {
		if store == nil {
			return
		}
		r.Get("/atlas", getAtlas(store, limit))
		r.With(writes).Put("/atlas", putAtlas(store, limit))
	}
}

// getAtlas godoc
// @Summary The signed-in user's Atlas
// @Description Ids are resolved now: a node pin's id is the node with that key, and an observer pin whose key has a node row is returned as a node pin.
// @Tags Accounts
// @Produce json
// @Security UserSession
// @Success 200 {object} api.Atlas
// @Failure 401 {object} map[string]APIError
// @Router /me/atlas [get]
func getAtlas(store AtlasStore, limit int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		u, _, _ := mw.UserFrom(r.Context())
		a, err := store.UserAtlas(r.Context(), u.ID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal error")
			return
		}
		a.Limit = limit
		respond(w, http.StatusOK, a)
	}
}

// putAtlas godoc
// @Summary Replace the Atlas
// @Description Replaces every pin when baseRevision is the stored revision and returns the new document. Otherwise 409 atlas_conflict with the stored Atlas under current.
// @Tags Accounts
// @Accept json
// @Produce json
// @Security UserSession
// @Param body body api.PutAtlasRequest true "Body at most 32 KiB"
// @Success 200 {object} api.Atlas
// @Failure 400,401,403,413,415,422,429,503 {object} map[string]APIError
// @Failure 409 {object} api.AtlasConflict
// @Router /me/atlas [put]
func putAtlas(store AtlasStore, limit int) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var in api.PutAtlasRequest
		if !decodeBodyMax(w, r, &in, maxAtlasBody) {
			return
		}
		u, _, _ := mw.UserFrom(r.Context())
		keys, err := store.ObserverKeysByIDs(r.Context(), in.ObserverIDs())
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal error")
			return
		}
		rng, pins, err := api.ValidateAtlasPut(in, keys, limit)
		if err != nil {
			var bad *api.AtlasInvalid
			if errors.As(err, &bad) {
				respondCode(w, http.StatusUnprocessableEntity, "invalid_input", bad.Message)
				return
			}
			respondError(w, http.StatusInternalServerError, "internal error")
			return
		}
		doc, ok, err := store.ReplaceUserAtlas(r.Context(), u.ID, in.BaseRevision, rng, pins)
		if errors.Is(err, users.ErrNotFound) {
			respondCode(w, http.StatusUnauthorized, "not_signed_in", "Sign in first.")
			return
		}
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal error")
			return
		}
		if ok {
			doc.Limit = limit
			respond(w, http.StatusOK, doc)
			return
		}
		// A conflict reads the stored Atlas fresh.
		cur, err := store.UserAtlas(r.Context(), u.ID)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal error")
			return
		}
		cur.Limit = limit
		respond(w, http.StatusConflict, api.AtlasConflict{
			Error:   api.AtlasConflictError{Code: "atlas_conflict", Message: "The Atlas was changed elsewhere."},
			Current: cur,
		})
	}
}
