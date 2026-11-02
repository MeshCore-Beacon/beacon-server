// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/meshmapper"
)

// ScopeCatalogueSource supplies immutable, already-cached public metadata.
type ScopeCatalogueSource interface {
	Catalogues() []meshmapper.PublicScopeCatalogue
}

// ScopeCatalogues godoc
// @Summary Cached MeshMapper regional scope catalogues
// @Description Discovered sources only. Counts describe the source region, not individual nodes or links. No upstream request is made. An empty list means no configured importer; checkedAt=0 means no successful check. lastError or a past freshUntil marks stale metadata. Manual Beacon scopes and packet evidence are separate.
// @Tags Scopes
// @Produce json
// @Success 200 {array} meshmapper.PublicScopeCatalogue
// @Router /scope-catalogues [get]
func ScopeCatalogues(source ScopeCatalogueSource) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		items := []meshmapper.PublicScopeCatalogue{}
		if source != nil {
			items = source.Catalogues()
		}
		w.Header().Set("Cache-Control", "public, max-age=60")
		respond(w, http.StatusOK, items)
	}
}
