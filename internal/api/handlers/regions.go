// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// RegionsRouter mounts all /regions routes onto a subrouter.
//
// GET  /regions            → listRegions
// GET  /regions/{regionId} → getRegion
//
// Note: region creation and IATA assignment are managed via the server config
// file, not the API (v1). These endpoints are read-only.
func RegionsRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listRegions(reader))
	r.Get("/{regionId}", getRegion(reader))
	return r
}

// listRegions godoc
//
//	@Summary	List all regions
//	@Tags		Regions
//	@Produce	json
//	@Success	200	{array}		api.RegionSummary
//	@Failure	404	{object}	handlers.APIError
//	@Router		/regions [get]
func listRegions(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		regions, err := reader.ListRegions(r.Context())
		if err != nil {
			respondError(w, http.StatusNotFound, "no regions found")
			return
		}
		respond(w, http.StatusOK, regions)
	}
}

// getRegion godoc
//
//	@Summary	Get a single region
//	@Tags		Regions
//	@Produce	json
//	@Param		regionId	path		int	true	"Region ID"
//	@Success	200			{object}	api.Region
//	@Failure	400			{object}	handlers.APIError
//	@Failure	404			{object}	handlers.APIError
//	@Router		/regions/{regionId} [get]
func getRegion(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		regionID := chi.URLParam(r, "regionId")
		regionInt, err := strconv.ParseInt(regionID, 10, 32)
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid region ID")
			return
		}
		region, err := reader.GetRegion(r.Context(), int32(regionInt))
		if err != nil {
			respondError(w, http.StatusNotFound, "region not found")
			return
		}
		respond(w, http.StatusOK, region)
	}
}

// parseIATAs splits a comma-separated iatas query param and uppercases each value.
func parseIATAs(r *http.Request) []string {
	raw := r.URL.Query().Get("iatas")
	if raw == "" {
		// fall back to single iata param for backwards compatibility
		if single := r.URL.Query().Get("iata"); single != "" {
			return []string{strings.ToUpper(strings.TrimSpace(single))}
		}
		return nil
	}
	parts := strings.Split(raw, ",")
	iatas := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			iatas = append(iatas, strings.ToUpper(p))
		}
	}
	return iatas
}

// resolveRegionIATAs expands a regionId or region slug to a slice of IATA codes.
func resolveRegionIATAs(ctx context.Context, regionID, regionSlug string, reader api.Reader) ([]string, error) {
	var region *api.Region
	var err error
	switch {
	case regionID != "":
		rid, e := strconv.ParseInt(regionID, 10, 32)
		if e != nil {
			return nil, fmt.Errorf("invalid regionId: %w", e)
		}
		region, err = reader.GetRegion(ctx, int32(rid))
	case regionSlug != "":
		region, err = reader.GetRegionBySlug(ctx, regionSlug)
	default:
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("region not found: %w", err)
	}
	return region.IATAs, nil
}
