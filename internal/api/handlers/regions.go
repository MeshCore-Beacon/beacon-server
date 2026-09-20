// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

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

// parseStatsWindow rounds both endpoints down to UTC hours. Polls within an
// hour share a cache key; the response reports the effective window. A valid
// sub-hour request can contain no complete buckets and return an empty result.
func parseStatsWindow(r *http.Request) (time.Time, time.Time, error) {
	q := r.URL.Query()
	for _, key := range []string{"since", "until"} {
		if len(q[key]) != 1 || q.Get(key) == "" {
			return time.Time{}, time.Time{}, fmt.Errorf("%s must be supplied exactly once", key)
		}
	}
	since, errSince := strconv.ParseInt(q.Get("since"), 10, 64)
	until, errUntil := strconv.ParseInt(q.Get("until"), 10, 64)
	if errSince != nil || errUntil != nil || since < 0 || until <= since || until > 253402300799999 || until-since > int64((30*24*time.Hour)/time.Millisecond) {
		return time.Time{}, time.Time{}, fmt.Errorf("since and until must be epoch milliseconds between 0 and 253402300799999, with a positive window of at most 30 days")
	}
	return time.UnixMilli(since).UTC().Truncate(time.Hour), time.UnixMilli(until).UTC().Truncate(time.Hour), nil
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

// parseInt16CSVOrSingle reads a comma-separated list from pluralParam, falling back to a
// single value from singularParam if pluralParam is absent -- same precedence as parseIATAs.
func parseInt16CSVOrSingle(r *http.Request, pluralParam, singularParam string) ([]int16, error) {
	raw := r.URL.Query().Get(pluralParam)
	if raw == "" {
		single := r.URL.Query().Get(singularParam)
		if single == "" {
			return nil, nil
		}
		v, err := strconv.ParseInt(single, 10, 16)
		if err != nil {
			return nil, err
		}
		return []int16{int16(v)}, nil
	}
	parts := strings.Split(raw, ",")
	out := make([]int16, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		v, err := strconv.ParseInt(p, 10, 16)
		if err != nil {
			return nil, err
		}
		out = append(out, int16(v))
	}
	return out, nil
}

// parseCSVOrSingle reads a comma-separated list from pluralParam, falling back to a single
// value from singularParam if pluralParam is absent -- same precedence as parseIATAs, but
// without the uppercasing (used for scope names, which are case-sensitive, e.g. "#bc").
func parseCSVOrSingle(r *http.Request, pluralParam, singularParam string) []string {
	raw := r.URL.Query().Get(pluralParam)
	if raw == "" {
		if single := r.URL.Query().Get(singularParam); single != "" {
			return []string{single}
		}
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
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
