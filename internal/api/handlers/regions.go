package handlers

import (
	"net/http"

	"tower/internal/api"

	"github.com/go-chi/chi/v5"
)

// RegionsRouter mounts all /regions routes onto a subrouter.
//
// GET  /regions                              → ListRegions
// GET  /regions/{regionId}                   → GetRegion
//
// Note: region creation and IATA assignment are managed via the server config
// file, not the API (v1). These endpoints are read-only.
func RegionsRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()

	r.Get("/", ListRegions)
	r.Get("/{regionId}", GetRegion)

	return r
}

// ListRegions handles GET /api/v1/regions
//
// Returns all super-regions with their associated IATA codes, center
// coordinates, and zoom level for map initialisation.
func ListRegions(w http.ResponseWriter, r *http.Request) {
	// TODO: query regions + region_iatas, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}

// GetRegion handles GET /api/v1/regions/{regionId}
//
// Returns detail for a single super-region including its full IATA membership
// list and recent aggregate stats.
func GetRegion(w http.ResponseWriter, r *http.Request) {
	// regionId := chi.URLParam(r, "regionId")
	// TODO: fetch region + iatas, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}
