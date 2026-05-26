package handlers

import (
	"net/http"
	"strconv"

	"github.com/MeshCore-Tower/tower-server/internal/api"

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

	// GET  /regions                              → ListRegions
	//
	// Returns all super-regions with their associated IATA codes, center
	// coordinates, and zoom level for map initialisation.
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		regions, err := reader.ListRegions(r.Context())
		if err != nil {
			respondError(w, http.StatusNotFound, "no regions found")
		}
		respond(w, http.StatusOK, regions)
	})

	// GET  /regions/{regionId}                   → GetRegion
	//
	// Returns detail for a single super-region including its full IATA membership
	// list and recent aggregate stats.
	r.Get("/{regionId}", func(w http.ResponseWriter, r *http.Request) {
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
	})

	return r
}
