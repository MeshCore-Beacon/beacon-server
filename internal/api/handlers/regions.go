package handlers

import (
	"net/http"
	"strconv"

	"github.com/MeshCore-Tower/tower-server/internal/api"
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
