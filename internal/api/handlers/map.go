package handlers

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/MeshCore-Tower/tower-server/internal/api"

	"github.com/go-chi/chi/v5"
)

// MapRouter mounts read-only map endpoints.
//
// GET /map/state?iata=YOW
// GET /map/state?regionId=1
func MapRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()

	r.Get("/state", func(w http.ResponseWriter, r *http.Request) {
		filter, ok := mapStateFilter(w, r, reader)
		if !ok {
			return
		}

		state, err := reader.GetMapState(r.Context(), filter)
		if err != nil {
			respond(w, http.StatusInternalServerError, map[string]string{"error": "map state unavailable"})
			return
		}
		respond(w, http.StatusOK, state)
	})

	return r
}

func mapStateFilter(w http.ResponseWriter, r *http.Request, reader api.Reader) (api.MapStateFilter, bool) {
	iata := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("iata")))
	regionIDRaw := strings.TrimSpace(r.URL.Query().Get("regionId"))
	if iata != "" && iata != "*" && regionIDRaw != "" {
		respond(w, http.StatusBadRequest, map[string]string{"error": "use either iata or regionId, not both"})
		return api.MapStateFilter{}, false
	}

	if iata != "" && iata != "*" {
		if len(iata) != 3 {
			respond(w, http.StatusBadRequest, map[string]string{"error": "iata must be a 3-letter code"})
			return api.MapStateFilter{}, false
		}
		return api.MapStateFilter{IATAs: []string{iata}}, true
	}

	if regionIDRaw == "" {
		return api.MapStateFilter{}, true
	}

	regionID, err := strconv.ParseInt(regionIDRaw, 10, 32)
	if err != nil || regionID <= 0 {
		respond(w, http.StatusBadRequest, map[string]string{"error": "invalid regionId"})
		return api.MapStateFilter{}, false
	}

	region, err := reader.GetRegion(r.Context(), int32(regionID))
	if err != nil {
		respond(w, http.StatusNotFound, map[string]string{"error": "region not found"})
		return api.MapStateFilter{}, false
	}
	id := int(regionID)
	return api.MapStateFilter{IATAs: region.IATAs, RegionID: &id}, true
}
