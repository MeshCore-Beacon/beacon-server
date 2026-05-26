package handlers

import (
	"net/http"

	"github.com/MeshCore-Tower/tower-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// ObserversRouter mounts all /observers routes onto a subrouter.
//
// GET  /observers                            → ListObservers
// GET  /observers/{observerId}               → GetObserver
// GET  /observers/{observerId}/telemetry     → GetObserverTelemetry
// GET  /observers/{observerId}/adverts       → ListObserverAdverts
func ObserversRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()

	// GET /api/v1/observers
	//
	// Query params (all optional):
	//
	//	iata=YOW
	//	type=meshcoretomqtt
	//	broker=mqtt1
	//	status=online
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		iata := r.URL.Query().Get("iata")
		observerType := r.URL.Query().Get("type")
		broker := r.URL.Query().Get("broker")
		status := r.URL.Query().Get("status")
		observers, err := reader.ListObservers(r.Context(), iata, observerType, broker, status)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to get list of observers")
			return
		}
		respond(w, http.StatusOK, observers)
	})

	r.Route("/{observerId}", func(r chi.Router) {
		// GET /api/v1/observers/{observerId}
		//
		// Returns full observer detail including broker badges, type, and recent stats.
		// Note: observer_owners data is never exposed via the public API.
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			observerID := chi.URLParam(r, "observerId")
			id, err := uuid.Parse(observerID)
			if err != nil {
				respondError(w, http.StatusBadRequest, "falied to parse observer UUID")
				return
			}

			obs, err := reader.GetObserver(r.Context(), id)
			if err != nil {
				respondError(w, http.StatusNotFound, "observer not found")
				return
			}
			respond(w, http.StatusOK, obs)
		})
		r.Get("/telemetry", GetObserverTelemetry)
		r.Get("/adverts", ListObserverAdverts)
	})

	return r
}

// GetObserverTelemetry handles GET /api/v1/observers/{observerId}/telemetry
//
// Query params (all optional):
//
//	range=24h              duration string: 24h, 7d, 30d
//	afterId=<status id>    for deterministic WS reconnection backfill
//	limit=100
//
// Returns a time-bucketed array of telemetry points suitable for charting
// (battery, airtime, noise floor, uptime, queue depth, receive errors).
func GetObserverTelemetry(w http.ResponseWriter, r *http.Request) {
	// observerId := chi.URLParam(r, "observerId")
	// TODO: query status_metadata history, bucket by interval, write JSON response.
	// afterId (int64): WHERE id > afterId ORDER BY id ASC LIMIT limit
	w.WriteHeader(http.StatusNotImplemented)
}

// ListObserverAdverts handles GET /api/v1/observers/{observerId}/adverts
//
// Query params (all optional):
//
//	limit=50
//	cursor=<opaque>
func ListObserverAdverts(w http.ResponseWriter, r *http.Request) {
	// observerId := chi.URLParam(r, "observerId")
	// TODO: fetch advert packets heard by this observer, paginate, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}
