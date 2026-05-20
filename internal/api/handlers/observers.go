package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// ObserversRouter mounts all /observers routes onto a subrouter.
//
// GET  /observers                            → ListObservers
// GET  /observers/{observerId}               → GetObserver
// GET  /observers/{observerId}/telemetry     → GetObserverTelemetry
// GET  /observers/{observerId}/adverts       → ListObserverAdverts
func ObserversRouter() http.Handler {
	r := chi.NewRouter()

	r.Get("/", ListObservers)

	r.Route("/{observerId}", func(r chi.Router) {
		r.Get("/", GetObserver)
		r.Get("/telemetry", GetObserverTelemetry)
		r.Get("/adverts", ListObserverAdverts)
	})

	return r
}

// ListObservers handles GET /api/v1/observers
//
// Query params (all optional):
//
//	iata=YOW
//	type=meshcoretomqtt
//	broker=mqtt1
//	status=online
func ListObservers(w http.ResponseWriter, r *http.Request) {
	// TODO: query observers with optional filters, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}

// GetObserver handles GET /api/v1/observers/{observerId}
//
// Returns full observer detail including broker badges, type, and recent stats.
// Note: observer_owners data is never exposed via the public API.
func GetObserver(w http.ResponseWriter, r *http.Request) {
	// observerId := chi.URLParam(r, "observerId")
	// TODO: fetch observer (exclude observer_owners fields), write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
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
