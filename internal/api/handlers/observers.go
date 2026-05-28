package handlers

import (
	"net/http"
	"strconv"
	"time"

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
	//	cursor=<int>     last_seen epoch ms of last observer for pagination
	//	limit=50
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		iata := r.URL.Query().Get("iata")
		observerType := r.URL.Query().Get("type")
		broker := r.URL.Query().Get("broker")
		name := r.URL.Query().Get("name")
		status := r.URL.Query().Get("status")
		var cursor int64
		if cursorParam := r.URL.Query().Get("cursor"); cursorParam != "" {
			c, err := strconv.ParseInt(cursorParam, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "cursor must be an integer")
				return
			}
			cursor = c
		}
		var limit int32 = 50
		if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
			l, err := strconv.ParseInt(limitParam, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = int32(l)
		}
		observers, err := reader.ListObservers(r.Context(), iata, observerType, broker, status, name, cursor, limit)
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
		// /api/v1/observers/{observerId}/adverts
		//
		// Query params (all optional):
		//
		//	limit=50
		//	cursor=<opaque>

		r.Get("/adverts", func(w http.ResponseWriter, r *http.Request) {
			observerID, err := uuid.Parse(chi.URLParam(r, "observerId"))
			if err != nil {
				respondError(w, http.StatusBadRequest, "invalid observer ID")
				return
			}

			var cursor int64
			if cursorParam := r.URL.Query().Get("cursor"); cursorParam != "" {
				c, err := strconv.ParseInt(cursorParam, 10, 64)
				if err != nil {
					respondError(w, http.StatusBadRequest, "cursor must be an integer")
					return
				}
				cursor = c
			}

			var limit int32 = 50
			if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
				l, err := strconv.ParseInt(limitParam, 10, 32)
				if err != nil {
					respondError(w, http.StatusBadRequest, "limit must be an integer")
					return
				}
				limit = int32(l)
			}

			adverts, err := reader.ListObserverAdverts(r.Context(), observerID, cursor, limit)
			if err != nil {
				respondError(w, http.StatusInternalServerError, "internal server error")
				return
			}
			respond(w, http.StatusOK, adverts)
		})
		// GET /api/v1/observers/{observerId}/telemetry
		//
		// Query params (all optional):
		//
		//	range=24h              duration string: 24h, 7d, 30d
		//	afterId=<status id>    for deterministic WS reconnection backfill
		//
		// Returns a time-bucketed array of telemetry points suitable for charting
		// (battery, airtime, noise floor, uptime, queue depth, receive errors).
		r.Get("/telemetry", func(w http.ResponseWriter, r *http.Request) {
			observerID, err := uuid.Parse(chi.URLParam(r, "observerId"))
			if err != nil {
				respondError(w, http.StatusBadRequest, "invalid observer ID")
				return
			}

			rangeParam := r.URL.Query().Get("range")
			if rangeParam == "" {
				rangeParam = "24h"
			}

			duration, err := time.ParseDuration(rangeParam)
			if err != nil {
				respondError(w, http.StatusBadRequest, "invalid range, use e.g. 24h, 48h, 168h")
				return
			}

			afterID := int64(0)
			if afterIDParam := r.URL.Query().Get("afterId"); afterIDParam != "" {
				id, err := strconv.ParseInt(afterIDParam, 10, 64)
				if err != nil {
					respondError(w, http.StatusBadRequest, "afterId must be an integer")
					return
				}
				afterID = id
			}

			since := time.Now().Add(-duration)
			until := time.Time{} // no upper bound

			telemetry, err := reader.GetObserverTelemetry(r.Context(), observerID, since, until, afterID)
			if err != nil {
				respondError(w, http.StatusInternalServerError, "internal server error")
				return
			}

			telemetry.Range = rangeParam
			telemetry.Interval = r.URL.Query().Get("interval") // echoed back, not used server-side yet
			respond(w, http.StatusOK, telemetry)
		})
	})

	return r
}
