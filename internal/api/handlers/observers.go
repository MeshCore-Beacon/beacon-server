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
// GET  /observers                        → listObservers
// GET  /observers/{observerId}           → getObserver
// GET  /observers/{observerId}/telemetry → getObserverTelemetry
// GET  /observers/{observerId}/adverts   → listObserverAdverts
func ObserversRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listObservers(reader))
	r.Route("/{observerId}", func(r chi.Router) {
		r.Get("/", getObserver(reader))
		r.Get("/adverts", listObserverAdverts(reader))
		r.Get("/telemetry", getObserverTelemetry(reader))
	})
	return r
}

// listObservers godoc
//
//	@Summary	List observers
//	@Tags		Observers
//	@Produce	json
//	@Param		iata			query		string	false	"Filter by single IATA code (case-insensitive)"
//	@Param		iatas			query		string	false	"Filter by multiple IATA codes, comma-separated e.g. YVR,YYJ"
//	@Param		regionId		query		int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region			query		string	false	"Filter by region slug, expands to member IATAs"
//	@Param		type	query		string	false	"Filter by observer type (e.g. meshcoretomqtt, meshcore-ha)"
//	@Param		broker	query		string	false	"Filter by broker name"
//	@Param		status	query		string	false	"Filter by status (online or offline)"
//	@Param		name	query		string	false	"Partial case-insensitive display name match"
//	@Param		cursor	query		int		false	"last_seen epoch ms of last item for pagination"
//	@Param		limit	query		int		false	"Max results (default 50)"
//	@Success	200		{object}	api.Page[api.ObserverSummary]
//	@Failure	400		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Router		/observers [get]
func listObservers(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		observers, err := reader.ListObservers(r.Context(), iatas, observerType, broker, status, name, cursor, limit)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to get list of observers")
			return
		}
		respond(w, http.StatusOK, observers)
	}
}

// getObserver godoc
//
//	@Summary	Get observer detail
//	@Tags		Observers
//	@Produce	json
//	@Param		observerId	path		string	true	"Observer UUID"
//	@Success	200			{object}	api.Observer
//	@Failure	400			{object}	handlers.APIError
//	@Failure	404			{object}	handlers.APIError
//	@Router		/observers/{observerId} [get]
func getObserver(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		observerID := chi.URLParam(r, "observerId")
		id, err := uuid.Parse(observerID)
		if err != nil {
			respondError(w, http.StatusBadRequest, "failed to parse observer UUID")
			return
		}
		obs, err := reader.GetObserver(r.Context(), id)
		if err != nil {
			respondError(w, http.StatusNotFound, "observer not found")
			return
		}
		respond(w, http.StatusOK, obs)
	}
}

// listObserverAdverts godoc
//
//	@Summary	List advert packets heard by an observer
//	@Tags		Observers
//	@Produce	json
//	@Param		observerId	path		string	true	"Observer UUID"
//	@Param		cursor		query		int		false	"Observation ID of last item for pagination"
//	@Param		limit		query		int		false	"Max results (default 50)"
//	@Success	200			{object}	api.Page[api.AdvertObservation]
//	@Failure	400			{object}	handlers.APIError
//	@Failure	500			{object}	handlers.APIError
//	@Router		/observers/{observerId}/adverts [get]
func listObserverAdverts(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
	}
}

// getObserverTelemetry godoc
//
//	@Summary	Get observer telemetry history
//	@Tags		Observers
//	@Produce	json
//	@Param		observerId	path		string	true	"Observer UUID"
//	@Param		range		query		string	false	"Duration window e.g. 24h, 48h, 168h (default 24h)"
//	@Param		afterId		query		int		false	"Return points after this telemetry ID for WS reconnection backfill"
//	@Param		interval	query		string	false	"Bucketing interval, echoed back in the response; not yet applied server-side"
//	@Success	200			{object}	api.ObserverTelemetry
//	@Failure	400			{object}	handlers.APIError
//	@Failure	500			{object}	handlers.APIError
//	@Router		/observers/{observerId}/telemetry [get]
func getObserverTelemetry(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
	}
}
