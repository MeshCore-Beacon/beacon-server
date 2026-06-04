package handlers

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/MeshCore-Tower/tower-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// StatsRouter mounts all /stats routes onto a subrouter.
//
// GET  /stats/overview          → getStatsOverview
// GET  /stats/observations      → getStatsObservations
// GET  /stats/payload-breakdown → getStatsPayloadBreakdown
// GET  /stats/top-nodes         → getStatsTopNodes
// GET  /stats/top-observers     → getStatsTopObservers
// GET  /stats/radio-presets     → getStatsRadioPresets
// GET  /stats/scopes            → GetStatsScopes
//
// All endpoints accept an optional iata= filter (case-insensitive).
func StatsRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/overview", getStatsOverview(reader))
	r.Get("/observations", getStatsObservations(reader))
	r.Get("/payload-breakdown", getStatsPayloadBreakdown(reader))
	r.Get("/top-nodes", getStatsTopNodes(reader))
	r.Get("/top-observers", getStatsTopObservers(reader))
	r.Get("/radio-presets", getStatsRadioPresets(reader))
	r.Get("/scopes", getStatsScopes(reader))
	return r
}

// getStatsOverview godoc
//
//	@Summary	Network overview stats (last 24h)
//	@Tags		Stats
//	@Produce	json
//	@Param		iata	query		string	false	"Filter by IATA code (case-insensitive)"
//	@Success	200		{object}	api.StatsOverview
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/overview [get]
func getStatsOverview(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iata := r.URL.Query().Get("iata")
		overview, err := reader.GetStatsOverview(r.Context(), iata)
		if err != nil {
			log.Printf("api: GetStatsOverview failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, overview)
	}
}

// getStatsObservations godoc
//
//	@Summary	Hourly observation time series
//	@Tags		Stats
//	@Produce	json
//	@Param		iata	query		string	false	"Filter by IATA code (case-insensitive)"
//	@Param		since	query		int		false	"Start of window epoch ms (default 7 days ago)"
//	@Success	200		{array}		api.ObservationPoint
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/observations [get]
func getStatsObservations(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iata := r.URL.Query().Get("iata")
		var since time.Time
		if p := r.URL.Query().Get("since"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
				return
			}
			since = time.UnixMilli(ms)
		}
		points, err := reader.GetStatsObservations(r.Context(), iata, since)
		if err != nil {
			log.Printf("api: GetStatsObservations failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, points)
	}
}

// getStatsPayloadBreakdown godoc
//
//	@Summary	Observation counts by payload type (last 24h by default)
//	@Tags		Stats
//	@Produce	json
//	@Param		iata	query		string	false	"Filter by IATA code (case-insensitive)"
//	@Param		since	query		int		false	"Start of window epoch ms (default last 24h)"
//	@Success	200		{array}		api.PayloadBreakdownItem
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/payload-breakdown [get]
func getStatsPayloadBreakdown(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iata := r.URL.Query().Get("iata")
		var since time.Time
		if p := r.URL.Query().Get("since"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
				return
			}
			since = time.UnixMilli(ms)
		}
		breakdown, err := reader.GetStatsPayloadBreakdown(r.Context(), iata, since)
		if err != nil {
			log.Printf("api: GetStatsPayloadBreakdown failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, breakdown)
	}
}

// getStatsTopNodes godoc
//
//	@Summary	Top N nodes by observation count (from materialized view)
//	@Tags		Stats
//	@Produce	json
//	@Param		iata	query		string	false	"Filter by exact IATA code (case-sensitive)"
//	@Param		limit	query		int		false	"Max results (default 10)"
//	@Success	200		{array}		api.TopNode
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/top-nodes [get]
func getStatsTopNodes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iata := r.URL.Query().Get("iata")
		var limit int32 = 10
		if p := r.URL.Query().Get("limit"); p != "" {
			l, err := strconv.ParseInt(p, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = int32(l)
		}
		nodes, err := reader.GetStatsTopNodes(r.Context(), iata, limit)
		if err != nil {
			log.Printf("api: GetStatsTopNodes failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, nodes)
	}
}

// getStatsTopObservers godoc
//
//	@Summary	Top N observers by observation count (last 24h by default)
//	@Tags		Stats
//	@Produce	json
//	@Param		iata	query		string	false	"Filter by IATA code (case-insensitive)"
//	@Param		since	query		int		false	"Start of window epoch ms (default last 24h)"
//	@Param		limit	query		int		false	"Max results (default 10)"
//	@Success	200		{array}		api.TopObserver
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/top-observers [get]
func getStatsTopObservers(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iata := r.URL.Query().Get("iata")
		var since time.Time
		if p := r.URL.Query().Get("since"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
				return
			}
			since = time.UnixMilli(ms)
		}
		var limit int32 = 10
		if p := r.URL.Query().Get("limit"); p != "" {
			l, err := strconv.ParseInt(p, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = int32(l)
		}
		observers, err := reader.GetStatsTopObservers(r.Context(), iata, since, limit)
		if err != nil {
			log.Printf("api: GetStatsTopObservers failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, observers)
	}
}

// getStatsRadioPresets godoc
//
//	@Summary	Radio preset usage by IATA
//	@Tags		Stats
//	@Produce	json
//	@Param		preset	query		string	false	"Filter by preset string e.g. 910.525,62.5,7"
//	@Param		iata	query		string	false	"Filter by IATA code"
//	@Success	200		{object}	[]api.RadioPreset
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/radio-presets [get]
func getStatsRadioPresets(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		preset := r.URL.Query().Get("preset")
		iata := r.URL.Query().Get("iata")
		presets, err := reader.GetRadioPresets(r.Context(), preset, iata)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, presets)
	}
}

// getStatsScopes godoc
//
//	@Summary	Scope statistics
//	@Tags		Stats
//	@Produce	json
//	@Success	200	{object}	[]api.ScopeStats
//	@Failure	500	{object}	handlers.APIError
//	@Router		/stats/scopes [get]
func getStatsScopes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats, err := reader.GetScopeStats(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, stats)
	}
}
