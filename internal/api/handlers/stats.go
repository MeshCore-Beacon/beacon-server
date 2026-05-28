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
// GET  /stats/overview         → GetStatsOverview
// GET  /stats/observations     → GetStatsObservations
// GET  /stats/payload-breakdown → GetStatsPayloadBreakdown
// GET  /stats/top-nodes         → GetStatsTopNodes
// GET  /stats/top-observers     → GetStatsTopObservers
//
// All endpoints accept an optional iata= filter (case-insensitive).
// regionId= expansion and comma-separated IATAs are not yet implemented.
func StatsRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()

	// GET /api/v1/stats/overview
	//
	// Query params (all optional):
	//
	//	iata=<code>   filter to a single IATA (case-insensitive)
	r.Get("/overview", func(w http.ResponseWriter, r *http.Request) {
		iata := r.URL.Query().Get("iata")
		overview, err := reader.GetStatsOverview(r.Context(), iata)
		if err != nil {
			log.Printf("api: GetStatsOverview failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, overview)
	})

	// GET /api/v1/stats/observations
	//
	// Query params (all optional):
	//
	//	iata=<code>        filter to a single IATA (case-insensitive)
	//	since=<epoch ms>   start of window; defaults to 7 days ago
	r.Get("/observations", func(w http.ResponseWriter, r *http.Request) {
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
	})

	// GET /api/v1/stats/payload-breakdown
	//
	// Query params (all optional):
	//
	//	iata=<code>        filter to a single IATA (case-insensitive)
	//	since=<epoch ms>   start of window; defaults to last 24h
	r.Get("/payload-breakdown", func(w http.ResponseWriter, r *http.Request) {
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
	})

	// GET /api/v1/stats/top-nodes
	//
	// Query params (all optional):
	//
	//	iata=<code>   filter to a single IATA (case-insensitive)
	//	limit=10
	r.Get("/top-nodes", func(w http.ResponseWriter, r *http.Request) {
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
	})

	// GET /api/v1/stats/top-observers
	//
	// Query params (all optional):
	//
	//	iata=<code>        filter to a single IATA (case-insensitive)
	//	since=<epoch ms>   start of window; defaults to last 24h
	//	limit=10
	r.Get("/top-observers", func(w http.ResponseWriter, r *http.Request) {
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
	})

	return r
}
