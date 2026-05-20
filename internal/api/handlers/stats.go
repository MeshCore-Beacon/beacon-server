package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// StatsRouter mounts all /stats routes onto a subrouter.
//
// GET  /stats/overview                       → GetStatsOverview
// GET  /stats/observations                   → GetStatsObservations
// GET  /stats/payloadBreakdown               → GetStatsPayloadBreakdown
// GET  /stats/topNodes                       → GetStatsTopNodes
// GET  /stats/topObservers                   → GetStatsTopObservers
//
// All endpoints accept either iata= (one or comma-separated) or regionId=
// (expands to all IATAs in that super-region via region_iatas).
func StatsRouter() http.Handler {
	r := chi.NewRouter()

	r.Get("/overview", GetStatsOverview)
	r.Get("/observations", GetStatsObservations)
	r.Get("/payloadBreakdown", GetStatsPayloadBreakdown)
	r.Get("/topNodes", GetStatsTopNodes)
	r.Get("/topObservers", GetStatsTopObservers)

	return r
}

// GetStatsOverview handles GET /api/v1/stats/overview
//
// Query params (all optional):
//   iata=YOW          (one or comma-separated)
//   regionId=<id>
//
// Returns top-line figures: total packets and observations last 24h,
// active observers, active IATAs, unique nodes seen.
func GetStatsOverview(w http.ResponseWriter, r *http.Request) {
	// TODO: query mv_hourly_iata_stats + live aggregates, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}

// GetStatsObservations handles GET /api/v1/stats/observations
//
// Query params (all optional):
//   iata=YOW
//   regionId=<id>
//   range=24h          (duration string: 24h, 7d, 30d)
//   interval=1h        (bucket size: 5m, 1h, 1d)
//
// Returns a time series of observation counts bucketed by interval,
// suitable for charting.
func GetStatsObservations(w http.ResponseWriter, r *http.Request) {
	// TODO: query mv_hourly_iata_stats, group by bucket, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}

// GetStatsPayloadBreakdown handles GET /api/v1/stats/payloadBreakdown
//
// Query params (all optional):
//   iata=YOW
//   regionId=<id>
//   range=24h
//
// Returns observation counts grouped by payload_type for the given window.
func GetStatsPayloadBreakdown(w http.ResponseWriter, r *http.Request) {
	// TODO: query packets + observations filtered by time/iata, group by payload_type.
	w.WriteHeader(http.StatusNotImplemented)
}

// GetStatsTopNodes handles GET /api/v1/stats/topNodes
//
// Query params (all optional):
//   iata=YOW
//   regionId=<id>
//   range=24h
//   limit=10
//
// Returns the top N nodes by observation contribution count using
// mv_top_nodes_by_iata.
func GetStatsTopNodes(w http.ResponseWriter, r *http.Request) {
	// TODO: query mv_top_nodes_by_iata, apply filters, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}

// GetStatsTopObservers handles GET /api/v1/stats/topObservers
//
// Query params (all optional):
//   iata=YOW
//   regionId=<id>
//   range=24h
//   limit=10
//
// Returns the top N observers by observation count for the given window.
func GetStatsTopObservers(w http.ResponseWriter, r *http.Request) {
	// TODO: query packet_observations grouped by observer_id, apply filters.
	w.WriteHeader(http.StatusNotImplemented)
}
