// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"log"
	"net/http"
	"strconv"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// StatsRouter mounts all /stats routes onto a subrouter.
//
// GET  /stats/overview          → getStatsOverview
// GET  /stats/observations      → getStatsObservations
// GET  /stats/payload-breakdown → getStatsPayloadBreakdown
// GET  /stats/top-nodes         → getStatsTopNodes
// GET  /stats/top-observers     → getStatsTopObservers
// GET  /stats/top-advertisers   → getStatsTopAdvertisers
// GET  /stats/top-talkers       → getStatsTopTalkers
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
	r.Get("/top-advertisers", getStatsTopAdvertisers(reader))
	r.Get("/clock-drift", getStatsClockDrift(reader))
	r.Get("/top-talkers", getStatsTopTalkers(reader))
	r.Get("/radio-presets", getStatsRadioPresets(reader))
	r.Get("/scopes", getStatsScopes(reader))
	r.Get("/node-types", getStatsNodeTypes(reader))
	return r
}

// getStatsOverview godoc
//
//	@Summary	Network overview stats (last 24h)
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Success	200		{object}	api.StatsOverview
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/overview [get]
func getStatsOverview(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		overview, err := reader.GetStatsOverview(r.Context(), iatas)
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
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		since	query		int		false	"Start of window epoch ms (default 7 days ago)"
//	@Success	200		{array}		api.ObservationPoint
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/observations [get]
func getStatsObservations(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		var since time.Time
		if p := r.URL.Query().Get("since"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
				return
			}
			since = time.UnixMilli(ms)
		}
		points, err := reader.GetStatsObservations(r.Context(), iatas, since)
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
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		since	query		int		false	"Start of window epoch ms (default last 24h)"
//	@Success	200		{array}		api.PayloadBreakdownItem
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/payload-breakdown [get]
func getStatsPayloadBreakdown(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		var since time.Time
		if p := r.URL.Query().Get("since"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
				return
			}
			since = time.UnixMilli(ms)
		}
		breakdown, err := reader.GetStatsPayloadBreakdown(r.Context(), iatas, since)
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
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		limit	query		int		false	"Max results (default 10); must be positive, values above 200 are clamped" minimum(1) maximum(200)
//	@Success	200		{array}		api.TopNode
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/top-nodes [get]
func getStatsTopNodes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		limit, err := parseLimit(r, 10)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		nodes, err := reader.GetStatsTopNodes(r.Context(), iatas, limit)
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
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		since	query		int		false	"Start of window epoch ms (default last 24h)"
//	@Param		limit	query		int		false	"Max results (default 10); must be positive, values above 200 are clamped" minimum(1) maximum(200)
//	@Success	200		{array}		api.TopObserver
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/top-observers [get]
func getStatsTopObservers(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		var since time.Time
		if p := r.URL.Query().Get("since"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
				return
			}
			since = time.UnixMilli(ms)
		}
		limit, err := parseLimit(r, 10)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		observers, err := reader.GetStatsTopObservers(r.Context(), iatas, since, limit)
		if err != nil {
			log.Printf("api: GetStatsTopObservers failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, observers)
	}
}

// getStatsTopAdvertisers godoc
//
//	@Summary	Top N nodes by distinct ADVERT packet count (last 24h by default)
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		since	query		int		false	"Start of window epoch ms (default last 24h)"
//	@Param		limit	query		int		false	"Max results (default 10); must be positive, values above 200 are clamped" minimum(1) maximum(200)
//	@Success	200		{array}		api.TopAdvertiser
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/top-advertisers [get]
func getStatsTopAdvertisers(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		var since time.Time
		if p := r.URL.Query().Get("since"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
				return
			}
			since = time.UnixMilli(ms)
		}
		limit, err := parseLimit(r, 10)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		advertisers, err := reader.GetStatsTopAdvertisers(r.Context(), iatas, since, limit)
		if err != nil {
			log.Printf("api: GetStatsTopAdvertisers failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, advertisers)
	}
}

// getStatsClockDrift godoc
//
//	@Summary	Repeaters/room servers whose clock has drifted beyond the configured threshold, worst first
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		limit	query		int		false	"Max results (default 10); must be positive, values above 200 are clamped" minimum(1) maximum(200)
//	@Success	200		{array}		api.ClockDriftEntry
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/clock-drift [get]
func getStatsClockDrift(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		limit, err := parseLimit(r, 10)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		entries, err := reader.GetStatsClockDrift(r.Context(), iatas, limit)
		if err != nil {
			log.Printf("api: GetStatsClockDrift failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, entries)
	}
}

// getStatsTopTalkers godoc
//
//	@Summary	Top N companion names by decrypted channel message count (last 24h by default)
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		since	query		int		false	"Start of window epoch ms (default last 24h)"
//	@Param		limit	query		int		false	"Max results (default 10); must be positive, values above 200 are clamped" minimum(1) maximum(200)
//	@Success	200		{array}		api.TopTalker
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/top-talkers [get]
func getStatsTopTalkers(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		var since time.Time
		if p := r.URL.Query().Get("since"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
				return
			}
			since = time.UnixMilli(ms)
		}
		limit, err := parseLimit(r, 10)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		talkers, err := reader.GetStatsTopTalkers(r.Context(), iatas, since, limit)
		if err != nil {
			log.Printf("api: GetStatsTopTalkers failed: %v", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, talkers)
	}
}

// getStatsRadioPresets godoc
//
//	@Summary	Radio preset usage by IATA
//	@Tags		Stats
//	@Produce	json
//	@Param		preset	query		string	false	"Filter by preset string e.g. 910.525,62.5,7"
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Success	200		{object}	[]api.RadioPreset
//	@Failure	500		{object}	handlers.APIError
//	@Router		/stats/radio-presets [get]
func getStatsRadioPresets(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		preset := r.URL.Query().Get("preset")
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		presets, err := reader.GetRadioPresets(r.Context(), preset, iatas)
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

// getStatsNodeTypes godoc
//
//	@Summary	Node type breakdown
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query		string	false	"Comma-separated IATA codes"
//	@Param		regionId	query		int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query		string	false	"Filter by region slug, expands to member IATAs"
//	@Success	200			{array}		api.NodeTypeCount
//	@Failure	500			{object}	handlers.APIError
//	@Router		/stats/node-types [get]
func getStatsNodeTypes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		result, err := reader.GetStatsNodeTypes(r.Context(), iatas)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to get node type stats")
			return
		}
		respond(w, http.StatusOK, result)
	}
}
