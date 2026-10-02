// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"errors"
	"log/slog"
	"net/http"
	"slices"
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
// StatsOptions configures StatsRouter.
type StatsOptions struct {
	Scopes       ScopeMembership // region-filtered scope lists; nil lists none
	SeriesWindow time.Duration   // longest /stats/series window, the rollup retention; 0 means 90 days
}

// parseSince reads an optional epoch-ms since; zero means the endpoint's default window.
func parseSince(r *http.Request) (time.Time, error) {
	p := r.URL.Query().Get("since")
	if p == "" {
		return time.Time{}, nil
	}
	ms, err := strconv.ParseInt(p, 10, 64)
	if err != nil {
		return time.Time{}, errors.New("since must be epoch milliseconds")
	}
	return time.UnixMilli(ms), nil
}

func StatsRouter(reader api.Reader, opts StatsOptions) http.Handler {
	scopes := opts.Scopes
	r := chi.NewRouter()
	r.Get("/series", getStatsSeries(reader, opts.SeriesWindow))
	r.Get("/overview", getStatsOverview(reader))
	r.Get("/observations", getStatsObservations(reader))
	r.Get("/signal", getSignalStats(reader))
	r.Get("/paths", getPathStats(reader))
	r.Get("/observer-comparison", getObserverComparison(reader))
	r.Get("/payload-breakdown", getStatsPayloadBreakdown(reader))
	r.Get("/top-nodes", getStatsTopNodes(reader))
	r.Get("/top-observers", getStatsTopObservers(reader))
	r.Get("/top-advertisers", getStatsTopAdvertisers(reader))
	r.Get("/clock-drift", getStatsClockDrift(reader))
	r.Get("/top-talkers", getStatsTopTalkers(reader))
	r.Get("/radio-presets", getStatsRadioPresets(reader))
	r.Get("/scopes", getStatsScopes(reader, scopes))
	r.Get("/node-types", getStatsNodeTypes(reader))
	return r
}

// getStatsOverview godoc
//
//	@Summary	Network overview stats (last 24 rolled hours)
//	@Description	Summarises the 24 most recent hours that can have been rolled (an hour rolls about 95 minutes after it closes); since/until report that window. Packets count once per hour heard. Use /stats/series for sparklines.
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
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""} // an empty region matches nothing, not everything
			}
		}
		overview, err := reader.GetStatsOverview(r.Context(), iatas)
		if err != nil {
			slog.Error("api: GetStatsOverview failed", "component", "api", "error", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, overview)
	}
}

// getStatsObservations godoc
//
//	@Summary	Hourly observation counts per IATA
//	@Description	Observation counts only; distinct packet and observer counts don't sum across IATAs, so read them from /stats/series.
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
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""} // an empty region matches nothing, not everything
			}
		}
		since, err := parseSince(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		points, err := reader.GetStatsObservations(r.Context(), iatas, since)
		if err != nil {
			slog.Error("api: GetStatsObservations failed", "component", "api", "error", err)
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
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""} // an empty region matches nothing, not everything
			}
		}
		since, err := parseSince(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		breakdown, err := reader.GetStatsPayloadBreakdown(r.Context(), iatas, since)
		if err != nil {
			slog.Error("api: GetStatsPayloadBreakdown failed", "component", "api", "error", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, breakdown)
	}
}

// getStatsTopNodes godoc
//
//	@Summary	Top N nodes by advert hearings (last 7 days by default)
//	@Description	nodeId is null when the node row has been deleted; publicKey always identifies it.
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs"
//	@Param		since	query		int		false	"Start of window epoch ms, rounded down to the hour (default 7 days ago)"
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
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""} // an empty region matches nothing, not everything
			}
		}
		limit, err := parseLimit(r, 10)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		since, err := parseSince(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		nodes, err := reader.GetStatsTopNodes(r.Context(), iatas, since, limit)
		if err != nil {
			slog.Error("api: GetStatsTopNodes failed", "component", "api", "error", err)
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
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""} // an empty region matches nothing, not everything
			}
		}
		since, err := parseSince(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		limit, err := parseLimit(r, 10)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		observers, err := reader.GetStatsTopObservers(r.Context(), iatas, since, limit)
		if err != nil {
			slog.Error("api: GetStatsTopObservers failed", "component", "api", "error", err)
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, observers)
	}
}

// getStatsTopAdvertisers godoc
//
//	@Summary	Top N nodes by distinct ADVERT packet count (last 24h by default)
//	@Description	Each advert counts once per hour heard, however many requested IATAs heard it. nodeId is null when the node row has been deleted.
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
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""} // an empty region matches nothing, not everything
			}
		}
		since, err := parseSince(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		limit, err := parseLimit(r, 10)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		advertisers, err := reader.GetStatsTopAdvertisers(r.Context(), iatas, since, limit)
		if err != nil {
			slog.Error("api: GetStatsTopAdvertisers failed", "component", "api", "error", err)
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
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""} // an empty region matches nothing, not everything
			}
		}
		limit, err := parseLimit(r, 10)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		entries, err := reader.GetStatsClockDrift(r.Context(), iatas, limit)
		if err != nil {
			slog.Error("api: GetStatsClockDrift failed", "component", "api", "error", err)
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
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""} // an empty region matches nothing, not everything
			}
		}
		since, err := parseSince(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		limit, err := parseLimit(r, 10)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		talkers, err := reader.GetStatsTopTalkers(r.Context(), iatas, since, limit)
		if err != nil {
			slog.Error("api: GetStatsTopTalkers failed", "component", "api", "error", err)
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
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""} // an empty region matches nothing, not everything
			}
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
//	@Description	Packets are those heard since the window start, each counted once per hour however many of the requested IATAs heard it. Observers and nodes are current memberships: observers filter by the IATA they last reported from, nodes by their IATA memberships. hourly splits packetCount by UTC hour and adds the distinct observers and advertising nodes active in the scope each hour (empty hours omitted; outage gaps come from /stats/series hour status). With filters, lists only manual scopes configured for a matching region and imported scopes whose current MeshMapper catalogue includes a matching IATA; those remain listed with zero counts. An empty region returns an empty array.
//	@Tags		Stats
//	@Produce	json
//	@Param		iatas		query	string	false	"Comma-separated IATA codes"
//	@Param		iata		query	string	false	"Single IATA code; used when iatas is absent"
//	@Param		regionId	query	int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query	string	false	"Filter by region slug, expands to member IATAs; combined with explicit IATAs"
//	@Param		since	query		int		false	"Start of packet window epoch ms, rounded down to the hour (default 7 days ago)"
//	@Success	200	{object}	[]api.ScopeStats
//	@Failure	400	{object}	handlers.APIError
//	@Failure	500	{object}	handlers.APIError
//	@Router		/stats/scopes [get]
func getStatsScopes(reader api.Reader, scopes ScopeMembership) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionID := r.URL.Query().Get("regionId"); regionID != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionID, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				respond(w, http.StatusOK, []api.ScopeStats{})
				return
			}
		}
		since, err := parseSince(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		stats, err := reader.GetScopeStats(r.Context(), iatas, since)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		if len(iatas) > 0 {
			members := scopeNamesFor(scopes, iatas)
			stats = slices.DeleteFunc(slices.Clone(stats), func(s api.ScopeStats) bool {
				_, found := slices.BinarySearch(members, s.Name)
				return !found
			})
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
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
			if len(iatas) == 0 {
				iatas = []string{""} // an empty region matches nothing, not everything
			}
		}
		result, err := reader.GetStatsNodeTypes(r.Context(), iatas)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "failed to get node type stats")
			return
		}
		respond(w, http.StatusOK, result)
	}
}
