// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// TracesRouter mounts all /traces routes onto a subrouter.
//
// GET /traces        → listTraceTags
// GET /traces/{tag}  → getTrace
func TracesRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listTraceTags(reader))
	r.Get("/{tag}", getTrace(reader))
	return r
}

// listTraceTags godoc
//
//	@Summary	List trace tags
//	@Tags		Traces
//	@Produce	json
//	@Param		iatas		query		string	false	"Filter by IATA code(s), comma-separated"
//	@Param		region		query		string	false	"Filter by region slug"
//	@Param		regionId	query		int		false	"Filter by region ID"
//	@Param		scope		query		string	false	"Filter by transport scope name"
//	@Param		type		query		string	false	"Filter by type: TRACE or PING (default: all)"
//	@Param		since		query		int		false	"Filter by first_heard_at >= since (epoch ms)"
//	@Param		until		query		int		false	"Filter by first_heard_at <= until (epoch ms)"
//	@Param		cursor		query		int		false	"last_heard_at epoch ms of last item for pagination"
//	@Param		limit		query		int		false	"Max results (default 50)"
//	@Success	200			{object}	[]api.TraceTagSummary
//	@Failure	500			{object}	handlers.APIError
//	@Router		/traces [get]
func listTraceTags(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var limit int32 = 50
		if l := r.URL.Query().Get("limit"); l != "" {
			if v, err := strconv.ParseInt(l, 10, 32); err == nil {
				limit = int32(v)
			}
		}
		var since, until, cursor time.Time
		if v := r.URL.Query().Get("since"); v != "" {
			if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
				since = time.UnixMilli(ms)
			}
		}
		if v := r.URL.Query().Get("until"); v != "" {
			if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
				until = time.UnixMilli(ms)
			}
		}
		if v := r.URL.Query().Get("cursor"); v != "" {
			if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
				cursor = time.UnixMilli(ms)
			}
		}
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), r.URL.Query().Get("regionId"), r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		scope := r.URL.Query().Get("scope")
		traceType := strings.ToUpper(r.URL.Query().Get("type"))
		tags, err := reader.ListTraceTags(r.Context(), iatas, scope, traceType, since, until, cursor, limit)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, tags)
	}
}

// getTrace godoc
//
//	@Summary	Get full trace detail by tag
//	@Tags		Traces
//	@Produce	json
//	@Param		tag	path		string	true	"Trace tag hex e.g. a3f1b2c4"
//	@Success	200	{object}	api.TraceDetail
//	@Failure	404	{object}	handlers.APIError
//	@Failure	500	{object}	handlers.APIError
//	@Router		/traces/{tag} [get]
func getTrace(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		tag := chi.URLParam(r, "tag")
		trace, err := reader.GetTraceByTag(r.Context(), tag)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		if trace == nil {
			respondError(w, http.StatusNotFound, "trace not found")
			return
		}
		respond(w, http.StatusOK, trace)
	}
}
