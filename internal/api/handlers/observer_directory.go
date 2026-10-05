// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

// listObserverDirectory godoc
//
// @Summary List observers with windowed traffic counts
// @Description Counts cover completed UTC hourly analytics. Incomplete windows return null counts and name ordering. Repeat filters and explicit since/until on later pages. Results may move as data changes.
// @Tags Observers
// @Produce json
// @Param iata query string false "Single IATA"
// @Param iatas query string false "Comma-separated IATAs"
// @Param regionId query int false "Region ID"
// @Param region query string false "Region slug"
// @Param type query string false "Observer type"
// @Param broker query string false "Broker membership"
// @Param status query string false "online or offline"
// @Param name query string false "Case-insensitive partial name"
// @Param scope query string false "Transport scope membership"
// @Param sort query string false "traffic (default) or name"
// @Param since query int64 false "Inclusive epoch ms, rounded down to UTC hour; default 24 hours before until"
// @Param until query int64 false "Exclusive epoch ms, rounded down to UTC hour; default end of latest rollable hour"
// @Param cursor query int64 false "nextCursor returned by preceding page"
// @Param limit query int false "Page size, default 50, maximum 200"
// @Success 200 {object} api.ObserverDirectory
// @Failure 400 {object} handlers.APIError
// @Failure 500 {object} handlers.APIError
// @Router /observers/directory [get]
func listObserverDirectory(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		values := r.URL.Query()
		allowed := map[string]bool{"iata": true, "iatas": true, "regionId": true, "region": true, "type": true, "broker": true, "status": true, "name": true, "scope": true, "sort": true, "since": true, "until": true, "cursor": true, "limit": true}
		for k, v := range values {
			if !allowed[k] || len(v) != 1 {
				respondError(w, 400, "unknown or repeated directory parameter")
				return
			}
		}
		q := api.ObserverDirectoryQuery{}
		limit, err := parseLimit(r, 50)
		if err != nil {
			respondError(w, 400, err.Error())
			return
		}
		q.Limit = limit
		if values.Has("cursor") {
			q.Cursor, err = strconv.ParseInt(values.Get("cursor"), 10, 32)
			if err != nil || q.Cursor < 0 || q.Cursor > 2147483447 {
				respondError(w, 400, "invalid directory cursor")
				return
			}
		}
		if q.Cursor > 0 && (!values.Has("since") || !values.Has("until")) {
			respondError(w, 400, "continuation requires since and until from the first page")
			return
		}
		q.Sort = values.Get("sort")
		if q.Sort == "" {
			q.Sort = "traffic"
		}
		if q.Sort != "traffic" && q.Sort != "name" {
			respondError(w, 400, "sort must be traffic or name")
			return
		}
		q.Type = values.Get("type")
		q.Broker = values.Get("broker")
		q.Status = values.Get("status")
		q.Name = strings.ToLower(values.Get("name"))
		q.Scope = values.Get("scope")
		if q.Status != "" && q.Status != "online" && q.Status != "offline" {
			respondError(w, 400, "status must be online or offline")
			return
		}
		for _, v := range []string{q.Type, q.Broker, q.Name, q.Scope} {
			if len(v) > 200 {
				respondError(w, 400, "directory filters must be at most 200 bytes")
				return
			}
		}
		now := time.Now().UTC()
		until := now.Add(-95 * time.Minute).Truncate(time.Hour).Add(time.Hour)
		if values.Has("until") {
			n, e := strconv.ParseInt(values.Get("until"), 10, 64)
			if e != nil || n <= 0 || n > now.UnixMilli() {
				respondError(w, 400, "until must be a past epoch millisecond timestamp")
				return
			}
			until = time.UnixMilli(n).UTC().Truncate(time.Hour)
		}
		since := until.Add(-24 * time.Hour)
		if values.Has("since") {
			n, e := strconv.ParseInt(values.Get("since"), 10, 64)
			if e != nil || n <= 0 {
				respondError(w, 400, "since must be positive epoch milliseconds")
				return
			}
			since = time.UnixMilli(n).UTC().Truncate(time.Hour)
		}
		if !since.Before(until) || until.Sub(since) > 31*24*time.Hour {
			respondError(w, 400, "directory window must be between one hour and 31 days")
			return
		}
		q.Since = since.UnixMilli()
		q.Until = until.UnixMilli()
		q.IATAs = parseIATAs(r)
		if values.Get("regionId") != "" || values.Get("region") != "" {
			iatas, err := resolveRegionIATAs(r.Context(), values.Get("regionId"), values.Get("region"), reader)
			if err != nil {
				respondRegionError(w, err)
				return
			}
			q.IATAs = append(q.IATAs, iatas...)
			q.MatchNone = len(q.IATAs) == 0
		}
		if len(q.IATAs) > 200 {
			respondError(w, 400, "too many IATA filters")
			return
		}
		slices.Sort(q.IATAs)
		q.IATAs = slices.Compact(q.IATAs)
		page, err := reader.ListObserverDirectory(r.Context(), q)
		w.Header().Set("Cache-Control", "no-store")
		if err != nil {
			respondError(w, 500, "failed to get observer directory")
			return
		}
		respond(w, 200, page)
	}
}
