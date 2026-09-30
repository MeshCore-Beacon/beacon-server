// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package handlers

import (
	"errors"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

func routeEvidenceQuery(r *http.Request, iata, key string, now time.Time) (api.RouteEvidenceQuery, error) {
	q := api.RouteEvidenceQuery{}
	values := r.URL.Query()
	for _, name := range []string{"pageCursor", "since", "until", "range", "limit"} {
		if len(values[name]) > 1 {
			return q, api.ErrRouteEvidenceInput
		}
	}
	if values.Has("cursor") {
		return q, api.ErrRouteEvidenceInput
	}
	limit, err := parseLimit(r, 50)
	if err != nil {
		return q, err
	}
	q.Limit = limit
	if values.Has("pageCursor") {
		c, err := api.ParseRouteEvidenceCursor(values.Get("pageCursor"))
		if err != nil || c.IATA != iata || c.PathKey != key {
			return q, api.ErrRouteEvidenceInput
		}
		q.Cursor = c
		q.Since = c.Since
		q.Until = c.Until
	} else {
		q.Until = now.UTC().Truncate(time.Millisecond)
		q.Since = q.Until.Add(-24 * time.Hour)
	}
	if values.Has("range") {
		if values.Has("pageCursor") || values.Has("since") || values.Has("until") {
			return q, api.ErrRouteEvidenceInput
		}
		duration, err := time.ParseDuration(values.Get("range"))
		if err != nil || duration < time.Millisecond || duration > api.MaxRouteEvidenceWindow || duration%time.Millisecond != 0 {
			return q, api.ErrRouteEvidenceInput
		}
		q.Since = q.Until.Add(-duration)
	}
	if values.Has("since") || values.Has("until") {
		if !values.Has("since") || !values.Has("until") {
			return q, api.ErrRouteEvidenceInput
		}
		since, e1 := strconv.ParseInt(values.Get("since"), 10, 64)
		until, e2 := strconv.ParseInt(values.Get("until"), 10, 64)
		if e1 != nil || e2 != nil || since < 0 || until < 0 || since > 253402300799999 || until > 253402300799999 {
			return q, api.ErrRouteEvidenceInput
		}
		q.Since = time.UnixMilli(since).UTC()
		q.Until = time.UnixMilli(until).UTC()
		if q.Cursor != nil && (!q.Since.Equal(q.Cursor.Since) || !q.Until.Equal(q.Cursor.Until)) {
			return q, api.ErrRouteEvidenceInput
		}
	}
	if !api.ValidRouteEvidenceWindow(q.Since, q.Until) || q.Until.After(now.Add(clockSkewTolerance)) {
		return q, api.ErrRouteEvidenceInput
	}
	return q, nil
}

// getRouteEvidence godoc
//
// @Summary Get retained reports matching a full saved route prefix sequence
// @Description Matches the saved IATA, complete path bytes and hash width. Excludes TRACE and unclassified reports; other widths and search-result subsegments are not included. Byte matches do not prove hop identities or delivery. The route and its historical counter can outlive raw evidence. Cursors retain full timestamp precision and pin route/window scope.
// @Tags Routes
// @Produce json
// @Param iata path string true "Three-character IATA code"
// @Param pathKey path string true "Stable 32-hex pathKey from a known-route response"
// @Param range query string false "Server-anchored duration (default 24h, max 720h); exclusive with since/until/pageCursor"
// @Param since query int false "Window start epoch ms; provide with until (default last 24h)"
// @Param until query int false "Exclusive window end epoch ms; maximum span 30d, end may be up to 5 minutes ahead of server time (clock skew tolerance)"
// @Param pageCursor query string false "Opaque precise cursor from nextPageCursor; window is pinned"
// @Param limit query int false "Default 50; positive, capped at 200" minimum(1) maximum(200)
// @Success 200 {object} api.RouteEvidence
// @Failure 400 {object} handlers.APIError
// @Failure 404 {object} handlers.APIError
// @Failure 500 {object} handlers.APIError
// @Router /routes/{iata}/{pathKey}/observations [get]
func getRouteEvidence(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iata, key := strings.ToUpper(chi.URLParam(r, "iata")), strings.ToLower(chi.URLParam(r, "pathKey"))
		if !api.ValidRouteEvidenceKey(iata, key) {
			respondError(w, 400, "invalid IATA or route pathKey")
			return
		}
		query, err := routeEvidenceQuery(r, iata, key, time.Now())
		if err != nil {
			respondError(w, 400, "invalid route evidence window, limit or page cursor")
			return
		}
		result, err := reader.GetRouteEvidence(r.Context(), iata, key, query)
		switch {
		case errors.Is(err, pgx.ErrNoRows), err == nil && result == nil:
			respondError(w, 404, "saved route not found")
		case errors.Is(err, api.ErrRouteEvidenceInput):
			respondError(w, 400, "invalid route evidence request")
		case err != nil:
			respondError(w, 500, "internal server error")
		default:
			respond(w, 200, result)
		}
	}
}
