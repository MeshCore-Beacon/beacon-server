// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// RoutesRouter mounts all /routes routes onto a subrouter.
//
// GET /routes          → listKnownRoutes
// GET /routes/search   → searchKnownRoutes
func RoutesRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listKnownRoutes(reader))
	r.Get("/cross", searchCrossIATARoutes(reader))
	r.Get("/search", searchKnownRoutes(reader))
	r.Get("/{iata}/{pathKey}/observations", getRouteEvidence(reader))
	return r
}

// listKnownRoutes godoc
//
//	@Summary	List known routes
//	@Tags		Routes
//	@Produce	json
//	@Param		iata		query		string	false	"Filter by IATA code"
//	@Param		hopCount	query		int		false	"Filter by exact hop count"
//	@Param		cursor		query		int		false	"Epoch ms timestamp of last item for pagination"
//	@Param		cursorId	query		int		false	"id of the last item; with cursor, also returns later routes sharing that millisecond"
//	@Param		limit		query		int		false	"Max results (default 50); must be positive, values above 200 are clamped" minimum(1) maximum(200)
//	@Failure	400			{object}	handlers.APIError
//	@Success	200			{object}	[]api.KnownRoute
//	@Failure	500			{object}	handlers.APIError
//	@Router		/routes [get]
func listKnownRoutes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iata := r.URL.Query().Get("iata")
		var hopCount int32
		if v := r.URL.Query().Get("hopCount"); v != "" {
			if h, err := strconv.ParseInt(v, 10, 32); err == nil {
				hopCount = int32(h)
			}
		}
		var cursor time.Time
		if v := r.URL.Query().Get("cursor"); v != "" {
			if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
				cursor = time.UnixMilli(ms)
			}
		}
		// cursorId breaks ties within the cursor's millisecond.
		var cursorID int64
		if v := r.URL.Query().Get("cursorId"); v != "" {
			id, err := strconv.ParseInt(v, 10, 64)
			if err != nil || id <= 0 {
				respondError(w, http.StatusBadRequest, "cursorId must be a positive integer")
				return
			}
			if cursor.IsZero() {
				respondError(w, http.StatusBadRequest, "cursorId requires cursor")
				return
			}
			cursorID = id
		}
		limit, err := parseLimit(r, 50)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		routes, err := reader.ListKnownRoutes(r.Context(), iata, hopCount, cursor, cursorID, limit)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, routes)
	}
}

// maxRouteSearchIATAs bounds the iatas list; the searches themselves are bounded by row limits.
const maxRouteSearchIATAs = 100

// searchKnownRoutes godoc
//
//	@Summary		Search known routes by source and destination hash
//	@Description	Routes within one IATA that run from the source hash to the destination hash,
//	@Description	trimmed to that span. iatas is optional (omitted searches every IATA); the single
//	@Description	iata is still accepted. At most 500, shortest first.
//	@Tags			Routes
//	@Produce		json
//	@Param			iatas	query		string	false	"Comma-separated IATA codes to search (max 100)"
//	@Param			iata	query		string	false	"Single IATA code (when iatas is omitted)"
//	@Param			from	query		string	true	"Source node hash prefix (hex, 1-4 bytes)"
//	@Param			to		query		string	true	"Destination node hash prefix (hex, 1-4 bytes)"
//	@Success		200		{object}	[]api.KnownRoute
//	@Failure		400		{object}	handlers.APIError
//	@Failure		500		{object}	handlers.APIError
//	@Failure		503		{object}	handlers.APIError
//	@Router			/routes/search [get]
func searchKnownRoutes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas, iataErr := routeIATAs(r)
		from := strings.ToLower(r.URL.Query().Get("from"))
		to := strings.ToLower(r.URL.Query().Get("to"))
		if from == "" || to == "" {
			respondError(w, http.StatusBadRequest, "from and to are required")
			return
		}
		if !validHopHash(from) || !validHopHash(to) {
			respondError(w, http.StatusBadRequest, "from and to must be 1-4 bytes of hex")
			return
		}
		if iataErr != nil {
			respondError(w, http.StatusBadRequest, iataErr.Error())
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		routes, err := reader.SearchKnownRoutes(ctx, iatas, from, to)
		if respondRouteSearchError(w, ctx, err) {
			return
		}
		respond(w, http.StatusOK, routes)
	}
}

// searchCrossIATARoutes godoc
//
//	@Summary		Search for routes that cross IATA boundaries
//	@Description	Pass iatas (comma-separated, optional; omitted searches every IATA) or the
//	@Description	single directed pair fromIata/toIata, not both.
//	@Tags			Routes
//	@Produce		json
//	@Param			fromHash	query		string	true	"Source node hash prefix (hex, 1-4 bytes)"
//	@Param			toHash		query		string	true	"Destination node hash prefix (hex, 1-4 bytes)"
//	@Param			iatas		query		string	false	"Comma-separated IATA codes to search (2-100)"
//	@Param			fromIata	query		string	false	"Source IATA code (with toIata)"
//	@Param			toIata		query		string	false	"Destination IATA code (with fromIata)"
//	@Success		200			{object}	[]api.CrossIATARoute
//	@Failure		400			{object}	handlers.APIError
//	@Failure		500			{object}	handlers.APIError
//	@Failure		503			{object}	handlers.APIError
//	@Router			/routes/cross [get]
func searchCrossIATARoutes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q, err := parseCrossRouteSearch(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		routes, err := reader.SearchCrossIATARoutes(ctx, q)
		if respondRouteSearchError(w, ctx, err) {
			return
		}
		if routes == nil {
			routes = []api.CrossIATARoute{}
		}
		respond(w, http.StatusOK, routes)
	}
}

func parseCrossRouteSearch(r *http.Request) (api.CrossRouteSearch, error) {
	v := r.URL.Query()
	q := api.CrossRouteSearch{
		FromHash: strings.ToLower(v.Get("fromHash")),
		ToHash:   strings.ToLower(v.Get("toHash")),
	}
	if q.FromHash == "" || q.ToHash == "" {
		return q, errors.New("fromHash and toHash are required")
	}
	if !validHopHash(q.FromHash) || !validHopHash(q.ToHash) {
		return q, errors.New("fromHash and toHash must be 1-4 bytes of hex")
	}
	fromIATA, toIATA := strings.ToUpper(v.Get("fromIata")), strings.ToUpper(v.Get("toIata"))
	if fromIATA != "" || toIATA != "" {
		if fromIATA == "" || toIATA == "" {
			return q, errors.New("fromIata and toIata must be given together")
		}
		if v.Has("iatas") {
			return q, errors.New("use iatas or fromIata/toIata, not both")
		}
		q.FromIATAs, q.ToIATAs = []string{fromIATA}, []string{toIATA}
		return q, nil
	}
	if !v.Has("iatas") {
		return q, nil
	}
	iatas, err := routeIATAs(r)
	if err != nil {
		return q, err
	}
	if len(iatas) < 2 {
		return q, fmt.Errorf("iatas must list 2-%d distinct codes", maxRouteSearchIATAs)
	}
	q.FromIATAs, q.ToIATAs = iatas, iatas
	return q, nil
}

// respondRouteSearchError writes the response for a failed route search and reports whether it did.
func respondRouteSearchError(w http.ResponseWriter, ctx context.Context, err error) bool {
	switch {
	case err == nil:
		return false
	case errors.Is(err, api.ErrRouteSearchTooBroad):
		respondError(w, http.StatusBadRequest, err.Error())
	case errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded):
		respondError(w, http.StatusServiceUnavailable, "route search timed out; use a longer hash or fewer iatas")
	case errors.Is(err, context.Canceled):
		// client went away; nothing to write
	default:
		respondError(w, http.StatusInternalServerError, "internal server error")
	}
	return true
}

// routeIATAs dedupes the iatas (or iata) list, failing as soon as it passes the cap so a
// huge list costs no more than parsing it.
func routeIATAs(r *http.Request) ([]string, error) {
	var out []string
	seen := make(map[string]struct{})
	for _, c := range parseIATAs(r) {
		if len(c) != 3 {
			return nil, errors.New("iatas must be 3-letter codes")
		}
		if _, dup := seen[c]; dup {
			continue
		}
		if len(out) == maxRouteSearchIATAs {
			return nil, fmt.Errorf("iatas must list at most %d codes", maxRouteSearchIATAs)
		}
		seen[c] = struct{}{}
		out = append(out, c)
	}
	return out, nil
}

func validHopHash(h string) bool {
	if len(h) < 2 || len(h) > 8 {
		return false
	}
	_, err := hex.DecodeString(h)
	return err == nil
}
