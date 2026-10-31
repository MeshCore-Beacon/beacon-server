// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"encoding/hex"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// NodesRouter mounts all /nodes routes onto a subrouter.
//
// GET  /nodes                       → listNodes
// GET  /nodes/{nodeId}              → getNode
// GET  /nodes/{nodeId}/observations → listNodeObservations
func NodesRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listNodes(reader))
	r.Post("/scopes", nodeScopeBatch(reader))
	r.Route("/{nodeId}", func(r chi.Router) {
		r.Get("/", getNode(reader))
		r.Get("/observations", listNodeObservations(reader))
		r.Get("/neighbors", listNodeNeighbors(reader))
	})
	return r
}

// listNodes godoc
//
//	@Summary	List nodes
//	@Tags		Nodes
//	@Produce	json
//	@Param		type					query		int		false	"Node type integer (1=companion, 2=repeater, 3=room_server, 4=sensor)"
//	@Param		typeName				query		string	false	"Node type name (companion, repeater, room_server, sensor)"
//	@Param		iata			query		string	false	"Filter by single IATA code (case-insensitive)"
//	@Param		iatas			query		string	false	"Filter by multiple IATA codes, comma-separated e.g. YVR,YYJ"
//	@Param		regionId		query		int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region			query		string	false	"Filter by region slug, expands to member IATAs"
//	@Param		name					query		string	false	"Partial case-insensitive name match"
//	@Param		scope	query		string	false	"Filter by transport scope name e.g. %23bc (URL-encoded #bc)"
//	@Param		pubkey					query		string	false	"Exact public key match (hex)"
//	@Param		pubkeyPrefix			query		string	false	"Partial public key match: hex prefix, case-insensitive"
//	@Param		supportsMultibytePaths	query		bool	false	"Filter by multibyte path support (true/false); omit for no filter"
//	@Param		supportsMultibyteTraces	query		bool	false	"Filter by multibyte trace support (true/false); omit for no filter"
//	@Param		neighbors				query		bool	false	"Include each node's known neighbor IDs (neighborIds field). Bare ?neighbors or ?neighbors=true enables it; omit/false for none"
//	@Param		cursor					query		int		false	"last_seen epoch ms of last item for pagination"
//	@Param		pageCursor				query		string	false	"Opaque nextPageCursor from a previous response; keeps millisecond ties. Wins over cursor when both are sent"
//	@Param		sort					query		string	false	"Order: lastSeen (default), name, type or neighbors. Unnamed nodes come last in either name order; other ties go newest first, lastSeen ties by id. Page any order but lastSeen desc with pageCursor only"	Enums(lastSeen, name, type, neighbors)
//	@Param		order					query		string	false	"asc or desc. Defaults to asc, but to desc (newest first) for lastSeen; lastSeen asc lists the oldest heard first"	Enums(asc, desc)
//	@Param		limit					query		int		false	"Max results (default 50); must be positive, values above 2000 are clamped" minimum(1) maximum(2000)
//	@Success	200						{object}	api.NodePage
//	@Failure	400						{object}	handlers.APIError
//	@Failure	500						{object}	handlers.APIError
//	@Failure	503						{object}	handlers.APIError	"The read took too long: retry after Retry-After seconds"
//	@Router		/nodes [get]
func listNodes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		var nodeType int16
		if typeParam := r.URL.Query().Get("type"); typeParam != "" {
			t, err := strconv.ParseInt(typeParam, 10, 16)
			if err != nil {
				respondError(w, http.StatusBadRequest, "type must be an integer")
				return
			}
			nodeType = int16(t)
		} else if typeName := r.URL.Query().Get("typeName"); typeName != "" {
			nodeType = api.NodeTypeFromString(typeName)
		}
		limit, err := parseLimitUpTo(r, 50, maxNodeListLimit)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
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
		sort, sorted, err := parseNodeSort(r)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		var pageCursor *api.NodeCursor
		var sortCursor *api.NodeSortCursor
		if raw := r.URL.Query().Get("pageCursor"); raw != "" && sorted {
			sc, err := api.ParseNodeSortCursor(raw)
			if err != nil || sc.Sort != sort {
				respondError(w, http.StatusBadRequest, "invalid pageCursor for this sort")
				return
			}
			sortCursor = sc
		} else if raw != "" {
			pc, err := api.ParseNodeCursor(raw)
			if err != nil {
				respondError(w, http.StatusBadRequest, "invalid pageCursor")
				return
			}
			pageCursor = pc
		}
		if sorted && cursor != 0 {
			respondError(w, http.StatusBadRequest, "cursor works only with the default newest-first order; page with pageCursor")
			return
		}
		var pubkey []byte
		if pubkeyParam := strings.ToLower(r.URL.Query().Get("pubkey")); pubkeyParam != "" {
			b, err := hex.DecodeString(pubkeyParam)
			if err != nil {
				respondError(w, http.StatusBadRequest, "pubkey must be a valid hex string")
				return
			}
			pubkey = b
		}
		pubkeyPrefix := strings.ToLower(r.URL.Query().Get("pubkeyPrefix"))
		if pubkeyPrefix != "" && !isHexString(pubkeyPrefix) {
			respondError(w, http.StatusBadRequest, "pubkeyPrefix must be a hex string")
			return
		}
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		name := r.URL.Query().Get("name")
		scope := r.URL.Query().Get("scope")
		var supportsMultibytePaths *bool
		if v := r.URL.Query().Get("supportsMultibytePaths"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				respondError(w, http.StatusBadRequest, "invalid supportsMultibytePaths value")
				return
			}
			supportsMultibytePaths = &b
		}
		var supportsMultibyteTraces *bool
		if v := r.URL.Query().Get("supportsMultibyteTraces"); v != "" {
			b, err := strconv.ParseBool(v)
			if err != nil {
				respondError(w, http.StatusBadRequest, "invalid supportsMultibyteTraces value")
				return
			}
			supportsMultibyteTraces = &b
		}
		var includeNeighbors bool
		if vals, ok := r.URL.Query()["neighbors"]; ok {
			v := vals[0] // bare `?neighbors` (no `=`) parses as ""
			if v == "" {
				includeNeighbors = true
			} else {
				b, err := strconv.ParseBool(v)
				if err != nil {
					respondError(w, http.StatusBadRequest, "invalid neighbors value")
					return
				}
				includeNeighbors = b
			}
		}
		var nodes api.NodePage
		if sorted {
			nodes, err = reader.ListNodesSorted(r.Context(), nodeType, iatas, supportsMultibytePaths, supportsMultibyteTraces, pubkey, pubkeyPrefix, name, scope, sort, sortCursor, limit, includeNeighbors)
			nodes.Sort, nodes.Order = sort.By, "asc"
			if sort.Descending {
				nodes.Order = "desc"
			}
		} else {
			nodes, err = reader.ListNodes(r.Context(), nodeType, iatas, supportsMultibytePaths, supportsMultibyteTraces, pubkey, pubkeyPrefix, name, scope, cursor, pageCursor, limit, includeNeighbors)
			nodes.Sort, nodes.Order = "lastSeen", "desc"
		}
		if err != nil {
			respondReaderError(w, err, "internal server error")
			return
		}
		if r.URL.Query().Get("scopeEvidence") == "true" && !includeNeighbors {
			nodes.Items = append([]api.NodeSummary(nil), nodes.Items...)
			ids := []uuid.UUID{}
			for _, n := range nodes.Items {
				if n.NodeType == 2 {
					ids = append(ids, n.ID)
				}
			}
			if len(ids) > 0 {
				evidence, err := reader.GetNodeScopeEvidence(r.Context(), ids)
				if err != nil {
					respondReaderError(w, err, "failed to load scope evidence")
					return
				}
				for i := range nodes.Items {
					if v, ok := evidence[nodes.Items[i].ID.String()]; ok {
						nodes.Items[i].ScopeEvidence = &v
					}
				}
			}
		}
		respond(w, http.StatusOK, nodes)
	}
}

// parseNodeSort reads sort and order; sorted is false for the default order, last seen newest first.
func parseNodeSort(r *http.Request) (sort api.NodeSort, sorted bool, err error) {
	by, order := r.URL.Query().Get("sort"), r.URL.Query().Get("order")
	if order != "" && order != "asc" && order != "desc" {
		return sort, false, errors.New("order must be asc or desc")
	}
	switch {
	case by == "" || by == api.NodeSortLastSeen:
		// Newest first stays on the legacy path so the numeric cursor keeps working.
		if order == "asc" {
			return api.NodeSort{By: api.NodeSortLastSeen}, true, nil
		}
		return sort, false, nil
	case api.ValidNodeSort(by):
		return api.NodeSort{By: by, Descending: order == "desc"}, true, nil
	default:
		return sort, false, errors.New("sort must be lastSeen, name, type or neighbors")
	}
}

// getNode godoc
//
//	@Summary	Get node detail
//	@Tags		Nodes
//	@Produce	json
//	@Param		nodeId	path		string	true	"Node UUID"
//	@Success	200		{object}	api.Node
//	@Failure	400		{object}	handlers.APIError
//	@Failure	404		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Failure	503		{object}	handlers.APIError	"The read took too long: retry after Retry-After seconds"
//	@Router		/nodes/{nodeId} [get]
func getNode(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeID, err := uuid.Parse(chi.URLParam(r, "nodeId"))
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid node ID")
			return
		}
		node, err := reader.GetNode(r.Context(), nodeID)
		switch {
		case errors.Is(err, pgx.ErrNoRows), err == nil && node == nil:
			respondError(w, http.StatusNotFound, "node not found")
			return
		case err != nil:
			respondReaderError(w, err, "internal server error")
			return
		}
		respond(w, http.StatusOK, node)
	}
}

// listNodeObservations godoc
//
//	@Summary	List packet observations originating from a node
//	@Tags		Nodes
//	@Produce	json
//	@Param		nodeId	path		string	true	"Node UUID"
//	@Param		cursor	query		int		false	"Observation ID of last item for pagination"
//	@Param		limit	query		int		false	"Max results (default 50); must be positive, values above 200 are clamped" minimum(1) maximum(200)
//	@Success	200		{object}	api.Page[api.PacketObservationSummary]
//	@Failure	400		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Failure	503		{object}	handlers.APIError	"The read took too long: retry after Retry-After seconds"
//	@Router		/nodes/{nodeId}/observations [get]
func listNodeObservations(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeID, err := uuid.Parse(chi.URLParam(r, "nodeId"))
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid node ID")
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
		limit, err := parseLimit(r, 50)
		if err != nil {
			respondError(w, http.StatusBadRequest, err.Error())
			return
		}
		observations, err := reader.ListNodeObservations(r.Context(), nodeID, cursor, limit)
		if err != nil {
			respondReaderError(w, err, "internal server error")
			return
		}
		respond(w, http.StatusOK, observations)
	}
}

// listNodeNeighbors godoc
//
//	@Summary	List neighbors for a node
//	@Tags		Nodes
//	@Produce	json
//	@Param		nodeId	path		string	true	"Node UUID"
//	@Success	200		{object}	[]api.NodeNeighbor
//	@Failure	400		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Failure	503		{object}	handlers.APIError	"The read took too long: retry after Retry-After seconds"
//	@Router		/nodes/{nodeId}/neighbors [get]
func listNodeNeighbors(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeID, err := uuid.Parse(chi.URLParam(r, "nodeId"))
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid node ID")
			return
		}
		neighbors, err := reader.GetNodeNeighbors(r.Context(), nodeID)
		if err != nil {
			respondReaderError(w, err, "internal server error")
			return
		}
		respond(w, http.StatusOK, neighbors)
	}
}

// isHexString reports whether s consists only of hex digits (any case). Used to validate
// pubkeyPrefix, which is matched as text (not decoded to bytes, since a prefix can be an odd
// number of nibbles) directly against a SQL ILIKE pattern -- restricting the charset to hex
// digits keeps that safe from ILIKE wildcard injection (%, _) as a side effect.
func isHexString(s string) bool {
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F')) {
			return false
		}
	}
	return true
}
