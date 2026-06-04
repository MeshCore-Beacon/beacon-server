package handlers

import (
	"encoding/hex"
	"net/http"
	"strconv"

	"github.com/MeshCore-Tower/tower-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// NodesRouter mounts all /nodes routes onto a subrouter.
//
// GET  /nodes                       → listNodes
// GET  /nodes/{nodeId}              → getNode
// GET  /nodes/{nodeId}/observations → listNodeObservations
func NodesRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listNodes(reader))
	r.Route("/{nodeId}", func(r chi.Router) {
		r.Get("/", getNode(reader))
		r.Get("/observations", listNodeObservations(reader))
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
//	@Param		supportsMultibytePaths	query		bool	false	"Filter by multibyte path support (true/false); omit for no filter"
//	@Param		supportsMultibyteTraces	query		bool	false	"Filter by multibyte trace support (true/false); omit for no filter"
//	@Param		cursor					query		int		false	"last_seen epoch ms of last item for pagination"
//	@Param		limit					query		int		false	"Max results (default 50)"
//	@Success	200						{object}	api.Page[api.NodeSummary]
//	@Failure	400						{object}	handlers.APIError
//	@Failure	500						{object}	handlers.APIError
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
		var limit int32 = 50
		if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
			l, err := strconv.ParseInt(limitParam, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = int32(l)
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
		var pubkey []byte
		if pubkeyParam := r.URL.Query().Get("pubkey"); pubkeyParam != "" {
			b, err := hex.DecodeString(pubkeyParam)
			if err != nil {
				respondError(w, http.StatusBadRequest, "pubkey must be a valid hex string")
				return
			}
			pubkey = b
		}
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionIDStr, r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
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
		nodes, err := reader.ListNodes(r.Context(), nodeType, iatas, supportsMultibytePaths, supportsMultibyteTraces, pubkey, name, scope, cursor, limit)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, nodes)
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
//	@Router		/nodes/{nodeId} [get]
func getNode(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		nodeID, err := uuid.Parse(chi.URLParam(r, "nodeId"))
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid node ID")
			return
		}
		node, err := reader.GetNode(r.Context(), nodeID)
		if err != nil {
			respondError(w, http.StatusNotFound, "node not found")
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
//	@Param		limit	query		int		false	"Max results (default 50)"
//	@Success	200		{object}	api.Page[api.PacketObservationSummary]
//	@Failure	400		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
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
		var limit int32 = 50
		if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
			l, err := strconv.ParseInt(limitParam, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = int32(l)
		}
		observations, err := reader.ListNodeObservations(r.Context(), nodeID, cursor, limit)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, observations)
	}
}
