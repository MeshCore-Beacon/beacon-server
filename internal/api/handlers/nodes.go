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
// GET  /nodes                                → ListNodes
// GET  /nodes/{nodeId}                       → GetNode
// GET  /nodes/{nodeId}/observations          → ListNodeObservations
func NodesRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()

	// GET /api/v1/nodes
	//
	// Query params (all optional):
	//
	//	type=<int>               node type integer (1=companion, 2=repeater, 3=room_server, 4=sensor)
	//	typeName=<string>        node type name (companion, repeater, room_server, sensor)
	//	iata=<code>              filter by IATA code (case-insensitive)
	//	name=<string>            partial case-insensitive name match
	//	pubkey=<hex>             exact public key match
	//	supportsMultibytePaths=true   filter to nodes with firmware >= 1.14.0
	//	supportsMultibyteTraces=true  filter to nodes with firmware >= 1.11.0
	//	cursor=<int>             last_seen epoch ms of last item for pagination
	//	limit=50
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
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

		iata := r.URL.Query().Get("iata")
		name := r.URL.Query().Get("name")
		supportsMultibytePaths := r.URL.Query().Get("supportsMultibytePaths") == "true"
		supportsMultibyteTraces := r.URL.Query().Get("supportsMultibyteTraces") == "true"

		nodes, err := reader.ListNodes(r.Context(), nodeType, iata, supportsMultibytePaths, supportsMultibyteTraces, pubkey, name, cursor, limit)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, nodes)
	})

	r.Route("/{nodeId}", func(r chi.Router) {
		// GET /api/v1/nodes/{nodeId}
		//
		// Returns full node detail including firmware capability flags,
		// location source, first/last seen timestamps, and raw metadata.
		// Use the nodes list endpoint with pubkey filter to look up a node by public key.
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
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
		})
		r.Get("/observations", func(w http.ResponseWriter, r *http.Request) {
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
		})
	})

	return r
}
