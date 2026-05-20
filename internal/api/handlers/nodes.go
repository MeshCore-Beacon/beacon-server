package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// NodesRouter mounts all /nodes routes onto a subrouter.
//
// GET  /nodes                                → ListNodes
// GET  /nodes/{nodeId}                       → GetNode
// GET  /nodes/{nodeId}/observations          → ListNodeObservations
func NodesRouter() http.Handler {
	r := chi.NewRouter()

	r.Get("/", ListNodes)

	r.Route("/{nodeId}", func(r chi.Router) {
		r.Get("/", GetNode)
		r.Get("/observations", ListNodeObservations)
	})

	return r
}

// ListNodes handles GET /api/v1/nodes
//
// Query params (all optional):
//   type=2              (node_type: 1=companion, 2=repeater, 3=room server)
//   iata=YOW
//   firmwareTier=1.14.0
//   limit=50
//   cursor=<opaque>
func ListNodes(w http.ResponseWriter, r *http.Request) {
	// TODO: query nodes with optional filters, paginate, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}

// GetNode handles GET /api/v1/nodes/{nodeId}
//
// Returns full node detail including iatasHeardIn, firmware capability flags,
// minFirmwareVersion, and the latest advert payload.
func GetNode(w http.ResponseWriter, r *http.Request) {
	// nodeId := chi.URLParam(r, "nodeId")
	// TODO: fetch node, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}

// ListNodeObservations handles GET /api/v1/nodes/{nodeId}/observations
//
// Query params (all optional):
//   since=<epoch ms>
//   limit=50
//   cursor=<opaque>
func ListNodeObservations(w http.ResponseWriter, r *http.Request) {
	// nodeId := chi.URLParam(r, "nodeId")
	// TODO: fetch observations for node, paginate, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}
