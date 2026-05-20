package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// PacketsRouter mounts all /packets routes onto a subrouter.
//
// GET  /packets                              → ListPackets
// GET  /packets/{packetHash}                 → GetPacket
func PacketsRouter() http.Handler {
	r := chi.NewRouter()

	r.Get("/", ListPackets)
	r.Get("/{packetHash}", GetPacket)

	return r
}

// ListPackets handles GET /api/v1/packets
//
// Query params (all optional):
//
//	iata=YOW
//	payloadType=4
//	routeType=1
//	since=<epoch ms>
//	until=<epoch ms>
//	afterId=<observation id>   used for deterministic WS reconnection backfill
//	limit=50
//	cursor=<opaque>
//
// Returns a paginated list of packet summaries with the latest observation
// rolled in, newest first.
func ListPackets(w http.ResponseWriter, r *http.Request) {
	// TODO: parse query params, query DB/cache, write JSON response.
	//
	// afterId (int64) takes precedence over cursor for reconnection backfill:
	//   WHERE id > afterId ORDER BY id ASC LIMIT limit
	// Normal pagination uses cursor (opaque, encodes last seen id+timestamp).
	w.WriteHeader(http.StatusNotImplemented)
}

// GetPacket handles GET /api/v1/packets/{packetHash}
//
// Returns the full packet with all observations and each observation's
// resolved path inline.
func GetPacket(w http.ResponseWriter, r *http.Request) {
	// packetHash := chi.URLParam(r, "packetHash")
	// TODO: fetch packet + observations, resolve paths, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}
