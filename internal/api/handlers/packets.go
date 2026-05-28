package handlers

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"time"

	"github.com/MeshCore-Tower/tower-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// PacketsRouter mounts all /packets routes onto a subrouter.
//
// GET  /packets                              → ListPackets
// GET  /packets/{packetHash}                 → GetPacket
func PacketsRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()

	// GET /api/v1/packets
	//
	// Query params (all optional):
	//
	//	payloadType=<int>        filter by payload type integer
	//	payloadTypeName=<string> filter by payload type name (advert, grp_txt, txt_msg, trace, anon_req)
	//	routeType=<int>          filter by route type integer (0=transport_flood, 1=flood, 2=direct, 3=transport_direct)
	//	iata=<code>              filter by latest observation IATA (case-insensitive)
	//	since=<epoch ms>         filter by first_heard_at >= since
	//	until=<epoch ms>         filter by first_heard_at <= until
	//	cursor=<int>             last_heard_at epoch ms of last item for pagination
	//	limit=50
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		var payloadType int16
		if p := r.URL.Query().Get("payloadType"); p != "" {
			t, err := strconv.ParseInt(p, 10, 16)
			if err != nil {
				respondError(w, http.StatusBadRequest, "payloadType must be an integer")
				return
			}
			payloadType = int16(t)
		}
		if payloadType == 0 {
			if p := r.URL.Query().Get("payloadTypeName"); p != "" {
				payloadType = api.PayloadTypeFromString(p)
			}
		}
		var routeType int16
		if p := r.URL.Query().Get("routeType"); p != "" {
			t, err := strconv.ParseInt(p, 10, 16)
			if err != nil {
				respondError(w, http.StatusBadRequest, "routeType must be an integer")
				return
			}
			routeType = int16(t)
		}
		var since, until time.Time
		if p := r.URL.Query().Get("since"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
				return
			}
			since = time.UnixMilli(ms)
		}
		if p := r.URL.Query().Get("until"); p != "" {
			ms, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "until must be epoch milliseconds")
				return
			}
			until = time.UnixMilli(ms)
		}
		var cursor int64
		if p := r.URL.Query().Get("cursor"); p != "" {
			c, err := strconv.ParseInt(p, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "cursor must be an integer")
				return
			}
			cursor = c
		}
		var limit int32 = 50
		if p := r.URL.Query().Get("limit"); p != "" {
			l, err := strconv.ParseInt(p, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = int32(l)
		}
		iata := r.URL.Query().Get("iata")
		packets, err := reader.ListPackets(r.Context(), payloadType, routeType, iata, since, until, cursor, limit)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, packets)
	})

	// GET /api/v1/packets/{packetHash}
	//
	// Returns full packet detail including all observations and resolved paths.
	r.Get("/{packetHash}", func(w http.ResponseWriter, r *http.Request) {
		hashHex := chi.URLParam(r, "packetHash")
		hash, err := hex.DecodeString(hashHex)
		if err != nil {
			respondError(w, http.StatusBadRequest, "invalid packet hash")
			return
		}
		packet, err := reader.GetPacket(r.Context(), hash)
		if err != nil {
			respondError(w, http.StatusNotFound, "packet not found")
			return
		}
		respond(w, http.StatusOK, packet)
	})

	return r
}
