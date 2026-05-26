package handlers

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"time"

	"github.com/MeshCore-Tower/tower-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// ChannelsRouter mounts all /channels routes onto a subrouter.
//
// GET  /channels                           → ListChannels
// GET  /channels/{channelID}               → GetChannel
// GET  /channels/{channelID}/messages      → ListChannelMessages
func ChannelsRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()

	// GET /api/v1/channels
	//
	// Query params (all optional):
	//
	//	hash=<hex>       filter by single-byte channel hash
	//	iata=<code>      filter by IATA code (channels with messages heard in that IATA)
	//	limit=50
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		var limit int64 = 50
		if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
			l, err := strconv.ParseInt(limitParam, 10, 32)
			if err != nil {
				respondError(w, http.StatusBadRequest, "limit must be an integer")
				return
			}
			limit = l
		}
		iata := r.URL.Query().Get("iata")
		var hashHex []byte
		if hash := r.URL.Query().Get("hash"); hash != "" {
			h, decodeErr := hex.DecodeString(hash)
			if decodeErr != nil {
				respondError(w, http.StatusBadRequest, "invalid channel hash")
				return
			}
			if len(h) != 1 {
				respondError(w, http.StatusBadRequest, "hash must be a single hex byte")
				return
			}
			hashHex = h
		}
		channels, err := reader.ListChannels(r.Context(), int32(limit), hashHex, iata)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, channels)
	})

	r.Route("/{channelID}", func(r chi.Router) {
		// GET /api/v1/channels/{channelID}
		//
		// Returns channel detail including key for hashtag channels and message count.
		// Other channel keys are server-side config; key material is never exposed via the API.
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			var id int64
			if channelID := chi.URLParam(r, "channelID"); channelID != "" {
				i, err := strconv.ParseInt(channelID, 10, 32)
				if err != nil {
					respondError(w, http.StatusBadRequest, "channelID should be an int 32")
					return
				}
				id = i
			}
			channel, err := reader.GetChannel(r.Context(), int32(id))
			if err != nil {
				respondError(w, http.StatusNotFound, "channel not found")
				return
			}
			respond(w, http.StatusOK, channel)
		})
		// GET /api/v1/channels/{channelID}/messages
		//
		// Query params (all optional):
		//
		//	since=<epoch ms>   return messages after this timestamp
		//	iata=<code>        filter by IATA code
		//	limit=50
		//
		// Returns paginated decrypted channel messages.
		r.Get("/messages", func(w http.ResponseWriter, r *http.Request) {
			var id int64
			if channelID := chi.URLParam(r, "channelID"); channelID != "" {
				i, err := strconv.ParseInt(channelID, 10, 32)
				if err != nil {
					respondError(w, http.StatusBadRequest, "channelID should be an int 32")
					return
				}
				id = i
			}
			var limit int64 = 50
			if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
				l, err := strconv.ParseInt(limitParam, 10, 32)
				if err != nil {
					respondError(w, http.StatusBadRequest, "limit must be an integer")
					return
				}
				limit = l
			}
			var since time.Time
			if sinceParam := r.URL.Query().Get("since"); sinceParam != "" {
				ms, err := strconv.ParseInt(sinceParam, 10, 64)
				if err != nil {
					respondError(w, http.StatusBadRequest, "since must be epoch milliseconds")
					return
				}
				since = time.UnixMilli(ms)
			}
			iata := r.URL.Query().Get("iata")
			chanID := int32(id)
			messages, err := reader.ListChannelMessages(r.Context(), &chanID, since, int32(limit), iata)
			if err != nil {
				respondError(w, http.StatusInternalServerError, "internal server error")
				return
			}
			respond(w, http.StatusOK, messages)
		})
	})

	return r
}
