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

	// ListChannels handles GET /api/v1/channels
	//
	// Query params (all optional):
	//
	//	hash=<hex>
	//	limit=50
	//	cursor=<opaque>
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		var limit int64 = 50
		if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
			l, err := strconv.ParseInt(limitParam, 10, 32)
			if err != nil {
				respond(w, http.StatusBadRequest, map[string]string{"error": "limit must be an integer"})
				return
			}
			limit = l
		}
		var channels []api.ChannelSummary
		var err error
		if hash := r.URL.Query().Get("hash"); hash != "" {
			hashHex, decodeErr := hex.DecodeString(hash)
			if decodeErr != nil {
				respond(w, http.StatusBadRequest, map[string]string{"error": "invalid channel hash"})
				return
			}
			if len(hashHex) != 1 {
				respond(w, http.StatusBadRequest, map[string]string{"error": "hash must be a single hex byte"})
				return
			}
			channels, err = reader.ListChannels(r.Context(), int32(limit), hashHex)
		} else {
			channels, err = reader.ListChannels(r.Context(), int32(limit), nil)
		}
		if err != nil {
			respond(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
			return
		}
		respond(w, http.StatusOK, channels)
	})

	r.Route("/{channelID}", func(r chi.Router) {
		// GetChannel handles GET /api/v1/channels/{channelID}
		//
		// Returns channel detail including key for hashtag channels and message count.
		// Other channel keys are server-side config; key material is never exposed via the API.
		r.Get("/", func(w http.ResponseWriter, r *http.Request) {
			var id int64
			if channelID := chi.URLParam(r, "channelID"); channelID != "" {
				i, err := strconv.ParseInt(channelID, 10, 32)
				if err != nil {
					respond(w, http.StatusBadRequest, map[string]string{"error": "channelID should be an int 32"})
					return
				}
				id = i
			}
			channel, err := reader.GetChannel(r.Context(), int32(id))
			if err != nil {
				respond(w, http.StatusNotFound, map[string]string{"error": "channel not found"})
				return
			}
			respond(w, http.StatusOK, channel)
		})
		// ListChannelMessages handles GET /api/v1/channels/{channelID}/messages
		//
		// Query params (all optional):
		//
		//	since=<epoch ms>
		//	limit=50
		//	cursor=<opaque>
		//
		// Returns paginated decrypted channel messages.
		r.Get("/messages", func(w http.ResponseWriter, r *http.Request) {
			var id int64
			if channelID := chi.URLParam(r, "channelID"); channelID != "" {
				i, err := strconv.ParseInt(channelID, 10, 32)
				if err != nil {
					respond(w, http.StatusBadRequest, map[string]string{"error": "channelID should be an int 32"})
					return
				}
				id = i
			}
			var limit int64 = 50
			if limitParam := r.URL.Query().Get("limit"); limitParam != "" {
				l, err := strconv.ParseInt(limitParam, 10, 32)
				if err != nil {
					respond(w, http.StatusBadRequest, map[string]string{"error": "limit must be an integer"})
					return
				} else {
					limit = l
				}
			}
			var since time.Time
			if sinceParam := r.URL.Query().Get("since"); sinceParam != "" {
				ms, err := strconv.ParseInt(sinceParam, 10, 64)
				if err != nil {
					respond(w, http.StatusBadRequest, map[string]string{"error": "since must be epoch milliseconds"})
					return
				}
				since = time.UnixMilli(ms)
			}
			chanID := int32(id)
			messages, err := reader.ListChannelMessages(r.Context(), &chanID, since, int32(limit))
			if err != nil {
				respond(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
				return
			}
			respond(w, http.StatusOK, messages)
		})
	})

	return r
}
