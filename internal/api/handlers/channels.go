package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// ChannelsRouter mounts all /channels routes onto a subrouter.
//
// GET  /channels                             → ListChannels
// GET  /channels/{channelHash}               → GetChannel
// GET  /channels/{channelHash}/messages      → ListChannelMessages
func ChannelsRouter() http.Handler {
	r := chi.NewRouter()

	r.Get("/", ListChannels)

	r.Route("/{channelHash}", func(r chi.Router) {
		r.Get("/", GetChannel)
		r.Get("/messages", ListChannelMessages)
	})

	return r
}

// ListChannels handles GET /api/v1/channels
//
// Query params (all optional):
//   limit=50
func ListChannels(w http.ResponseWriter, r *http.Request) {
	// TODO: query channels ORDER BY last_seen DESC, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}

// GetChannel handles GET /api/v1/channels/{channelHash}
//
// Returns channel detail including key_known status and message count.
// Channel keys are server-side config; key material is never exposed via the API.
func GetChannel(w http.ResponseWriter, r *http.Request) {
	// channelHash := chi.URLParam(r, "channelHash")
	// TODO: fetch channel, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}

// ListChannelMessages handles GET /api/v1/channels/{channelHash}/messages
//
// Query params (all optional):
//   since=<epoch ms>
//   limit=50
//   cursor=<opaque>
//
// Returns paginated decrypted channel messages. Messages where key_known=false
// will have content=null.
func ListChannelMessages(w http.ResponseWriter, r *http.Request) {
	// channelHash := chi.URLParam(r, "channelHash")
	// TODO: fetch channel_messages, paginate, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}
