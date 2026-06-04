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
// GET  /channels                      → listChannels
// GET  /channels/{channelID}          → getChannel
// GET  /channels/{channelID}/messages → listChannelMessages
func ChannelsRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listChannels(reader))
	r.Route("/{channelID}", func(r chi.Router) {
		r.Get("/", getChannel(reader))
		r.Get("/messages", listChannelMessages(reader))
	})
	return r
}

// listChannels godoc
//
//	@Summary	List channels
//	@Tags		Channels
//	@Produce	json
//	@Param		hash	query		string	false	"Single-byte channel hash (hex)"
//	@Param		iata	query		string	false	"Filter by IATA code (case-insensitive)"
//	@Param		cursor	query		int		false	"last_seen epoch ms of last item for pagination"
//	@Param		limit	query		int		false	"Max results (default 50)"
//	@Success	200		{object}	api.Page[api.ChannelSummary]
//	@Failure	400		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Router		/channels [get]
func listChannels(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		var cursor int64
		if cursorParam := r.URL.Query().Get("cursor"); cursorParam != "" {
			c, err := strconv.ParseInt(cursorParam, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "cursor must be an integer")
				return
			}
			cursor = c
		}
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
		channels, err := reader.ListChannels(r.Context(), int32(limit), hashHex, iata, cursor)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, channels)
	}
}

// getChannel godoc
//
//	@Summary	Get channel detail
//	@Tags		Channels
//	@Produce	json
//	@Param		channelID	path		int	true	"Channel integer ID"
//	@Success	200			{object}	api.Channel
//	@Failure	400			{object}	handlers.APIError
//	@Failure	404			{object}	handlers.APIError
//	@Router		/channels/{channelID} [get]
func getChannel(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
	}
}

// listChannelMessages godoc
//
//	@Summary	List messages for a channel
//	@Tags		Channels
//	@Produce	json
//	@Param		channelID	path		int		true	"Channel integer ID"
//	@Param		since		query		int		false	"Return messages after this epoch ms"
//	@Param		iatas		query		string	false	"Filter by IATA code(s), comma-separated e.g. YVR or YVR,YYJ"
//	@Param		regionId	query		int		false	"Filter by region ID, expands to member IATAs"
//	@Param		region		query		string	false	"Filter by region slug, expands to member IATAs"
//	@Param		scope		query		string	false	"Filter by transport scope name e.g. %23bc (URL-encoded #bc)"
//	@Param		cursor		query		int		false	"Message ID of last item for pagination"
//	@Param		limit		query		int		false	"Max results (default 50)"
//	@Success	200			{object}	object
//	@Failure	400			{object}	handlers.APIError
//	@Failure	500			{object}	handlers.APIError
//	@Router		/channels/{channelID}/messages [get]
func listChannelMessages(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
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
		iatas := parseIATAs(r)
		var cursor int64
		if cursorParam := r.URL.Query().Get("cursor"); cursorParam != "" {
			c, err := strconv.ParseInt(cursorParam, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "cursor must be an integer")
				return
			}
			cursor = c
		}
		scope := r.URL.Query().Get("scope")
		chanID := int32(id)
		messages, err := reader.ListChannelMessages(r.Context(), &chanID, since, int32(limit), iatas, scope, cursor)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, messages)
	}
}
