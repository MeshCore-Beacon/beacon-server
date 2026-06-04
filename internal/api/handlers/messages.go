package handlers

import (
	"encoding/hex"
	"net/http"
	"strconv"
	"time"

	"github.com/MeshCore-Tower/tower-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// MessagesRouter mounts all /messages routes onto a subrouter.
//
// GET  /messages → listMessages
func MessagesRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listMessages(reader))
	return r
}

// listMessages godoc
//
//	@Summary	List channel messages
//	@Tags		Messages
//	@Produce	json
//	@Param		channelID	query		int		false	"Filter by channel integer ID (mutually exclusive with channelHash)"
//	@Param		channelHash	query		string	false	"Filter by channel hash byte hex (mutually exclusive with channelID)"
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
//	@Router		/messages [get]
func listMessages(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		channelIDParam := r.URL.Query().Get("channelID")
		channelHashParam := r.URL.Query().Get("channelHash")
		if channelIDParam != "" && channelHashParam != "" {
			respondError(w, http.StatusBadRequest, "filter by either channelId or channelHash, not both")
			return
		}

		var id int64
		if channelIDParam != "" {
			i, err := strconv.ParseInt(channelIDParam, 10, 32)
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
		var cursor int64
		if cursorParam := r.URL.Query().Get("cursor"); cursorParam != "" {
			c, err := strconv.ParseInt(cursorParam, 10, 64)
			if err != nil {
				respondError(w, http.StatusBadRequest, "cursor must be an integer")
				return
			}
			cursor = c
		}
		iatas := parseIATAs(r)
		scope := r.URL.Query().Get("scope")
		var messages api.Page[api.ChannelMessage]
		var err error
		if channelHashParam != "" {
			hashHex, decodeErr := hex.DecodeString(channelHashParam)
			if decodeErr != nil {
				respondError(w, http.StatusBadRequest, "invalid channel hash")
				return
			}
			if len(hashHex) != 1 {
				respondError(w, http.StatusBadRequest, "channel hash must be a single hex byte")
				return
			}
			messages, err = reader.ListChannelMessagesByHash(r.Context(), hashHex, since, int32(limit), iatas, scope, cursor)
		} else if channelIDParam != "" {
			chanID := int32(id)
			messages, err = reader.ListChannelMessages(r.Context(), &chanID, since, int32(limit), iatas, scope, cursor)
		} else {
			messages, err = reader.ListChannelMessages(r.Context(), nil, since, int32(limit), iatas, scope, cursor)
		}
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, messages)
	}
}
