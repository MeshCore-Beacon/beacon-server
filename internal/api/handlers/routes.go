package handlers

import (
	"net/http"
	"strconv"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// RoutesRouter mounts all /routes routes onto a subrouter.
//
// GET /routes          → listKnownRoutes
// GET /routes/search   → searchKnownRoutes
func RoutesRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listKnownRoutes(reader))
	r.Get("/search", searchKnownRoutes(reader))
	return r
}

// listKnownRoutes godoc
//
//	@Summary	List known routes
//	@Tags		Routes
//	@Produce	json
//	@Param		iata		query		string	false	"Filter by IATA code"
//	@Param		hopCount	query		int		false	"Filter by exact hop count"
//	@Param		cursor		query		int		false	"Route ID of last item for pagination"
//	@Param		limit		query		int		false	"Max results (default 50)"
//	@Success	200			{object}	[]api.KnownRoute
//	@Failure	500			{object}	handlers.APIError
//	@Router		/routes [get]
func listKnownRoutes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iata := r.URL.Query().Get("iata")
		var hopCount int32
		if v := r.URL.Query().Get("hopCount"); v != "" {
			if h, err := strconv.ParseInt(v, 10, 32); err == nil {
				hopCount = int32(h)
			}
		}
		var cursor time.Time
		if v := r.URL.Query().Get("cursor"); v != "" {
			if ms, err := strconv.ParseInt(v, 10, 64); err == nil {
				cursor = time.UnixMilli(ms)
			}
		}
		var limit int32 = 50
		if v := r.URL.Query().Get("limit"); v != "" {
			if l, err := strconv.ParseInt(v, 10, 32); err == nil {
				limit = int32(l)
			}
		}
		routes, err := reader.ListKnownRoutes(r.Context(), iata, hopCount, cursor, limit)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, routes)
	}
}

// searchKnownRoutes godoc
//
//	@Summary	Search known routes by source and destination hash
//	@Tags		Routes
//	@Produce	json
//	@Param		iata	query		string	true	"IATA code to search within"
//	@Param		from	query		string	true	"Source node hash prefix (hex)"
//	@Param		to		query		string	true	"Destination node hash prefix (hex)"
//	@Success	200		{object}	[]api.KnownRoute
//	@Failure	400		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Router		/routes/search [get]
func searchKnownRoutes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iata := r.URL.Query().Get("iata")
		from := r.URL.Query().Get("from")
		to := r.URL.Query().Get("to")
		if iata == "" || from == "" || to == "" {
			respondError(w, http.StatusBadRequest, "iata, from and to are required")
			return
		}
		routes, err := reader.SearchKnownRoutes(r.Context(), iata, from, to)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, routes)
	}
}
