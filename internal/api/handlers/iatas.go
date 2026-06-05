package handlers

import (
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// IATAsRouter mounts all /iatas routes onto a subrouter.
//
// GET  /iatas        → listIATAs
// GET  /iatas/{iata} → getIATA
func IATAsRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listIATAs(reader))
	r.Get("/{iata}", getIATA(reader))
	return r
}

// listIATAs godoc
//
//	@Summary	List all IATA codes
//	@Tags		IATAs
//	@Produce	json
//	@Success	200	{array}		api.IATA
//	@Failure	404	{object}	handlers.APIError
//	@Router		/iatas [get]
func listIATAs(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas, err := reader.ListIATAs(r.Context())
		if err != nil {
			respondError(w, http.StatusNotFound, "no IATAs found")
			return
		}
		respond(w, http.StatusOK, iatas)
	}
}

// getIATA godoc
//
//	@Summary	Get a single IATA code
//	@Tags		IATAs
//	@Produce	json
//	@Param		iata	path		string	true	"3-letter IATA code"
//	@Success	200		{object}	api.IATA
//	@Failure	404		{object}	handlers.APIError
//	@Router		/iatas/{iata} [get]
func getIATA(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iata := chi.URLParam(r, "iata")
		result, err := reader.GetIATA(r.Context(), iata)
		if err != nil {
			respondError(w, http.StatusNotFound, "IATA not found")
			return
		}
		respond(w, http.StatusOK, result)
	}
}
