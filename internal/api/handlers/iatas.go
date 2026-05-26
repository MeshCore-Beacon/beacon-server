package handlers

import (
	"net/http"

	"github.com/MeshCore-Tower/tower-server/internal/api"

	"github.com/go-chi/chi/v5"
)

// IATAsRouter mounts all /iatas routes onto a subrouter.
//
// GET  /iatas                                → ListIATAs
// GET  /iatas/{iata}                         → GetIATA
func IATAsRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()

	// Returns all known IATA codes with display name and coordinates where set.
	// IATAs are auto-created on first packet arrival; config file overrides name/coords.
	r.Get("/", func(w http.ResponseWriter, r *http.Request) {
		iatas, err := reader.ListIATAs(r.Context())
		if err != nil {
			respondError(w, http.StatusNotFound, "no IATAs found")
			return
		}
		respond(w, http.StatusOK, iatas)
	})

	// Returns detail for a single IATA code including associated region memberships
	// and basic recent stats.
	r.Get("/{iata}", func(w http.ResponseWriter, r *http.Request) {
		iata := chi.URLParam(r, "iata")
		result, err := reader.GetIATA(r.Context(), iata)
		if err != nil {
			respondError(w, http.StatusNotFound, "IATA not found")
			return
		}
		respond(w, http.StatusOK, result)
	})

	return r
}
