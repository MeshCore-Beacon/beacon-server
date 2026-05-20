package handlers

import (
	"net/http"

	"github.com/go-chi/chi/v5"
)

// IATAsRouter mounts all /iatas routes onto a subrouter.
//
// GET  /iatas                                → ListIATAs
// GET  /iatas/{iata}                         → GetIATA
func IATAsRouter() http.Handler {
	r := chi.NewRouter()

	r.Get("/", ListIATAs)
	r.Get("/{iata}", GetIATA)

	return r
}

// ListIATAs handles GET /api/v1/iatas
//
// Returns all known IATA codes with display name and coordinates where set.
// IATAs are auto-created on first packet arrival; config file overrides name/coords.
func ListIATAs(w http.ResponseWriter, r *http.Request) {
	// TODO: query iata_codes, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}

// GetIATA handles GET /api/v1/iatas/{iata}
//
// Returns detail for a single IATA code including associated region memberships
// and basic recent stats.
func GetIATA(w http.ResponseWriter, r *http.Request) {
	// iata := chi.URLParam(r, "iata")
	// TODO: fetch iata_codes row + region memberships, write JSON response.
	w.WriteHeader(http.StatusNotImplemented)
}
