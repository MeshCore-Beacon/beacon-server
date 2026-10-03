// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"errors"
	"net/http"
	"strings"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

// IATAsRouter mounts all /iatas routes onto a subrouter.
//
// GET  /iatas               → listIATAs
// GET  /iatas/{iata}        → getIATA
// GET  /iatas/{iata}/border → getIATABorder
func IATAsRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listIATAs(reader))
	r.Get("/{iata}", getIATA(reader))
	r.Get("/{iata}/border", getIATABorder(reader))
	return r
}

// pathIATA uppercases the {iata} param and accepts only 3 ASCII letters, which also keeps it
// from reaching into other cache keys.
func pathIATA(r *http.Request) (string, bool) {
	iata := strings.ToUpper(chi.URLParam(r, "iata"))
	if len(iata) != 3 {
		return "", false
	}
	for i := range len(iata) {
		if iata[i] < 'A' || iata[i] > 'Z' {
			return "", false
		}
	}
	return iata, true
}

// listIATAs godoc
//
//	@Summary	List all IATA codes
//	@Tags		IATAs
//	@Produce	json
//	@Success	200	{array}		api.IATA
//	@Failure	500	{object}	handlers.APIError
//	@Router		/iatas [get]
func listIATAs(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas, err := reader.ListIATAs(r.Context())
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		if iatas == nil {
			iatas = []api.IATA{}
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
//	@Failure	500		{object}	handlers.APIError
//	@Router		/iatas/{iata} [get]
func getIATA(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iata, ok := pathIATA(r)
		if !ok {
			respondError(w, http.StatusNotFound, "IATA not found")
			return
		}
		result, err := reader.GetIATA(r.Context(), iata)
		switch {
		case errors.Is(err, pgx.ErrNoRows), err == nil && result == nil:
			respondError(w, http.StatusNotFound, "IATA not found")
			return
		case err != nil:
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, result)
	}
}

// getIATABorder godoc
//
//	@Summary	Get an IATA's GeoJSON border, if configured
//	@Tags		IATAs
//	@Produce	json
//	@Param		iata	path	string	true	"3-letter IATA code"
//	@Success	200		{object}	object	"GeoJSON Feature (Polygon or MultiPolygon geometry, with bbox)"
//	@Success	204		"IATA exists but has no border configured"
//	@Failure	404		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Router		/iatas/{iata}/border [get]
func getIATABorder(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iata, ok := pathIATA(r)
		if !ok {
			respondError(w, http.StatusNotFound, "IATA not found")
			return
		}
		border, err := reader.GetIATABorder(r.Context(), iata)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			respondError(w, http.StatusNotFound, "IATA not found")
			return
		case err != nil:
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		// A cache round-trip re-encodes a nil json.RawMessage as the literal 4-byte JSON
		// "null" rather than leaving it empty, so both must be treated as "no border set".
		if len(border) == 0 || string(border) == "null" {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		w.Header().Set("Content-Type", "application/geo+json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(border)
	}
}
