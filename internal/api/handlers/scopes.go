// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"errors"
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
)

// ScopeMembership lists scopes by configured region and imported catalogue membership.
type ScopeMembership interface {
	NamesForIATAs(iatas []string) []string // sorted
}

func scopeNamesFor(scopes ScopeMembership, iatas []string) []string {
	if scopes == nil {
		return []string{}
	}
	return scopes.NamesForIATAs(iatas)
}

// ScopesRouter mounts all /scopes routes onto a subrouter.
//
// GET /scopes         → listScopes
// GET /scopes/{name}  → getScope
func ScopesRouter(reader api.Reader, scopes ScopeMembership) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listScopes(reader, scopes))
	r.Get("/{name}", getScope(reader))
	return r
}

// listScopes godoc
//
//	@Summary	List transport scopes
//	@Description	Without filters, lists every stored scope name, including imported names retained after an importer is disabled or a source is removed. With IATA or region filters, lists only manual scopes configured for a matching region and imported scopes whose current MeshMapper catalogue includes a matching IATA; observed traffic does not add scopes.
//	@Tags		Scopes
//	@Produce	json
//	@Param		iatas		query		string	false	"Filter by IATA code(s), comma-separated"
//	@Param		region		query		string	false	"Filter by region slug"
//	@Param		regionId	query		int		false	"Filter by region ID"
//	@Success	200			{array}		string
//	@Failure	400			{object}	handlers.APIError
//	@Failure	500			{object}	handlers.APIError
//	@Router		/scopes [get]
func listScopes(reader api.Reader, scopes ScopeMembership) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		regionID, region := r.URL.Query().Get("regionId"), r.URL.Query().Get("region")
		if regionID != "" || region != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), regionID, region, reader)
			if err != nil {
				respondRegionError(w, err)
				return
			}
			iatas = append(iatas, regionIATAs...)
		} else if len(iatas) == 0 {
			names, err := reader.GetScopeNames(r.Context())
			if err != nil {
				respondError(w, http.StatusInternalServerError, "internal server error")
				return
			}
			respond(w, http.StatusOK, names)
			return
		}
		respond(w, http.StatusOK, scopeNamesFor(scopes, iatas))
	}
}

// getScope godoc
//
//	@Summary	Get scope detail by name
//	@Description	packetCount sums packets per hour across every retained rollup hour; iatas are the IATAs that heard the scope's packets. Observer and node counts are current memberships.
//	@Tags		Scopes
//	@Produce	json
//	@Param		name	path		string	true	"Scope name e.g. %23bc (URL-encoded #bc)"
//	@Success	200		{object}	api.ScopeDetail
//	@Failure	404		{object}	handlers.APIError
//	@Failure	500		{object}	handlers.APIError
//	@Router		/scopes/{name} [get]
func getScope(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		name := chi.URLParam(r, "name")
		scope, err := reader.GetScopeByName(r.Context(), name)
		switch {
		case errors.Is(err, pgx.ErrNoRows), err == nil && scope == nil:
			respondError(w, http.StatusNotFound, "scope not found")
			return
		case err != nil:
			respondError(w, http.StatusInternalServerError, "failed to load scope")
			return
		}
		respond(w, http.StatusOK, scope)
	}
}
