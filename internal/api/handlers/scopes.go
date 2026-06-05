package handlers

import (
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

// ScopesRouter mounts all /scopes routes onto a subrouter.
//
// GET /scopes         → listScopes
// GET /scopes/{name}  → getScope
func ScopesRouter(reader api.Reader) http.Handler {
	r := chi.NewRouter()
	r.Get("/", listScopes(reader))
	r.Get("/{name}", getScope(reader))
	return r
}

// listScopes godoc
//
//	@Summary	List transport scopes
//	@Tags		Scopes
//	@Produce	json
//	@Param		iatas		query		string	false	"Filter by IATA code(s), comma-separated"
//	@Param		region		query		string	false	"Filter by region slug"
//	@Param		regionId	query		int		false	"Filter by region ID"
//	@Success	200			{object}	object
//	@Failure	500			{object}	handlers.APIError
//	@Router		/scopes [get]
func listScopes(reader api.Reader) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		iatas := parseIATAs(r)
		if regionIDStr := r.URL.Query().Get("regionId"); regionIDStr != "" || r.URL.Query().Get("region") != "" {
			regionIATAs, err := resolveRegionIATAs(r.Context(), r.URL.Query().Get("regionId"), r.URL.Query().Get("region"), reader)
			if err != nil {
				respondError(w, http.StatusBadRequest, err.Error())
				return
			}
			iatas = append(iatas, regionIATAs...)
		}
		if len(iatas) == 0 {
			names, err := reader.GetScopeNames(r.Context())
			if err != nil {
				respondError(w, http.StatusInternalServerError, "internal server error")
				return
			}
			respond(w, http.StatusOK, names)
			return
		}
		scopes, err := reader.GetScopesByIATAs(r.Context(), iatas)
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal server error")
			return
		}
		respond(w, http.StatusOK, scopes)
	}
}

// getScope godoc
//
//	@Summary	Get scope detail by name
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
		if err != nil {
			respondError(w, http.StatusNotFound, "scope not found")
			return
		}
		respond(w, http.StatusOK, scope)
	}
}
