// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"strings"

	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

// PortalCookie holds the browser's half of a portal sign-in for ten minutes.
const PortalCookie = "beacon_portal"

const portalCookiePath = "/api/v1/auth/portal"

// PortalAuth is the portal sign-in the auth routes call. It is *users.Portal.
type PortalAuth interface {
	Start(ctx context.Context, returnPath string) (cookie, redirect string, err error)
	Callback(ctx context.Context, cookie, state, code string, meta users.SessionMeta) (token, returnPath string, err error)
}

// portalManaged answers the local routes a portal instance does not serve.
func portalManaged(w http.ResponseWriter, _ *http.Request) {
	respondCode(w, http.StatusNotFound, "portal_managed", "Accounts are managed at the sign-in portal.")
}

func setPortalCookie(w http.ResponseWriter, opts AuthOptions, v string, maxAge int) {
	http.SetCookie(w, &http.Cookie{
		Name: PortalCookie, Value: v, Path: portalCookiePath, MaxAge: maxAge,
		HttpOnly: true, Secure: opts.CookieSecure, SameSite: http.SameSiteLaxMode,
	})
}

// portalStart godoc
// @Summary Start a sign-in through the portal
// @Description Portal mode only. Stores a state and a PKCE verifier for 10 minutes against the beacon_portal cookie and redirects (302) to the portal. return is a same-origin relative path; anything else becomes /.
// @Tags Accounts
// @Param return query string false "Path and query to return to after sign-in"
// @Success 302 "Redirect to the portal"
// @Failure 404,429,503 {object} map[string]APIError
// @Router /auth/portal/start [get]
func portalStart(opts AuthOptions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if opts.ReadOnly {
			respondCode(w, http.StatusServiceUnavailable, "read_only", "This server is read-only.")
			return
		}
		cookie, redirect, err := opts.Portal.Start(r.Context(), r.URL.Query().Get("return"))
		if err != nil {
			respondError(w, http.StatusInternalServerError, "internal error")
			return
		}
		setPortalCookie(w, opts, cookie, 600)
		http.Redirect(w, r, redirect, http.StatusFound)
	}
}

// portalCallback godoc
// @Summary Finish a sign-in through the portal
// @Description Portal mode only. Checks the state against the cookie (single use), trades the code with the portal, signs the user in and redirects to the return path. Any failure, including a callback without a code, redirects to /?view=signin&error=portal.
// @Tags Accounts
// @Param code query string false "One-time code from the portal"
// @Param state query string false "State from the start redirect"
// @Success 302 "Redirect into the web app, with the beacon_session cookie set"
// @Failure 404,429,503 {object} map[string]APIError
// @Router /auth/portal/callback [get]
func portalCallback(opts AuthOptions) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if opts.ReadOnly {
			respondCode(w, http.StatusServiceUnavailable, "read_only", "This server is read-only.")
			return
		}
		pc, _ := r.Cookie(PortalCookie)
		cookie := ""
		if pc != nil {
			cookie = pc.Value
		}
		setPortalCookie(w, opts, "", -1)
		q := r.URL.Query()
		token, ret, err := opts.Portal.Callback(r.Context(), cookie, q.Get("state"), q.Get("code"), sessionMeta(r))
		base := strings.TrimRight(opts.PublicOrigin, "/")
		if err != nil {
			http.Redirect(w, r, base+"/?view=signin&error=portal", http.StatusFound)
			return
		}
		setSessionCookie(w, opts, token)
		http.Redirect(w, r, base+ret, http.StatusFound)
	}
}
