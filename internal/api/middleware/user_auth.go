// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/MeshCore-Beacon/beacon-server/internal/users"
)

// CSRFHeader must be "1" on a cookie-authenticated write.
const CSRFHeader = "X-Beacon-CSRF"

// Authenticator resolves a session token; *users.Service satisfies it.
type Authenticator interface {
	Authenticate(ctx context.Context, token string) (users.Session, users.User, error)
}

type userCtxKey struct{}

type userCtx struct {
	user      users.User
	session   users.Session
	viaCookie bool
	// failed: the session lookup itself failed, so nothing is known about the visitor.
	failed bool
}

// UserFrom returns the signed-in user and session UserAuth attached.
func UserFrom(ctx context.Context) (users.User, users.Session, bool) {
	c, ok := ctx.Value(userCtxKey{}).(userCtx)
	return c.user, c.session, ok && !c.failed
}

// SessionViaCookie reports whether the session in ctx came from the cookie rather than a bearer token.
func SessionViaCookie(ctx context.Context) bool {
	c, ok := ctx.Value(userCtxKey{}).(userCtx)
	return ok && c.viaCookie
}

// UserAuth attaches the user when a valid session is present, from the Authorization bearer token or else the
// cookie. It never rejects: a missing, expired or unknown token just leaves the request signed out.
func UserAuth(svc Authenticator, cookieName string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			token, viaCookie := sessionToken(r, cookieName)
			if token != "" {
				sess, u, err := svc.Authenticate(r.Context(), token)
				switch {
				case err == nil:
					r = r.WithContext(context.WithValue(r.Context(), userCtxKey{}, userCtx{user: u, session: sess, viaCookie: viaCookie}))
				case !errors.Is(err, users.ErrTokenInvalid):
					slog.Error("session lookup failed", "component", "api", "error", err)
					r = r.WithContext(context.WithValue(r.Context(), userCtxKey{}, userCtx{failed: true}))
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}

// sessionToken prefers a bearer token; a request that sends one never falls back to the cookie.
func sessionToken(r *http.Request, cookieName string) (token string, viaCookie bool) {
	if v := r.Header.Values("Authorization"); len(v) > 0 {
		if len(v) == 1 {
			if scheme, t, ok := strings.Cut(v[0], " "); ok && strings.EqualFold(scheme, "Bearer") {
				return strings.TrimSpace(t), false
			}
		}
		return "", false
	}
	if c, err := r.Cookie(cookieName); err == nil {
		return c.Value, true
	}
	return "", false
}

func writeUserError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = io.WriteString(w, `{"error":{"code":"`+code+`","message":"`+message+`"}}`+"\n")
}

// RequireUser answers 401 not_signed_in unless UserAuth attached a user, and 503 service_unavailable when the
// session could not be looked up: a client that took that for an invalid session would drop a token that is good.
func RequireUser(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if c, _ := r.Context().Value(userCtxKey{}).(userCtx); c.failed {
			w.Header().Set("Retry-After", "5")
			writeUserError(w, http.StatusServiceUnavailable, "service_unavailable", "Try again in a moment.")
			return
		}
		if _, _, ok := UserFrom(r.Context()); !ok {
			writeUserError(w, http.StatusUnauthorized, "not_signed_in", "Sign in to continue.")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// RequireCSRF refuses a POST, PUT, PATCH or DELETE authenticated by the cookie unless it carries X-Beacon-CSRF: 1
// and, when it has an Origin, that origin is the public one or in allowedOrigins ("*" never counts). Bearer
// requests and signed-out requests pass: only a cookie lets a foreign page act as the user.
func RequireCSRF(publicOrigin string, allowedOrigins []string) func(http.Handler) http.Handler {
	ok := map[string]bool{strings.TrimRight(publicOrigin, "/"): true}
	for _, o := range allowedOrigins {
		if o = strings.TrimRight(o, "/"); o != "" && o != "*" {
			ok[o] = true
		}
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			switch r.Method {
			case http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete:
				if SessionViaCookie(r.Context()) {
					origins := r.Header.Values("Origin")
					if r.Header.Get(CSRFHeader) != "1" || len(origins) > 1 || (len(origins) == 1 && !ok[origins[0]]) {
						writeUserError(w, http.StatusForbidden, "csrf_rejected", "Request rejected.")
						return
					}
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
