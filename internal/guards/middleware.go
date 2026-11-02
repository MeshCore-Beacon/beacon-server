// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package guards

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/httprate"

	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
)

// Middleware classifies each request, attaches its guard state and, when a
// guard refused the request, replaces the handler's 5xx with the refusal.
// TrustedProxyIP must run first so the client key is the real client address.
func Middleware(g *Guard) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			key, err := httprate.KeyByIP(r)
			if err != nil {
				key = r.RemoteAddr
			}
			family, class := Classify(r)
			ctx, finish := g.Begin(r.Context(), key, family, class)
			st := stateFrom(ctx)
			r = r.WithContext(ctx)
			defer func() {
				finish()
				route := "unmatched"
				if rc := chi.RouteContext(r.Context()); rc != nil {
					if p := rc.RoutePattern(); p != "" {
						route = p
					}
				}
				metrics.HTTPRequest.WithLabelValues(route, string(family)).Observe(time.Since(start).Seconds())
				st.mu.Lock()
				armed, last := st.armedAt, st.lastFinish
				st.mu.Unlock()
				if !armed.IsZero() && !last.IsZero() {
					metrics.GuardDB.WithLabelValues(string(family), class.String()).Observe(last.Sub(armed).Seconds())
				}
			}()
			next.ServeHTTP(&refusalWriter{ResponseWriter: w, st: st}, r)
		})
	}
}

// refusalWriter swaps a handler's 5xx for the guard refusal, if there is one.
type refusalWriter struct {
	http.ResponseWriter
	st      *reqState
	wrote   bool
	discard bool
}

func (w *refusalWriter) WriteHeader(code int) {
	if code < 200 {
		w.ResponseWriter.WriteHeader(code)
		return
	}
	if w.wrote {
		return
	}
	w.wrote = true
	if code >= 500 {
		if ref := w.st.currentRefusal(); ref != nil {
			WriteRefusal(w.ResponseWriter, ref)
			w.discard = true
			return
		}
	}
	w.ResponseWriter.WriteHeader(code)
}

func (w *refusalWriter) Write(b []byte) (int, error) {
	if !w.wrote {
		w.WriteHeader(http.StatusOK)
	}
	if w.discard {
		return len(b), nil
	}
	return w.ResponseWriter.Write(b)
}

// Unwrap exposes the underlying writer to http.ResponseController.
func (w *refusalWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
