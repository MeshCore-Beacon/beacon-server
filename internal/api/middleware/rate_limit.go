// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"net/http"
	"time"

	"github.com/go-chi/httprate"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

// RateLimit applies the minute budget and one-second burst cap to the API.
// TrustedProxyIP must run first; the access logger must wrap this middleware.
func RateLimit(cfg config.ResolvedRateLimitConfig) func(http.Handler) http.Handler {
	if !cfg.Enabled {
		return func(next http.Handler) http.Handler { return next }
	}
	options := []httprate.Option{
		httprate.WithLimitHandler(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"Request rate limit exceeded. Retry later."}}`))
		}),
		// Only expose the backoff header, since the two windows have different budgets.
		httprate.WithResponseHeaders(httprate.ResponseHeaders{RetryAfter: "Retry-After"}),
	}
	// RemoteAddr has already been resolved and forwarding headers removed.
	minute := httprate.LimitBy(cfg.RequestsPerMinute, time.Minute, httprate.KeyByIP, options...)
	burst := httprate.LimitBy(cfg.Burst, time.Second, httprate.KeyByIP, options...)
	return func(next http.Handler) http.Handler {
		return minute(burst(next))
	}
}

// RateLimitRoute limits one route (or a few that share the instance) to n requests per window for each client IP,
// answering 429 rate_limited with Retry-After. TrustedProxyIP must run first.
func RateLimitRoute(n int, window time.Duration) func(http.Handler) http.Handler {
	return httprate.LimitBy(n, window, httprate.KeyByIP,
		httprate.WithLimitHandler(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_, _ = w.Write([]byte(`{"error":{"code":"rate_limited","message":"Too many requests. Retry later."}}`))
		}),
		httprate.WithResponseHeaders(httprate.ResponseHeaders{RetryAfter: "Retry-After"}),
	)
}
