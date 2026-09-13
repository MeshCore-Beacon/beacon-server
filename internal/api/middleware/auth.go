// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"crypto/subtle"
	"io"
	"net/http"
	"strings"
)

// BearerAuth protects admin routes with one operator key. An empty key disables
// access; public routes must be mounted outside this middleware.
func BearerAuth(apiKey string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		status := http.StatusServiceUnavailable
		body := `{"error":{"code":"service_unavailable","message":"admin authentication is not configured"}}`
		if apiKey != "" {
			if values := r.Header.Values("Authorization"); len(values) == 1 {
				scheme, token, found := strings.Cut(values[0], " ")
				token = strings.TrimLeft(token, " ") // Bearer permits one or more spaces.
				if found && strings.EqualFold(scheme, "Bearer") && token != "" && !strings.ContainsAny(token, " \t\r\n,") &&
					subtle.ConstantTimeCompare([]byte(token), []byte(apiKey)) == 1 {
					next.ServeHTTP(w, r)
					return
				}
			}
			status = http.StatusUnauthorized
			body = `{"error":{"code":"unauthorized","message":"valid bearer token required"}}`
			w.Header().Set("WWW-Authenticate", "Bearer")
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		_, _ = io.WriteString(w, body+"\n")
	})
}
