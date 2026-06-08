// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: agpl

package middleware

import "net/http"

// NoopAuth is a placeholder for the authentication middleware that will be
// wired onto the private route group when auth is implemented (see Future
// Features → Admin authentication in the design doc).
//
// Replace this with a real JWT/session validation middleware before shipping
// any write endpoints or admin functionality.
func NoopAuth(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// TODO: validate bearer token, set user in context, return 401 on failure.
		next.ServeHTTP(w, r)
	})
}
