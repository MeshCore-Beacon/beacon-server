// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
)

// MaxListParamValues caps the entries in one comma-separated list param. It sits well
// above any real selection (prod knows a few hundred IATAs) and keeps per-entry work in
// handlers bounded, since a ~1 MB URL could otherwise carry about 250K entries.
const MaxListParamValues = 1000

// listParams are the comma-separated list query params the API accepts.
var listParams = []string{"iatas", "scopes", "payloadTypes", "routeTypes"}

// LimitListParams rejects a request with 400 when any list param holds more than
// MaxListParamValues non-empty entries, counted the way the handlers split them.
func LimitListParams(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.RawQuery != "" {
			q := r.URL.Query()
			for _, name := range listParams {
				for _, v := range q[name] {
					if exceedsListLimit(v) {
						writeBadRequest(w, name+": at most "+strconv.Itoa(MaxListParamValues)+" values")
						return
					}
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

// exceedsListLimit counts non-empty trimmed entries, stopping once past the cap.
func exceedsListLimit(raw string) bool {
	n := 0
	for raw != "" {
		var part string
		part, raw, _ = strings.Cut(raw, ",")
		if strings.TrimSpace(part) != "" {
			if n++; n > MaxListParamValues {
				return true
			}
		}
	}
	return false
}

func writeBadRequest(w http.ResponseWriter, message string) {
	type apiError struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusBadRequest)
	_ = json.NewEncoder(w).Encode(map[string]apiError{"error": {Code: "bad_request", Message: message}})
}
