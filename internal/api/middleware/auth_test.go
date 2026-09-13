// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestBearerAuth(t *testing.T) {
	const key = "synthetic-test-key"
	for _, tc := range []struct {
		name, key string
		headers   []string
		status    int
	}{
		{"disabled", "", nil, 503}, {"disabled with token", "", []string{"Bearer " + key}, 503},
		{"missing", key, nil, 401}, {"empty", key, []string{""}, 401},
		{"wrong scheme", key, []string{"Basic " + key}, 401}, {"wrong token", key, []string{"Bearer wrong"}, 401},
		{"empty token", key, []string{"Bearer "}, 401}, {"no separator", key, []string{"Bearer" + key}, 401},
		{"tab separator", key, []string{"Bearer\t" + key}, 401}, {"extra token", key, []string{"Bearer " + key + " extra"}, 401},
		{"line break", key, []string{"Bearer " + key + "\r\n"}, 401},
		{"duplicate", key, []string{"Bearer " + key, "Bearer " + key}, 401},
		{"combined", key, []string{"Bearer " + key + ", Bearer " + key}, 401},
		{"case sensitive token", key, []string{"Bearer SYNTHETIC-TEST-KEY"}, 401},
		{"valid", key, []string{"Bearer " + key}, 200},
		{"scheme case", key, []string{"bEaReR " + key}, 200},
		{"multiple spaces", key, []string{"Bearer   " + key}, 200},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			handler := BearerAuth(tc.key, http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				calls++
				w.WriteHeader(200)
			}))
			// Query and form tokens alone must never grant access.
			req := httptest.NewRequest("POST", "/admin?access_token="+key, strings.NewReader("access_token="+key))
			req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			for _, value := range tc.headers {
				req.Header.Add("Authorization", value)
			}
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			if w.Code != tc.status || (calls == 1) != (tc.status == 200) {
				t.Fatalf("status=%d downstream calls=%d", w.Code, calls)
			}
			if w.Header().Get("Cache-Control") != "no-store" || strings.Contains(w.Body.String(), key) {
				t.Fatal("cache policy missing or credential reflected")
			}
			if tc.status == 200 {
				return
			}
			var body struct {
				Error struct{ Code, Message string }
			}
			if w.Header().Get("Content-Type") != "application/json" || json.Unmarshal(w.Body.Bytes(), &body) != nil || body.Error.Message == "" {
				t.Fatal("invalid JSON error contract")
			}
			wantCode := "service_unavailable"
			if tc.status == 401 {
				wantCode = "unauthorized"
				if w.Header().Get("WWW-Authenticate") != "Bearer" {
					t.Fatal("missing bearer challenge")
				}
			}
			if body.Error.Code != wantCode {
				t.Fatalf("error code=%s", body.Error.Code)
			}
		})
	}
}
