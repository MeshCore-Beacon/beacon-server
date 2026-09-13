// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

func TestValidateOrigins(t *testing.T) {
	for _, tc := range []struct {
		input []string
		valid bool
	}{
		{[]string{"*"}, true}, {[]string{" HTTPS://Example.test ", "https://*.example.test", "http://127.0.0.1:8080", "http://[::1]:8080"}, true},
		{nil, false}, {[]string{}, false}, {make([]string, 33), false}, {[]string{""}, false}, {[]string{"null"}, false},
		{[]string{"*", "https://example.test"}, false}, {[]string{"ftp://example.test"}, false}, {[]string{"https://"}, false},
		{[]string{"https://example.test/"}, false}, {[]string{"https://example.test?"}, false}, {[]string{"https://example.test#"}, false},
		{[]string{"https://user:secret@example.test"}, false}, {[]string{"https://example.test:65536"}, false}, {[]string{"https://example.test:"}, false},
		{[]string{"https://*.*.test"}, false}, {[]string{"https://example.test\n"}, false}, {[]string{"https://é.test"}, false}, {[]string{"https://K.test"}, false}, {[]string{strings.Repeat("x", 513)}, false},
	} {
		_, err := validateOrigins(tc.input)
		if (err == nil) != tc.valid {
			t.Fatalf("origins=%q valid=%v error=%v", tc.input, tc.valid, err)
		}
	}
	input := []string{" HTTPS://Example.test "}
	got, err := validateOrigins(input)
	if err != nil || got[0] != "https://example.test" || input[0] != " HTTPS://Example.test " {
		t.Fatal("normalization changed caller state")
	}
}

func TestRuntimePolicyAndConcurrency(t *testing.T) {
	initial := api.AdminConfig{Auth: api.AdminAuthConfig{Configured: true}, Ingest: api.AdminIngestConfig{BrokerCount: 2},
		CORS: api.AdminCORSConfig{AllowedOrigins: []string{"https://before.test"}, AllowedMethods: []string{"GET", "PUT"}, AllowedHeaders: []string{"Authorization"}, AllowCredentials: true, MaxAge: 600}}
	state := NewRuntimeConfig(initial, []string{"Retry-After"})
	initial.CORS.AllowedOrigins[0] = "https://caller-change.test"
	handler := state.CORS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(204) }))
	preflight := func(origin string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("OPTIONS", "/api/v1/brokers", nil)
		r.Header.Set("Origin", origin)
		r.Header.Set("Access-Control-Request-Method", "PUT")
		r.Header.Set("Access-Control-Request-Headers", "Authorization")
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	if preflight("https://before.test").Header().Get("Access-Control-Allow-Origin") == "" {
		t.Fatal("startup policy changed with caller slice")
	}
	origins := []string{"https://*.after.test"}
	updated, err := state.UpdateOrigins(origins)
	if err != nil {
		t.Fatal(err)
	}
	origins[0] = "https://caller-change.test"
	updated.CORS.AllowedOrigins[0] = "https://response-change.test"
	if preflight("https://before.test").Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("old policy remains active")
	}
	w := preflight("https://app.after.test")
	if w.Code != 200 || w.Header().Get("Access-Control-Allow-Origin") != "https://app.after.test" || w.Header().Get("Access-Control-Max-Age") != "600" || w.Header().Get("Access-Control-Allow-Credentials") != "true" {
		t.Fatal("new policy/options not enforced")
	}
	r := httptest.NewRequest("GET", "/", nil)
	r.Header.Set("Origin", "https://app.after.test")
	w = httptest.NewRecorder()
	handler.ServeHTTP(w, r)
	if w.Header().Get("Access-Control-Expose-Headers") != "Retry-After" {
		t.Fatal("exposed headers lost")
	}
	before := state.Snapshot()
	if _, err := state.UpdateOrigins(nil); err == nil || !reflect.DeepEqual(state.Snapshot(), before) {
		t.Fatal("invalid update changed config")
	}
	var group sync.WaitGroup
	for i := 0; i < 8; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			for j := 0; j < 40; j++ {
				origin := "https://a.test"
				if i%2 != 0 {
					origin = "https://b.test"
				}
				if _, err := state.UpdateOrigins([]string{origin}); err != nil {
					t.Error(err)
				}
				snapshot := state.Snapshot()
				if len(snapshot.CORS.AllowedOrigins) != 1 || !snapshot.Auth.Configured || snapshot.Ingest.BrokerCount != 2 {
					t.Error("torn config")
				}
				preflight(origin)
			}
		}(i)
	}
	group.Wait()
	final := state.Snapshot()
	if preflight(final.CORS.AllowedOrigins[0]).Header().Get("Access-Control-Allow-Origin") == "" {
		t.Fatal("reported/enforced final policy differs")
	}
}
