// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

func TestRuntimeConfigUpdate(t *testing.T) {
	newRouter := func() http.Handler {
		return New(nil, nil, nil, 5, 1000, config.CORSConfig{AllowedOrigins: []string{"https://before.test"}}, config.ServerConfig{}, config.AuthConfig{APIKey: "test-key"}, config.ResolvedRateLimitConfig{})
	}
	handler := newRouter()
	request := func(method, path, body, token, media, origin string) *httptest.ResponseRecorder {
		r := httptest.NewRequest(method, path, strings.NewReader(body))
		r.Header.Set("Content-Type", media)
		if token != "" {
			r.Header.Set("Authorization", "Bearer "+token)
		}
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		if method == "OPTIONS" {
			r.Header.Set("Access-Control-Request-Method", "GET")
			r.Header.Set("Access-Control-Request-Headers", "Authorization")
		}
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, r)
		return w
	}
	body := `{"cors":{"allowed_origins":["https://after.test"]}}`
	for _, token := range []string{"", "wrong"} {
		if request("PUT", "/api/v1/admin/config", body, token, "application/json", "").Code != 401 {
			t.Fatal("unauthorized update")
		}
	}
	w := request("PUT", "/api/v1/admin/config", body, "test-key", "application/json", "")
	var result api.UpdateAdminConfigResponse
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Persisted || result.RequiresRestart || w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("update contract")
	}
	if result.Config.CORS.AllowedOrigins[0] != "https://after.test" {
		t.Fatal("wrong returned policy")
	}
	read := request("GET", "/api/v1/admin/config", "", "test-key", "", "")
	var actual api.AdminConfig
	if json.Unmarshal(read.Body.Bytes(), &actual) != nil || actual.CORS.AllowedOrigins[0] != "https://after.test" {
		t.Fatal("GET still reports old config")
	}
	for _, tc := range []struct {
		body, media string
		status      int
	}{
		{`{}`, "application/json", 400}, {`null`, "application/json", 400}, {`{"cors":null}`, "application/json", 400},
		{`{"cors":{"allowed_origins":null}}`, "application/json", 400}, {`{"cors":{"allowed_origins":[]}}`, "application/json", 400},
		{`{"auth":{"api_key":"secret-sentinel"}}`, "application/json", 400}, {`{"ingest":{"worker_count":4}}`, "application/json", 400},
		{`{"cors":{"allowed_origins":["*"],"max_age":0}}`, "application/json", 400},
		{`{"cors":{"allowed_origins":["https://user:secret-sentinel@site.test"]}}`, "application/json", 400},
		{body + ` {}`, "application/json", 400}, {body, "text/plain", 415}, {body + strings.Repeat(" ", 16384), "application/json", 413},
	} {
		bad := request("PUT", "/api/v1/admin/config", tc.body, "test-key", tc.media, "")
		if bad.Code != tc.status || strings.Contains(bad.Body.String(), "secret-sentinel") {
			t.Fatalf("invalid update status=%d want=%d", bad.Code, tc.status)
		}
		if request("GET", "/api/v1/admin/config", "", "test-key", "", "").Body.String() != read.Body.String() {
			t.Fatal("invalid update changed state")
		}
	}
	if request("OPTIONS", "/api/v1/brokers", "", "", "", "https://before.test").Header().Get("Access-Control-Allow-Origin") != "" {
		t.Fatal("old origin allowed")
	}
	if request("OPTIONS", "/api/v1/brokers", "", "", "", "https://after.test").Header().Get("Access-Control-Allow-Origin") == "" {
		t.Fatal("new origin blocked")
	}
	if exposed := request("GET", "/api/v1/brokers", "", "", "", "https://after.test").Header().Get("Access-Control-Expose-Headers"); !strings.EqualFold(exposed, "Retry-After") {
		t.Fatalf("runtime update lost rate-limit backoff header: %q", exposed)
	}
	if request("GET", "/api/v1/brokers", "", "", "", "").Code != 200 {
		t.Fatal("public reads blocked")
	}
	handler = newRouter()
	if request("OPTIONS", "/api/v1/brokers", "", "", "", "https://before.test").Header().Get("Access-Control-Allow-Origin") == "" {
		t.Fatal("new router did not reload startup policy")
	}
}
