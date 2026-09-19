// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
)

func TestAdminConfig(t *testing.T) {
	const key = "admin-key-sentinel"
	for _, variable := range []string{"POSTGRES_DSN", "MQTT_BROKER_1_URL", "MQTT_BROKER_1_USERNAME", "MQTT_BROKER_1_PASSWORD", "REDIS_PASSWORD"} {
		t.Setenv(variable, "environment-secret-sentinel")
	}
	for _, custom := range []bool{false, true} {
		cfg := config.CORSConfig{}
		want := map[string]any{
			"allowed_origins": []any{"*"}, "allowed_methods": []any{"GET", "HEAD", "OPTIONS"},
			"allowed_headers": []any{"Accept", "Authorization", "Content-Type"}, "allow_credentials": false, "max_age": float64(300),
		}
		if custom {
			cfg = config.CORSConfig{AllowedOrigins: []string{"https://example.test"}, AllowedMethods: []string{"GET"},
				AllowedHeaders: []string{"Authorization", "X-Operator"}, AllowCredentials: true, MaxAge: 600}
			want = map[string]any{"allowed_origins": []any{"https://example.test"}, "allowed_methods": []any{"GET"},
				"allowed_headers": []any{"Authorization", "X-Operator"}, "allow_credentials": true, "max_age": float64(600)}
		}
		handler := New(nil, nil, []*ingest.Worker{{}, {}}, Options{MaxConnsPerIP: 5, MaxConnectsPerMinute: 1000, CORS: cfg, Auth: config.AuthConfig{APIKey: key}})
		if custom {
			cfg.AllowedOrigins[0] = "https://changed.test"
			cfg.AllowedMethods[0] = "DELETE"
			cfg.AllowedHeaders[0] = "X-Changed"
		}
		request := func(method, path string, headers map[string]string) *httptest.ResponseRecorder {
			req := httptest.NewRequest(method, path, nil)
			for name, value := range headers {
				req.Header.Set(name, value)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, req)
			return response
		}
		headers := map[string]string{"Authorization": "Bearer " + key}
		response := request("GET", "/api/v1/admin/config", headers)
		if response.Code != 200 || response.Header().Get("Content-Type") != "application/json" || response.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("config read status/headers: %d %v", response.Code, response.Header())
		}
		var body map[string]any
		if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		expected := map[string]any{"cors": want, "auth": map[string]any{"configured": true}, "ingest": map[string]any{"broker_count": float64(2)}}
		if !reflect.DeepEqual(body, expected) {
			t.Fatalf("unexpected config shape or values: %v", body)
		}
		if strings.Contains(response.Body.String(), "sentinel") {
			t.Fatal("credential setting was exposed")
		}
		for _, auth := range []string{"", "Bearer wrong"} {
			denied := request("GET", "/api/v1/admin/config", map[string]string{"Authorization": auth})
			if denied.Code != 401 || strings.Contains(denied.Body.String(), "allowed_origins") {
				t.Fatal("config escaped authentication")
			}
		}
		if request("PUT", "/api/v1/admin/config", headers).Code != http.StatusUnsupportedMediaType {
			t.Fatal("untyped config write was accepted")
		}
		again := request("GET", "/api/v1/admin/config", headers)
		if again.Body.String() != response.Body.String() {
			t.Fatal("startup snapshot changed")
		}
		preflight := request("OPTIONS", "/api/v1/admin/config", map[string]string{
			"Origin": "https://example.test", "Access-Control-Request-Method": "GET", "Access-Control-Request-Headers": "Authorization",
		})
		if preflight.Code != 200 || preflight.Header().Get("Access-Control-Allow-Origin") == "" || preflight.Header().Get("Access-Control-Max-Age") != map[bool]string{false: "300", true: "600"}[custom] {
			t.Fatal("CORS behavior does not match reported options")
		}
	}
}
