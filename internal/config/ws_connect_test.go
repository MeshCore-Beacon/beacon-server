// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestWebSocketConnectConfig(t *testing.T) {
	for _, tc := range []struct {
		name, yaml string
		want       int
		wantError  bool
	}{
		{"omitted", "{}", 10, false},
		{"zero default", "websocket: {max_connects_per_minute: 0}", 10, false},
		{"custom", "websocket: {max_connects_per_minute: 30}", 30, false},
		{"negative", "websocket: {max_connects_per_minute: -1}", 0, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(path, []byte(tc.yaml), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if (err != nil) != tc.wantError {
				t.Fatalf("Load error = %v, want error = %v", err, tc.wantError)
			}
			if err == nil && (Resolve(cfg).MaxConnectsPerMinute != tc.want || Resolve(cfg).MaxConnsPerIP != 5) {
				t.Fatalf("unexpected resolved WebSocket limits: %+v", Resolve(cfg))
			}
		})
	}
}

func TestWebSocketAllowedOrigins(t *testing.T) {
	for _, tc := range []struct {
		name, origin string
		wantError    bool
	}{
		{"https", "https://example.com", false},
		{"http with port", "http://localhost:5173", false},
		{"wildcard", "*", true},
		{"wildcard subdomain", "https://*.example.com", false},
		{"wildcard subdomain with port", "http://*.example.com:8443", false},
		{"wildcard scheme only", "https://*", true},
		{"wildcard over tld", "https://*.com", true},
		{"wildcard label prefix", "https://a*.example.com", true},
		{"wildcard label suffix", "https://*a.example.com", true},
		{"wildcard inner label", "https://x.*.example.com", true},
		{"double wildcard", "https://*.*.example.com", true},
		{"wildcard empty label", "https://*..com", true},
		{"question mark", "https://ex?mple.com", true},
		{"character class", "https://[ab].example.com", true},
		{"backslash", `https://*.example.com\`, true},
		{"no scheme", "example.com", true},
		{"other scheme", "ftp://example.com", true},
		{"path", "https://example.com/", true},
		{"query", "https://example.com?x=1", true},
		{"userinfo", "https://user@example.com", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yaml")
			yaml := "websocket: {allowed_origins: [\"" + tc.origin + "\"]}"
			if err := os.WriteFile(path, []byte(yaml), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(path)
			if (err != nil) != tc.wantError {
				t.Fatalf("Load error = %v, want error = %v", err, tc.wantError)
			}
			if err == nil && (len(cfg.WebSocket.AllowedOrigins) != 1 || cfg.WebSocket.AllowedOrigins[0] != tc.origin) {
				t.Fatalf("allowed_origins = %v", cfg.WebSocket.AllowedOrigins)
			}
		})
	}
}
