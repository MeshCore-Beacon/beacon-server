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
