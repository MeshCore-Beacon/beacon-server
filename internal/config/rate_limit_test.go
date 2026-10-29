// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestRateLimitConfig(t *testing.T) {
	for _, tc := range []struct {
		name, yaml string
		want       ResolvedRateLimitConfig
		wantError  bool
	}{
		{"omitted", "{}", ResolvedRateLimitConfig{true, 600, 600}, false},
		{"disabled", "ratelimit: {enabled: false}", ResolvedRateLimitConfig{false, 600, 600}, false},
		{"custom", "ratelimit: {enabled: true, requests_per_minute: 900, burst: 100}", ResolvedRateLimitConfig{true, 900, 100}, false},
		{"burst default follows budget", "ratelimit: {requests_per_minute: 1200}", ResolvedRateLimitConfig{true, 1200, 1200}, false},
		{"zero defaults", "ratelimit: {requests_per_minute: 0, burst: 0}", ResolvedRateLimitConfig{true, 600, 600}, false},
		{"negative minute", "ratelimit: {requests_per_minute: -1}", ResolvedRateLimitConfig{}, true},
		{"negative burst", "ratelimit: {burst: -1}", ResolvedRateLimitConfig{}, true},
		{"invalid disabled config", "ratelimit: {enabled: false, burst: -1}", ResolvedRateLimitConfig{}, true},
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
			if err == nil && Resolve(cfg).RateLimit != tc.want {
				t.Fatalf("resolved limits = %+v, want %+v", Resolve(cfg).RateLimit, tc.want)
			}
		})
	}
	cfg, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err != nil || Resolve(cfg).RateLimit != (ResolvedRateLimitConfig{true, 600, 600}) {
		t.Fatalf("missing config did not enable defaults: %v", err)
	}
}
