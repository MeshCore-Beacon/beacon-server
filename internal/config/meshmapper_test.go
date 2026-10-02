// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMeshMapperConfig(t *testing.T) {
	valid := "meshmapper:\n  scopes:\n    enabled: true\n"
	for _, tc := range []struct {
		name, text string
		bad        bool
	}{
		{"valid", valid, false},
		{"daily", valid + "    refresh_interval: 24h\n", false},
		{"legacy sources ignored", valid + "    sources:\n      yow: http://127.0.0.1/\n", false},
		{"hourly", valid + "    refresh_interval: 1h\n", false},
		{"under the rate limit", valid + "    refresh_interval: 55m\n", true},
		{"fast", valid + "    refresh_interval: 1s\n", true},
		{"slow", valid + "    refresh_interval: 25h\n", true},
		{"negative", valid + "    refresh_interval: -1h\n", true},
		{"disabled", strings.Replace(valid, "enabled: true", "enabled: false", 1), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(p, []byte(tc.text), 0600); err != nil {
				t.Fatal(err)
			}
			_, err := Load(p)
			if (err != nil) != tc.bad {
				t.Fatalf("error=%v", err)
			}
		})
	}
	if (MeshMapperScopesConfig{}).Interval() != time.Hour {
		t.Fatal("wrong default")
	}
}

func TestMeshMapperZonesConfig(t *testing.T) {
	valid := "regions:\n  - slug: ottawa\n    iatas: [YOW, YOW]\n  - slug: toronto\n    iatas: [YYZ]\nmeshmapper:\n  zones:\n    enabled: true\n"
	for _, tc := range []struct {
		name, text string
		bad        bool
		interval   time.Duration
	}{
		{"default", valid, false, 24 * time.Hour},
		{"daily", valid + "    refresh_interval: 24h\n", false, 24 * time.Hour},
		{"weekly", valid + "    refresh_interval: 168h\n", false, 168 * time.Hour},
		{"under the rate limit", valid + "    refresh_interval: 23h30m\n", true, 0},
		{"hourly", valid + "    refresh_interval: 1h\n", true, 0},
		{"too rare", valid + "    refresh_interval: 169h\n", true, 0},
		{"no regions", "meshmapper:\n  zones:\n    enabled: true\n", false, 24 * time.Hour},
		{"groups", valid + "    import_groups: true\n", false, 24 * time.Hour},
		{"groups without zones", "meshmapper:\n  zones:\n    import_groups: true\n", true, 0},
		{"disabled", "meshmapper:\n  zones:\n    enabled: false\n", false, 24 * time.Hour},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p := filepath.Join(t.TempDir(), "config.yaml")
			if err := os.WriteFile(p, []byte(tc.text), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(p)
			if (err != nil) != tc.bad {
				t.Fatalf("error=%v", err)
			}
			if err == nil && cfg.MeshMapper.Zones.Interval() != tc.interval {
				t.Fatal(cfg.MeshMapper.Zones.Interval())
			}
		})
	}
}
