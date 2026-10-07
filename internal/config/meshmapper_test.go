// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestLoadMeshMapperAPIKey(t *testing.T) {
	for _, tc := range []struct {
		name, env, want string
		file, set       bool
	}{
		{"missing", "", "", false, false},
		{"YAML", "", "yaml-integration-test-key", true, false},
		{"environment overrides", "env-integration-test-key", "env-integration-test-key", true, true},
		{"empty environment overrides", "", "", true, true},
		{"environment without file", "env-integration-test-key", "env-integration-test-key", false, true},
		{"trim whitespace", " env-integration-test-key\n", "env-integration-test-key", true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("MESHMAPPER_API_KEY", tc.env)
			if !tc.set {
				if err := os.Unsetenv("MESHMAPPER_API_KEY"); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if tc.file {
				if err := os.WriteFile(path, []byte("meshmapper:\n  api_key: yaml-integration-test-key\n  scopes:\n    enabled: true\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.MeshMapper.APIKey != tc.want {
				t.Fatal("MeshMapper key not loaded with expected precedence")
			}
			encoded, err := json.Marshal(cfg)
			if err != nil {
				t.Fatal(err)
			}
			if strings.Contains(string(encoded), "integration-test-key") || strings.Contains(Resolve(cfg).String(), "integration-test-key") {
				t.Fatal("MeshMapper key exposed in JSON or startup summary")
			}
		})
	}
}

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
	if (MeshMapperScopesConfig{}).Interval() != 24*time.Hour {
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
