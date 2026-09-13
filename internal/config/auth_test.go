// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadAuth(t *testing.T) {
	for _, tc := range []struct {
		name, env, want string
		file, set       bool
	}{
		{"no config", "", "", false, false},
		{"YAML", "", "yaml-test-key", true, false},
		{"environment overrides", "env-test-key", "env-test-key", true, true},
		{"empty environment disables", "", "", true, true},
		{"environment without file", "env-test-key", "env-test-key", false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("BEACON_API_KEY", tc.env)
			if !tc.set {
				if err := os.Unsetenv("BEACON_API_KEY"); err != nil {
					t.Fatal(err)
				}
			}
			path := filepath.Join(t.TempDir(), "config.yaml")
			if tc.file {
				if err := os.WriteFile(path, []byte("auth:\n  api_key: yaml-test-key\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			cfg, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Auth.APIKey != tc.want {
				t.Fatal("wrong auth configuration precedence")
			}
			encoded, err := json.Marshal(cfg)
			if err != nil || strings.Contains(string(encoded), "test-key") {
				t.Fatal("key exposed in JSON")
			}
			if strings.Contains(Resolve(cfg).String(), "test-key") {
				t.Fatal("key exposed in startup summary")
			}
		})
	}
}
