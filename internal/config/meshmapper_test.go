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
	valid := "regions:\n  - slug: ottawa\n    iatas: [YOW]\nmeshmapper:\n  scopes:\n    enabled: true\n    sources:\n      YOW: https://yow.meshmapper.net/get_scopes.php\n"
	for _, tc := range []struct {
		name, text string
		bad        bool
	}{
		{"valid", valid, false},
		{"unknown region", strings.Replace(valid, "iatas: [YOW]", "iatas: [YVR]", 1), true},
		{"http", strings.Replace(valid, "https:", "http:", 1), true},
		{"private host", strings.Replace(valid, "yow.meshmapper.net", "127.0.0.1", 1), true},
		{"credentials", strings.Replace(valid, "https://", "https://secret@", 1), true},
		{"query", strings.Replace(valid, "get_scopes.php", "get_scopes.php?key=secret", 1), true},
		{"non API", strings.Replace(valid, "get_scopes.php", "index.php", 1), true},
		{"fast", valid + "    refresh_interval: 1s\n", true},
		{"negative", valid + "    refresh_interval: -1h\n", true},
		{"disabled", strings.Replace(valid, "enabled: true", "enabled: false", 1), false},
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
			if err == nil && cfg.MeshMapper.Scopes.Interval() != time.Hour {
				t.Fatal("wrong default")
			}
		})
	}
	cfg := Config{MeshMapper: MeshMapperConfig{Scopes: MeshMapperScopesConfig{Enabled: true}}}
	if err := cfg.validateMeshMapper(); err == nil {
		t.Fatal("empty sources accepted")
	}
}
