// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

func TestProxyLimitWarning(t *testing.T) {
	limited := config.ResolvedConfig{RateLimit: config.ResolvedRateLimitConfig{Enabled: true}, MaxConnectsPerMinute: 10}
	if got := proxyLimitWarning(limited, 0); !strings.Contains(got, "server.trusted_proxies") || !strings.Contains(got, "X-Real-IP") {
		t.Errorf("rate limited without proxies: %q", got)
	}
	wsOnly := config.ResolvedConfig{MaxConnectsPerMinute: 10}
	if got := proxyLimitWarning(wsOnly, 0); !strings.Contains(got, "WebSocket") || strings.Contains(got, "REST") {
		t.Errorf("WebSocket limit only: %q", got)
	}
	if got := proxyLimitWarning(limited, 1); got != "" {
		t.Errorf("proxies configured: %q", got)
	}
}

func TestStartupLogging(t *testing.T) {
	if os.Getenv("BEACON_TEST_LOG_STARTUP") == "1" {
		main()
		return
	}
	for _, level := range []string{"debug", "info", "warn", "error"} {
		t.Run(level, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "config.yaml")
			if err := os.WriteFile(path, []byte("log: {level: debug, format: text}\n"), 0600); err != nil {
				t.Fatal(err)
			}
			cmd := exec.Command(os.Args[0], "-test.run=^TestStartupLogging$")
			cmd.Dir = dir
			// Invalid port fails during parsing, before any database/network connection.
			cmd.Env = append(os.Environ(), "BEACON_TEST_LOG_STARTUP=1", "CONFIG_PATH="+path, "LOG_LEVEL="+level, "LOG_FORMAT=json", "POSTGRES_DSN=postgres://test:do-not-log-this@localhost:invalid/beacon")
			out, err := cmd.CombinedOutput()
			if err == nil {
				t.Fatal("startup unexpectedly succeeded")
			}
			if strings.Contains(string(out), "do-not-log-this") {
				t.Fatal("DSN credential leaked")
			}
			foundError := false
			for _, line := range bytes.Split(bytes.TrimSpace(out), []byte("\n")) {
				var record map[string]any
				if err := json.Unmarshal(line, &record); err != nil {
					t.Fatalf("non-JSON startup output: %s", line)
				}
				if record["level"] == "ERROR" {
					foundError = true
				}
				if level == "error" && record["level"] != "ERROR" {
					t.Fatal("error threshold ignored")
				}
				if level == "warn" && record["level"] != "WARN" && record["level"] != "ERROR" {
					t.Fatal("warn threshold ignored")
				}
			}
			if !foundError {
				t.Fatal("startup failure was silent")
			}
		})
	}
}
