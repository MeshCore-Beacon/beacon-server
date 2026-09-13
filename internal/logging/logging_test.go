// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package logging

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

func TestLevelsAndFormats(t *testing.T) {
	for _, format := range []string{"text", "json"} {
		for i, level := range []string{"debug", "info", "warn", "error"} {
			t.Run(format+"/"+level, func(t *testing.T) {
				t.Setenv("LOG_LEVEL", "")
				t.Setenv("LOG_FORMAT", "")
				var out bytes.Buffer
				logger, err := New(&out, config.LogConfig{Level: level, Format: format})
				if err != nil {
					t.Fatal(err)
				}
				logger = logger.With("component", "ingest", "broker", "test")
				logger.Debug("debug message")
				logger.Info("info message")
				logger.Warn("warn message")
				logger.Error("error message")
				lines := strings.Split(strings.TrimSpace(out.String()), "\n")
				if len(lines) != 4-i {
					t.Fatalf("got %d records, want %d: %s", len(lines), 4-i, out.String())
				}
				for _, line := range lines {
					if format == "json" {
						var record map[string]any
						if err := json.Unmarshal([]byte(line), &record); err != nil {
							t.Fatal(err)
						}
						if record["component"] != "ingest" || record["broker"] != "test" || record["time"] == nil {
							t.Fatalf("missing fields: %v", record)
						}
					} else if !strings.Contains(line, "component=ingest") || !strings.Contains(line, "broker=test") {
						t.Fatalf("missing fields: %s", line)
					}
				}
				if !strings.Contains(out.String(), "error message") {
					t.Fatal("error suppressed")
				}
			})
		}
	}
}

func TestDefaultsAndEnvironment(t *testing.T) {
	t.Setenv("LOG_LEVEL", "")
	t.Setenv("LOG_FORMAT", "")
	var out bytes.Buffer
	logger, err := New(&out, config.LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	if logger.Enabled(t.Context(), slog.LevelDebug) || !logger.Enabled(t.Context(), slog.LevelInfo) {
		t.Fatal("default must be info")
	}
	t.Setenv("LOG_LEVEL", " WARN ")
	t.Setenv("LOG_FORMAT", "JSON")
	logger, err = New(&out, config.LogConfig{Level: "debug", Format: "text"})
	if err != nil {
		t.Fatal(err)
	}
	logger.Info("hidden")
	logger.Error("visible")
	var record map[string]any
	if err = json.Unmarshal(bytes.TrimSpace(out.Bytes()), &record); err != nil {
		t.Fatal(err)
	}
	if record["level"] != "ERROR" || record["msg"] != "visible" {
		t.Fatalf("env overrides ignored: %v", record)
	}
}

func TestInvalidOptions(t *testing.T) {
	for _, cfg := range []config.LogConfig{{Level: "verbose"}, {Level: "off"}, {Level: "INFO+2"}, {Format: "xml"}} {
		t.Setenv("LOG_LEVEL", "")
		t.Setenv("LOG_FORMAT", "")
		if _, err := New(&bytes.Buffer{}, cfg); err == nil {
			t.Fatalf("accepted invalid options: %+v", cfg)
		}
	}
	t.Setenv("LOG_LEVEL", "invalid")
	if _, err := New(&bytes.Buffer{}, config.LogConfig{Level: "info"}); err == nil {
		t.Fatal("invalid environment override accepted")
	}
}
