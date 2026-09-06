// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"os"
	"strings"
	"testing"
	"time"
)

func TestLoad_FileNotFound(t *testing.T) {
	cfg, err := Load("/nonexistent/path/config.yaml")
	if err != nil {
		t.Fatalf("expected nil error for missing file, got %v", err)
	}
	if cfg == nil {
		t.Fatal("expected empty config, got nil")
	}
}

func TestLoad_ValidFile(t *testing.T) {
	f, err := os.CreateTemp("", "beacon-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())

	_, _ = f.WriteString(`
iatas:
  YVR:
    name: Vancouver
    lat: 49.1967
    lng: -123.1815
regions:
  - slug: bc
    name: British Columbia
    display_order: 1
    iatas: [YVR]
observers:
  delete_after: 720h
`)
	f.Close()

	cfg, err := Load(f.Name())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := cfg.IATAs["YVR"]; !ok {
		t.Error("expected YVR in IATAs")
	}
	if len(cfg.Regions) != 1 {
		t.Errorf("expected 1 region, got %d", len(cfg.Regions))
	}
	if cfg.Regions[0].Slug != "bc" {
		t.Errorf("expected slug bc, got %s", cfg.Regions[0].Slug)
	}
	if Resolve(cfg).ObserverDeleteAfter != 30*24*time.Hour {
		t.Error("observers.delete_after was not loaded from YAML")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	f, err := os.CreateTemp("", "beacon-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())

	_, _ = f.WriteString(`not: valid: yaml: [`)
	f.Close()

	_, err = Load(f.Name())
	if err == nil {
		t.Fatal("expected error for invalid YAML, got nil")
	}
}

func TestResolve_Defaults(t *testing.T) {
	r := Resolve(&Config{})
	if r.TelemetryResolution != time.Hour {
		t.Errorf("expected TelemetryResolution 1h, got %v", r.TelemetryResolution)
	}
	if r.TelemetryRetention != 28*24*time.Hour {
		t.Errorf("expected TelemetryRetention 672h, got %v", r.TelemetryRetention)
	}
	if r.PacketRetention != 30*24*time.Hour {
		t.Errorf("expected PacketRetention 720h, got %v", r.PacketRetention)
	}
	if r.MaxConnsPerIP != 5 {
		t.Errorf("expected MaxConnsPerIP 5, got %d", r.MaxConnsPerIP)
	}
	if r.ViewRefreshInterval != time.Hour {
		t.Errorf("expected ViewRefreshInterval 1h, got %v", r.ViewRefreshInterval)
	}
	if r.ReconfirmInterval != time.Hour {
		t.Errorf("expected ReconfirmInterval 1h, got %v", r.ReconfirmInterval)
	}
	if r.CleanupInterval != time.Hour {
		t.Errorf("expected CleanupInterval 1h, got %v", r.CleanupInterval)
	}
	if r.ClockDriftThreshold != 5*time.Minute {
		t.Errorf("expected ClockDriftThreshold 5m, got %v", r.ClockDriftThreshold)
	}
	if r.NodeStaleThreshold != 24*time.Hour {
		t.Errorf("expected NodeStaleThreshold 24h, got %v", r.NodeStaleThreshold)
	}
	if r.NodeDeleteAfter != 30*24*time.Hour {
		t.Errorf("expected NodeDeleteAfter 720h (same default as PacketRetention), got %v", r.NodeDeleteAfter)
	}
	if r.ObserverDeleteAfter != 0 {
		t.Errorf("observer deletion must be disabled by default, got %v", r.ObserverDeleteAfter)
	}
}

func TestResolve_ExplicitValues(t *testing.T) {
	cfg := &Config{}
	cfg.Telemetry.Resolution.Duration = 30 * time.Minute
	cfg.Telemetry.Retention.Duration = 14 * 24 * time.Hour
	cfg.Packets.Retention.Duration = 7 * 24 * time.Hour
	cfg.WebSocket.MaxConnectionsPerIP = 10
	cfg.Background.ViewRefresh.Duration = 2 * time.Hour
	cfg.Background.Reconfirm.Duration = 3 * time.Hour
	cfg.Background.Cleanup.Duration = 4 * time.Hour
	cfg.Observers.DeleteAfter.Duration = 45 * 24 * time.Hour

	r := Resolve(cfg)
	if r.TelemetryResolution != 30*time.Minute {
		t.Errorf("expected 30m, got %v", r.TelemetryResolution)
	}
	if r.MaxConnsPerIP != 10 {
		t.Errorf("expected 10, got %d", r.MaxConnsPerIP)
	}
	if r.ViewRefreshInterval != 2*time.Hour {
		t.Errorf("expected 2h, got %v", r.ViewRefreshInterval)
	}
	if r.ObserverDeleteAfter != 45*24*time.Hour {
		t.Errorf("expected observer delete_after 1080h, got %v", r.ObserverDeleteAfter)
	}
}

func TestResolvedConfig_String(t *testing.T) {
	r := Resolve(&Config{})
	s := r.String()
	if s == "" {
		t.Error("expected non-empty string")
	}
	if !strings.Contains(s, "telemetryResolution=") {
		t.Error("expected telemetryResolution in string")
	}
	if !strings.Contains(s, "maxConnsPerIP=") {
		t.Error("expected maxConnsPerIP in string")
	}
}

func TestResolve_RouteDefaults(t *testing.T) {
	r := Resolve(&Config{})
	if r.RouteRetention != 336*time.Hour {
		t.Errorf("RouteRetention = %s, want 336h", r.RouteRetention)
	}
	if r.RouteGrace != 168*time.Hour {
		t.Errorf("RouteGrace = %s, want 168h", r.RouteGrace)
	}
	if r.RouteMinObservations != 3 {
		t.Errorf("RouteMinObservations = %d, want 3", r.RouteMinObservations)
	}
}
