// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
	"github.com/MeshCore-Beacon/beacon-server/internal/guards"
)

func TestLoadLogConfig(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("log:\n  level: warn\n  format: json\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Log.Level != "warn" || cfg.Log.Format != "json" {
		t.Fatalf("log config ignored: %+v", cfg.Log)
	}
}

func TestLoadTrustedProxies(t *testing.T) {
	for _, tc := range []struct {
		name, yaml string
		count      int
		wantError  bool
	}{
		{"omitted", "{}", 0, false},
		{"empty", "server: {trusted_proxies: []}", 0, false},
		{"CIDRs", "server: {trusted_proxies: ['192.0.2.0/24', '2001:db8::/32']}", 2, false},
		{"bare IP", "server: {trusted_proxies: ['192.0.2.1']}", 0, true},
		{"invalid CIDR", "server: {trusted_proxies: ['192.0.2.0/99']}", 0, true},
		{"empty entry", "server: {trusted_proxies: ['', '192.0.2.0/24']}", 0, true},
		{"null entry", "server: {trusted_proxies: [null, '192.0.2.0/24']}", 0, true},
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
			if err == nil && len(cfg.Server.TrustedProxies) != tc.count {
				t.Fatalf("got %d proxy prefixes, want %d", len(cfg.Server.TrustedProxies), tc.count)
			}
		})
	}
}

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
	if r.TelemetryRetention != 31*24*time.Hour {
		t.Errorf("expected TelemetryRetention 744h, got %v", r.TelemetryRetention)
	}
	if r.PacketRetention != 7*24*time.Hour {
		t.Errorf("expected PacketRetention 168h, got %v", r.PacketRetention)
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
		t.Errorf("expected NodeDeleteAfter 720h, got %v", r.NodeDeleteAfter)
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
	if r.RouteReconfirmRowsPerHour != 81000 {
		t.Errorf("RouteReconfirmRowsPerHour = %d, want 81000", r.RouteReconfirmRowsPerHour)
	}
	if !strings.Contains(r.String(), "routeReconfirmRowsPerHour=81000") {
		t.Errorf("String() = %q, want routeReconfirmRowsPerHour=81000", r.String())
	}
}

func TestLoad_ReconfirmRowsPerHour(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("routes:\n  reconfirm_rows_per_hour: 12000\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := Resolve(cfg).RouteReconfirmRowsPerHour; got != 12000 {
		t.Errorf("RouteReconfirmRowsPerHour = %d, want 12000", got)
	}
}

func TestLoad_NegativeReconfirmRowsRejected(t *testing.T) {
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("routes:\n  reconfirm_rows_per_hour: -1\n"), 0600); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "routes.reconfirm_rows_per_hour must be positive or zero for the default") {
		t.Errorf("err = %v, want the reconfirm_rows_per_hour error", err)
	}
}

func TestResolve_RollupRetentionDefaults(t *testing.T) {
	r := Resolve(&Config{})
	if r.RollupRetention != 90*24*time.Hour {
		t.Errorf("expected RollupRetention 2160h (90 days), got %v", r.RollupRetention)
	}
}

func TestResolve_TelemetryRetentionDefault(t *testing.T) {
	r := Resolve(&Config{})
	if r.TelemetryRetention != 31*24*time.Hour {
		t.Errorf("expected TelemetryRetention 744h (31 days), got %v", r.TelemetryRetention)
	}
}

func TestResolve_RollupRetentionExplicitOverride(t *testing.T) {
	cfg := &Config{}
	cfg.Analytics.RollupRetention.Duration = 60 * 24 * time.Hour
	r := Resolve(cfg)
	if r.RollupRetention != 60*24*time.Hour {
		t.Errorf("expected RollupRetention 1440h (60 days), got %v", r.RollupRetention)
	}
}

func TestLoad_PacketsRetentionTooShort(t *testing.T) {
	f, err := os.CreateTemp("", "beacon-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())

	_, _ = f.WriteString("packets:\n  retention: 1h\n")
	f.Close()

	_, err = Load(f.Name())
	if err == nil {
		t.Fatal("expected error for packets.retention < 24h, got nil")
	}
	if !strings.Contains(err.Error(), "packets.retention must be at least 24h") {
		t.Errorf("expected error about packets.retention, got: %v", err)
	}
}

func TestLoad_AnalyticsRollupRetentionTooShort(t *testing.T) {
	f, err := os.CreateTemp("", "beacon-config-*.yaml")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	defer os.Remove(f.Name())

	_, _ = f.WriteString("analytics:\n  rollup_retention: 1h\n")
	f.Close()

	_, err = Load(f.Name())
	if err == nil {
		t.Fatal("expected error for analytics.rollup_retention < 24h, got nil")
	}
	if !strings.Contains(err.Error(), "analytics.rollup_retention must be at least 24h") {
		t.Errorf("expected error about analytics.rollup_retention, got: %v", err)
	}
}

func TestLoad_NegativeDurationsRejected(t *testing.T) {
	for _, key := range []string{
		"background:\n  cleanup: -1h", "background:\n  view_refresh: -1h", "background:\n  reconfirm: -1m",
		"presence:\n  flush_interval: -1s", "presence:\n  packet_ttl: -1s",
		"telemetry:\n  resolution: -1h", "telemetry:\n  retention: -1h",
		"routes:\n  retention: -1h", "routes:\n  grace: -1h",
		"nodes:\n  clock_drift_threshold: -1m", "nodes:\n  stale_threshold: -1h", "nodes:\n  delete_after: -1h",
		"cache:\n  ttl: -1m", "cache:\n  ttls:\n    stats: -1m",
		"meshmapper:\n  scopes:\n    refresh_interval: -1h",
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(key+"\n"), 0600); err != nil {
			t.Fatal(err)
		}
		_, err := Load(path)
		if err == nil || !strings.Contains(err.Error(), "must not be negative") {
			t.Errorf("%q: err = %v, want a negative-duration error", key, err)
		}
	}
	// Nonpositive still means "disabled" here.
	path := filepath.Join(t.TempDir(), "config.yaml")
	if err := os.WriteFile(path, []byte("observers:\n  delete_after: -1h\n"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err != nil {
		t.Errorf("observers.delete_after: %v", err)
	}
}

func TestLoadMinAppVersion(t *testing.T) {
	for _, tc := range []struct {
		name, yaml, want string
		wantError        bool
	}{
		{"omitted", "{}", "", false},
		{"empty", "mobile: {min_app_version: ''}", "", false},
		{"valid", "mobile: {min_app_version: '0.1.1'}", "0.1.1", false},
		{"multi-digit", "mobile: {min_app_version: ' 10.20.30 '}", "10.20.30", false},
		{"v prefix", "mobile: {min_app_version: 'v0.1.1'}", "", true},
		{"two parts", "mobile: {min_app_version: '0.1'}", "", true},
		{"pre-release", "mobile: {min_app_version: '0.1.1-beta'}", "", true},
		{"four parts", "mobile: {min_app_version: '0.1.1.2'}", "", true},
		{"letters", "mobile: {min_app_version: 'a.b.c'}", "", true},
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
			if err == nil && cfg.Mobile.MinAppVersion != tc.want {
				t.Fatalf("min_app_version = %q, want %q", cfg.Mobile.MinAppVersion, tc.want)
			}
		})
	}
}

func TestLoadMinWebVersion(t *testing.T) {
	for _, tc := range []struct {
		name, yaml, want string
		wantError        bool
	}{
		{"omitted", "{}", "", false},
		{"valid", "web: {min_web_version: ' 2.0.4 '}", "2.0.4", false},
		{"v prefix", "web: {min_web_version: 'v2.0.4'}", "", true},
		{"two parts", "web: {min_web_version: '2.0'}", "", true},
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
			if err == nil && cfg.Web.MinWebVersion != tc.want {
				t.Fatalf("min_web_version = %q, want %q", cfg.Web.MinWebVersion, tc.want)
			}
		})
	}
}

func TestResolve_DatabaseGuardsDefaults(t *testing.T) {
	r := Resolve(&Config{})
	if r.APIPlanLayout != planpolicy.CustomAll {
		t.Errorf("APIPlanLayout = %q, want %q", r.APIPlanLayout, planpolicy.CustomAll)
	}
	if r.GuardsEnforce == nil || len(r.GuardsEnforce) != 0 {
		t.Errorf("GuardsEnforce = %#v, want non-nil empty", r.GuardsEnforce)
	}
	if r.GuardsBudget != "shadow" {
		t.Errorf("GuardsBudget = %q, want shadow", r.GuardsBudget)
	}
	if r.AdminListen != "" {
		t.Errorf("AdminListen = %q, want empty", r.AdminListen)
	}
}

func TestLoad_DatabaseGuardsValidation(t *testing.T) {
	for _, tc := range []struct {
		yaml, err string
	}{
		{"database:\n  api_plan_layout: generic\n", "database.api_plan_layout must be custom_all, split or single"},
		{"database:\n  slow_pool_size: 0\n", "database.slow_pool_size must be at least 1"},
		{"database:\n  slow_pool_size: -3\n", "database.slow_pool_size must be at least 1"},
		{"database:\n  slow_pool_size: 1001\n", "database.slow_pool_size must be at most 1000"},
		{"guards:\n  enforce: [routes, bogus]\n", `guards.enforce[1]: unknown family "bogus" (routes, channel_messages, packets_global, packets_region, packets_window, packet_detail, stats, other)`},
		{"guards:\n  budget: enforce\n", "guards.budget must be shadow (budget enforcement is not available)"},
		{"server:\n  admin_listen: \"9090\"\n", "server.admin_listen must be host:port, e.g. 127.0.0.1:9090"},
		{"server:\n  admin_listen: \"127.0.0.1:\"\n", "server.admin_listen must be host:port, e.g. 127.0.0.1:9090"},
		{"server:\n  admin_listen: \":\"\n", "server.admin_listen must be host:port, e.g. 127.0.0.1:9090"},
		{"server:\n  admin_listen: \"127.0.0.1:metrics\"\n", "server.admin_listen must be host:port, e.g. 127.0.0.1:9090"},
		{"server:\n  admin_listen: \"127.0.0.1:70000\"\n", "server.admin_listen must be host:port, e.g. 127.0.0.1:9090"},
		{"server:\n  admin_listen: \"127.0.0.1:0\"\n", "server.admin_listen must be host:port, e.g. 127.0.0.1:9090"},
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(tc.yaml), 0600); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(path); err == nil || err.Error() != tc.err {
			t.Errorf("Load(%q) error = %v, want %q", tc.yaml, err, tc.err)
		}
	}

	path := filepath.Join(t.TempDir(), "config.yaml")
	yml := "server:\n  trusted_proxies: [10.0.0.0/8]\n  admin_listen: \":9090\"\n" +
		"database:\n  api_plan_layout: split\nguards:\n  enforce: [stats, routes, stats]\n  budget: shadow\n"
	if err := os.WriteFile(path, []byte(yml), 0600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Server.TrustedProxies) != 1 || cfg.Server.TrustedProxies[0].String() != "10.0.0.0/8" ||
		cfg.Server.AdminListen != ":9090" {
		t.Fatalf("server config = %+v", cfg.Server)
	}
	r := Resolve(cfg)
	if r.AdminListen != ":9090" || r.APIPlanLayout != planpolicy.Split || r.GuardsBudget != "shadow" ||
		strings.Join(r.GuardsEnforce, ",") != "routes,stats" {
		t.Fatalf("resolved = %s", r)
	}
}

// database.slow_pool_size sizes api_slow: 2 when absent, any positive value as given.
func TestSlowPoolSize(t *testing.T) {
	for yml, want := range map[string]int32{"": 2, "database:\n  api_plan_layout: split\n": 2, "database:\n  slow_pool_size: 8\n": 8, "database:\n  slow_pool_size: 1\n": 1} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(yml), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("%q: %v", yml, err)
		}
		if got := Resolve(cfg).SlowPoolSize; got != want {
			t.Errorf("%q: SlowPoolSize = %d, want %d", yml, got, want)
		}
		if !strings.Contains(Resolve(cfg).String(), fmt.Sprintf("slowPoolSize=%d", want)) {
			t.Errorf("%q: startup line lacks slowPoolSize=%d", yml, want)
		}
	}
}

// TestGuardFamiliesMatch keeps the config's family list in step with the guards package.
func TestGuardFamiliesMatch(t *testing.T) {
	var want []string
	for _, f := range guards.Families {
		want = append(want, string(f))
	}
	if strings.Join(guardFamilies, ",") != strings.Join(want, ",") {
		t.Errorf("guardFamilies = %v, want %v", guardFamilies, want)
	}
}

func TestLoad_ServerReadOnly(t *testing.T) {
	for _, tc := range []struct {
		yaml string
		want bool
	}{
		{"server:\n  read_only: true\n", true},
		{"server:\n  admin_listen: \"\"\n", false},
		{"server:\n  read_only: yes\n", true},
		{"server:\n  read_only: false\n", false},
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(tc.yaml), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(path)
		if err != nil {
			t.Fatalf("Load(%q): %v", tc.yaml, err)
		}
		if cfg.Server.ReadOnly != tc.want {
			t.Errorf("Load(%q): ReadOnly = %v, want %v", tc.yaml, cfg.Server.ReadOnly, tc.want)
		}
		if r := Resolve(cfg); r.ReadOnly != tc.want || !strings.Contains(r.String(), "readOnly="+strconv.FormatBool(tc.want)) {
			t.Errorf("Resolve(%q): ReadOnly = %v (%s), want %v", tc.yaml, r.ReadOnly, r, tc.want)
		}
	}
}

func TestLoad_PlanguardDailyCheck(t *testing.T) {
	for _, tc := range []struct {
		yaml    string
		want    bool
		wantErr string
	}{
		{"", false, ""},
		{"planguard: {}\n", false, ""},
		{"planguard:\n  daily_check: true\n", true, ""},
		{"planguard:\n  daily_check: false\n", false, ""},
		{"planguard:\n  daily_check: true\nserver:\n  read_only: true\n", false, "planguard.daily_check cannot be enabled with server.read_only"},
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(tc.yaml), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(path)
		if tc.wantErr != "" {
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("Load(%q): err %v, want %q", tc.yaml, err, tc.wantErr)
			}
			continue
		}
		if err != nil {
			t.Fatalf("Load(%q): %v", tc.yaml, err)
		}
		if r := Resolve(cfg); r.PlanguardDailyCheck != tc.want || !strings.Contains(r.String(), "planguardDailyCheck="+strconv.FormatBool(tc.want)) {
			t.Errorf("Resolve(%q): PlanguardDailyCheck = %v (%s), want %v", tc.yaml, r.PlanguardDailyCheck, r, tc.want)
		}
	}
}

func TestLoad_ReadModelsCmi(t *testing.T) {
	for _, c := range []struct {
		yaml string
		want bool
	}{
		{"", true},
		{"readmodels:\n  cmi:\n    enabled: false\n", false},
		{"readmodels:\n  cmi:\n    enabled: true\n", true},
		{"readmodels:\n  cmi: {}\n", true},
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(c.yaml), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(path)
		if err != nil {
			t.Fatal(err)
		}
		r := Resolve(cfg)
		if r.ReadModelCmiEnabled != c.want {
			t.Errorf("%q: ReadModelCmiEnabled = %v, want %v", c.yaml, r.ReadModelCmiEnabled, c.want)
		}
		if !strings.Contains(r.String(), fmt.Sprintf("readModelCmi=%t", c.want)) {
			t.Errorf("%q: resolved line %q", c.yaml, r.String())
		}
	}
}

func TestLoad_ReadModelsPacketSites(t *testing.T) {
	for _, c := range []struct {
		yaml string
		want bool
	}{
		{"", true},
		{"readmodels:\n  packet_sites:\n    enabled: false\n", false},
		{"readmodels:\n  packet_sites:\n    enabled: true\n", true},
		{"readmodels:\n  cmi:\n    enabled: false\n", true},
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(c.yaml), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(path)
		if err != nil {
			t.Fatal(err)
		}
		r := Resolve(cfg)
		if r.ReadModelPacketSitesEnabled != c.want {
			t.Errorf("%q: ReadModelPacketSitesEnabled = %v, want %v", c.yaml, r.ReadModelPacketSitesEnabled, c.want)
		}
		if !strings.Contains(r.String(), fmt.Sprintf("readModelPacketSites=%t", c.want)) {
			t.Errorf("%q: resolved line %q", c.yaml, r.String())
		}
	}
}

func TestLoad_ReadModelsFill(t *testing.T) {
	type fill struct {
		duty      float64
		minPause  time.Duration
		slice     int64
		cmiBatch  int64
		resolved  string
		errSubstr string
	}
	defaults := fill{0.5, 100 * time.Millisecond, 500_000, 10_000, "fillDuty=0.5 fillMinPause=100ms fillPacketSitesSlice=500000 fillCmiBatch=10000", ""}
	for _, c := range []struct {
		yaml string
		want fill
	}{
		{"", defaults},
		{"readmodels:\n  fill: {}\n", defaults},
		{"readmodels:\n  fill:\n    duty: 0.25\n    min_pause: 1s\n    packet_sites_slice: 100000\n    cmi_batch: 2000\n",
			fill{0.25, time.Second, 100_000, 2_000, "fillDuty=0.25 fillMinPause=1s fillPacketSitesSlice=100000 fillCmiBatch=2000", ""}},
		{"readmodels:\n  fill:\n    duty: 1\n    min_pause: 0s\n", fill{1, 0, 500_000, 10_000, "fillDuty=1 fillMinPause=0s", ""}},
		{"readmodels:\n  fill:\n    duty: 0\n", fill{errSubstr: "readmodels.fill.duty"}},
		{"readmodels:\n  fill:\n    duty: -0.5\n", fill{errSubstr: "readmodels.fill.duty"}},
		{"readmodels:\n  fill:\n    duty: 1.5\n", fill{errSubstr: "readmodels.fill.duty"}},
		{"readmodels:\n  fill:\n    duty: .nan\n", fill{errSubstr: "readmodels.fill.duty"}},
		{"readmodels:\n  fill:\n    min_pause: -1s\n", fill{errSubstr: "readmodels.fill.min_pause"}},
		{"readmodels:\n  fill:\n    min_pause: 2m\n", fill{errSubstr: "readmodels.fill.min_pause"}},
		{"readmodels:\n  fill:\n    packet_sites_slice: 0\n", fill{errSubstr: "readmodels.fill.packet_sites_slice"}},
		{"readmodels:\n  fill:\n    packet_sites_slice: 20000000\n", fill{errSubstr: "readmodels.fill.packet_sites_slice"}},
		{"readmodels:\n  fill:\n    cmi_batch: -1\n", fill{errSubstr: "readmodels.fill.cmi_batch"}},
		{"readmodels:\n  fill:\n    cmi_batch: 2000000\n", fill{errSubstr: "readmodels.fill.cmi_batch"}},
	} {
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err := os.WriteFile(path, []byte(c.yaml), 0600); err != nil {
			t.Fatal(err)
		}
		cfg, err := Load(path)
		if c.want.errSubstr != "" {
			if err == nil || !strings.Contains(err.Error(), c.want.errSubstr) {
				t.Errorf("%q: Load error %v, want one naming %s", c.yaml, err, c.want.errSubstr)
			}
			continue
		}
		if err != nil {
			t.Fatalf("%q: %v", c.yaml, err)
		}
		r := Resolve(cfg)
		got := fill{r.FillDuty, r.FillMinPause, r.FillPacketSitesSlice, r.FillCmiBatch, c.want.resolved, ""}
		if got != c.want {
			t.Errorf("%q: resolved %+v, want %+v", c.yaml, got, c.want)
		}
		if !strings.Contains(r.String(), c.want.resolved) {
			t.Errorf("%q: resolved line %q lacks %q", c.yaml, r.String(), c.want.resolved)
		}
	}
}
