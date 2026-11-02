// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api/handlers"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// lastRunSeries counts the beacon_planguard_last_run_timestamp_seconds series the registry exports.
func lastRunSeries(t *testing.T) int {
	t.Helper()
	n, err := testutil.GatherAndCount(metrics.Registry, "beacon_planguard_last_run_timestamp_seconds")
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPlanguardWiring(t *testing.T) {
	get := func(t *testing.T, src handlers.PlanguardSource, target string) (int, string) {
		t.Helper()
		w := httptest.NewRecorder()
		handlers.DBAdminRouter(nil, src).ServeHTTP(w, httptest.NewRequest("GET", target, nil))
		var body map[string]handlers.APIError
		_ = json.Unmarshal(w.Body.Bytes(), &body)
		return w.Code, body["error"].Message
	}

	// The default config leaves the check off: no task, and /planguard says so, fresh or not.
	if config.Resolve(&config.Config{}).PlanguardDailyCheck {
		t.Fatal("planguard.daily_check is on by default")
	}
	src, task := planguardWiring(context.Background(), false, true, nil)
	if task != nil {
		t.Errorf("disabled check scheduled a task %q", task.Name)
	}
	for _, target := range []string{"/planguard", "/planguard?fresh=true"} {
		if code, msg := get(t, src, target); code != 404 || msg != "planguard daily check is disabled" {
			t.Errorf("disabled %s: %d %q", target, code, msg)
		}
	}

	if n := lastRunSeries(t); n != 0 {
		t.Errorf("disabled check exports %d last-run series, want 0", n)
	}

	// On: a daily task on the hourly tick, and 404 "has not run yet" until its first run.
	src, task = planguardWiring(context.Background(), true, true, nil)
	if task == nil || task.Name != "planguard" || task.Interval.Hours() != 1 {
		t.Fatalf("enabled check: task %+v", task)
	}
	if code, msg := get(t, src, "/planguard"); code != 404 || msg != "planguard has not run yet" {
		t.Errorf("enabled before the first run: %d %q", code, msg)
	}
	if n := lastRunSeries(t); n != 1 {
		t.Errorf("enabled check exports %d last-run series before its first run, want 1 (at 0)", n)
	}

	// On without jobs: no task and no route.
	src, task = planguardWiring(context.Background(), true, false, nil)
	if task != nil || src != nil {
		t.Errorf("enabled without jobs: task %v source %v", task, src)
	}
	if code, _ := get(t, src, "/planguard"); code != 404 {
		t.Errorf("enabled without jobs: %d", code)
	}
}
