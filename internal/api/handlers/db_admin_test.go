// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

type stubStatements struct {
	items []api.DBStatement
	err   error
}

func (s stubStatements) DBStatements(context.Context) ([]api.DBStatement, error) {
	return s.items, s.err
}

func serveStatements(t *testing.T, reader DBStatementsReader) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	DBAdminRouter(reader, nil).ServeHTTP(w, httptest.NewRequest("GET", "/statements", nil))
	return w
}

func TestDBAdminStatements(t *testing.T) {
	items := []api.DBStatement{
		{Name: "ListKnownRoutes", Calls: 7, ExecMs: 12.345, PlanMs: 0.5, SharedHit: 100, SharedRead: 3, Rows: 70, WALBytes: 0},
		{Name: "(unnamed)", Calls: 2, ExecMs: 1.001, SharedHit: 4, Rows: 2, WALBytes: 512},
	}
	w := serveStatements(t, stubStatements{items: items})
	if w.Code != 200 {
		t.Fatalf("status = %d; body %s", w.Code, w.Body)
	}
	var raw map[string][]map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
		t.Fatal(err)
	}
	if len(raw["items"]) != 2 {
		t.Fatalf("items = %v", raw)
	}
	for _, key := range []string{"name", "calls", "exec_ms", "plan_ms", "shared_hit", "shared_read", "rows", "wal_bytes"} {
		if _, ok := raw["items"][0][key]; !ok {
			t.Errorf("item lacks %q: %v", key, raw["items"][0])
		}
	}
	var got api.DBStatementsResponse
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.Items[0] != items[0] || got.Items[1] != items[1] {
		t.Fatalf("items = %+v; want %+v", got.Items, items)
	}
}

func TestDBAdminStatementsEmptyIsArray(t *testing.T) {
	w := serveStatements(t, stubStatements{})
	if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"items":[]}` {
		t.Fatalf("got %d %s", w.Code, w.Body)
	}
}

func TestDBAdminStatementsErrors(t *testing.T) {
	for _, tc := range []struct {
		err     error
		status  int
		code    string
		message string
	}{
		{api.ErrStatementsUnavailable, 503, "service_unavailable", "pg_stat_statements is not available: create the extension and add it to shared_preload_libraries"},
		{errors.New("PRIVATE_CANARY"), 500, "internal_server_error", "cannot read statement statistics"},
	} {
		w := serveStatements(t, stubStatements{err: tc.err})
		var body map[string]APIError
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Code != tc.status || body["error"].Code != tc.code || body["error"].Message != tc.message {
			t.Errorf("err %v: got %d %s", tc.err, w.Code, w.Body)
		}
	}
}

type stubPlanguard struct {
	last    api.PlanguardReport
	lastErr error
	fresh   api.PlanguardReport
	runErr  error
	runs    int
}

func (s *stubPlanguard) Last() (api.PlanguardReport, error) { return s.last, s.lastErr }

func (s *stubPlanguard) RunNow(context.Context) (api.PlanguardReport, error) {
	s.runs++
	return s.fresh, s.runErr
}

func servePlanguard(t *testing.T, pg PlanguardSource, target string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	DBAdminRouter(stubStatements{}, pg).ServeHTTP(w, httptest.NewRequest("GET", target, nil))
	return w
}

func errorBody(t *testing.T, w *httptest.ResponseRecorder) APIError {
	t.Helper()
	var body map[string]APIError
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("%v: %s", err, w.Body)
	}
	return body["error"]
}

func TestDBAdminPlanguard(t *testing.T) {
	report := api.PlanguardReport{
		Tier:       "prod",
		StartedAt:  time.Date(2026, 10, 19, 1, 2, 3, 0, time.UTC),
		DurationMs: 1234.5,
		Clean:      false,
		Counts:     map[string]int{"ok": 1, "MISSED": 1},
		Failures:   []api.PlanguardRun{{Entry: "b", Stmt: "S", Mode: "generic", Verdict: "MISSED", Failures: []string{"driver: x"}}},
		Runs: []api.PlanguardRun{
			{Entry: "a", Stmt: "S", Mode: "custom", Verdict: "ok", Drivers: []string{"known_routes:Index Scan:ix"}},
			{Entry: "b", Stmt: "S", Mode: "generic", Verdict: "MISSED", Failures: []string{"driver: x"}},
		},
	}

	t.Run("not run yet", func(t *testing.T) {
		pg := &stubPlanguard{lastErr: api.ErrPlanguardNotRun}
		w := servePlanguard(t, pg, "/planguard")
		if e := errorBody(t, w); w.Code != 404 || e.Code != "not_found" || e.Message != "planguard has not run yet" {
			t.Fatalf("got %d %s", w.Code, w.Body)
		}
		if pg.runs != 0 {
			t.Fatalf("RunNow called %d times", pg.runs)
		}
	})

	t.Run("last report", func(t *testing.T) {
		pg := &stubPlanguard{last: report}
		w := servePlanguard(t, pg, "/planguard")
		if w.Code != 200 || pg.runs != 0 {
			t.Fatalf("got %d (runs %d) %s", w.Code, pg.runs, w.Body)
		}
		var raw map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &raw); err != nil {
			t.Fatal(err)
		}
		for _, key := range []string{"tier", "started_at", "duration_ms", "clean", "counts", "failures", "runs"} {
			if _, ok := raw[key]; !ok {
				t.Errorf("report lacks %q: %s", key, w.Body)
			}
		}
		if raw["tier"] != "prod" || raw["clean"] != false || raw["started_at"] != "2026-10-19T01:02:03Z" {
			t.Errorf("report = %s", w.Body)
		}
		run := raw["runs"].([]any)[0].(map[string]any)
		for _, key := range []string{"entry", "stmt", "mode", "verdict", "drivers"} {
			if _, ok := run[key]; !ok {
				t.Errorf("run lacks %q: %v", key, run)
			}
		}
		if _, ok := run["failures"]; ok {
			t.Errorf("a run without failures carries the key: %v", run)
		}
	})

	t.Run("empty failures is an array", func(t *testing.T) {
		clean := api.PlanguardReport{Tier: "prod", Clean: true, Counts: map[string]int{}, Failures: []api.PlanguardRun{}, Runs: []api.PlanguardRun{}}
		w := servePlanguard(t, &stubPlanguard{last: clean}, "/planguard")
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"failures":[]`) {
			t.Fatalf("got %d %s", w.Code, w.Body)
		}
	})

	t.Run("fresh", func(t *testing.T) {
		fresh := report
		fresh.DurationMs = 99
		pg := &stubPlanguard{lastErr: api.ErrPlanguardNotRun, fresh: fresh}
		w := servePlanguard(t, pg, "/planguard?fresh=true")
		if w.Code != 200 || pg.runs != 1 || !strings.Contains(w.Body.String(), `"duration_ms":99`) {
			t.Fatalf("got %d (runs %d) %s", w.Code, pg.runs, w.Body)
		}
	})

	t.Run("fresh fails", func(t *testing.T) {
		pg := &stubPlanguard{last: report, runErr: errors.New("PRIVATE_CANARY")}
		w := servePlanguard(t, pg, "/planguard?fresh=true")
		if e := errorBody(t, w); w.Code != 500 || e.Message != "cannot run the plan check" || strings.Contains(w.Body.String(), "PRIVATE_CANARY") {
			t.Fatalf("got %d %s", w.Code, w.Body)
		}
	})

	t.Run("bad fresh", func(t *testing.T) {
		pg := &stubPlanguard{last: report}
		w := servePlanguard(t, pg, "/planguard?fresh=maybe")
		if w.Code != 400 || pg.runs != 0 {
			t.Fatalf("got %d (runs %d) %s", w.Code, pg.runs, w.Body)
		}
	})

	t.Run("statements still served", func(t *testing.T) {
		w := servePlanguard(t, &stubPlanguard{last: report}, "/statements")
		if w.Code != 200 || strings.TrimSpace(w.Body.String()) != `{"items":[]}` {
			t.Fatalf("got %d %s", w.Code, w.Body)
		}
	})

	t.Run("disabled", func(t *testing.T) {
		for _, target := range []string{"/planguard", "/planguard?fresh=true"} {
			pg := &stubPlanguard{lastErr: api.ErrPlanguardDisabled, runErr: api.ErrPlanguardDisabled}
			w := servePlanguard(t, pg, target)
			if e := errorBody(t, w); w.Code != 404 || e.Code != "not_found" || e.Message != "planguard daily check is disabled" {
				t.Fatalf("%s: got %d %s", target, w.Code, w.Body)
			}
		}
	})

	t.Run("no source", func(t *testing.T) {
		w := servePlanguard(t, nil, "/planguard?fresh=true")
		if w.Code != 404 {
			t.Fatalf("got %d %s", w.Code, w.Body)
		}
	})
}
