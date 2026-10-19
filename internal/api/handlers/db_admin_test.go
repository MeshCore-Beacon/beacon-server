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
	DBAdminRouter(reader).ServeHTTP(w, httptest.NewRequest("GET", "/statements", nil))
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
