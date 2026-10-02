// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

func TestListTraceTags_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/traces", listTraceTags(stubReader{
		listTraceTags: func(_ context.Context, _ []string, _, _ string, _, _ time.Time, _ time.Time, _ string, _ int32) ([]api.TraceTagSummary, error) {
			return []api.TraceTagSummary{{TraceTag: "trace-001"}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/traces", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestListTraceTags_CursorTag(t *testing.T) {
	var gotCursor time.Time
	var gotTag string
	r := chi.NewRouter()
	r.Get("/traces", listTraceTags(stubReader{
		listTraceTags: func(_ context.Context, _ []string, _, _ string, _, _ time.Time, cursor time.Time, cursorTag string, _ int32) ([]api.TraceTagSummary, error) {
			gotCursor, gotTag = cursor, cursorTag
			return nil, nil
		},
	}))
	for _, tc := range []struct {
		query string
		code  int
		tag   string
	}{
		{"cursor=1700000000000&cursorTag=A1B2C3D4", http.StatusOK, "a1b2c3d4"},
		{"cursor=1700000000000", http.StatusOK, ""},
		{"cursor=1700000000000&cursorTag=zz", http.StatusBadRequest, ""},
		{"cursorTag=a1b2c3d4", http.StatusBadRequest, ""},
	} {
		gotTag = ""
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/traces?"+tc.query, nil))
		if w.Code != tc.code {
			t.Errorf("%s: code %d, want %d", tc.query, w.Code, tc.code)
			continue
		}
		if tc.code == http.StatusOK && (gotTag != tc.tag || gotCursor.UnixMilli() != 1700000000000) {
			t.Errorf("%s: cursor %v tag %q", tc.query, gotCursor, gotTag)
		}
	}
}

func TestGetTrace_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/traces/{tag}", getTrace(stubReader{
		getTraceByTag: func(_ context.Context, tag string) (*api.TraceDetail, error) {
			return &api.TraceDetail{TraceTag: tag}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/traces/trace-001", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetTrace_NotFound(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/traces/{tag}", getTrace(stubReader{
		getTraceByTag: func(_ context.Context, _ string) (*api.TraceDetail, error) {
			return nil, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/traces/missing", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Errorf("expected 404, got %d", w.Code)
	}
}
