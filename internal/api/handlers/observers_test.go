// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestGetObserverTelemetry_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/telemetry", getObserverTelemetry(stubReader{}))

	req := httptest.NewRequest(http.MethodGet, "/observers/not-a-uuid/telemetry", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetObserverTelemetry_InvalidRange(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/telemetry", getObserverTelemetry(stubReader{}))

	req := httptest.NewRequest(http.MethodGet, "/observers/00000000-0000-0000-0000-000000000001/telemetry?range=banana", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetObserverTelemetry_InvalidAfterID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/telemetry", getObserverTelemetry(stubReader{}))

	req := httptest.NewRequest(http.MethodGet, "/observers/00000000-0000-0000-0000-000000000001/telemetry?afterId=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetObserverTelemetry_InvalidInterval(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/telemetry", getObserverTelemetry(stubReader{}))

	req := httptest.NewRequest(http.MethodGet, "/observers/00000000-0000-0000-0000-000000000001/telemetry?interval=2h", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListObservers_OK(t *testing.T) {
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/observers", listObservers(stubReader{
		listObservers: func(_ context.Context, _ []string, _, _, _, _, _ string, _ int64, _ int32) (api.Page[api.ObserverSummary], error) {
			return api.Page[api.ObserverSummary]{Items: []api.ObserverSummary{{ID: observerID}}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/observers", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetObserver_OK(t *testing.T) {
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/observers/{observerId}", getObserver(stubReader{
		getObserver: func(_ context.Context, id uuid.UUID) (*api.Observer, error) {
			return &api.Observer{ObserverSummary: api.ObserverSummary{ID: id}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/observers/"+observerID.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetObserver_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/observers/{observerId}", getObserver(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/observers/not-a-uuid", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetObserverTelemetry_OK(t *testing.T) {
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/telemetry", getObserverTelemetry(stubReader{
		getObserverTelemetry: func(_ context.Context, _ uuid.UUID, _, _ time.Time, _ int64) (*api.ObserverTelemetry, error) {
			return &api.ObserverTelemetry{Points: []api.ObserverTelemetryPoint{}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/observers/"+observerID.String()+"/telemetry", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetObserverTelemetry_Bucketed_OK(t *testing.T) {
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/telemetry", getObserverTelemetry(stubReader{
		getObserverTelemetryBucketed: func(_ context.Context, _ uuid.UUID, _, _ time.Time, _ int32) ([]api.ObserverTelemetryPoint, error) {
			return []api.ObserverTelemetryPoint{}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/observers/"+observerID.String()+"/telemetry?interval=6h", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestGetObserverTelemetry_Bucketed_StoreError(t *testing.T) {
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/telemetry", getObserverTelemetry(stubReader{
		getObserverTelemetryBucketed: func(_ context.Context, _ uuid.UUID, _, _ time.Time, _ int32) ([]api.ObserverTelemetryPoint, error) {
			return nil, errors.New("boom")
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/observers/"+observerID.String()+"/telemetry?interval=6h", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusInternalServerError {
		t.Fatalf("expected 500, got %d", w.Code)
	}
	var body map[string]APIError
	if err := json.NewDecoder(w.Body).Decode(&body); err != nil {
		t.Fatalf("expected JSON error body, got %q: %v", w.Body.String(), err)
	}
	if body["error"].Code != "internal_server_error" {
		t.Errorf("expected internal_server_error, got %q", body["error"].Code)
	}
}

func TestListObserverAdverts_OK(t *testing.T) {
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/adverts", listObserverAdverts(stubReader{
		listObserverAdverts: func(_ context.Context, _ uuid.UUID, _ int64, _ int32) (api.Page[api.AdvertObservation], error) {
			return api.Page[api.AdvertObservation]{Items: []api.AdvertObservation{}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/observers/"+observerID.String()+"/adverts", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestListObserverAdverts_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/adverts", listObserverAdverts(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/observers/not-a-uuid/adverts", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetObserverActivity_Defaults(t *testing.T) {
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	var gotWindow, gotInterval time.Duration
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/activity", getObserverActivity(stubReader{
		getObserverActivity: func(_ context.Context, _ uuid.UUID, window, interval time.Duration) (*api.ObserverActivity, error) {
			gotWindow, gotInterval = window, interval
			return &api.ObserverActivity{Points: []api.ObserverActivityPoint{}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/observers/"+observerID.String()+"/activity", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if gotWindow != 24*time.Hour {
		t.Errorf("expected window 24h, got %s", gotWindow)
	}
	if gotInterval != 15*time.Minute {
		t.Errorf("expected interval 15m, got %s", gotInterval)
	}
	var body api.ObserverActivity
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Range != "24h" || body.Interval != "15m" {
		t.Errorf("expected range 24h interval 15m, got %q/%q", body.Range, body.Interval)
	}
}

func TestGetObserverActivity_CustomRangeAndInterval(t *testing.T) {
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	var gotWindow, gotInterval time.Duration
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/activity", getObserverActivity(stubReader{
		getObserverActivity: func(_ context.Context, _ uuid.UUID, window, interval time.Duration) (*api.ObserverActivity, error) {
			gotWindow, gotInterval = window, interval
			return &api.ObserverActivity{Points: []api.ObserverActivityPoint{}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/observers/"+observerID.String()+"/activity?range=168h&interval=1h", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if gotWindow != 168*time.Hour {
		t.Errorf("expected window 168h, got %s", gotWindow)
	}
	if gotInterval != time.Hour {
		t.Errorf("expected interval 1h, got %s", gotInterval)
	}
	var body api.ObserverActivity
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Range != "168h" || body.Interval != "1h" {
		t.Errorf("expected range 168h interval 1h, got %q/%q", body.Range, body.Interval)
	}
}

func TestGetObserverActivity_SubHourRangeLimit(t *testing.T) {
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	r := chi.NewRouter()
	r.Get("/observers/{observerId}/activity", getObserverActivity(stubReader{
		getObserverActivity: func(_ context.Context, _ uuid.UUID, _, _ time.Duration) (*api.ObserverActivity, error) {
			t.Fatal("reader should not be called")
			return nil, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/observers/"+observerID.String()+"/activity?range=168h&interval=15m", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d (%s)", w.Code, w.Body.String())
	}
	var errBody map[string]APIError
	if err := json.Unmarshal(w.Body.Bytes(), &errBody); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if errBody["error"].Message != "range must be 48h or less for intervals under 1h" {
		t.Errorf("unexpected error message %q", errBody["error"].Message)
	}

	var gotWindow, gotInterval time.Duration
	ok := chi.NewRouter()
	ok.Get("/observers/{observerId}/activity", getObserverActivity(stubReader{
		getObserverActivity: func(_ context.Context, _ uuid.UUID, window, interval time.Duration) (*api.ObserverActivity, error) {
			gotWindow, gotInterval = window, interval
			return &api.ObserverActivity{Points: []api.ObserverActivityPoint{}}, nil
		},
	}))
	req = httptest.NewRequest(http.MethodGet, "/observers/"+observerID.String()+"/activity?range=48h&interval=15m", nil)
	w = httptest.NewRecorder()
	ok.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 at 48h/15m, got %d (%s)", w.Code, w.Body.String())
	}
	if gotWindow != 48*time.Hour || gotInterval != 15*time.Minute {
		t.Errorf("expected 48h/15m, got %s/%s", gotWindow, gotInterval)
	}
}

func TestGetObserverActivity_BadRequests(t *testing.T) {
	tests := []struct {
		name  string
		path  string
		query string
	}{
		{"invalid uuid", "not-a-uuid", ""},
		{"unparseable range", "00000000-0000-0000-0000-000000000001", "?range=banana"},
		{"range too long", "00000000-0000-0000-0000-000000000001", "?range=721h"},
		{"zero range", "00000000-0000-0000-0000-000000000001", "?range=0"},
		{"invalid interval", "00000000-0000-0000-0000-000000000001", "?interval=2h"},
		{"too many buckets", "00000000-0000-0000-0000-000000000001", "?range=720h&interval=5m"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := chi.NewRouter()
			r.Get("/observers/{observerId}/activity", getObserverActivity(stubReader{
				getObserverActivity: func(_ context.Context, _ uuid.UUID, _, _ time.Duration) (*api.ObserverActivity, error) {
					t.Fatal("reader should not be called")
					return nil, nil
				},
			}))
			req := httptest.NewRequest(http.MethodGet, "/observers/"+tt.path+"/activity"+tt.query, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != http.StatusBadRequest {
				t.Errorf("expected 400, got %d (%s)", w.Code, w.Body.String())
			}
		})
	}
}

func TestGetObserverActivity_NotFound(t *testing.T) {
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/activity", getObserverActivity(stubReader{
		getObserverActivity: func(_ context.Context, _ uuid.UUID, _, _ time.Duration) (*api.ObserverActivity, error) {
			return nil, pgx.ErrNoRows
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/observers/"+observerID.String()+"/activity", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
	var body struct {
		Error APIError `json:"error"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Error.Code != "not_found" {
		t.Errorf("expected code not_found, got %q", body.Error.Code)
	}
}

func TestGetObserverActivity_ReaderError(t *testing.T) {
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/activity", getObserverActivity(stubReader{
		getObserverActivity: func(_ context.Context, _ uuid.UUID, _, _ time.Duration) (*api.ObserverActivity, error) {
			return nil, errors.New("boom")
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/observers/"+observerID.String()+"/activity", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("expected 500, got %d", w.Code)
	}
}

func TestGetObserverActivity_NullsSerialise(t *testing.T) {
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/observers/{observerId}/activity", getObserverActivity(stubReader{
		getObserverActivity: func(_ context.Context, _ uuid.UUID, _, _ time.Duration) (*api.ObserverActivity, error) {
			return &api.ObserverActivity{
				PayloadTypes: []api.PayloadBreakdownItem{},
				Points:       []api.ObserverActivityPoint{{T: 1757376000000, Observations: 3}},
			}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/observers/"+observerID.String()+"/activity", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	radio, ok := body["radio"]
	if !ok {
		t.Fatal("expected radio key in body")
	}
	if radio != nil {
		t.Errorf("expected radio null, got %v", radio)
	}
	points, ok := body["points"].([]any)
	if !ok || len(points) != 1 {
		t.Fatalf("expected 1 point, got %v", body["points"])
	}
	point := points[0].(map[string]any)
	airtime, ok := point["airtimeMs"]
	if !ok {
		t.Fatal("expected airtimeMs key in point")
	}
	if airtime != nil {
		t.Errorf("expected airtimeMs null, got %v", airtime)
	}
}
