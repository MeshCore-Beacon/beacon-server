// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func nodePollingGet(t *testing.T, sr stubReader, id string) *httptest.ResponseRecorder {
	t.Helper()
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/polling", getNodePolling(sr))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nodes/"+id+"/polling", nil))
	return w
}

func TestGetNodePolling_Shape(t *testing.T) {
	poller, standby := uuid.New(), uuid.New()
	at := int64(1794702630000)
	sr := stubReader{getNodePolling: func(context.Context, uuid.UUID) (*api.NodePolling, error) {
		return &api.NodePolling{Polled: true, LatestReadingAt: &at,
			Poller:   &api.NodePollingObserver{ObserverID: poller, Name: "HOWL V4.2"},
			Standbys: []api.NodePollingObserver{{ObserverID: standby, Name: "b", Rank: 1}}}, nil
	}}
	w := nodePollingGet(t, sr, uuid.NewString())
	want := `{"polled":true,"latestReadingAt":1794702630000,"poller":{"observerId":"` + poller.String() +
		`","name":"HOWL V4.2"},"standbys":[{"observerId":"` + standby.String() + `","name":"b","rank":1}]}`
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != want {
		t.Fatalf("%d %s\nwant %s", w.Code, w.Body, want)
	}
}

func TestGetNodePolling_NobodyPolls(t *testing.T) {
	w := nodePollingGet(t, stubReader{}, uuid.NewString())
	want := `{"polled":false,"latestReadingAt":null,"poller":null,"standbys":[]}`
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != want {
		t.Fatalf("%d %s\nwant %s", w.Code, w.Body, want)
	}
}

func TestGetNodePolling_NilStandbysMarshalAsEmpty(t *testing.T) {
	sr := stubReader{getNodePolling: func(context.Context, uuid.UUID) (*api.NodePolling, error) {
		return &api.NodePolling{}, nil
	}}
	if w := nodePollingGet(t, sr, uuid.NewString()); !strings.Contains(w.Body.String(), `"standbys":[]`) {
		t.Errorf("body %s", w.Body)
	}
}

func TestGetNodePolling_Errors(t *testing.T) {
	notFound := stubReader{getNodePolling: func(context.Context, uuid.UUID) (*api.NodePolling, error) { return nil, pgx.ErrNoRows }}
	if w := nodePollingGet(t, notFound, uuid.NewString()); w.Code != http.StatusNotFound {
		t.Errorf("unknown node: %d", w.Code)
	}
	boom := stubReader{getNodePolling: func(context.Context, uuid.UUID) (*api.NodePolling, error) { return nil, errors.New("boom") }}
	if w := nodePollingGet(t, boom, uuid.NewString()); w.Code != http.StatusInternalServerError {
		t.Errorf("store error: %d", w.Code)
	}
	if w := nodePollingGet(t, stubReader{}, "nope"); w.Code != http.StatusBadRequest {
		t.Errorf("bad id: %d", w.Code)
	}
}

func TestGetNodePolling_NilResultIsNotFound(t *testing.T) {
	sr := stubReader{getNodePolling: func(context.Context, uuid.UUID) (*api.NodePolling, error) { return nil, nil }}
	if w := nodePollingGet(t, sr, uuid.NewString()); w.Code != http.StatusNotFound {
		t.Errorf("nil result: %d, want 404", w.Code)
	}
}

// pollingOffRoutes serves the node router with polling disabled over a reader that has data for
// every node, so any read of it would show in the answer.
func pollingOffRoutes(known bool, reads *int) http.Handler {
	at := int64(1794702630000)
	sr := stubReader{
		getNode: func(context.Context, uuid.UUID) (*api.Node, error) {
			if !known {
				return nil, nil
			}
			return &api.Node{}, nil
		},
		getNodeReadings: func(context.Context, uuid.UUID, time.Time, time.Time, bool) ([]api.NodeReading, bool, int, error) {
			*reads++
			return []api.NodeReading{{SampledAt: time.Now(), Trusted: true, BatteryMV: func() *int32 { v := int32(4100); return &v }()}}, true, 7, nil
		},
		getNodePolling: func(context.Context, uuid.UUID) (*api.NodePolling, error) {
			*reads++
			return &api.NodePolling{Polled: true, LatestReadingAt: &at, Poller: &api.NodePollingObserver{Name: "x"}}, nil
		},
	}
	m := chi.NewRouter()
	m.Mount("/nodes", NodesRouter(sr, false))
	return m
}

func TestNodeRoutesPollingOffAnswerUnpolled(t *testing.T) {
	var reads int
	h := pollingOffRoutes(true, &reads)
	id := uuid.NewString()
	for path, want := range map[string]string{
		"/nodes/" + id + "/telemetry?range=24h": `{"range":"24h","interval":"1h","polled":false,"untrustedReadings":0,"points":[]}`,
		"/nodes/" + id + "/polling":             `{"polled":false,"latestReadingAt":null,"poller":null,"standbys":[]}`,
	} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != want {
			t.Errorf("%s: %d %s\nwant %s", path, w.Code, w.Body, want)
		}
	}
	if reads != 0 {
		t.Errorf("polling tables read %d times with polling off", reads)
	}
}

func TestNodeRoutesPollingOffKeepErrors(t *testing.T) {
	var reads int
	for path, want := range map[string]int{
		"/nodes/" + uuid.NewString() + "/telemetry?range=9h": http.StatusBadRequest,
		"/nodes/nope/polling":                                http.StatusBadRequest,
	} {
		w := httptest.NewRecorder()
		pollingOffRoutes(true, &reads).ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != want {
			t.Errorf("%s: %d, want %d", path, w.Code, want)
		}
	}
	for _, path := range []string{"/telemetry", "/polling"} {
		w := httptest.NewRecorder()
		pollingOffRoutes(false, &reads).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nodes/"+uuid.NewString()+path, nil))
		if w.Code != http.StatusNotFound {
			t.Errorf("unknown node %s: %d, want 404", path, w.Code)
		}
	}
}
