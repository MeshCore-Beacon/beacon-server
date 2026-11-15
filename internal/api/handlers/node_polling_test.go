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
