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

const notAPoller = `{"claim":null,"readings24h":0,"targets":[]}`

func nodePollerGet(t *testing.T, sr stubReader, id string) *httptest.ResponseRecorder {
	t.Helper()
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/poller", getNodePoller(sr))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nodes/"+id+"/poller", nil))
	return w
}

func TestGetNodePoller_Shape(t *testing.T) {
	node := uuid.New()
	at := int64(1795799000000)
	name, rank := "Ridge", 0
	claimed, evidence := float32(9.5), float32(8.1)
	keyA, keyB := strings.Repeat("0a", 32), strings.Repeat("0b", 32)
	sr := stubReader{getNodePoller: func(context.Context, uuid.UUID) (*api.NodePoller, error) {
		return &api.NodePoller{
			Claim: &api.NodePollerClaim{ReceivedAt: 1795800000000, Live: true, AcceptsRequests: true, PollIntervalMinutes: new(60),
				Slots: api.NodePollerSlots{Max: 8, Used: 2, Own: 1, Requested: 1, Free: 3}},
			Readings24h: 212,
			Targets: []api.NodePollerTarget{
				{NodeID: &node, PublicKey: keyA, Name: &name, Role: "poller", Rank: &rank, State: "ok",
					LastAttempt: &at, LastOk: &at, ClaimedSnr: &claimed, EvidenceSnr: &evidence, EvidenceCount: 14, LatestReadingAt: &at},
				{PublicKey: keyB, Requested: true, Role: "unassigned", State: "login_failed", Misses: 3},
			},
		}, nil
	}}
	w := nodePollerGet(t, sr, uuid.NewString())
	want := `{"claim":{"receivedAt":1795800000000,"live":true,"acceptsRequests":true,"pollIntervalMinutes":60,` +
		`"slots":{"max":8,"used":2,"own":1,"requested":1,"free":3}},"readings24h":212,"targets":[` +
		`{"nodeId":"` + node.String() + `","publicKey":"` + keyA + `","name":"Ridge","requested":false,"role":"poller","rank":0,` +
		`"state":"ok","misses":0,"lastAttempt":1795799000000,"lastOk":1795799000000,"claimedSnr":9.5,"evidenceSnr":8.1,` +
		`"evidenceCount":14,"latestReadingAt":1795799000000},` +
		`{"nodeId":null,"publicKey":"` + keyB + `","name":null,"requested":true,"role":"unassigned","rank":null,` +
		`"state":"login_failed","misses":3,"lastAttempt":null,"lastOk":null,"claimedSnr":null,"evidenceSnr":null,` +
		`"evidenceCount":0,"latestReadingAt":null}]}`
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != want {
		t.Fatalf("%d %s\nwant %s", w.Code, w.Body, want)
	}
	if cc := w.Header().Get("Cache-Control"); cc != "" {
		t.Errorf("Cache-Control = %q, want none", cc)
	}
}

func TestGetNodePoller_NotAPoller(t *testing.T) {
	w := nodePollerGet(t, stubReader{}, uuid.NewString())
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != notAPoller {
		t.Fatalf("%d %s\nwant %s", w.Code, w.Body, notAPoller)
	}
}

func TestGetNodePoller_NilTargetsMarshalAsEmpty(t *testing.T) {
	sr := stubReader{getNodePoller: func(context.Context, uuid.UUID) (*api.NodePoller, error) {
		return &api.NodePoller{Claim: &api.NodePollerClaim{}}, nil
	}}
	if w := nodePollerGet(t, sr, uuid.NewString()); !strings.Contains(w.Body.String(), `"targets":[]`) {
		t.Errorf("body %s", w.Body)
	}
}

func TestGetNodePoller_Errors(t *testing.T) {
	notFound := stubReader{getNodePoller: func(context.Context, uuid.UUID) (*api.NodePoller, error) { return nil, pgx.ErrNoRows }}
	w := nodePollerGet(t, notFound, uuid.NewString())
	if w.Code != http.StatusNotFound || !strings.Contains(w.Body.String(), "node not found") {
		t.Errorf("unknown node: %d %s", w.Code, w.Body)
	}
	boom := stubReader{getNodePoller: func(context.Context, uuid.UUID) (*api.NodePoller, error) { return nil, errors.New("boom") }}
	if w := nodePollerGet(t, boom, uuid.NewString()); w.Code != http.StatusInternalServerError {
		t.Errorf("store error: %d", w.Code)
	}
	if w := nodePollerGet(t, stubReader{}, "nope"); w.Code != http.StatusBadRequest {
		t.Errorf("bad id: %d", w.Code)
	}
	nilResult := stubReader{getNodePoller: func(context.Context, uuid.UUID) (*api.NodePoller, error) { return nil, nil }}
	if w := nodePollerGet(t, nilResult, uuid.NewString()); w.Code != http.StatusNotFound {
		t.Errorf("nil result: %d, want 404", w.Code)
	}
}

// pollerOffRoutes serves the node router with polling disabled over a reader that has a claim for every node.
func pollerOffRoutes(known bool, reads *int) http.Handler {
	sr := stubReader{
		getNode: func(context.Context, uuid.UUID) (*api.Node, error) {
			if !known {
				return nil, nil
			}
			return &api.Node{}, nil
		},
		getNodePoller: func(context.Context, uuid.UUID) (*api.NodePoller, error) {
			*reads++
			return &api.NodePoller{Claim: &api.NodePollerClaim{Live: true}, Readings24h: 9,
				Targets: []api.NodePollerTarget{{PublicKey: strings.Repeat("0a", 32)}}}, nil
		},
	}
	m := chi.NewRouter()
	m.Mount("/nodes", NodesRouter(sr, false))
	return m
}

func TestNodePollerPollingOff(t *testing.T) {
	var reads int
	w := httptest.NewRecorder()
	pollerOffRoutes(true, &reads).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nodes/"+uuid.NewString()+"/poller", nil))
	if w.Code != http.StatusOK || strings.TrimSpace(w.Body.String()) != notAPoller {
		t.Errorf("%d %s\nwant %s", w.Code, w.Body, notAPoller)
	}

	w = httptest.NewRecorder()
	pollerOffRoutes(false, &reads).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nodes/"+uuid.NewString()+"/poller", nil))
	if w.Code != http.StatusNotFound {
		t.Errorf("unknown node: %d, want 404", w.Code)
	}
	w = httptest.NewRecorder()
	pollerOffRoutes(true, &reads).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nodes/nope/poller", nil))
	if w.Code != http.StatusBadRequest {
		t.Errorf("bad id: %d, want 400", w.Code)
	}
	if reads != 0 {
		t.Errorf("polling tables read %d times with polling off", reads)
	}
}

func TestNodePollerRoutePollingOn(t *testing.T) {
	sr := stubReader{getNodePoller: func(context.Context, uuid.UUID) (*api.NodePoller, error) {
		return &api.NodePoller{Readings24h: 4}, nil
	}}
	m := chi.NewRouter()
	m.Mount("/nodes", NodesRouter(sr, true))
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nodes/"+uuid.NewString()+"/poller", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"readings24h":4`) {
		t.Errorf("%d %s", w.Code, w.Body)
	}
}
