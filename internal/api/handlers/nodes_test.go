// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

func TestGetNode_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}", getNode(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/not-a-uuid", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeObservations_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/observations", listNodeObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/bad/observations", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeObservations_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/observations", listNodeObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/observations?cursor=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeObservations_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/observations", listNodeObservations(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/00000000-0000-0000-0000-000000000001/observations?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidType(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?type=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?cursor=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidPubkey(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?pubkey=nothex!!", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidSupportsMultibytePaths(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?supportsMultibytePaths=notabool", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_InvalidSupportsMultibyteTraces(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?supportsMultibyteTraces=notabool", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_OK(t *testing.T) {
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{
		listNodes: func(_ context.Context, _ int16, _ []string, _, _ *bool, _ []byte, _, _, _ string, _ int64, _ int32, _ bool) (api.Page[api.NodeSummary], error) {
			return api.Page[api.NodeSummary]{Items: []api.NodeSummary{{ID: nodeID}}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/nodes", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestListNodes_NeighborsParam_PassedThrough(t *testing.T) {
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	neighborID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	var gotIncludeNeighbors bool
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{
		listNodes: func(_ context.Context, _ int16, _ []string, _, _ *bool, _ []byte, _, _, _ string, _ int64, _ int32, includeNeighbors bool) (api.Page[api.NodeSummary], error) {
			gotIncludeNeighbors = includeNeighbors
			return api.Page[api.NodeSummary]{Items: []api.NodeSummary{{ID: nodeID, NeighborIDs: []uuid.UUID{neighborID}}}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?neighbors=true", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if !gotIncludeNeighbors {
		t.Error("expected neighbors=true query param to be passed through as includeNeighbors=true")
	}
	if !strings.Contains(w.Body.String(), "neighborIds") {
		t.Errorf("expected response body to include neighborIds, got %s", w.Body.String())
	}
}

func TestListNodes_NeighborsParam_InvalidValue(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?neighbors=notabool", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_NeighborsParam_BareFlagMeansTrue(t *testing.T) {
	var gotIncludeNeighbors bool
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{
		listNodes: func(_ context.Context, _ int16, _ []string, _, _ *bool, _ []byte, _, _, _ string, _ int64, _ int32, includeNeighbors bool) (api.Page[api.NodeSummary], error) {
			gotIncludeNeighbors = includeNeighbors
			return api.Page[api.NodeSummary]{}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?neighbors", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if !gotIncludeNeighbors {
		t.Error("expected bare ?neighbors (no value) to be treated as true")
	}
}

func TestListNodes_PubkeyPrefixParam_PassedThrough(t *testing.T) {
	var gotPubkeyPrefix string
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{
		listNodes: func(_ context.Context, _ int16, _ []string, _, _ *bool, _ []byte, pubkeyPrefix, _, _ string, _ int64, _ int32, _ bool) (api.Page[api.NodeSummary], error) {
			gotPubkeyPrefix = pubkeyPrefix
			return api.Page[api.NodeSummary]{}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?pubkeyPrefix=AB12", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if gotPubkeyPrefix != "ab12" {
		t.Errorf("expected pubkeyPrefix to be lowercased and passed through, got %q", gotPubkeyPrefix)
	}
}

func TestListNodes_PubkeyPrefixParam_InvalidHex(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?pubkeyPrefix=zz", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodes_PubkeyPrefixParam_RejectsWildcards(t *testing.T) {
	// isHexString should reject ILIKE wildcard characters, since pubkeyPrefix is matched as
	// raw text rather than decoded to bytes -- % or _ getting through would let a caller
	// widen the match pattern beyond a literal prefix.
	r := chi.NewRouter()
	r.Get("/nodes", listNodes(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes?pubkeyPrefix=ab%25", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetNode_OK(t *testing.T) {
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}", getNode(stubReader{
		getNode: func(_ context.Context, id uuid.UUID) (*api.Node, error) {
			return &api.Node{NodeSummary: api.NodeSummary{ID: id}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/"+nodeID.String(), nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if strings.Contains(w.Body.String(), `"meshmapper"`) {
		t.Error("meshmapper should be omitted when MeshMapper doesn't list the node")
	}
}

func TestGetNode_MeshMapper(t *testing.T) {
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	hw, checked := "RAK4631", int64(1791100000000)
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}", getNode(stubReader{
		getNode: func(_ context.Context, id uuid.UUID) (*api.Node, error) {
			return &api.Node{NodeSummary: api.NodeSummary{ID: id}, MeshMapper: &api.MeshMapperRepeater{
				IATA: "YOW", FetchedAt: 1791234600000, Site: api.MeshMapperSite{Hardware: &hw}, Carpeater: true,
				Admins: []string{}, Scopes: []api.MeshMapperScope{}, ScopesCheckedAt: &checked,
				DefaultScope: &api.MeshMapperDefaultScope{State: "none"},
			}}, nil
		},
	}))
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/nodes/"+nodeID.String(), nil))
	want := `"meshmapper":{"iata":"YOW","fetchedAt":1791234600000,"site":{"hardware":"RAK4631"},"carpeater":true,` +
		`"admins":[],"scopes":[],"scopesCheckedAt":1791100000000,"defaultScope":{"state":"none"}}`
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), want) {
		t.Fatalf("status %d, body %s", w.Code, w.Body.String())
	}
}

func TestListNodeNeighbors_OK(t *testing.T) {
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/neighbors", listNodeNeighbors(stubReader{
		getNodeNeighbors: func(_ context.Context, _ uuid.UUID) ([]api.NodeNeighbor, error) {
			return []api.NodeNeighbor{{ID: nodeID}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/"+nodeID.String()+"/neighbors", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestListNodeNeighbors_InvalidUUID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/neighbors", listNodeNeighbors(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/not-a-uuid/neighbors", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListNodeObservations_OK(t *testing.T) {
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	r := chi.NewRouter()
	r.Get("/nodes/{nodeId}/observations", listNodeObservations(stubReader{
		listNodeObservations: func(_ context.Context, _ uuid.UUID, _ int64, _ int32) (api.Page[api.PacketObservationSummary], error) {
			return api.Page[api.PacketObservationSummary]{Items: []api.PacketObservationSummary{{ID: 1}}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/nodes/"+nodeID.String()+"/observations", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}
