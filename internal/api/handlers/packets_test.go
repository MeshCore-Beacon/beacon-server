// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
)

func TestGetPacket_InvalidHex(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets/{packetHash}", getPacket(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets/nothex!!", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidPayloadType(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?payloadType=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidRouteType(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?routeType=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidUntil(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?until=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?cursor=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPacketsBackfill_MissingAfterID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets/backfill", listPacketsBackfill(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets/backfill", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPacketsBackfill_InvalidAfterID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets/backfill", listPacketsBackfill(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets/backfill?afterObservationId=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPacketsBackfill_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets/backfill", listPacketsBackfill(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets/backfill?afterObservationId=1&limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{
		listPackets: func(_ context.Context, _, _ []int16, _ []string, _ []string, _, _ time.Time, _ int64, _ int32) (api.Page[api.PacketSummary], error) {
			return api.Page[api.PacketSummary]{Items: []api.PacketSummary{{PacketHash: "deadbeef"}}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/packets", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestListPackets_PluralParams_PassedThrough(t *testing.T) {
	var gotPayloadTypes, gotRouteTypes []int16
	var gotScopes []string
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{
		listPackets: func(_ context.Context, payloadTypes, routeTypes []int16, _ []string, scopes []string, _, _ time.Time, _ int64, _ int32) (api.Page[api.PacketSummary], error) {
			gotPayloadTypes = payloadTypes
			gotRouteTypes = routeTypes
			gotScopes = scopes
			return api.Page[api.PacketSummary]{}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/packets?payloadTypes=2,4&routeTypes=0,1&scopes=%23bc,%23west", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if len(gotPayloadTypes) != 2 || gotPayloadTypes[0] != 2 || gotPayloadTypes[1] != 4 {
		t.Errorf("expected payloadTypes [2 4], got %v", gotPayloadTypes)
	}
	if len(gotRouteTypes) != 2 || gotRouteTypes[0] != 0 || gotRouteTypes[1] != 1 {
		t.Errorf("expected routeTypes [0 1], got %v", gotRouteTypes)
	}
	if len(gotScopes) != 2 || gotScopes[0] != "#bc" || gotScopes[1] != "#west" {
		t.Errorf("expected scopes [#bc #west], got %v", gotScopes)
	}
}

func TestListPackets_SingularParams_StillWork(t *testing.T) {
	var gotPayloadTypes, gotRouteTypes []int16
	var gotScopes []string
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{
		listPackets: func(_ context.Context, payloadTypes, routeTypes []int16, _ []string, scopes []string, _, _ time.Time, _ int64, _ int32) (api.Page[api.PacketSummary], error) {
			gotPayloadTypes = payloadTypes
			gotRouteTypes = routeTypes
			gotScopes = scopes
			return api.Page[api.PacketSummary]{}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/packets?payloadType=4&routeType=1&scope=%23bc", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if len(gotPayloadTypes) != 1 || gotPayloadTypes[0] != 4 {
		t.Errorf("expected payloadTypes [4], got %v", gotPayloadTypes)
	}
	if len(gotRouteTypes) != 1 || gotRouteTypes[0] != 1 {
		t.Errorf("expected routeTypes [1], got %v", gotRouteTypes)
	}
	if len(gotScopes) != 1 || gotScopes[0] != "#bc" {
		t.Errorf("expected scopes [#bc], got %v", gotScopes)
	}
}

func TestListPackets_PluralParams_TakePrecedenceOverSingular(t *testing.T) {
	// Mirrors parseIATAs' precedence: when both are present, the plural param wins outright
	// rather than merging with the singular one.
	var gotPayloadTypes []int16
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{
		listPackets: func(_ context.Context, payloadTypes, _ []int16, _ []string, _ []string, _, _ time.Time, _ int64, _ int32) (api.Page[api.PacketSummary], error) {
			gotPayloadTypes = payloadTypes
			return api.Page[api.PacketSummary]{}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/packets?payloadType=9&payloadTypes=2,4", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if len(gotPayloadTypes) != 2 || gotPayloadTypes[0] != 2 || gotPayloadTypes[1] != 4 {
		t.Errorf("expected payloadTypes [2 4] (plural wins), got %v", gotPayloadTypes)
	}
}

func TestListPackets_InvalidPayloadTypes(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?payloadTypes=2,notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListPackets_InvalidRouteTypes(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/packets?routeTypes=0,notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetPacket_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets/{packetHash}", getPacket(stubReader{
		getPacket: func(_ context.Context, hash []byte) (*api.Packet, error) {
			return &api.Packet{PacketHash: "deadbeef"}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/packets/deadbeef", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestListPacketsBackfill_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets/backfill", listPacketsBackfill(stubReader{
		listPacketsAfterID: func(_ context.Context, _ int64, _, _ int16, _ []string, _ string, _ int32) ([]api.PacketSummary, error) {
			return []api.PacketSummary{{PacketHash: "deadbeef"}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/packets/backfill?afterObservationId=1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestHashPrefixRange(t *testing.T) {
	tests := []struct {
		in     string
		lo, hi string
	}{
		{"6ddf62fb", "6ddf62fb", "6ddf62fc"},
		{"6DDF62FB", "6ddf62fb", "6ddf62fc"},
		{"6ddf6", "6ddf60", "6ddf70"},
		{"12ff", "12ff", "1300"},
		{"0012f", "0012f0", "001300"},
		{"ffff", "ffff", ""},
		{"fffff", "fffff0", ""},
		{"6ddf62fb5b9a969e", "6ddf62fb5b9a969e", "6ddf62fb5b9a969f"},
	}
	for _, tt := range tests {
		lo, hi, err := hashPrefixRange(tt.in)
		if err != nil {
			t.Fatalf("%s: %v", tt.in, err)
		}
		if hex.EncodeToString(lo) != tt.lo || hex.EncodeToString(hi) != tt.hi {
			t.Errorf("%s: got [%x, %x), want [%s, %s)", tt.in, lo, hi, tt.lo, tt.hi)
		}
		if tt.hi == "" && hi != nil {
			t.Errorf("%s: want nil hi", tt.in)
		}
	}
	for _, bad := range []string{"", "abc", "6ddg", "6ddf62fb5b9a969e0"} {
		if _, _, err := hashPrefixRange(bad); err == nil {
			t.Errorf("%q: expected error", bad)
		}
	}
}

func TestListPackets_HashPrefix(t *testing.T) {
	var gotLo, gotHi []byte
	var gotIATAs []string
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{
		listPackets: func(_ context.Context, _, _ []int16, _ []string, _ []string, _, _ time.Time, _ int64, _ int32) (api.Page[api.PacketSummary], error) {
			t.Error("ListPackets called for a hashPrefix search")
			return api.Page[api.PacketSummary]{}, nil
		},
		searchPacketsByHashPrefix: func(_ context.Context, lo, hi []byte, _, _ []int16, iatas []string, _ []string, _, _ time.Time, _ int64, _ int32) (api.Page[api.PacketSummary], error) {
			gotLo, gotHi, gotIATAs = lo, hi, iatas
			return api.Page[api.PacketSummary]{Items: []api.PacketSummary{{PacketHash: "6ddf62fb5b9a969e"}}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/packets?hashPrefix=6DDF62FB&iatas=yow", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	if hex.EncodeToString(gotLo) != "6ddf62fb" || hex.EncodeToString(gotHi) != "6ddf62fc" {
		t.Errorf("range = [%x, %x)", gotLo, gotHi)
	}
	if len(gotIATAs) != 1 || gotIATAs[0] != "YOW" {
		t.Errorf("iatas = %v", gotIATAs)
	}
}

func TestListPackets_HashPrefixInvalid(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/packets", listPackets(stubReader{}))
	for _, q := range []string{"zz12", "abc", "00112233445566778"} {
		req := httptest.NewRequest(http.MethodGet, "/packets?hashPrefix="+q, nil)
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Errorf("%s: expected 400, got %d", q, w.Code)
		}
	}
}
