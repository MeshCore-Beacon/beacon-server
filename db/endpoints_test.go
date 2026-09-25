// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"sort"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
	"go.uber.org/mock/gomock"
)

func TestParsePacketEndpoints(t *testing.T) {
	msg := append([]byte{0xbb, 0xaa, 0, 0}, make([]byte, 16)...)
	key := []byte{1, 2, 3}
	for _, tc := range []struct {
		name     string
		kind     uint8
		raw, key []byte
		want     packetEndpoints
	}{
		{"direct message", meshcore.PayloadTypeTxtMsg, msg, nil, packetEndpoints{source: []byte{0xaa}, destination: []byte{0xbb}}},
		{"request", meshcore.PayloadTypeReq, msg, nil, packetEndpoints{source: []byte{0xaa}, destination: []byte{0xbb}}},
		{"advert", meshcore.PayloadTypeAdvert, nil, key, packetEndpoints{advert: true, advertKey: key}},
		{"channel message", meshcore.PayloadTypeGrpTxt, msg, nil, packetEndpoints{}},
		{"truncated", meshcore.PayloadTypeTxtMsg, []byte{0xbb}, nil, packetEndpoints{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := parsePacketEndpoints(int16(tc.kind), tc.raw, tc.key)
			if got.advert != tc.want.advert || string(got.advertKey) != string(tc.want.advertKey) ||
				string(got.source) != string(tc.want.source) || string(got.destination) != string(tc.want.destination) {
				t.Fatalf("got %+v, want %+v", got, tc.want)
			}
		})
	}
}

func hopNames(h *api.ResolvedHop) []string {
	if h == nil {
		return nil
	}
	names := []string{h.Confidence}
	for _, n := range h.Nodes {
		names = append(names, *n.Name)
	}
	sort.Strings(names[1:])
	return names
}

func TestResolveEndpointsBatchesAPage(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	store := &Store{q: mock}
	name := func(s string) *string { return &s }
	alice, bob, carol := uuid.New(), uuid.New(), uuid.New()
	advertKey := []byte{0xaa, 0x11}

	mock.EXPECT().ResolveEndpointHashPairs(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, p sqlc.ResolveEndpointHashPairsParams) ([]sqlc.ResolveEndpointHashPairsRow, error) {
			if len(p.Iatas) != 2 || len(p.Hashes) != 2 {
				t.Errorf("want distinct IATAs and hashes, got %v %v", p.Iatas, p.Hashes)
			}
			return []sqlc.ResolveEndpointHashPairsRow{
				{Iata: "YYZ", Hash: []byte{0xaa}, NodeID: alice, Name: name("Alice")},
				{Iata: "YYZ", Hash: []byte{0xaa}, NodeID: bob, Name: name("Bob")},
				{Iata: "YYZ", Hash: []byte{0xbb}, NodeID: carol, Name: name("Carol")},
				{Iata: "YVR", Hash: []byte{0xaa}, NodeID: alice, Name: name("Alice")},
				{Iata: "YVR", Hash: []byte{0xbb}, NodeID: carol, Name: name("Carol")}, // cross-product extra
			}, nil
		})
	mock.EXPECT().GetNodesByPubkeys(gomock.Any(), [][]byte{advertKey}).Return(
		[]sqlc.GetNodesByPubkeysRow{{ID: alice, PublicKey: advertKey, Name: name("Alice")}}, nil)

	sources, destinations := store.resolveEndpoints(context.Background(), []endpointLookup{
		{iata: "YYZ", ep: packetEndpoints{source: []byte{0xaa}, destination: []byte{0xbb}}},
		{iata: "YVR", ep: packetEndpoints{source: []byte{0xaa}}},
		{iata: "YYZ", ep: packetEndpoints{advert: true, advertKey: advertKey}},
		{iata: "YYZ", ep: packetEndpoints{advert: true, advertKey: advertKey}},
		{iata: "YYZ", ep: packetEndpoints{advert: true}},
		{},
	})

	for i, tc := range []struct{ source, destination []string }{
		{[]string{"ambiguous", "Alice", "Bob"}, []string{"high", "Carol"}},
		{[]string{"high", "Alice"}, nil},
		{[]string{"high", "Alice"}, nil},
		{[]string{"high", "Alice"}, nil},
		{[]string{"none"}, nil},
		{nil, nil},
	} {
		if got := hopNames(sources[i]); !equalStrings(got, tc.source) {
			t.Errorf("row %d source = %v, want %v", i, got, tc.source)
		}
		if got := hopNames(destinations[i]); !equalStrings(got, tc.destination) {
			t.Errorf("row %d destination = %v, want %v", i, got, tc.destination)
		}
	}
}

func TestResolveEndpointsFailedLookupLeavesHopsEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	store := &Store{q: mock}
	mock.EXPECT().ResolveEndpointHashPairs(gomock.Any(), gomock.Any()).Return(nil, errors.New("boom"))

	sources, destinations := store.resolveEndpoints(context.Background(), []endpointLookup{
		{iata: "YYZ", ep: packetEndpoints{source: []byte{0xaa}, destination: []byte{0xbb}}},
	})
	if sources[0] != nil || destinations[0] != nil {
		t.Fatal("a failed lookup must not report endpoints as unresolved")
	}
}

func TestResolveEndpointsSkipsQueriesWhenNothingToResolve(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := &Store{q: mockdb.NewMockQuerier(ctrl)} // no EXPECT: any query fails the test
	sources, _ := store.resolveEndpoints(context.Background(), []endpointLookup{{}, {iata: "YYZ"}})
	if len(sources) != 2 || sources[0] != nil || sources[1] != nil {
		t.Fatal("rows without endpoints must stay empty")
	}
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
