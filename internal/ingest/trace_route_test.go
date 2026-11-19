// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/hex"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

type traceRouteDB struct {
	*stubDB
	routes [][][]byte
}

func (s *traceRouteDB) ResolvePathHashes(_ context.Context, _ string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	out := map[string][]api.ResolvedPathEntry{}
	for _, h := range hashes {
		out[hex.EncodeToString(h)] = []api.ResolvedPathEntry{{NodeID: uuid.UUID{h[0]}}}
	}
	return out, nil
}

func (s *traceRouteDB) UpsertKnownRoute(_ context.Context, _ []uuid.UUID, prefixes [][]byte, _ string, _ int32) error {
	s.routes = append(s.routes, prefixes)
	return nil
}

// A TRACE names the hops it asks for; only those that added an SNR byte carried it, so only they make a known route.
func TestTraceKnownRouteStopsAtTheLastHopReached(t *testing.T) {
	for _, tc := range []struct {
		name    string
		snr     []byte
		wantHex string // the hop prefixes of the stored route, "" for none
	}{
		{"nothing reached", nil, ""},
		{"one hop reached", []byte{8}, ""},
		{"two of three reached", []byte{8, 252}, "aabb"},
		{"all reached", []byte{8, 252, 4}, "aabbcc"},
		{"more SNR bytes than hops", []byte{8, 252, 4, 4, 4}, "aabbcc"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, base := newTestWorker()
			d := &traceRouteDB{stubDB: base}
			w.db = d
			payload, err := (&meshcore.Trace{Tag: 7, PathHashes: []byte{0xaa, 0xbb, 0xcc}}).ToBytes()
			if err != nil {
				t.Fatal(err)
			}
			pkt := &meshcore.Packet{Header: meshcore.MakeHeader(meshcore.RouteTypeDirect, meshcore.PayloadTypeTrace, 0),
				PathLength: uint8(len(tc.snr)), Path: tc.snr, Payload: payload}
			w.handlePacket(context.Background(), "YKF", "0102", packetEnvelope(t, pkt))
			got := ""
			for _, route := range d.routes {
				for _, prefix := range route {
					got += hex.EncodeToString(prefix)
				}
				got += " "
			}
			want := tc.wantHex
			if want != "" {
				want += " "
			}
			if got != want {
				t.Fatalf("known routes %q, want %q", got, want)
			}
		})
	}
}
