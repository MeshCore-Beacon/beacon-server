// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"math"
	"reflect"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

type endpointCaptureDB struct {
	*stubDB
	node     api.ResolvedNode
	observed []InsertObservationParams
	lookups  int
}

func (s *endpointCaptureDB) GetNodeByPubkey(context.Context, []byte) (uuid.UUID, error) {
	return s.node.ID, nil
}
func (s *endpointCaptureDB) GetNodesByIDs(context.Context, []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error) {
	return map[uuid.UUID]*api.ResolvedNode{s.node.ID: &s.node}, nil
}
func (s *endpointCaptureDB) ResolvePathHashes(_ context.Context, iata string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	s.lookups++
	if iata != "YYZ" {
		return nil, nil
	}
	return map[string][]api.ResolvedPathEntry{
		"aa": {{NodeID: s.node.ID, Name: s.node.Name, PublicKey: []byte{0xaa}}, {NodeID: uuid.Nil, PublicKey: []byte{0xab}}},
	}, nil
}

// Share fixture results with the dedicated endpoint resolver when both PRs are combined.
func (s *endpointCaptureDB) ResolveEndpointHashes(ctx context.Context, iata string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	return s.ResolvePathHashes(ctx, iata, hashes)
}

func (s *endpointCaptureDB) InsertObservation(_ context.Context, observation InsertObservationParams) (bool, error) {
	s.observed = append(s.observed, observation)
	return true, nil
}

func TestHandlePacketCapturesEndpoints(t *testing.T) {
	name := "Companion 👋"
	for _, kind := range []string{"advert", "direct message", "unaddressed", "encoding failure"} {
		t.Run(kind, func(t *testing.T) {
			w, base := newTestWorker()
			db := &endpointCaptureDB{stubDB: base, node: api.ResolvedNode{ID: uuid.New(), Name: &name, PublicKey: "aa"}}
			w.db = db
			packet := buildAdvertPacket(t, false)
			want := api.PacketEndpointSnapshot{}
			switch kind {
			case "advert":
				hop := api.ResolveExactNode(&db.node)
				want.Source = &hop
			case "direct message":
				// Destination, source, MAC and a minimal ciphertext envelope.
				packet = &meshcore.Packet{Header: meshcore.MakeHeader(meshcore.RouteTypeFlood, meshcore.PayloadTypeTxtMsg, 0), Payload: append([]byte{0xbb, 0xaa, 0, 0}, make([]byte, 16)...)}
				resolved, _ := db.ResolvePathHashes(context.Background(), "YYZ", [][]byte{{0xaa}})
				source := api.BuildResolvedPath([][]byte{{0xaa}}, resolved)[0]
				destination := api.BuildResolvedPath([][]byte{{0xbb}}, nil)[0]
				want.Source, want.Destination = &source, &destination
				db.lookups = 0
			case "unaddressed":
				packet = buildTracePacket(t)
			case "encoding failure":
				nan := math.NaN()
				db.node.Latitude = &nan
			}
			w.handlePacket(context.Background(), "YYZ", hex.EncodeToString([]byte{1, 2}), packetEnvelope(t, packet))
			if len(db.observed) != 1 {
				t.Fatalf("observation was lost or written twice: %d", len(db.observed))
			}
			if kind == "encoding failure" {
				if db.observed[0].ResolvedEndpoints != nil {
					t.Fatal("failed optional enrichment must remain NULL")
				}
				return
			}
			var got api.PacketEndpointSnapshot
			if err := json.Unmarshal(db.observed[0].ResolvedEndpoints, &got); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("captured endpoints differ: got %+v, want %+v", got, want)
			}
			if kind == "direct message" && db.lookups != 2 {
				t.Fatalf("endpoint resolution was repeated: %d lookups", db.lookups)
			}
		})
	}
}
