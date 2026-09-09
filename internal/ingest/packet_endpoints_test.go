// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

type endpointCaptureDB struct {
	*stubDB
	node        api.ResolvedNode
	observed    []InsertObservationParams
	lookups     int
	pathLookups int
	missingNode bool
}

func (s *endpointCaptureDB) GetNodeByPubkey(context.Context, []byte) (uuid.UUID, error) {
	if s.missingNode {
		return uuid.Nil, errors.New("node not yet advertised")
	}
	return s.node.ID, nil
}

func (s *endpointCaptureDB) UpsertNode(context.Context, UpsertNodeParams, RadioSettings) (uuid.UUID, error) {
	s.missingNode = false
	return s.node.ID, nil
}
func (s *endpointCaptureDB) GetNodesByIDs(context.Context, []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error) {
	return map[uuid.UUID]*api.ResolvedNode{s.node.ID: &s.node}, nil
}
func (s *endpointCaptureDB) ResolvePathHashes(_ context.Context, _ string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	if len(hashes) > 0 {
		s.pathLookups++
	}
	return nil, nil // a companion must not be found by the relay-only resolver
}

func (s *endpointCaptureDB) ResolveEndpointHashes(_ context.Context, iata string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	s.lookups++
	if iata != "YYZ" || s.missingNode {
		return nil, nil
	}
	return map[string][]api.ResolvedPathEntry{
		"aa": {{NodeID: s.node.ID, Name: s.node.Name, PublicKey: []byte{0xaa}}, {NodeID: uuid.Nil, PublicKey: []byte{0xab}}},
	}, nil
}

func (s *endpointCaptureDB) InsertObservation(_ context.Context, observation InsertObservationParams) (bool, error) {
	s.observed = append(s.observed, observation)
	return true, nil
}

func TestHandlePacketCapturesEndpoints(t *testing.T) {
	name := "Companion 👋"
	for _, kind := range []string{"advert", "first advert", "direct message", "unresolved direct", "unaddressed", "encoding failure"} {
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
			case "first advert":
				db.missingNode = true
			case "direct message", "unresolved direct":
				// Destination, source, MAC and a minimal ciphertext envelope.
				packet = &meshcore.Packet{Header: meshcore.MakeHeader(meshcore.RouteTypeFlood, meshcore.PayloadTypeTxtMsg, 0), Payload: append([]byte{0xbb, 0xaa, 0, 0}, make([]byte, 16)...)}
				db.missingNode = kind == "unresolved direct"
				resolved, _ := db.ResolveEndpointHashes(context.Background(), "YYZ", [][]byte{{0xaa}})
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
			if kind == "first advert" || kind == "unresolved direct" || kind == "unaddressed" || kind == "encoding failure" {
				if db.observed[0].ResolvedEndpoints != nil {
					t.Fatal("empty or failed endpoint resolution must remain SQL NULL")
				}
				if kind == "first advert" && db.missingNode {
					t.Fatal("advert side effects did not make the node available for a later lookup")
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
			if kind == "direct message" && (db.lookups != 2 || db.pathLookups != 0) {
				t.Fatalf("wrong resolver or repeated work: endpoint=%d relay=%d", db.lookups, db.pathLookups)
			}
		})
	}
}
