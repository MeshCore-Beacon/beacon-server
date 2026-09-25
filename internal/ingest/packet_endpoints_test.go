// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

type endpointCaptureDB struct {
	*stubDB
	node     api.ResolvedNode
	observed int
}

func (s *endpointCaptureDB) GetNodeByPubkey(context.Context, []byte) (uuid.UUID, error) {
	return s.node.ID, nil
}
func (s *endpointCaptureDB) UpsertNode(context.Context, UpsertNodeParams, RadioSettings) (uuid.UUID, error) {
	return s.node.ID, nil
}
func (s *endpointCaptureDB) GetNodesByIDs(context.Context, []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error) {
	return map[uuid.UUID]*api.ResolvedNode{s.node.ID: &s.node}, nil
}
func (s *endpointCaptureDB) ResolveEndpointHashes(_ context.Context, _ string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	if len(hashes) == 1 && hashes[0][0] == 0xaa {
		return map[string][]api.ResolvedPathEntry{"aa": {{NodeID: s.node.ID, Name: s.node.Name, PublicKey: []byte{0xaa}}}}, nil
	}
	return nil, nil
}
func (s *endpointCaptureDB) InsertObservation(context.Context, InsertObservationParams) (bool, error) {
	s.observed++
	return true, nil
}

// Stored rows resolve endpoints at read time, but live events still carry them.
func TestHandlePacketBroadcastsEndpoints(t *testing.T) {
	name := "Companion"
	for _, kind := range []string{"advert", "direct message"} {
		t.Run(kind, func(t *testing.T) {
			w, base := newTestWorker()
			db := &endpointCaptureDB{stubDB: base, node: api.ResolvedNode{ID: uuid.New(), Name: &name, PublicKey: "aa"}}
			w.db = db
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go w.hub.Run()
			client := w.hub.NewClient()
			w.hub.AddScope(client, "endpoints", hub.Scope{Events: []hub.EventType{hub.EventPacketObservation, hub.EventObserverStatus}})
			defer w.hub.Remove(client)
			waitForSummarySubscriber(t, ctx, w.hub, client)

			packet := buildAdvertPacket(t, false)
			wantSource := api.ResolveExactNode(&db.node)
			var wantDestination *api.ResolvedHop
			if kind == "direct message" {
				packet = &meshcore.Packet{Header: meshcore.MakeHeader(meshcore.RouteTypeFlood, meshcore.PayloadTypeTxtMsg, 0), Payload: append([]byte{0xbb, 0xaa, 0, 0}, make([]byte, 16)...)}
				wantSource = api.BuildResolvedPath([][]byte{{0xaa}}, map[string][]api.ResolvedPathEntry{"aa": {{NodeID: db.node.ID, Name: &name, PublicKey: []byte{0xaa}}}})[0]
				d := api.BuildResolvedPath([][]byte{{0xbb}}, nil)[0]
				wantDestination = &d
			}
			w.handlePacket(ctx, "YYZ", "0102", packetEnvelope(t, packet))
			if db.observed != 1 {
				t.Fatalf("observation written %d times", db.observed)
			}
			for {
				select {
				case event := <-client.Send:
					if event.Type != hub.EventPacketObservation {
						continue
					}
					var got struct {
						Observation struct {
							ResolvedSource      *api.ResolvedHop `json:"resolvedSource"`
							ResolvedDestination *api.ResolvedHop `json:"resolvedDestination"`
						} `json:"observation"`
					}
					if err := json.Unmarshal(event.Payload, &got); err != nil {
						t.Fatal(err)
					}
					if !reflect.DeepEqual(got.Observation.ResolvedSource, &wantSource) || !reflect.DeepEqual(got.Observation.ResolvedDestination, wantDestination) {
						t.Fatalf("live endpoints: got %+v / %+v", got.Observation.ResolvedSource, got.Observation.ResolvedDestination)
					}
					return
				case <-ctx.Done():
					t.Fatal("packet observation event missing")
				}
			}
		})
	}
}
