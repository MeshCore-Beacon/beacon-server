// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

type advertEndpointDB struct {
	*endpointCaptureDB
	found, duplicate bool
	lookups, upserts int
}

func (s *advertEndpointDB) GetNodeByPubkey(context.Context, []byte) (uuid.UUID, error) {
	if !s.found {
		return uuid.Nil, errors.New("node not found")
	}
	return s.node.ID, nil
}

func (s *advertEndpointDB) GetNodesByIDs(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error) {
	s.lookups++
	return s.endpointCaptureDB.GetNodesByIDs(ctx, ids)
}

func (s *advertEndpointDB) UpsertNode(_ context.Context, n UpsertNodeParams, _ RadioSettings) (uuid.UUID, error) {
	s.node.Name = &n.Name
	s.found = true
	s.upserts++
	return s.node.ID, nil
}

func (s *advertEndpointDB) InsertObservation(context.Context, InsertObservationParams) (bool, error) {
	return !s.duplicate, nil
}

func TestAdvertEndpointUsesUpdatedName(t *testing.T) {
	for _, existing := range []bool{false, true} {
		t.Run(map[bool]string{false: "first advert", true: "renamed node"}[existing], func(t *testing.T) {
			w, base := newTestWorker()
			old := "Previous repeater"
			store := &advertEndpointDB{endpointCaptureDB: &endpointCaptureDB{stubDB: base, node: api.ResolvedNode{ID: uuid.New(), Name: &old, PublicKey: "aa"}}, found: existing}
			w.db = store
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go w.hub.Run()
			client := w.hub.NewClient()
			w.hub.AddScope(client, "rename", hub.Scope{Events: []hub.EventType{hub.EventPacketObservation, hub.EventObserverStatus}})
			defer w.hub.Remove(client)
			waitForSummarySubscriber(t, ctx, w.hub, client)
			packet := buildAdvertPacketWithData(t, append([]byte{meshcore.AdvertTypeRepeater | meshcore.AdvertNameMask}, []byte("Renamed repeater")...), false)
			w.handlePacket(ctx, "YYZ", "0102", packetEnvelope(t, packet))
			for {
				select {
				case event := <-client.Send:
					if event.Type != hub.EventPacketObservation {
						continue
					}
					var got packetObservationEvent
					if err := json.Unmarshal(event.Payload, &got); err != nil {
						t.Fatal(err)
					}
					hop := got.Observation.ResolvedSource
					if hop == nil || len(hop.Nodes) != 1 || hop.Nodes[0].Name == nil || *hop.Nodes[0].Name != "Renamed repeater" {
						t.Fatalf("live source did not use the newly saved name: %+v", hop)
					}
					if store.upserts != 1 || hop.Nodes[0].ID != store.node.ID || hop.Confidence != "high" {
						t.Fatal("advert identity or confidence changed")
					}
					return
				case <-ctx.Done():
					t.Fatal("packet event missing")
				}
			}
		})
	}
}

func TestDuplicateAdvertSkipsLiveEndpointLookup(t *testing.T) {
	w, base := newTestWorker()
	store := &advertEndpointDB{endpointCaptureDB: &endpointCaptureDB{stubDB: base, node: api.ResolvedNode{ID: uuid.New(), PublicKey: "aa"}}, found: true, duplicate: true}
	w.db = store
	w.handlePacket(context.Background(), "YYZ", "0102", packetEnvelope(t, buildAdvertPacket(t, false)))
	if store.lookups != 0 || store.upserts != 0 {
		t.Fatalf("duplicate observation performed live-only work: lookups=%d upserts=%d", store.lookups, store.upserts)
	}
}
