// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/MeshCore-Beacon/beacon-server/internal/keystore"
	"github.com/meshcore-go/meshcore-go"
)

type scopeMessageDB struct {
	*stubDB
	next *InsertedChannelMessage
}

func (s *scopeMessageDB) InsertChannelMessage(context.Context, InsertChannelMessageParams) (*InsertedChannelMessage, error) {
	value := s.next
	s.next = nil
	return value, nil
}

func TestChannelMessageScopeLive(t *testing.T) {
	for _, status := range []string{"matched", "unscoped", "unknown", "unavailable"} {
		t.Run(status, func(t *testing.T) {
			w, base := newTestWorker()
			stored := &InsertedChannelMessage{ID: 42, ScopeStatus: api.ChannelScopeStatus(status)}
			name := "#stored"
			if status == "matched" {
				stored.Scope = &name
			}
			w.db = &scopeMessageDB{stubDB: base, next: stored}
			psk := make([]byte, 16)
			w.keys = &mapKeys{entries: map[byte][]keystore.Entry{0x11: {{Key: psk, Fingerprint: []byte{1}, Name: "fixture"}}}}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			go w.hub.Run()
			client := w.hub.NewClient()
			defer w.hub.Remove(client)
			w.hub.AddScope(client, "scope", hub.Scope{Events: []hub.EventType{hub.EventChannelMessage, hub.EventObserverStatus}})
			waitForSummarySubscriber(t, ctx, w.hub, client)
			packet := &meshcore.Packet{Header: meshcore.MakeHeader(meshcore.RouteTypeTransportFlood, meshcore.PayloadTypeGrpTxt, 0), Payload: encryptedGroupText(t, 0x11, psk, "Fixture", "test")}
			latest := "#different-later-reception"
			w.handlePayloadTypeSideEffects(ctx, packet, "YOW", []byte{1}, RadioSettings{}, nil, &latest, nil, 0)
			select {
			case event := <-client.Send:
				var got channelMessageEvent
				if err := json.Unmarshal(event.Payload, &got); err != nil {
					t.Fatal(err)
				}
				if got.ID != 42 || got.ScopeStatus != stored.ScopeStatus || (got.Scope == nil) != (stored.Scope == nil) || (got.Scope != nil && *got.Scope != name) {
					t.Fatal("live scope did not use stored evidence", got)
				}
			case <-ctx.Done():
				t.Fatal("no live message")
			}
			w.handlePayloadTypeSideEffects(ctx, packet, "YOW", []byte{1}, RadioSettings{}, nil, &latest, nil, 0)
			w.hub.Broadcast(hub.Event{Type: hub.EventObserverStatus})
			select {
			case event := <-client.Send:
				if event.Type == hub.EventChannelMessage {
					t.Fatal("duplicate broadcast")
				}
			case <-ctx.Done():
				t.Fatal("no barrier")
			}
		})
	}
}
