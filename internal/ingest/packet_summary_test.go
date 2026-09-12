// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/meshcore-go/meshcore-go"
)

// The hub has no subscription acknowledgement. Wait for a probe to make it
// through before ingesting the test packet, rather than assuming a short sleep.
func waitForSummarySubscriber(t *testing.T, ctx context.Context, h *hub.Hub, client *hub.Client) {
	t.Helper()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		h.Broadcast(hub.Event{Type: hub.EventObserverStatus})
		select {
		case <-client.Send:
			return
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("hub subscription did not become ready")
		}
	}
}

func TestAdvertSummaryLive(t *testing.T) {
	for _, tc := range []struct {
		name            string
		hasName, tamper bool
		want            string
	}{
		{"MD00-Repeater", true, false, "MD00-Repeater"},
		{"Relay 📡", true, false, "Relay 📡"},
		{"R\xffP", true, false, "R\uFFFDP"},
		{"", true, false, ""},
		{"", false, false, ""},
		{"unverified", true, true, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Run("wire", func(t *testing.T) {
				w, base := newTestWorker()
				db := &frameCaptureDB{stubDB: base}
				w.db = db
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				go w.hub.Run()
				client := w.hub.NewClient()
				w.hub.AddScope(client, "summary", hub.Scope{Events: []hub.EventType{hub.EventPacketObservation, hub.EventObserverStatus}})

				waitForSummarySubscriber(t, ctx, w.hub, client)
				defer w.hub.Remove(client)
				pub, priv, err := ed25519.GenerateKey(nil)
				if err != nil {
					t.Fatal(err)
				}
				identity, err := meshcore.NewIdentityFromBytes(pub)
				if err != nil {
					t.Fatal(err)
				}
				flags := byte(meshcore.AdvertTypeRepeater)
				if tc.hasName {
					flags |= 0x80
				}
				advert := &meshcore.Advert{PublicKey: identity, Timestamp: 12345, RawAppData: append([]byte{flags}, []byte(tc.name)...)}
				advert.Sign(priv)
				if tc.tamper {
					advert.RawAppData[0] ^= 1
				}
				payload, err := advert.ToBytes()
				if err != nil {
					t.Fatal(err)
				}
				packet := &meshcore.Packet{Header: meshcore.MakeHeader(meshcore.RouteTypeFlood, meshcore.PayloadTypeAdvert, 0), Payload: payload}
				w.handlePacket(ctx, "YVR", "0102", packetEnvelope(t, packet))

				for {
					select {
					case event := <-client.Send:
						if event.Type != hub.EventPacketObservation {
							continue
						}
						for _, raw := range []json.RawMessage{event.Payload, event.PayloadResolved} {
							var got struct {
								Packet struct {
									Summary *string `json:"summary"`
								} `json:"packet"`
							}
							if err := json.Unmarshal(raw, &got); err != nil {
								t.Fatal(err)
							}
							if tc.want == "" {
								if got.Packet.Summary != nil {
									t.Fatalf("unexpected summary %q", *got.Packet.Summary)
								}
							} else if got.Packet.Summary == nil || *got.Packet.Summary != tc.want {
								t.Fatalf("live summary = %v, want %q", got.Packet.Summary, tc.want)
							}
						}
						return
					case <-ctx.Done():
						t.Fatal("packet observation event missing")
					}
				}
			})
		})
	}
}
