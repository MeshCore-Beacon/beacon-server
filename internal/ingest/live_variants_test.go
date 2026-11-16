// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
)

func TestPacketVariantsMatchExactDemand(t *testing.T) {
	for _, tc := range []struct {
		name string
		opts []hub.ClientOptions
		want [3]bool // resolved, key only, both
	}{
		{"plain", []hub.ClientOptions{{}}, [3]bool{}},
		{"resolved", []hub.ClientOptions{{ResolvePath: true}}, [3]bool{true, false, false}},
		{"key only", []hub.ClientOptions{{IncludeObserverKey: true}}, [3]bool{false, true, false}},
		{"both", []hub.ClientOptions{{ResolvePath: true, IncludeObserverKey: true}}, [3]bool{false, false, true}},
		{"separate clients", []hub.ClientOptions{{ResolvePath: true}, {IncludeObserverKey: true}}, [3]bool{true, true, false}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w, _ := newTestWorker()
			go w.hub.Run()
			// A plain witness must never inherit another client's observer key.
			opts := append([]hub.ClientOptions{{}}, tc.opts...)
			clients := make([]*hub.Client, len(opts))
			for i, option := range opts {
				clients[i] = w.hub.NewClient()
				defer w.hub.Remove(clients[i])
				w.hub.AddScope(clients[i], "variant", hub.Scope{Events: []hub.EventType{hub.EventPacketObservation}})
				option.IncludeRepeats = i == len(opts)-1
				w.hub.Configure(clients[i], option)
			}
			deadline := time.Now().Add(time.Second)
			for !w.hub.RepeatsWanted() {
				if time.Now().After(deadline) {
					t.Fatal("hub did not configure clients")
				}
				time.Sleep(time.Millisecond)
			}
			evt, hashes, resolved := livePathFixture(3)
			key := strings.Repeat("ab", 32)
			w.broadcastPacketObservation("YKF", 4, evt, hashes, resolved, key, false)
			for i, client := range clients {
				select {
				case event := <-client.Send:
					got := [3]bool{event.PayloadResolved != nil, event.PayloadWithKey != nil, event.PayloadResolvedWithKey != nil}
					if got != tc.want {
						t.Errorf("constructed variants = %v, want %v", got, tc.want)
					}
					var payload packetObservationEvent
					if err := json.Unmarshal(event.Payload, &payload); err != nil {
						t.Fatal(err)
					}
					if opts[i].ResolvePath != (len(payload.Observation.ResolvedPath) == 3) {
						t.Errorf("client %d received wrong path variant", i)
					}
					if opts[i].IncludeObserverKey != (payload.Observation.ObserverPublicKey == key) {
						t.Errorf("client %d received wrong observer key variant", i)
					}
				case <-time.After(time.Second):
					t.Fatal("packet event did not arrive")
				}
			}
		})
	}
}

// Summary tests inspect both wire variants, so explicitly request the optional one.
func requireResolvedVariant(t *testing.T, h *hub.Hub) {
	t.Helper()
	c := h.NewClient()
	t.Cleanup(func() { h.Remove(c) })
	h.Configure(c, hub.ClientOptions{ResolvePath: true})
	deadline := time.Now().Add(time.Second)
	for {
		resolved, _, _ := h.PacketVariantsWanted()
		if resolved {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("resolved variant opt-in was not applied")
		}
		time.Sleep(time.Millisecond)
	}
}
