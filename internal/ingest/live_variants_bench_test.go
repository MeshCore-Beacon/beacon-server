// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/google/uuid"
)

func livePathFixture(hops int) (packetObservationEvent, [][]byte, map[string][]api.ResolvedPathEntry) {
	var evt packetObservationEvent
	evt.PacketHash = strings.Repeat("01", 32)
	evt.Packet.PayloadType, evt.Packet.PayloadTypeName = 4, "ADVERT"
	evt.Packet.RouteType, evt.Packet.RouteTypeName = 1, "FLOOD"
	evt.Observation.ObserverID = uuid.NewString()
	evt.Observation.ObserverName, evt.Observation.IATA = "Fixture observer", "YKF"
	evt.Observation.HeardAt = 1794844800000
	evt.Observation.PathLength.HopCount = uint8(hops)
	evt.Observation.PathLength.HashSize = 3
	hashes := make([][]byte, hops)
	resolved := make(map[string][]api.ResolvedPathEntry, hops)
	for i := range hashes {
		hashes[i] = []byte{byte(i), 1, 2}
		name, lat, lon := fmt.Sprintf("Fixture repeater %d", i), 43.4, -80.3
		resolved[hex.EncodeToString(hashes[i])] = []api.ResolvedPathEntry{{NodeID: uuid.New(), Name: &name, Latitude: &lat, Longitude: &lon, PublicKey: []byte(strings.Repeat("x", 32))}}
	}
	return evt, hashes, resolved
}

// BenchmarkPacketObservationVariants includes the live-only path shaping and
// serialization, plus one hub delivery. No DB, MQTT, RF or network is involved.
func BenchmarkPacketObservationVariants(b *testing.B) {
	for _, hops := range []int{3, 12} {
		for _, mode := range []struct {
			name string
			opts hub.ClientOptions
		}{
			{"plain", hub.ClientOptions{}},
			{"resolved", hub.ClientOptions{ResolvePath: true}},
			{"key_only", hub.ClientOptions{IncludeObserverKey: true}},
			{"both", hub.ClientOptions{ResolvePath: true, IncludeObserverKey: true}},
		} {
			b.Run(fmt.Sprintf("%dhops/%s", hops, mode.name), func(b *testing.B) {
				w, _ := newTestWorker()
				go w.hub.Run()
				client := w.hub.NewClient()
				defer w.hub.Remove(client)
				w.hub.AddScope(client, "bench", hub.Scope{Events: []hub.EventType{hub.EventPacketObservation}})
				mode.opts.IncludeRepeats = true // atomic flag is a setup barrier after subscribe/configure
				w.hub.Configure(client, mode.opts)
				deadline := time.Now().Add(time.Second)
				for !w.hub.RepeatsWanted() {
					if time.Now().After(deadline) {
						b.Fatal("hub did not configure benchmark client")
					}
					time.Sleep(time.Millisecond)
				}
				evt, hashes, resolved := livePathFixture(hops)
				key := strings.Repeat("ab", 32)
				b.ReportAllocs()
				b.ResetTimer()
				for range b.N {
					w.broadcastPacketObservation("YKF", 4, evt, hashes, resolved, key, false)
					<-client.Send
				}
			})
		}
	}
}
