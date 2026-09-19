// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/borders"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
	"github.com/meshcore-go/meshcore-go"
	"github.com/paulmach/orb"
)

func TestForeignNodeUpdates(t *testing.T) {
	local, err := borders.New([]orb.Polygon{{{{10, 10}, {20, 10}, {20, 20}, {10, 20}, {10, 10}}}})
	if err != nil {
		t.Fatal(err)
	}
	w, _ := newTestWorker()
	go w.hub.Run()
	client := w.hub.NewClient()
	defer w.hub.Remove(client)
	w.hub.AddScope(client, "foreign", hub.Scope{Events: []hub.EventType{hub.EventNodeUpdate, hub.EventObserverStatus}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	waitForSummarySubscriber(t, ctx, w.hub, client)
	for _, tc := range []struct {
		name              string
		enabled, position bool
		role              byte
		lat, lng          int32
		present           bool
		want              any
	}{
		{"disabled", false, true, 2, 25000000, 25000000, false, nil},
		{"inside", true, true, 2, 15000000, 15000000, true, false},
		{"outside", true, true, 2, 25000000, 25000000, true, true},
		{"omitted retains old position", true, false, 2, 0, 0, false, nil},
		{"explicit reset clears flag", true, true, 2, 0, 0, true, nil},
		{"invalid position clears flag", true, true, 2, 91000000, 25000000, true, nil},
		{"role change clears flag", true, false, meshcore.AdvertTypeChat, 0, 0, true, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w.cfg.LocalBorders = nil
			if tc.enabled {
				w.cfg.LocalBorders = local
			}
			data := []byte{tc.role}
			if tc.position {
				data[0] |= meshcore.AdvertLatLonMask
				data = binary.LittleEndian.AppendUint32(data, uint32(tc.lat))
				data = binary.LittleEndian.AppendUint32(data, uint32(tc.lng))
			}
			w.handlePayloadTypeSideEffects(ctx, buildAdvertPacketWithData(t, data, false), "AAA", []byte{1}, RadioSettings{}, nil, nil, nil, 0)
			for {
				select {
				case event := <-client.Send:
					if event.Type != hub.EventNodeUpdate {
						continue
					}
					var payload map[string]any
					if err := json.Unmarshal(event.Payload, &payload); err != nil {
						t.Fatal(err)
					}
					got, present := payload["possiblyForeign"]
					if present != tc.present || got != tc.want {
						t.Fatalf("flag = %v (present=%v); want %v (present=%v)", got, present, tc.want, tc.present)
					}
					return
				case <-ctx.Done():
					t.Fatal("node update not received")
				}
			}
		})
	}
}
