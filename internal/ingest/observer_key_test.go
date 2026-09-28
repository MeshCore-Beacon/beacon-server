// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/hub"
)

func TestBroadcastPacketObservation_ObserverKeyOptIn(t *testing.T) {
	const key = "ab01020304050607080910111213141516171819202122232425262728293031"
	w, _ := newTestWorker()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go w.hub.Run()
	client := w.hub.NewClient()
	defer w.hub.Remove(client)
	w.hub.AddScope(client, "all", hub.Scope{Events: []hub.EventType{hub.EventPacketObservation, hub.EventObserverStatus}})
	waitForSummarySubscriber(t, ctx, w.hub, client)

	broadcast := func() hub.Event {
		t.Helper()
		w.broadcastPacketObservation("YVR", 4, packetObservationEvent{}, []api.ResolvedHop{{}}, key, false)
		for {
			select {
			case evt := <-client.Send:
				if evt.Type == hub.EventPacketObservation {
					return evt
				}
			case <-ctx.Done():
				t.Fatal("packetObservation not delivered")
			}
		}
	}
	observation := func(raw json.RawMessage) map[string]json.RawMessage {
		t.Helper()
		var evt struct {
			Observation map[string]json.RawMessage `json:"observation"`
		}
		if err := json.Unmarshal(raw, &evt); err != nil {
			t.Fatal(err)
		}
		return evt.Observation
	}

	evt := broadcast()
	if evt.PayloadWithKey != nil || evt.PayloadResolvedWithKey != nil {
		t.Fatal("key variants built with no opted-in client")
	}
	if strings.Contains(string(evt.Payload)+string(evt.PayloadResolved), "observerPublicKey") {
		t.Fatal("default payloads must not carry observerPublicKey")
	}

	// client stays plain so its Payload is still the base variant.
	keyed := w.hub.NewClient()
	defer w.hub.Remove(keyed)
	w.hub.Configure(keyed, hub.ClientOptions{IncludeObserverKey: true})
	for !w.hub.ObserverKeyWanted() {
		time.Sleep(time.Millisecond)
	}
	evt = broadcast()
	if _, ok := observation(evt.Payload)["observerPublicKey"]; ok {
		t.Fatal("base payload must not carry observerPublicKey")
	}
	withKey := observation(evt.PayloadWithKey)
	if string(withKey["observerPublicKey"]) != `"`+key+`"` || string(withKey["resolvedPath"]) != "null" {
		t.Fatalf("key variant = %v", withKey)
	}
	both := observation(evt.PayloadResolvedWithKey)
	if string(both["observerPublicKey"]) != `"`+key+`"` || string(both["resolvedPath"]) == "null" {
		t.Fatalf("resolved key variant = %v", both)
	}
}
