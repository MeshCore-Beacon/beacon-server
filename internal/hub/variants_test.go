// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package hub

import (
	"encoding/json"
	"testing"
	"time"
)

func waitVariantDemand(t *testing.T, h *Hub, want [3]bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for {
		a, b, c := h.PacketVariantsWanted()
		if [3]bool{a, b, c} == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("variant demand = %v, want %v", [3]bool{a, b, c}, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPacketVariantDemandTracksConfigureAndRemove(t *testing.T) {
	h := runHub(t)
	a, b, c := h.NewClient(), h.NewClient(), h.NewClient()
	defer h.Remove(c)
	waitVariantDemand(t, h, [3]bool{})
	h.Configure(a, ClientOptions{ResolvePath: true})
	h.Configure(a, ClientOptions{ResolvePath: true}) // repeat configure must not double-count
	h.Configure(b, ClientOptions{IncludeObserverKey: true})
	waitVariantDemand(t, h, [3]bool{true, true, false})
	h.Configure(c, ClientOptions{ResolvePath: true, IncludeObserverKey: true})
	waitVariantDemand(t, h, [3]bool{true, true, true})
	h.Configure(a, ClientOptions{IncludeObserverKey: true})
	waitVariantDemand(t, h, [3]bool{false, true, true})
	h.Configure(a, ClientOptions{})
	h.Remove(b)
	waitVariantDemand(t, h, [3]bool{false, false, true})
	h.Configure(c, ClientOptions{})
	waitVariantDemand(t, h, [3]bool{})
	h.Configure(a, ClientOptions{IncludeObserverKey: true})
	waitVariantDemand(t, h, [3]bool{false, true, false})
	// Empty scope IDs take the registration path; an existing client isn't counted twice.
	h.AddScope(a, "", Scope{})
	h.Configure(a, ClientOptions{})
	waitVariantDemand(t, h, [3]bool{})
	h.Configure(a, ClientOptions{IncludeObserverKey: true})
	waitVariantDemand(t, h, [3]bool{false, true, false})
	h.Remove(a)
	h.Remove(a) // duplicate cleanup must not underflow demand counts
	waitVariantDemand(t, h, [3]bool{})
	h.Configure(c, ClientOptions{IncludeObserverKey: true})
	waitVariantDemand(t, h, [3]bool{false, true, false})
	h.Configure(c, ClientOptions{ResolvePath: true, IncludeObserverKey: true})
	waitVariantDemand(t, h, [3]bool{false, false, true})
	h.Remove(c)
	waitVariantDemand(t, h, [3]bool{})
	d := h.NewClient()
	h.Configure(d, ClientOptions{ResolvePath: true})
	waitVariantDemand(t, h, [3]bool{true, false, false})
	h.Remove(d)
	waitVariantDemand(t, h, [3]bool{})
}

func TestPreviouslyBuiltVariantRespectsLaterConfigureAndRemove(t *testing.T) {
	h := runHub(t)
	c := h.NewClient()
	h.AddScope(c, "all", Scope{})
	h.Configure(c, ClientOptions{ResolvePath: true, IncludeObserverKey: true})
	waitVariantDemand(t, h, [3]bool{false, false, true})
	// This event was constructed when only the combined variant was requested.
	evt := Event{Type: EventPacketObservation, Payload: json.RawMessage(`{"resolvedPath":null}`), PayloadResolvedWithKey: json.RawMessage(`{"resolvedPath":[{}],"observerPublicKey":"secret"}`)}
	for _, tc := range []struct {
		opts ClientOptions
		want [3]bool
	}{
		{ClientOptions{IncludeObserverKey: true}, [3]bool{false, true, false}},
		{ClientOptions{ResolvePath: true}, [3]bool{true, false, false}},
		{ClientOptions{}, [3]bool{}},
	} {
		h.Configure(c, tc.opts)
		waitVariantDemand(t, h, tc.want)
		h.Broadcast(evt)
		select {
		case got := <-c.Send:
			if string(got.Payload) != string(evt.Payload) {
				t.Fatal("old combined event leaked key/path fields into a narrower configuration")
			}
		case <-time.After(time.Second):
			t.Fatal("base fallback event was not delivered")
		}
	}
	h.Remove(c)
	select {
	case _, open := <-c.Send:
		if open {
			t.Fatal("removed client stayed open")
		}
	case <-time.After(time.Second):
		t.Fatal("client removal did not finish")
	}
	h.Broadcast(evt) // removal after construction prevents delivery to the closed client
	waitVariantDemand(t, h, [3]bool{})
}
