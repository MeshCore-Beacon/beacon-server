// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package hub

import (
	"strconv"
	"testing"
	"time"
)

func TestImmediateRemoveFollowsRegistration(t *testing.T) {
	for range 8 {
		h := New()
		clients := make([]*Client, 21)
		for i := range clients {
			c := h.NewClient()
			clients[i] = c
			h.Configure(c, ClientOptions{IncludeObserverKey: true, IncludeRepeats: true})
			h.Remove(c)
		}
		// A busy hub may not consume any controls before the socket has closed.
		go h.Run()
		t.Cleanup(func() {
			for _, c := range clients {
				h.Remove(c)
			}
		})
		deadline := time.After(time.Second)
		for _, c := range clients {
			select {
			case _, open := <-c.Send:
				if open {
					t.Fatal("closed client received an event")
				}
			case <-deadline:
				t.Fatal("client removal was lost before its registration")
			}
		}
		if h.ObserverKeyWanted() || h.RepeatsWanted() {
			t.Fatal("removed clients still keep optional event variants enabled")
		}
	}
}

func TestImmediateUnsubscribeFollowsSubscribe(t *testing.T) {
	for range 8 {
		h := New()
		c := h.NewClient()
		for i := range 16 {
			id := strconv.Itoa(i)
			h.AddScope(c, id, Scope{Events: []EventType{EventPacketObservation}})
			h.RemoveScope(c, id)
		}
		h.AddScope(c, "keep", Scope{Events: []EventType{EventNodeUpdate}})
		h.Configure(c, ClientOptions{IncludeRepeats: true})
		go h.Run()
		t.Cleanup(func() { h.Remove(c) })
		// Configure follows the earlier controls, so its atomic flag is a barrier.
		deadline := time.Now().Add(time.Second)
		for !h.RepeatsWanted() {
			if time.Now().After(deadline) {
				t.Fatal("hub did not process client controls")
			}
			time.Sleep(time.Millisecond)
		}
		h.Broadcast(Event{Type: EventPacketObservation})
		h.Broadcast(Event{Type: EventNodeUpdate})
		select {
		case e := <-c.Send:
			if e.Type != EventNodeUpdate {
				t.Fatal("a removed subscription still received a packet event")
			}
		case <-time.After(time.Second):
			t.Fatal("remaining subscription did not receive its event")
		}
	}
}

func TestControlBackpressureDrainsWithoutDeadlock(t *testing.T) {
	h := New()
	clients := make([]*Client, 64)
	for i := range clients {
		clients[i] = h.NewClient()
	}
	done := make(chan struct{})
	go func() {
		h.Configure(clients[0], ClientOptions{IncludeRepeats: true})
		for _, c := range clients {
			h.Remove(c)
		}
		close(done)
	}()
	go h.Run()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("queued caller did not resume when the hub drained its controls")
	}
	deadline := time.After(time.Second)
	for _, c := range clients {
		select {
		case <-c.Send:
		case <-deadline:
			t.Fatal("backpressured client was not removed")
		}
	}
}
