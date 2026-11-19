// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package hub

import (
	"testing"
	"time"
)

func TestGlobalOverflowSignalsRecoverableGap(t *testing.T) {
	h := New()
	for range cap(h.broadcast) + 3 {
		h.Broadcast(Event{Type: EventPacketObservation})
	}
	if h.pendingGap.Load() != 3 || len(h.gapNotify) != 1 {
		t.Fatal("global overflow not counted/coalesced")
	}
	// Register first, then signal a lost batch as a busy ingest producer would.
	other := New()
	c := other.NewClient()
	go other.Run()
	// Registration and Configure share FIFO order; the observer-key counter
	// acknowledges that the client is owned by the hub before the gap is sent.
	other.AddScope(c, "all", Scope{})
	other.Configure(c, ClientOptions{IncludeObserverKey: true})
	deadline := time.Now().Add(time.Second)
	for !other.ObserverKeyWanted() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !other.ObserverKeyWanted() {
		t.Fatal("client did not register")
	}
	other.pendingGap.Add(3)
	other.gapNotify <- struct{}{}
	select {
	case gap := <-c.LaggedCH():
		if gap.DroppedCount != 3 {
			t.Fatalf("gap=%+v", gap)
		}
	case <-time.After(time.Second):
		t.Fatal("client did not receive global gap")
	}
	other.Remove(c)
}

// A burst of drops gets one notice at once and one more when the interval is up, carrying the rest.
func TestGlobalGapNoticesAreSpacedOut(t *testing.T) {
	old := gapNoticeInterval
	gapNoticeInterval = 200 * time.Millisecond
	t.Cleanup(func() { gapNoticeInterval = old })
	h := New()
	c := h.NewClient()
	idle := h.NewClient() // no subscription: nothing was lost for it
	go h.Run()
	h.AddScope(c, "all", Scope{})
	h.Configure(c, ClientOptions{IncludeObserverKey: true})
	for deadline := time.Now().Add(time.Second); !h.ObserverKeyWanted() && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	signal := func(n int64) {
		h.pendingGap.Add(n)
		select {
		case h.gapNotify <- struct{}{}:
		default:
		}
	}
	signal(2)
	start := time.Now()
	if gap := <-c.LaggedCH(); gap.DroppedCount != 2 {
		t.Fatalf("first notice: %+v", gap)
	}
	for range 5 {
		signal(1)
		time.Sleep(5 * time.Millisecond)
	}
	select {
	case gap := <-c.LaggedCH():
		if gap.DroppedCount != 5 || time.Since(start) < gapNoticeInterval {
			t.Fatalf("second notice %+v after %s", gap, time.Since(start))
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the held drops were never reported")
	}
	select {
	case gap := <-c.LaggedCH():
		t.Fatalf("extra notice: %+v", gap)
	case gap := <-idle.LaggedCH():
		t.Fatalf("notice for a client with no subscription: %+v", gap)
	case <-time.After(50 * time.Millisecond):
	}
	h.Remove(c)
	h.Remove(idle)
}
