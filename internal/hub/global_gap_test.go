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
