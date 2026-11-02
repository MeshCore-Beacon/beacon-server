// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ws

import (
	"testing"
	"time"
)

func TestSendBudgetBurstAndRefill(t *testing.T) {
	start := time.Unix(0, 0)
	b := newSendBudget(100, 300, start)
	for i := range 3 {
		if !b.allow(100, start) {
			t.Fatalf("message %d inside the burst refused", i)
		}
	}
	if b.allow(1, start) {
		t.Fatal("message past the burst allowed")
	}
	if !b.allow(100, start.Add(time.Second)) {
		t.Fatal("a second's refill not applied")
	}
	if b.allow(1, start.Add(time.Second)) {
		t.Fatal("refill over-credited")
	}
	later := start.Add(time.Hour)
	for i := range 3 {
		if !b.allow(100, later) {
			t.Fatalf("message %d after an idle hour refused", i)
		}
	}
	if b.allow(1, later) {
		t.Fatal("idle refill passed the burst")
	}
}

func TestSendBudgetOversizeMessage(t *testing.T) {
	start := time.Unix(0, 0)
	b := newSendBudget(100, 50, start)
	if !b.allow(1000, start) {
		t.Fatal("a message bigger than the burst could never go out")
	}
	if b.allow(1, start.Add(5*time.Second)) {
		t.Fatal("the oversize message was not charged")
	}
	if !b.allow(1, start.Add(10*time.Second)) {
		t.Fatal("the debt was never paid off")
	}
}

func TestSendBudgetUnlimited(t *testing.T) {
	b := newSendBudget(0, 0, time.Now())
	if b != nil {
		t.Fatal("a zero rate should mean no budget")
	}
	if !b.allow(1<<30, time.Now()) {
		t.Fatal("a nil budget refused a message")
	}
}
