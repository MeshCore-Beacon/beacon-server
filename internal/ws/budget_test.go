// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package ws

import (
	"fmt"
	"testing"
	"time"
)

func TestControlBudgetBoundsWorkAndRecovers(t *testing.T) {
	b := newControlBudget()
	now := b.at
	for range 60 {
		if !b.allow(clientMessage{Type: "ping"}, now) {
			t.Fatal("initial budget rejected")
		}
	}
	if b.allow(clientMessage{Type: "ping"}, now) {
		t.Fatal("unbounded controls")
	}
	if !b.allow(clientMessage{Type: "ping"}, now.Add(time.Second)) {
		t.Fatal("budget never recovers")
	}
	b = newControlBudget()
	for i := range maxSubscriptions {
		b.active[fmt.Sprint(i)] = struct{}{}
	}
	msg := clientMessage{Type: "subscribe", Scope: &subscribeScope{IATAs: []string{"YKF"}}}
	if b.allow(msg, b.at) {
		t.Fatal("unbounded subscriptions")
	}
	delete(b.active, "0")
	if !b.allow(msg, b.at) {
		t.Fatal("unsubscribe did not free capacity")
	}
	msg.Scope.IATAs = make([]string, maxScopeValues+1)
	if b.allow(msg, b.at) {
		t.Fatal("unbounded filter expansion")
	}
}
