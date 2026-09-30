// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package presence

import (
	"context"
	"github.com/google/uuid"
	"testing"
	"time"
)

type brokerTestStore struct {
	Store
	initial       []bool
	seen, packets []time.Time
}

func (s *brokerTestStore) UpsertObserverBroker(_ context.Context, _ uuid.UUID, _ string, packet bool) error {
	s.initial = append(s.initial, packet)
	return nil
}
func (s *brokerTestStore) TouchObserverBrokers(_ context.Context, _ []uuid.UUID, _ []string, seen, packets []time.Time) error {
	s.seen = seen
	s.packets = packets
	return nil
}

func TestBrokerPresenceDoesNotInventPacketFreshness(t *testing.T) {
	ctx := context.Background()
	store := &brokerTestStore{}
	c := New(store, time.Second, time.Minute)
	now := time.Now()
	c.now = func() time.Time { return now }
	id := uuid.New()
	c.UpsertObserverBroker(ctx, id, "one", false)
	c.UpsertObserverBroker(ctx, id, "one", false)
	c.Flush(ctx)
	if len(store.initial) != 1 || store.initial[0] || !store.packets[0].IsZero() {
		t.Fatal("status-only activity invented a packet")
	}
	now = now.Add(time.Minute)
	packetTime := now
	c.UpsertObserverBroker(ctx, id, "one", true)
	now = now.Add(time.Minute)
	c.UpsertObserverBroker(ctx, id, "one", false)
	c.Flush(ctx)
	if !store.seen[0].Equal(now) || !store.packets[0].Equal(packetTime) {
		t.Fatal("later status changed packet freshness")
	}
}
