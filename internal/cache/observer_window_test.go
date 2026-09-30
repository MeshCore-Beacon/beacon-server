// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
)

type activityWindowReader struct {
	stubReader
	ends []time.Time
}

func (r *activityWindowReader) GetObserverActivity(_ context.Context, _ uuid.UUID, _, _ time.Duration, until time.Time) (*api.ObserverActivity, error) {
	r.ends = append(r.ends, until)
	return &api.ObserverActivity{WindowEnd: until.UnixMilli()}, nil
}

func TestObserverActivityCacheUsesEffectiveWindow(t *testing.T) {
	client, redis := newTestClient(t)
	inner := &activityWindowReader{}
	reader := &CachedReader{inner: inner, c: client}
	id := uuid.New()
	anchor := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	for _, offset := range []time.Duration{time.Second, time.Minute, 59 * time.Minute} {
		if _, err := reader.GetObserverActivity(context.Background(), id, 24*time.Hour, time.Hour, anchor.Add(offset)); err != nil {
			t.Fatal(err)
		}
	}
	if len(inner.ends) != 1 || !inner.ends[0].Equal(anchor) || len(redis.Keys()) != 1 {
		t.Fatalf("equivalent fixed windows missed the cache: %v, %v", inner.ends, redis.Keys())
	}
	for _, until := range []time.Time{anchor.Add(time.Hour), {}, {}} {
		if _, err := reader.GetObserverActivity(context.Background(), id, 24*time.Hour, time.Hour, until); err != nil {
			t.Fatal(err)
		}
	}
	if len(inner.ends) != 3 || !inner.ends[2].IsZero() || len(redis.Keys()) != 3 {
		t.Fatalf("distinct/live windows aliased: %v", inner.ends)
	}
}
