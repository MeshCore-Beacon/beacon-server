// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package obscount

import (
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestActivityWindow(t *testing.T) {
	start := time.Date(2026, 1, 1, 10, 7, 0, 0, time.UTC)
	clock := start
	a := NewActivity(func() time.Time { return clock })
	obs, other := uuid.New(), uuid.New()
	airtime := float32(12.5)
	// The 10:45 slot is the first none of whose observations can predate the counter (10:07 + 31 min, rounded up).
	a.Add(time.Date(2026, 1, 1, 10, 40, 0, 0, time.UTC), obs, 4, activityPtr(int16(-90)), activityPtr(float32(5.25)), &airtime)
	a.Add(time.Date(2026, 1, 1, 10, 44, 59, 999999000, time.UTC), obs, 5, activityPtr(int16(0)), activityPtr(float32(0)), nil) // no signal
	a.Add(time.Date(2026, 1, 1, 10, 45, 0, 0, time.UTC), obs, 4, activityPtr(int16(-100)), activityPtr(float32(-2.5)), nil)
	a.Add(time.Date(2026, 1, 1, 10, 50, 0, 0, time.UTC), other, 4, activityPtr(int16(-80)), activityPtr(float32(1)), nil)
	a.Add(time.Date(2026, 1, 1, 11, 20, 0, 0, time.UTC), obs, 4, activityPtr(int16(-70)), activityPtr(float32(3)), nil) // in until's slot

	w := a.Window(obs, time.Date(2026, 1, 1, 10, 0, 0, 0, time.UTC), time.Date(2026, 1, 1, 11, 25, 0, 0, time.UTC))
	if !w.From.Equal(time.Date(2026, 1, 1, 10, 45, 0, 0, time.UTC)) || !w.Until.Equal(time.Date(2026, 1, 1, 11, 15, 0, 0, time.UTC)) {
		t.Fatalf("window %v .. %v", w.From, w.Until)
	}
	if len(w.Buckets) != 1 {
		t.Fatalf("buckets %+v", w.Buckets)
	}
	b := w.Buckets[time.Date(2026, 1, 1, 10, 45, 0, 0, time.UTC)]
	if b.Observations != 1 || b.SNRN != 1 || b.RSSIN != 1 || b.SNRSum != -2.5 || b.SNRMin != -2.5 || b.RSSISum != -100 || b.AirtimeN != 0 || b.Types[4] != 1 {
		t.Fatalf("10:45 bucket %+v", b)
	}

	// The 10:30 slot started before 10:38, so it is never counted, even asked for directly.
	if w := a.Window(obs, time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC), time.Date(2026, 1, 1, 10, 45, 0, 0, time.UTC)); w.Live() {
		t.Fatalf("an uncovered slot was served: %+v", w)
	}
	// A later counter covers the 10:30 slot: signal sums skip the row with no signal, airtime counts only costed rows.
	b2 := NewActivity(func() time.Time { return start.Add(-time.Hour) })
	b2.Add(time.Date(2026, 1, 1, 10, 40, 0, 0, time.UTC), obs, 4, activityPtr(int16(-90)), activityPtr(float32(5.25)), &airtime)
	b2.Add(time.Date(2026, 1, 1, 10, 44, 59, 999999000, time.UTC), obs, 5, activityPtr(int16(0)), activityPtr(float32(0)), nil)
	got := b2.Window(obs, time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC), time.Date(2026, 1, 1, 10, 45, 0, 0, time.UTC)).Buckets[time.Date(2026, 1, 1, 10, 30, 0, 0, time.UTC)]
	if got.Observations != 2 || got.SNRN != 1 || got.RSSIN != 1 || got.SNRSum != 5.25 || got.RSSISum != -90 || got.AirtimeN != 1 || got.AirtimeSum != 12.5 || got.Types[4] != 1 || got.Types[5] != 1 {
		t.Fatalf("10:30 bucket %+v", got)
	}

	// A window's buckets are copies.
	w.Buckets[time.Date(2026, 1, 1, 10, 45, 0, 0, time.UTC)].Types[4] = 99
	if again := a.Window(obs, w.From, w.Until.Add(ActivitySlot)); again.Buckets[w.From].Types[4] != 1 {
		t.Fatal("the window shares its counts")
	}

	// Slots older than ActivityKeep are dropped, and a window never reaches before them.
	clock = start.Add(ActivityKeep + time.Hour) // drops the slots before 11:00 on the first day
	a.Add(clock, obs, 4, activityPtr(int16(-90)), activityPtr(float32(1)), nil)
	w = a.Window(obs, start, clock)
	if _, ok := w.Buckets[time.Date(2026, 1, 1, 10, 45, 0, 0, time.UTC)]; ok || !w.From.Equal(time.Date(2026, 1, 1, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("pruned slots served: %v %+v", w.From, w.Buckets)
	}

	var none *Activity
	none.Add(clock, obs, 4, activityPtr(int16(-90)), activityPtr(float32(1)), nil)
	if none.Window(obs, start, clock).Live() {
		t.Fatal("a nil counter is live")
	}
}

func activityPtr[T any](value T) *T { return &value }

func TestActivityMissingSignalFields(t *testing.T) {
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	at := start.Add(time.Hour)
	a := NewActivity(func() time.Time { return start })
	observer := uuid.New()
	a.Add(at, observer, 4, nil, nil, nil)
	a.Add(at, observer, 4, nil, activityPtr(float32(-2)), nil)
	a.Add(at, observer, 4, activityPtr(int16(-90)), nil, nil)
	a.Add(at, observer, 4, activityPtr(int16(-70)), activityPtr(float32(0)), nil)
	b := a.Window(observer, at, at.Add(ActivitySlot)).Buckets[at]
	if b.Observations != 4 || b.SNRN != 2 || b.RSSIN != 2 || b.SNRSum != -2 || b.SNRMin != -2 || b.RSSISum != -160 {
		t.Fatalf("missing fields became zero-valued samples: %+v", b)
	}
}
