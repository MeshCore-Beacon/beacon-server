// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package obscount

import (
	"slices"
	"testing"
	"time"

	"github.com/google/uuid"
)

var (
	obsA = uuid.MustParse("00000000-0000-0000-0000-00000000000a")
	obsB = uuid.MustParse("00000000-0000-0000-0000-00000000000b")
)

type row struct {
	hour     time.Time
	observer uuid.UUID
	iata     string
	n        int64
}

func rows(s Snapshot) []row {
	var out []row
	for i := range s.Hours {
		out = append(out, row{s.Hours[i], s.Observers[i], s.IATAs[i], s.Counts[i]})
	}
	slices.SortFunc(out, func(a, b row) int {
		if c := a.hour.Compare(b.hour); c != 0 {
			return c
		}
		if c := slices.Compare(a.observer[:], b.observer[:]); c != 0 {
			return c
		}
		if a.iata < b.iata {
			return -1
		}
		if a.iata > b.iata {
			return 1
		}
		return 0
	})
	return out
}

func TestCountsCoverOnlyHoursWhollyAfterStart(t *testing.T) {
	start := time.Date(2026, 10, 27, 10, 20, 0, 0, time.UTC)
	now := start
	c := New(func() time.Time { return now })
	// a stored heard_at can sit 30 minutes off its insert time, so 10:51 is the first whole minute
	// that can't predate the counter, and 11:00 the first whole hour
	now = time.Date(2026, 10, 27, 12, 40, 30, 0, time.UTC)
	s := c.Snapshot(now)
	if !s.From.Equal(time.Date(2026, 10, 27, 11, 0, 0, 0, time.UTC)) {
		t.Fatalf("from %v", s.From)
	}
	if !s.Until.Equal(time.Date(2026, 10, 27, 12, 40, 0, 0, time.UTC)) {
		t.Fatalf("until %v", s.Until)
	}
}

func TestCountsSumWholeMinutesPerHourObserverIATA(t *testing.T) {
	start := time.Date(2026, 10, 27, 8, 0, 0, 0, time.UTC)
	now := start
	c := New(func() time.Time { return now })
	now = time.Date(2026, 10, 27, 12, 40, 30, 0, time.UTC)
	at := func(h, m, s int) time.Time { return time.Date(2026, 10, 27, h, m, s, 0, time.UTC) }
	c.Add(at(10, 59, 59), obsA, "YVR")
	c.Add(at(11, 0, 0), obsA, "YVR")
	c.Add(at(11, 59, 59), obsA, "YVR")
	c.Add(at(11, 30, 0), obsA, "YYJ")
	c.Add(at(12, 5, 0), obsB, "YVR")
	c.Add(at(12, 39, 59), obsB, "YVR")
	c.Add(at(12, 40, 10), obsB, "YVR") // inside the partial minute: left to the raw remainder
	c.Add(at(13, 5, 0), obsB, "YVR")   // future-dated within the clamp: not yet in the window
	s := c.Snapshot(now)
	if !s.From.Equal(at(9, 0, 0)) || !s.Until.Equal(at(12, 40, 0)) {
		t.Fatalf("window %v .. %v", s.From, s.Until)
	}
	want := []row{
		{at(10, 0, 0), obsA, "YVR", 1},
		{at(11, 0, 0), obsA, "YVR", 2},
		{at(11, 0, 0), obsA, "YYJ", 1},
		{at(12, 0, 0), obsB, "YVR", 2},
	}
	if got := rows(s); !slices.Equal(got, want) {
		t.Fatalf("got %+v\nwant %+v", got, want)
	}
}

func TestCountsForgetOldMinutesAndMoveFromForward(t *testing.T) {
	start := time.Date(2026, 10, 27, 0, 0, 0, 0, time.UTC)
	now := start
	c := New(func() time.Time { return now })
	c.Add(start.Add(2*time.Hour), obsA, "YVR")
	now = start.Add(2*time.Hour + Keep + 10*time.Minute)
	c.Add(now, obsA, "YVR") // prunes on the way in
	s := c.Snapshot(now)
	if s.From.Before(now.Add(-Keep)) {
		t.Fatalf("from %v reaches back past the kept minutes (%v)", s.From, now.Add(-Keep))
	}
	for _, h := range s.Hours {
		if h.Before(s.From) {
			t.Fatalf("hour %v before from %v", h, s.From)
		}
	}
}

func TestNilCountsAreOff(t *testing.T) {
	var c *Counts
	c.Add(time.Now(), obsA, "YVR")
	if s := c.Snapshot(time.Now()); s.Live() {
		t.Fatal("nil counts reported live")
	}
}
