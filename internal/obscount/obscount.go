// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package obscount counts stored observations per minute, observer and IATA for the recent hours
// the analytics rollup hasn't covered yet. The observer directory adds these up instead of
// re-reading every raw observation of those hours, which on a large deployment means a heap page
// per row. It is exact only in the one process that ingests: every insert of a new observation
// row goes through Add.
package obscount

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

// Keep is how far back counts are kept: an hour is rolled about 95 minutes after it ends, and a
// later hour just reads raw rows.
const Keep = 4 * time.Hour

// heardAtSlack bounds how far a stored heard_at sits from its insert time: ingest replaces a
// timestamp more than 30 minutes off the server clock.
const heardAtSlack = 31 * time.Minute

type key struct {
	observer uuid.UUID
	iata     string
}

// Counts holds per-minute observation counts. A nil *Counts counts nothing and is never live.
type Counts struct {
	mu      sync.Mutex
	minutes map[int64]map[key]int64 // unix minute -> counts
	from    int64                   // first minute none of whose observations predate the counter
	pruned  int64                   // minutes before this were dropped
	now     func() time.Time
}

// New starts counting now.
func New(now func() time.Time) *Counts {
	first := now().Add(heardAtSlack)
	return &Counts{minutes: map[int64]map[key]int64{}, from: ceilMinute(first), now: now}
}

// Add counts one newly stored observation.
func (c *Counts) Add(heardAt time.Time, observer uuid.UUID, iata string) {
	if c == nil {
		return
	}
	m := heardAt.Unix() / 60
	c.mu.Lock()
	defer c.mu.Unlock()
	if cut := c.now().Add(-Keep).Unix() / 60; cut > c.pruned {
		for minute := range c.minutes {
			if minute < cut {
				delete(c.minutes, minute)
			}
		}
		c.pruned = cut
	}
	if m < c.pruned {
		return
	}
	counts := c.minutes[m]
	if counts == nil {
		counts = map[key]int64{}
		c.minutes[m] = counts
	}
	counts[key{observer, iata}]++
}

// Snapshot is the counts per hour, observer and IATA for every whole minute in [From, Until):
// From is an hour start, Until the last whole minute before the request's end.
type Snapshot struct {
	From, Until time.Time
	Hours       []time.Time
	Observers   []uuid.UUID
	IATAs       []string
	Counts      []int64
}

// Live reports whether the snapshot covers any hour.
func (s Snapshot) Live() bool { return !s.From.IsZero() && s.From.Before(s.Until) }

// Snapshot sums the counted minutes before until's whole minute, per hour from the first hour
// that is counted completely.
func (c *Counts) Snapshot(until time.Time) Snapshot {
	if c == nil {
		return Snapshot{}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	first := max(c.from, c.pruned)
	from := time.Unix(first*60, 0).UTC()
	if h := from.Truncate(time.Hour); h.Before(from) {
		from = h.Add(time.Hour)
	}
	end := until.UTC().Truncate(time.Minute)
	s := Snapshot{From: from, Until: end}
	type hourKey struct {
		hour int64
		key
	}
	sums := map[hourKey]int64{}
	for minute, counts := range c.minutes {
		t := minute * 60
		if t < from.Unix() || t >= end.Unix() {
			continue
		}
		hour := t - t%3600
		for k, n := range counts {
			sums[hourKey{hour, k}] += n
		}
	}
	for k, n := range sums {
		s.Hours = append(s.Hours, time.Unix(k.hour, 0).UTC())
		s.Observers = append(s.Observers, k.observer)
		s.IATAs = append(s.IATAs, k.iata)
		s.Counts = append(s.Counts, n)
	}
	return s
}

func ceilMinute(t time.Time) int64 {
	m := t.Unix() / 60
	if t.Unix()%60 != 0 || t.Nanosecond() != 0 {
		m++
	}
	return m
}
