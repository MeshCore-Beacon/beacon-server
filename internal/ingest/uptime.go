// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"sync"
	"time"
)

// rebootSlack allows for the delay between a device reading its uptime and the status arriving.
const rebootSlack = 30 * time.Second

// lateStatusWindow is how late a copy of a boot's first status may arrive and still be taken for a copy.
const lateStatusWindow = 10 * time.Minute

type uptimeSeen struct {
	uptime int64
	at     time.Time
	bucket time.Time // the resolution window the last snapshot went to

	bootUptime   int64     // first uptime seen for the current boot
	boot         time.Time // when that status says the device booted
	rebootBucket time.Time // the window that already has an immediate reboot row
	offGrid      bool      // the last row was an immediate one: the next window's row is stamped on arrival
}

type uptimeEntry struct {
	mu   sync.Mutex // held from Begin to Done, so the brokers' copies of one status are judged in turn
	ok   bool
	seen uptimeSeen
}

// UptimeTracker remembers each observer's last reported uptime, so a reboot between two status messages is stored
// even when the resolution window already has its snapshot. Share one between the workers of every broker: the
// same status arrives on each.
type UptimeTracker struct {
	mu      sync.Mutex
	entries map[string]*uptimeEntry
}

func NewUptimeTracker() *UptimeTracker {
	return &UptimeTracker{entries: map[string]*uptimeEntry{}}
}

// Snapshot is the tracker's verdict on one status. Call Done once the row is written, or was not.
type Snapshot struct {
	At    time.Time // the reported_at to store the status under
	Store bool      // false: the window has its row, or the status is a late copy

	entry *uptimeEntry
	next  uptimeSeen
}

// Done records the status as seen when its row was persisted (or needed none), and releases the observer. After a
// failed insert nothing is recorded, so the next delivery of the same status is judged the same way.
func (s *Snapshot) Done(persisted bool) {
	if s.entry == nil {
		return
	}
	if persisted {
		s.entry.seen, s.entry.ok = s.next, true
	}
	s.entry.mu.Unlock()
	s.entry = nil
}

// Begin decides where a status goes. A window gets one row, stamped at the window start. A reboot since the last
// status is stored at once (alerts read these rows), but only once per window; the row after it is stamped on
// arrival, so the gap between the two is the time that really passed. A lower uptime that is longer than the time
// since the last status is an old status replayed, and a late copy of the boot's first status is dropped.
func (t *UptimeTracker) Begin(observer string, uptime int64, now time.Time, resolution time.Duration) *Snapshot {
	bucket := now.Truncate(resolution)
	if t == nil {
		return &Snapshot{At: bucket, Store: true}
	}
	t.mu.Lock()
	e := t.entries[observer]
	if e == nil {
		e = &uptimeEntry{}
		t.entries[observer] = e
	}
	t.mu.Unlock()
	e.mu.Lock()

	prev := e.seen
	s := &Snapshot{At: bucket, Store: true, entry: e, next: prev}
	taken := e.ok && prev.bucket.Equal(bucket)
	booted := now.Add(-time.Duration(uptime) * time.Second)
	reboot := false
	switch {
	case !e.ok:
		s.next = uptimeSeen{bootUptime: uptime, boot: booted}
	case uptime >= prev.uptime:
	case uptime <= prev.bootUptime && booted.Sub(prev.boot) <= lateStatusWindow:
		// This boot's first status again, or one from before it, delivered late.
		s.Store = false
		return s
	case time.Duration(uptime)*time.Second > now.Sub(prev.at)+rebootSlack:
		// Old status replayed: it may fill a free window, but the baseline stays.
		s.Store = !taken
		return s
	default:
		reboot = true
		s.next.bootUptime, s.next.boot = uptime, booted
	}
	s.next.uptime, s.next.at = uptime, now
	switch {
	case !taken:
		if prev.offGrid {
			s.At = now
		}
		s.next.bucket, s.next.offGrid = bucket, false
	case reboot && !prev.rebootBucket.Equal(bucket):
		s.At = now
		s.next.rebootBucket, s.next.offGrid = bucket, true
	default:
		s.Store = false
	}
	return s
}
