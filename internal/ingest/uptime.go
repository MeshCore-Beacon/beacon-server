// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"sync"
	"time"
)

// rebootSlack allows for the delay between a device reading its uptime and the status arriving.
const rebootSlack = 30 * time.Second

type uptimeSeen struct {
	uptime int64
	at     time.Time
	bucket time.Time // the resolution window the last snapshot went to
}

// UptimeTracker remembers each observer's last reported uptime, so a reboot between two status messages is stored
// even when the resolution window already has its snapshot. Share one between the workers of every broker: the
// same status arrives on each.
type UptimeTracker struct {
	mu   sync.Mutex
	seen map[string]uptimeSeen
}

func NewUptimeTracker() *UptimeTracker {
	return &UptimeTracker{seen: map[string]uptimeSeen{}}
}

// SnapshotTime returns the reported_at to store a status under: the start of its resolution window, or now when
// the observer rebooted since its last status and that window is already taken. A lower uptime that is longer than
// the time since the last status is an old status replayed, not a reboot, and is ignored.
func (t *UptimeTracker) SnapshotTime(observer string, uptime int64, now time.Time, resolution time.Duration) time.Time {
	bucket := now.Truncate(resolution)
	if t == nil {
		return bucket
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	prev, ok := t.seen[observer]
	at := bucket
	if ok && uptime < prev.uptime {
		if time.Duration(uptime)*time.Second > now.Sub(prev.at)+rebootSlack {
			return bucket
		}
		if prev.bucket.Equal(bucket) {
			at = now
		}
	}
	t.seen[observer] = uptimeSeen{uptime: uptime, at: now, bucket: bucket}
	return at
}
