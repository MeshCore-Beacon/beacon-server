// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"testing"
	"time"
)

func TestUptimeTrackerSnapshotTime(t *testing.T) {
	const res = 15 * time.Minute
	base := time.Date(2026, 11, 26, 10, 30, 15, 0, time.UTC)
	bucket := base.Truncate(res)

	t.Run("steady reports stay on the bucket", func(t *testing.T) {
		tr := NewUptimeTracker()
		for i, up := range []int64{25216, 25516, 25816} {
			now := base.Add(time.Duration(i) * 5 * time.Minute)
			if got := tr.SnapshotTime("a", up, now, res); !got.Equal(bucket) {
				t.Fatalf("report %d stored at %s, want %s", i, got, bucket)
			}
		}
	})

	t.Run("a reboot inside a taken bucket is stored at its own time", func(t *testing.T) {
		tr := NewUptimeTracker()
		tr.SnapshotTime("a", 25216, base, res)
		now := base.Add(5 * time.Minute)
		got := tr.SnapshotTime("a", 20, now, res)
		if !got.Equal(now) {
			t.Fatalf("reboot stored at %s, want %s", got, now)
		}
		// The same status from the second broker, and the next report, dedupe as usual.
		if got := tr.SnapshotTime("a", 20, now.Add(time.Second), res); !got.Equal(bucket) {
			t.Fatalf("duplicate stored at %s, want %s", got, bucket)
		}
		if got := tr.SnapshotTime("a", 320, now.Add(5*time.Minute), res); !got.Equal(bucket) {
			t.Fatalf("next report stored at %s, want %s", got, bucket)
		}
	})

	t.Run("a reboot into a free bucket takes the bucket", func(t *testing.T) {
		tr := NewUptimeTracker()
		tr.SnapshotTime("a", 25216, base, res)
		now := base.Add(16 * time.Minute)
		if got := tr.SnapshotTime("a", 20, now, res); !got.Equal(now.Truncate(res)) {
			t.Fatalf("reboot stored at %s, want %s", got, now.Truncate(res))
		}
	})

	t.Run("a replayed old status is not a reboot", func(t *testing.T) {
		tr := NewUptimeTracker()
		tr.SnapshotTime("a", 25216, base, res)
		now := base.Add(5 * time.Minute)
		// Uptime far above the time since the last report: the device cannot have booted in between.
		if got := tr.SnapshotTime("a", 9000, now, res); !got.Equal(bucket) {
			t.Fatalf("replay stored at %s, want %s", got, bucket)
		}
		// The replay did not move the baseline, so a real reboot is still seen.
		now = now.Add(5 * time.Minute)
		if got := tr.SnapshotTime("a", 20, now, res); !got.Equal(now) {
			t.Fatalf("reboot after a replay stored at %s, want %s", got, now)
		}
	})

	t.Run("observers are tracked apart", func(t *testing.T) {
		tr := NewUptimeTracker()
		tr.SnapshotTime("a", 25216, base, res)
		if got := tr.SnapshotTime("b", 20, base.Add(time.Minute), res); !got.Equal(bucket) {
			t.Fatalf("first report of b stored at %s, want %s", got, bucket)
		}
	})
}
