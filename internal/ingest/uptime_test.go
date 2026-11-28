// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"testing"
	"time"
)

// row is one stored telemetry snapshot.
type row struct {
	at     time.Time
	uptime int64
}

// feed runs one status through the tracker as the handler does, with an insert that works.
func feed(tr *UptimeTracker, rows *[]row, observer string, uptime int64, now time.Time, res time.Duration) {
	s := tr.Begin(observer, uptime, now, res)
	if s.Store {
		dup := false
		for _, r := range *rows {
			dup = dup || r.at.Equal(s.At) // ON CONFLICT DO NOTHING
		}
		if !dup {
			*rows = append(*rows, row{s.At, uptime})
		}
	}
	s.Done(true)
}

// perWindow counts the stored rows in each window.
func perWindow(rows []row, res time.Duration) map[time.Time]int {
	out := map[time.Time]int{}
	for _, r := range rows {
		out[r.at.Truncate(res)]++
	}
	return out
}

// drops counts the uptime decreases between stored rows: what the reboot alert fires on.
func drops(rows []row) int {
	n := 0
	for i := 1; i < len(rows); i++ {
		if rows[i].uptime < rows[i-1].uptime {
			n++
		}
	}
	return n
}

func at(hhmmss string) time.Time {
	t, err := time.Parse("2006-01-02 15:04:05", "2026-11-26 "+hhmmss)
	if err != nil {
		panic(err)
	}
	return t
}

func TestUptimeTracker(t *testing.T) {
	const res = 15 * time.Minute

	t.Run("steady reports give one row per window", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		feed(tr, &rows, "a", 25216, at("10:30:15"), res)
		feed(tr, &rows, "a", 25516, at("10:35:15"), res)
		feed(tr, &rows, "a", 25816, at("10:40:15"), res)
		feed(tr, &rows, "a", 26116, at("10:45:15"), res)
		if len(rows) != 2 || !rows[0].at.Equal(at("10:30:00")) || !rows[1].at.Equal(at("10:45:00")) {
			t.Fatalf("rows = %v", rows)
		}
	})

	t.Run("a reboot inside a taken window is stored at its own time", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		feed(tr, &rows, "a", 25216, at("10:30:15"), res)
		feed(tr, &rows, "a", 20, at("10:35:15"), res)
		feed(tr, &rows, "a", 20, at("10:35:16"), res) // the second broker's copy
		feed(tr, &rows, "a", 320, at("10:40:15"), res)
		if len(rows) != 2 || !rows[1].at.Equal(at("10:35:15")) || rows[1].uptime != 20 {
			t.Fatalf("rows = %v", rows)
		}
	})

	t.Run("a reboot into a free window takes the window", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		feed(tr, &rows, "a", 25216, at("10:30:15"), res)
		feed(tr, &rows, "a", 20, at("10:46:15"), res)
		if len(rows) != 2 || !rows[1].at.Equal(at("10:45:00")) {
			t.Fatalf("rows = %v", rows)
		}
	})

	t.Run("a replayed old status is not a reboot", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		feed(tr, &rows, "a", 25216, at("10:30:15"), res)
		// Uptime far above the time since the last report: the device cannot have booted in between.
		feed(tr, &rows, "a", 9000, at("10:35:15"), res)
		if len(rows) != 1 {
			t.Fatalf("replay stored: %v", rows)
		}
		// The replay did not move the baseline, so a real reboot is still seen.
		feed(tr, &rows, "a", 20, at("10:40:15"), res)
		if len(rows) != 2 || !rows[1].at.Equal(at("10:40:15")) {
			t.Fatalf("rows = %v", rows)
		}
	})

	t.Run("observers are tracked apart", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		feed(tr, &rows, "a", 25216, at("10:30:15"), res)
		s := tr.Begin("b", 20, at("10:31:15"), res)
		if !s.Store || !s.At.Equal(at("10:30:00")) {
			t.Fatalf("first report of b: %+v", s)
		}
		s.Done(true)
	})

	t.Run("falling uptimes in one window store one reboot", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		feed(tr, &rows, "a", 25216, at("10:30:05"), res)
		now := at("10:30:35")
		for up := int64(30); up > 0; up-- { // 30, 29, 28 ... every 20 s
			feed(tr, &rows, "a", up, now, res)
			now = now.Add(20 * time.Second)
		}
		if len(rows) != 2 || !rows[1].at.Equal(at("10:30:35")) {
			t.Fatalf("want the window row and one reboot row, got %v", rows)
		}
	})

	t.Run("two reboots in one window store one immediate row", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		feed(tr, &rows, "a", 25216, at("10:30:05"), res)
		feed(tr, &rows, "a", 20, at("10:31:00"), res)
		feed(tr, &rows, "a", 320, at("10:36:00"), res)
		feed(tr, &rows, "a", 25, at("10:44:00"), res) // not a copy: the first boot never reported 25
		if len(rows) != 2 || !rows[1].at.Equal(at("10:31:00")) {
			t.Fatalf("want the window row and one reboot row, got %v", rows)
		}
		// The capped reboot still moved the baseline: the next window carries on from it.
		feed(tr, &rows, "a", 145, at("10:46:00"), res)
		if len(rows) != 3 || rows[2].uptime != 145 {
			t.Fatalf("rows = %v", rows)
		}
	})

	t.Run("a boot loop stores at most one immediate row per window", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		start := at("00:00:10")
		feed(tr, &rows, "a", 25216, start, res)
		// Boots every 4 minutes for a day, reporting at 20 s and 200 s of uptime.
		for boot := start.Add(time.Minute); boot.Before(start.Add(24 * time.Hour)); boot = boot.Add(4 * time.Minute) {
			feed(tr, &rows, "a", 20, boot.Add(20*time.Second), res)
			feed(tr, &rows, "a", 200, boot.Add(200*time.Second), res)
		}
		for w, n := range perWindow(rows, res) {
			if n > 2 { // the window's snapshot and one reboot
				t.Fatalf("window %s has %d rows", w, n)
			}
		}
		if n := drops(rows); n == 0 || n > 97 {
			t.Fatalf("%d reboots stored in a day of %d windows", n, 97)
		}
	})

	t.Run("a late copy of the reboot status is not a second reboot", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		feed(tr, &rows, "a", 10000, at("10:30:00"), res)
		feed(tr, &rows, "a", 20, at("10:40:00"), res)
		feed(tr, &rows, "a", 320, at("10:45:00"), res)
		feed(tr, &rows, "a", 20, at("10:45:05"), res) // the 10:40 status again, from the second broker
		feed(tr, &rows, "a", 620, at("10:50:00"), res)
		want := []row{{at("10:30:00"), 10000}, {at("10:40:00"), 20}, {at("10:45:00"), 320}}
		if len(rows) != len(want) {
			t.Fatalf("rows = %v", rows)
		}
		for i := range want {
			if !rows[i].at.Equal(want[i].at) || rows[i].uptime != want[i].uptime {
				t.Fatalf("row %d = %v, want %v", i, rows[i], want[i])
			}
		}
		if drops(rows) != 1 {
			t.Fatalf("%d reboots stored, want 1", drops(rows))
		}
	})

	t.Run("a late copy first into the next window is dropped", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		feed(tr, &rows, "a", 10000, at("10:30:00"), res)
		feed(tr, &rows, "a", 20, at("10:40:00"), res)
		feed(tr, &rows, "a", 260, at("10:44:00"), res)
		feed(tr, &rows, "a", 20, at("10:45:05"), res) // would take the free 10:45 window and read as a reboot
		feed(tr, &rows, "a", 560, at("10:49:00"), res)
		if len(rows) != 3 || rows[2].uptime != 560 || drops(rows) != 1 {
			t.Fatalf("rows = %v", rows)
		}
	})

	t.Run("an older status of the same boot is dropped too", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		feed(tr, &rows, "a", 10000, at("10:30:00"), res)
		feed(tr, &rows, "a", 20, at("10:40:00"), res)
		feed(tr, &rows, "a", 320, at("10:45:00"), res)
		feed(tr, &rows, "a", 12, at("10:45:05"), res)
		if len(rows) != 3 || drops(rows) != 1 {
			t.Fatalf("rows = %v", rows)
		}
	})

	t.Run("a failed insert leaves the reboot for the next delivery", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		feed(tr, &rows, "a", 10000, at("10:30:00"), res)
		s := tr.Begin("a", 20, at("10:40:00"), res)
		if !s.Store || !s.At.Equal(at("10:40:00")) {
			t.Fatalf("reboot: %+v", s)
		}
		s.Done(false)
		s = tr.Begin("a", 20, at("10:40:01"), res)
		if !s.Store || !s.At.Equal(at("10:40:01")) {
			t.Fatalf("second delivery: %+v", s)
		}
		s.Done(true)
		if s = tr.Begin("a", 20, at("10:40:02"), res); s.Store {
			t.Fatalf("third delivery stored again: %+v", s)
		}
		s.Done(true)
	})

	t.Run("the row after an immediate reboot row is stamped on arrival", func(t *testing.T) {
		tr := NewUptimeTracker()
		var rows []row
		feed(tr, &rows, "a", 10000, at("10:34:30"), res)
		feed(tr, &rows, "a", 20, at("10:44:30"), res)
		feed(tr, &rows, "a", 320, at("10:49:30"), res)
		feed(tr, &rows, "a", 320, at("10:49:31"), res) // second broker
		feed(tr, &rows, "a", 620, at("10:54:30"), res)
		feed(tr, &rows, "a", 1220, at("11:04:30"), res)
		want := []time.Time{at("10:30:00"), at("10:44:30"), at("10:49:30"), at("11:00:00")}
		if len(rows) != len(want) {
			t.Fatalf("rows = %v", rows)
		}
		for i := range want {
			if !rows[i].at.Equal(want[i]) {
				t.Fatalf("row %d at %s, want %s", i, rows[i].at, want[i])
			}
		}
		// The stored gap after the reboot covers the uptime that passed in it.
		if gap, grew := rows[2].at.Sub(rows[1].at), rows[2].uptime-rows[1].uptime; gap != time.Duration(grew)*time.Second {
			t.Fatalf("gap %s for %d s of uptime", gap, grew)
		}
	})
}
