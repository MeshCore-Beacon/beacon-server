// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func TestPacketTimestampFormatsAndSkew(t *testing.T) {
	want := time.Now().UTC().Add(-10 * time.Minute).Truncate(time.Second).Add(123456789 * time.Nanosecond)
	capture := func(t *testing.T, timestamp string) time.Time {
		t.Helper()
		w, base := newTestWorker()
		db := &frameCaptureDB{stubDB: base}
		w.db = db
		var envelope map[string]string
		if err := json.Unmarshal(packetEnvelope(t, buildGrpTxtPacket(t, 0x1a, make([]byte, 16))), &envelope); err != nil {
			t.Fatal(err)
		}
		envelope["timestamp"] = timestamp
		body, err := json.Marshal(envelope)
		if err != nil {
			t.Fatal(err)
		}
		w.handlePacket(context.Background(), "YOW", strings.Repeat("01", 32), body)
		if len(db.observed) != 1 {
			t.Fatal("timestamp prevented packet storage")
		}
		return db.observed[0].HeardAt
	}
	for _, zone := range []*time.Location{time.UTC, time.FixedZone("zero", 0), time.FixedZone("west", -4*60*60), time.FixedZone("east", 19800)} {
		for _, precision := range []time.Duration{time.Second, time.Millisecond, time.Microsecond, time.Nanosecond} {
			expected := want.Truncate(precision)
			stamp := expected.In(zone).Format("2006-01-02T15:04:05.999999999-07:00")
			if zone == time.UTC {
				stamp = expected.Format(time.RFC3339Nano)
			}
			t.Run(stamp, func(t *testing.T) {
				if got := capture(t, stamp); !got.Equal(expected) {
					t.Fatalf("reported time changed: got %s want %s", got, expected)
				}
			})
		}
	}
	for _, precision := range []time.Duration{time.Second, time.Microsecond, time.Nanosecond} {
		expected := want.Truncate(precision)
		for _, layout := range []string{"2006-01-02T15:04:05.999999999", "2006-01-02 15:04:05.999999999"} {
			stamp := expected.Format(layout)
			t.Run("naive "+stamp, func(t *testing.T) {
				if got := capture(t, stamp); !got.Equal(expected) {
					t.Fatalf("timezone-less UTC changed: got %s want %s", got, expected)
				}
			})
		}
	}
	west := time.FixedZone("west", -4*60*60)
	for _, layout := range []string{
		"2006-01-02 15:04:05.999999-07:00", // Python str(datetime)
		"2006-01-02T15:04:05.999-0700",
		"2006-01-02 15:04:05-0700",
	} {
		stamp := want.Truncate(time.Microsecond).In(west).Format(layout)
		expected, _ := time.Parse(layout, stamp)
		t.Run(stamp, func(t *testing.T) {
			if got := capture(t, stamp); !got.Equal(expected) {
				t.Fatalf("reported time changed: got %s want %s", got, expected)
			}
		})
	}
	for _, stamp := range []string{"", "invalid", "2026-09-29T25:00:00+00:00", time.Now().Add(31 * time.Minute).Format(time.RFC3339), time.Now().Add(-31 * time.Minute).Format(time.RFC3339)} {
		t.Run("guard "+stamp, func(t *testing.T) {
			before := time.Now().UTC()
			got := capture(t, stamp)
			if got.Before(before) || got.After(time.Now().UTC()) {
				t.Fatalf("invalid/skewed time escaped server-time guard: %s => %s", stamp, got)
			}
		})
	}
}
