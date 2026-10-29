// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"math"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
)

// A device clock more than about 68 years off stores its full drift: an advert timestamp of math.MaxUint32 is about
// 2.5e9 s ahead, past int32, and timestamp 1 about 1.8e9 s behind. The node page and the clock-drift stats report
// both exactly, worst first.
func TestClockDriftWideValuesPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	store := New(pool, pool, 5*time.Minute, time.Hour)
	ahead, err := store.UpsertNode(ctx, ingest.UpsertNodeParams{PublicKey: []byte{0xa1}, NodeType: 2, Name: "ahead",
		AdvertTimestamp: math.MaxUint32}, ingest.RadioSettings{})
	if err != nil {
		t.Fatal(err)
	}
	behind, err := store.UpsertNode(ctx, ingest.UpsertNodeParams{PublicKey: []byte{0xb2}, NodeType: 3, Name: "behind",
		AdvertTimestamp: 1}, ingest.RadioSettings{})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().Unix()
	near := func(got, want int64) bool { return got >= want-60 && got <= want+60 }

	for _, c := range []struct {
		name string
		id   uuid.UUID
		want int64
	}{{"ahead", ahead, math.MaxUint32 - now}, {"behind", behind, 1 - now}} {
		var stored int64
		if err := pool.QueryRow(ctx, "SELECT device_clock_drift_seconds FROM nodes WHERE id = $1", c.id).Scan(&stored); err != nil {
			t.Fatal(err)
		}
		if !near(stored, c.want) {
			t.Errorf("%s: stored drift %d, want about %d", c.name, stored, c.want)
		}
		node, err := store.GetNode(ctx, c.id)
		if err != nil {
			t.Fatal(err)
		}
		if node.ClockDriftSeconds == nil || !near(int64(*node.ClockDriftSeconds), c.want) || node.ClockOutOfSync == nil || !*node.ClockOutOfSync {
			t.Errorf("%s: node drift %v out of sync %v, want about %d and true", c.name, node.ClockDriftSeconds, node.ClockOutOfSync, c.want)
		}
	}

	entries, err := store.GetStatsClockDrift(ctx, nil, 10)
	if err != nil {
		t.Fatal(err)
	}
	name := func(e int) string {
		if e >= len(entries) || entries[e].NodeName == nil {
			return ""
		}
		return *entries[e].NodeName
	}
	if len(entries) != 2 || name(0) != "ahead" || !near(int64(entries[0].ClockDriftSeconds), math.MaxUint32-now) ||
		name(1) != "behind" || !near(int64(entries[1].ClockDriftSeconds), 1-now) {
		t.Errorf("clock drift stats %+v, want ahead (about %d) then behind (about %d)", entries, math.MaxUint32-now, 1-now)
	}
}
