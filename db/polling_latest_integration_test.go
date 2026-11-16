// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func latestNode(t *testing.T, ctx context.Context, pool *pgxpool.Pool, key byte, iata string) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO nodes (public_key, node_type, name) VALUES ($1, 2, 'n') RETURNING id`, pollKey(key)).Scan(&id); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO iata_codes (iata) VALUES ($1) ON CONFLICT DO NOTHING`, iata); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO node_iatas (node_id, iata, last_heard) VALUES ($1, $2, now())`, id, iata); err != nil {
		t.Fatal(err)
	}
	return id
}

func latestReading(t *testing.T, ctx context.Context, pool *pgxpool.Pool, key byte, at time.Time, tag int, trusted bool, battery *int32, temp *float32) {
	t.Helper()
	if _, err := pool.Exec(ctx, `INSERT INTO repeater_readings (target_public_key, observer_public_key, sampled_at, received_at, method, tag, battery_mv, temperature_c, trusted)
		VALUES ($1, $2, $3, $3, 'status', $4, $5, $6, $7)`, pollKey(key), pollKey(0xEE), at, tag, battery, temp, trusted); err != nil {
		t.Fatal(err)
	}
}

func TestListLatestNodeReadingsPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	now := time.Now().UTC().Truncate(time.Millisecond)
	a := latestNode(t, ctx, pool, 0xA, "YVR")
	b := latestNode(t, ctx, pool, 0xB, "YVR")
	c := latestNode(t, ctx, pool, 0xC, "YYZ")
	d := latestNode(t, ctx, pool, 0xD, "YVR")
	bat := func(v int32) *int32 { return &v }
	tmp := func(v float32) *float32 { return &v }

	// A: battery 3h ago, temperature 1h ago, and a newer untrusted reading that must be ignored.
	latestReading(t, ctx, pool, 0xA, now.Add(-3*time.Hour), 1, true, bat(4080), nil)
	latestReading(t, ctx, pool, 0xA, now.Add(-time.Hour), 2, true, nil, tmp(1.5))
	latestReading(t, ctx, pool, 0xA, now.Add(-5*time.Minute), 3, false, bat(9999), nil)
	// B: only a reading older than the window.
	latestReading(t, ctx, pool, 0xB, now.Add(-8*24*time.Hour), 1, true, bat(3700), nil)
	// C: other IATA.
	latestReading(t, ctx, pool, 0xC, now.Add(-10*time.Minute), 1, true, bat(3900), nil)
	// D: trusted readings that carry neither value.
	latestReading(t, ctx, pool, 0xD, now.Add(-10*time.Minute), 1, true, nil, nil)
	_ = b

	since := now.Add(-7 * 24 * time.Hour)
	got, err := s.ListLatestNodeReadings(ctx, []string{"YVR"}, since)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].NodeID != a {
		t.Fatalf("YVR items: %+v", got)
	}
	r := got[0]
	if r.BatteryMV == nil || *r.BatteryMV != 4080 || r.TemperatureC == nil || *r.TemperatureC != 1.5 {
		t.Errorf("values: %+v", r)
	}
	if want := now.Add(-time.Hour).UnixMilli(); r.SampledAt != want {
		t.Errorf("sampledAt %d, want %d", r.SampledAt, want)
	}

	got, err = s.ListLatestNodeReadings(ctx, nil, since)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[uuid.UUID]bool{}
	for _, r := range got {
		ids[r.NodeID] = true
	}
	if len(got) != 2 || !ids[a] || !ids[c] || ids[d] {
		t.Fatalf("all items: %+v", got)
	}
}
