// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"fmt"
	"io/fs"
	"testing"
	"time"

	pgdata "github.com/MeshCore-Beacon/beacon-server/db/planguard"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// todaySeriesSQL is today's GetStatsSeries, the frozen text the plan corpus keeps as a planted regression.
func todaySeriesSQL(t *testing.T) string {
	t.Helper()
	b, err := fs.ReadFile(pgdata.Files, "sql/planted/GetStatsSeries_dba66ca.sql")
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// statsProbeFixture spreads packets of types 2/4/5/9 over the 30 hours [now-33h, now-3h) (UTC hours):
// 36 IATAs P00..P35, 40 observers, 60 advert origins of which 10 have no nodes row. P35 is heard
// only in the two oldest hours.
const statsProbeFixture = `
INSERT INTO iata_codes (iata) SELECT 'P' || lpad(i::text, 2, '0') FROM generate_series(0, 35) i;
INSERT INTO transport_scopes (id, name, transport_key, key_fingerprint) VALUES
 (1, 'scope-one', decode(repeat('01', 16), 'hex'), decode(repeat('01', 8), 'hex')),
 (2, 'scope-two', decode(repeat('02', 16), 'hex'), decode(repeat('02', 8), 'hex'));
INSERT INTO observers (id, public_key, display_name)
SELECT ('00000000-0000-0000-0000-' || lpad(to_hex(i + 1), 12, '0'))::uuid, decode(lpad(to_hex(i + 1), 2, '0'), 'hex'), 'obs ' || i
FROM generate_series(0, 39) i;
INSERT INTO nodes (public_key, node_type, name)
SELECT decode(repeat(to_hex(k + 16), 32), 'hex'), 1 + k % 4, CASE WHEN k % 7 <> 0 THEN 'node ' || k END
FROM generate_series(0, 49) k;
INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, origin_pubkey, scope_id, raw_payload, raw_header, first_heard_at, last_heard_at)
SELECT int4send(j), pt, 0, CASE WHEN pt = 4 AND (j / 4) % 3 = 0 THEN 2 ELSE 1 END,
       CASE WHEN pt = 4 THEN decode(repeat(to_hex((j / 4) % 60 + 16), 32), 'hex') END,
       CASE WHEN (j / 30) % 3 = 0 THEN 1 + (j / 90) % 2 END, '\x00', '\x00', $1::timestamptz, $1::timestamptz
FROM (SELECT j, (ARRAY[2, 4, 5, 9])[j % 4 + 1] AS pt FROM generate_series(0, 2399) j) p;
INSERT INTO packet_observations (id, packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count, path_bytes, rssi, snr, airtime_ms, payload_type)
SELECT row_number() OVER (ORDER BY j, k), int4send(j),
       ('00000000-0000-0000-0000-' || lpad(to_hex((j * 7 + k * 13) % 40 + 1), 12, '0'))::uuid,
       CASE WHEN k = 9 THEN 'P35' ELSE 'P' || lpad(((j * 11 + k * 5) % 35)::text, 2, '0') END,
       $1::timestamptz + (j % 30) * interval '1 hour' + ((j * (k + 1)) % 60) * interval '1 minute',
       ((hs - 1) << 6) | hc, hs, hc, CASE WHEN hc > 0 THEN decode(repeat('ab', hs * hc), 'hex') END,
       -(60 + j % 50), (j % 20) - 10, 10 + j % 7, (ARRAY[2, 4, 5, 9])[j % 4 + 1]
FROM generate_series(0, 2399) j
CROSS JOIN LATERAL (SELECT k FROM generate_series(0, j % 3) k UNION ALL SELECT 9 WHERE j % 30 IN (0, 1) AND j < 600) ks
CROSS JOIN LATERAL (SELECT 1 + j % 3 AS hs, (j + k) % 5 AS hc) path;
`

func TestStatsProbeMatchesTodayPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var now time.Time // date_trunc('hour', now(), 'UTC')
	if err := pool.QueryRow(ctx, "SELECT date_trunc('hour', now(), 'UTC')").Scan(&now); err != nil {
		t.Fatal(err)
	}
	first, newest := now.Add(-33*time.Hour), now.Add(-4*time.Hour)
	if _, err := pool.Exec(ctx, statsProbeFixture, pgx.QueryExecModeSimpleProtocol, first); err != nil {
		t.Fatal(err)
	}
	store := New(pool, pool, 0, 0)
	r, ok, err := store.BeginRollup(ctx)
	if err != nil || !ok {
		t.Fatalf("BeginRollup: ok=%v err=%v", ok, err)
	}
	if err := r.RegisterHours(ctx, first); err != nil {
		t.Fatal(err)
	}
	rollAll(t, ctx, r)
	r.Close(ctx)
	// One partial and one missing hour for the series; one hearing row at the current UTC hour.
	if _, err := pool.Exec(ctx, `
UPDATE analytics_rollup_hours SET status = 'partial' WHERE hour = $1;
DELETE FROM analytics_rollup_hours WHERE hour = $2;
INSERT INTO analytics_hourly_advert_hearings (hour, iata, origin_pubkey, observations, last_heard, name, node_type)
VALUES ($3, 'P00', decode(repeat('10', 32), 'hex'), 500, $3::timestamptz + interval '1 minute', 'current', 2);`,
		pgx.QueryExecModeSimpleProtocol, first.Add(10*time.Hour), first.Add(20*time.Hour), now); err != nil {
		t.Fatal(err)
	}

	code := func(i int) string { return fmt.Sprintf("P%02d", i) }
	codes := func(n int) []string {
		out := make([]string, n)
		for i := range out {
			out[i] = code(i)
		}
		return out
	}
	lists := []struct {
		name  string
		iatas []string
	}{
		{"nil", nil}, {"empty", []string{}}, {"empty region", []string{""}}, {"one", codes(1)},
		{"6", codes(6)}, {"22", codes(22)}, {"32", codes(32)},
		{"duplicates and a trailing blank", []string{"P01", "P02", "P01", "P02 ", "P03"}},
		{"lowercase", []string{"p01"}}, {"unknown", []string{"ZZZ"}}, {"only outside the window", []string{"P35"}},
	}
	sinces := []struct {
		name string
		at   time.Time
	}{
		{"before the first rollup hour", first.Add(-7 * time.Hour)}, {"-1d", now.Add(-24 * time.Hour)},
		{"-7d", now.Add(-7 * 24 * time.Hour)}, {"-14d", now.Add(-14 * 24 * time.Hour)},
		{"newest rolled hour", newest}, {"current hour", now}, {"+2h", now.Add(2 * time.Hour)},
	}
	// Unbounded starts an API caller can send: since=0 and an empty since (Go's zero time). Both are
	// on the hour, so the store picks the probe; the probes must not walk from them.
	wideSinces := []struct {
		name string
		at   time.Time
	}{{"since=0", time.UnixMilli(0).UTC()}, {"zero since", time.Time{}}}
	farUntil := time.Date(9999, 12, 31, 0, 0, 0, 0, time.UTC)
	tstz := func(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	// A probe that walked from year 1 or 1970 would run for minutes; this turns it into an error.
	if _, err := tx.Exec(ctx, "SET LOCAL statement_timeout = '10s'"); err != nil {
		t.Fatal(err)
	}
	q := sqlc.New(tx)
	nonEmpty := map[string]int{}

	topNodes := func(name string, since time.Time, iatas []string, limit int32) {
		t.Helper()
		want, err := q.GetTopNodes(ctx, sqlc.GetTopNodesParams{Since: tstz(since), Iatas: iatas, RowLimit: limit})
		if err != nil {
			t.Fatal(err)
		}
		// The store's own pick: GetTopNodesAll for every IATA, else the probe.
		got, err := topNodesRows(ctx, q, iatas, tstz(since), limit)
		if err != nil {
			t.Fatalf("top nodes probe %s limit %d: %v", name, limit, err)
		}
		if !sameRows(got, want) {
			t.Errorf("top nodes %s limit %d:\nprobe %+v\ntoday %+v", name, limit, got, want)
		}
		if len(want) > 0 {
			nonEmpty["top nodes"]++
		}
	}
	advertisers := func(name string, since time.Time, iatas []string, limit int32, sort string) {
		t.Helper()
		want, err := q.GetStatsTopAdvertisers(ctx, sqlc.GetStatsTopAdvertisersParams{Sort: sort, Since: tstz(since), Iatas: iatas, RowLimit: limit})
		if err != nil {
			t.Fatal(err)
		}
		probe, err := q.GetStatsTopAdvertisersProbe(ctx, sqlc.GetStatsTopAdvertisersProbeParams{Sort: sort, Since: tstz(since), Iatas: iatas, RowLimit: limit})
		if err != nil {
			t.Fatalf("top advertisers probe %s limit %d sort %s: %v", name, limit, sort, err)
		}
		got := make([]sqlc.GetStatsTopAdvertisersRow, len(probe))
		for i, r := range probe {
			got[i] = sqlc.GetStatsTopAdvertisersRow(r)
		}
		if !sameRows(got, want) {
			t.Errorf("top advertisers %s limit %d sort %s:\nprobe %+v\ntoday %+v", name, limit, sort, got, want)
		}
		if len(want) > 0 {
			nonEmpty["top advertisers"]++
		}
	}
	paths := func(name string, since, until time.Time, iatas []string) {
		t.Helper()
		want, err := q.GetPathStats(ctx, sqlc.GetPathStatsParams{Since: tstz(since), Until: tstz(until), Iatas: iatas})
		if err != nil {
			t.Fatal(err)
		}
		probe, err := q.GetPathStatsProbe(ctx, sqlc.GetPathStatsProbeParams{Since: tstz(since), Until: tstz(until), Iatas: iatas})
		if err != nil {
			t.Fatalf("path stats probe %s until %s: %v", name, until, err)
		}
		got := make([]sqlc.GetPathStatsRow, len(probe))
		for i, r := range probe {
			got[i] = sqlc.GetPathStatsRow(r)
		}
		if !sameRows(got, want) {
			t.Errorf("path stats %s until %s:\nprobe %+v\ntoday %+v", name, until, got, want)
		}
		if len(want) > 0 {
			nonEmpty["path stats"]++
		}
	}
	todaySeries := todaySeriesSQL(t)
	series := func(name string, since, until time.Time, iatas []string) {
		t.Helper()
		got, err := q.GetStatsSeries(ctx, sqlc.GetStatsSeriesParams{Since: tstz(since), Until: tstz(until), Iatas: iatas})
		if err != nil {
			t.Fatal(err)
		}
		today := legacyRows[sqlc.GetStatsSeriesRow](t, ctx, tx, todaySeries, tstz(since), tstz(until), iatas)
		if !sameRows(got, today) {
			t.Errorf("series %s %s..%s:\nnew   %+v\ntoday %+v", name, since, until, got, today)
		}
		if len(today) > 0 && today[len(today)-1].UniquePackets > 0 && today[len(today)-1].ScopedPackets > 0 {
			nonEmpty["series"]++
		}
	}
	grid := func(sinces []struct {
		name string
		at   time.Time
	}, untils func(time.Time) []time.Time, withSeries bool) {
		for _, l := range lists {
			for _, s := range sinces {
				name := l.name + "/" + s.name
				for _, limit := range []int32{1, 10, 200} {
					topNodes(name, s.at, l.iatas, limit)
					for _, sort := range []string{"flood", "direct", "bogus"} {
						advertisers(name, s.at, l.iatas, limit, sort)
					}
				}
				for _, until := range untils(s.at) {
					paths(name, s.at, until, l.iatas)
					if withSeries {
						series(name, s.at, until, l.iatas)
						// The series also serves windows that do not start on the hour.
						series(name+" +30m", s.at.Add(30*time.Minute), until, l.iatas)
					}
				}
			}
		}
	}

	grid(sinces, func(at time.Time) []time.Time { return []time.Time{newest.Add(time.Hour), at.Add(time.Hour)} }, true)
	// The series has no probe and walks its own window, so the unbounded starts only run the probes.
	grid(wideSinces, func(time.Time) []time.Time { return []time.Time{newest.Add(time.Hour), farUntil} }, false)
	for _, s := range sinces {
		paths("far-future until/"+s.name, s.at, farUntil, codes(6))
	}
	// The comparisons above must cover real rows, not only empty results.
	for _, k := range []string{"top nodes", "top advertisers", "path stats", "series"} {
		if nonEmpty[k] < 20 {
			t.Errorf("%s: only %d non-empty comparisons", k, nonEmpty[k])
		}
	}

	// Both forms read the hearing at the current UTC hour.
	cur, err := q.GetTopNodesProbe(ctx, sqlc.GetTopNodesProbeParams{Since: tstz(now), Iatas: []string{"P00"}, RowLimit: 10})
	if err != nil || len(cur) != 1 || cur[0].ObservationCount != 500 {
		t.Errorf("current-hour hearing through the probe: %+v, %v", cur, err)
	}
	// The window holds more than 32 IATAs' worth of rows, and limit 200 returns past the first ten.
	wide, err := q.GetTopNodesProbe(ctx, sqlc.GetTopNodesProbeParams{Since: tstz(first), Iatas: codes(32), RowLimit: 200})
	if err != nil || len(wide) < 50 {
		t.Errorf("32-code top nodes: %d rows, %v", len(wide), err)
	}
	var partial, missing int
	if err := tx.QueryRow(ctx, "SELECT count(*) FILTER (WHERE status = 'partial'), 30 - count(*) FILTER (WHERE hour < $1) FROM analytics_rollup_hours WHERE hour >= $2",
		newest.Add(time.Hour), first).Scan(&partial, &missing); err != nil || partial != 1 || missing != 1 {
		t.Errorf("series fixture: %d partial and %d missing hours (%v), want 1 and 1", partial, missing, err)
	}

	// A fresh deployment: empty rollup tables. The probes walk no hours, whatever the window.
	if _, err := tx.Exec(ctx, "DELETE FROM analytics_hourly_advert_hearings; DELETE FROM analytics_hourly_paths"); err != nil {
		t.Fatal(err)
	}
	for _, s := range append(wideSinces, sinces...) {
		for _, l := range lists {
			name := "empty rollups/" + l.name + "/" + s.name
			start := time.Now()
			topNodes(name, s.at, l.iatas, 10)
			advertisers(name, s.at, l.iatas, 10, "flood")
			paths(name, s.at, farUntil, l.iatas)
			if d := time.Since(start); d > 2*time.Second {
				t.Errorf("%s: %s, want the probes to return at once", name, d)
			}
		}
	}
}
