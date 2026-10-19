// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"fmt"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
)

// legacyGetStatsSeries is today's GetStatsSeries, verbatim from origin/dev's db/sqlc/series.sql.go.
const legacyGetStatsSeries = `-- name: GetStatsSeries :many
WITH hours AS (
    SELECT g.h::timestamptz AS hour, COALESCE(r.status, 'missing')::text AS status
    FROM generate_series($1::timestamptz, $2::timestamptz, INTERVAL '1 hour') g(h)
    LEFT JOIN analytics_rollup_hours r ON r.hour = g.h
    WHERE g.h < $2::timestamptz
), complete AS (
    SELECT hour FROM hours WHERE status = 'complete'
), obs AS (
    SELECT o.hour, o.iata, o.observation_count FROM analytics_hourly_iata_observations o JOIN complete c ON c.hour = o.hour
    WHERE COALESCE(cardinality($3::bpchar[]), 0) = 0
    UNION ALL
    SELECT o.hour, o.iata, o.observation_count FROM analytics_hourly_iata_observations o JOIN complete c ON c.hour = o.hour
    WHERE cardinality($3::bpchar[]) > 0 AND o.iata = ANY($3::bpchar[])
), obs_agg AS (
    SELECT hour, sum(observation_count)::bigint AS observations, count(DISTINCT iata)::bigint AS active_iatas
    FROM obs GROUP BY GROUPING SETS ((), (hour))
), pkt AS (
    SELECT s.hour, s.packets FROM analytics_hourly_packet_sets s JOIN complete c ON c.hour = s.hour
    WHERE COALESCE(cardinality($3::bpchar[]), 0) = 0
    UNION ALL
    SELECT s.hour, s.packets FROM analytics_hourly_packet_sets s JOIN complete c ON c.hour = s.hour
    WHERE cardinality($3::bpchar[]) > 0 AND s.iatas && $3::bpchar[]
), pkt_agg AS (
    SELECT hour, sum(packets)::bigint AS unique_packets FROM pkt GROUP BY GROUPING SETS ((), (hour))
), obsr AS (
    SELECT i.hour, i.observer_id FROM analytics_hourly_observer_identity i JOIN complete c ON c.hour = i.hour
    WHERE COALESCE(cardinality($3::bpchar[]), 0) = 0
    UNION ALL
    SELECT i.hour, i.observer_id FROM analytics_hourly_observer_identity i JOIN complete c ON c.hour = i.hour
    WHERE cardinality($3::bpchar[]) > 0 AND i.iata = ANY($3::bpchar[])
), obsr_agg AS (
    SELECT hour, count(DISTINCT observer_id)::bigint AS active_observers FROM obsr GROUP BY GROUPING SETS ((), (hour))
), scp AS (
    SELECT s.hour, s.scope_id, s.packets FROM analytics_hourly_scope_sets s JOIN complete c ON c.hour = s.hour
    WHERE COALESCE(cardinality($3::bpchar[]), 0) = 0
    UNION ALL
    SELECT s.hour, s.scope_id, s.packets FROM analytics_hourly_scope_sets s JOIN complete c ON c.hour = s.hour
    WHERE cardinality($3::bpchar[]) > 0 AND s.iatas && $3::bpchar[]
), scp_agg AS (
    SELECT hour, sum(packets)::bigint AS scoped_packets, count(DISTINCT scope_id)::bigint AS active_scopes
    FROM scp GROUP BY GROUPING SETS ((), (hour))
), pth AS (
    SELECT p.hour, p.entries FROM analytics_hourly_paths p JOIN complete c ON c.hour = p.hour
    WHERE p.category = 0 AND COALESCE(cardinality($3::bpchar[]), 0) = 0
    UNION ALL
    SELECT p.hour, p.entries FROM analytics_hourly_paths p JOIN complete c ON c.hour = p.hour
    WHERE p.category = 0 AND cardinality($3::bpchar[]) > 0 AND p.iata = ANY($3::bpchar[])
), pth_agg AS (
    SELECT hour, max(entries)::integer AS max_path_entries FROM pth GROUP BY GROUPING SETS ((), (hour))
), sig AS (
    SELECT s.hour, s.snr_sum, s.snr_samples, s.rssi_sum, s.rssi_samples FROM analytics_hourly_signal s JOIN complete c ON c.hour = s.hour
    WHERE s.kind = 3 AND COALESCE(cardinality($3::bpchar[]), 0) = 0
    UNION ALL
    SELECT s.hour, s.snr_sum, s.snr_samples, s.rssi_sum, s.rssi_samples FROM analytics_hourly_signal s JOIN complete c ON c.hour = s.hour
    WHERE s.kind = 3 AND cardinality($3::bpchar[]) > 0 AND s.iata = ANY($3::bpchar[])
), sig_agg AS (
    SELECT hour, sum(snr_sum)::double precision AS snr_sum, sum(snr_samples)::bigint AS snr_samples,
           sum(rssi_sum)::double precision AS rssi_sum, sum(rssi_samples)::bigint AS rssi_samples
    FROM sig GROUP BY GROUPING SETS ((), (hour))
), keys AS (
    SELECT hour, status FROM hours
    UNION ALL
    SELECT NULL::timestamptz, 'summary'::text
)
SELECT k.hour, k.status,
       COALESCE(o.observations, 0)::bigint AS observations,
       COALESCE(p.unique_packets, 0)::bigint AS unique_packets,
       COALESCE(ob.active_observers, 0)::bigint AS active_observers,
       COALESCE(o.active_iatas, 0)::bigint AS active_iatas,
       COALESCE(s.scoped_packets, 0)::bigint AS scoped_packets,
       COALESCE(s.active_scopes, 0)::bigint AS active_scopes,
       COALESCE(pa.max_path_entries, 0)::integer AS max_path_entries,
       COALESCE(sg.snr_sum, 0)::double precision AS snr_sum,
       COALESCE(sg.snr_samples, 0)::bigint AS snr_samples,
       COALESCE(sg.rssi_sum, 0)::double precision AS rssi_sum,
       COALESCE(sg.rssi_samples, 0)::bigint AS rssi_samples
FROM keys k
LEFT JOIN obs_agg o ON o.hour IS NOT DISTINCT FROM k.hour
LEFT JOIN pkt_agg p ON p.hour IS NOT DISTINCT FROM k.hour
LEFT JOIN obsr_agg ob ON ob.hour IS NOT DISTINCT FROM k.hour
LEFT JOIN scp_agg s ON s.hour IS NOT DISTINCT FROM k.hour
LEFT JOIN pth_agg pa ON pa.hour IS NOT DISTINCT FROM k.hour
LEFT JOIN sig_agg sg ON sg.hour IS NOT DISTINCT FROM k.hour
ORDER BY k.hour NULLS LAST
`

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
	tstz := func(t time.Time) pgtype.Timestamptz { return pgtype.Timestamptz{Time: t, Valid: true} }

	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	q := sqlc.New(tx)
	nonEmpty := map[string]int{}

	for _, l := range lists {
		for _, s := range sinces {
			name := l.name + "/" + s.name
			for _, limit := range []int32{1, 10, 200} {
				want, err := q.GetTopNodes(ctx, sqlc.GetTopNodesParams{Since: tstz(s.at), Iatas: l.iatas, RowLimit: limit})
				if err != nil {
					t.Fatal(err)
				}
				probe, err := q.GetTopNodesProbe(ctx, sqlc.GetTopNodesProbeParams{Since: tstz(s.at), Iatas: l.iatas, RowLimit: limit})
				if err != nil {
					t.Fatal(err)
				}
				got := make([]sqlc.GetTopNodesRow, len(probe))
				for i, r := range probe {
					got[i] = sqlc.GetTopNodesRow(r)
				}
				if !sameRows(got, want) {
					t.Errorf("top nodes %s limit %d:\nprobe %+v\ntoday %+v", name, limit, got, want)
				}
				if len(want) > 0 {
					nonEmpty["top nodes"]++
				}
				for _, sort := range []string{"flood", "direct", "bogus"} {
					want, err := q.GetStatsTopAdvertisers(ctx, sqlc.GetStatsTopAdvertisersParams{Sort: sort, Since: tstz(s.at), Iatas: l.iatas, RowLimit: limit})
					if err != nil {
						t.Fatal(err)
					}
					probe, err := q.GetStatsTopAdvertisersProbe(ctx, sqlc.GetStatsTopAdvertisersProbeParams{Sort: sort, Since: tstz(s.at), Iatas: l.iatas, RowLimit: limit})
					if err != nil {
						t.Fatal(err)
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
			}
			for _, until := range []time.Time{newest.Add(time.Hour), s.at.Add(time.Hour)} {
				want, err := q.GetPathStats(ctx, sqlc.GetPathStatsParams{Since: tstz(s.at), Until: tstz(until), Iatas: l.iatas})
				if err != nil {
					t.Fatal(err)
				}
				probe, err := q.GetPathStatsProbe(ctx, sqlc.GetPathStatsProbeParams{Since: tstz(s.at), Until: tstz(until), Iatas: l.iatas})
				if err != nil {
					t.Fatal(err)
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
				series, err := q.GetStatsSeries(ctx, sqlc.GetStatsSeriesParams{Since: tstz(s.at), Until: tstz(until), Iatas: l.iatas})
				if err != nil {
					t.Fatal(err)
				}
				today := legacyRows[sqlc.GetStatsSeriesRow](t, ctx, tx, legacyGetStatsSeries, tstz(s.at), tstz(until), l.iatas)
				if !sameRows(series, today) {
					t.Errorf("series %s until %s:\nnew   %+v\ntoday %+v", name, until, series, today)
				}
				if len(today) > 0 && today[len(today)-1].UniquePackets > 0 && today[len(today)-1].ScopedPackets > 0 {
					nonEmpty["series"]++
				}
			}
		}
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
}
