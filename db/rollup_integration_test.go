// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// rollupFixture loads observations across two hours, h0 and h0+1h, and returns h0.
// h0+2h is left empty so a zero-observation hour gets registered too.
func rollupFixture(t *testing.T, ctx context.Context, pool *pgxpool.Pool) time.Time {
	t.Helper()
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	var h0 time.Time
	if err := pool.QueryRow(ctx, "SELECT date_trunc('hour', now(), 'UTC') - INTERVAL '4 hours'").Scan(&h0); err != nil {
		t.Fatal(err)
	}
	_, err := pool.Exec(ctx, `
INSERT INTO iata_codes (iata) VALUES ('YVR'), ('YYZ'), ('YOW');
INSERT INTO transport_scopes (id, name, transport_key, key_fingerprint)
VALUES (1, 'scope-one', decode(repeat('01', 16), 'hex'), decode(repeat('01', 8), 'hex'));
INSERT INTO observers (id, public_key, display_name, observer_type) VALUES
 ('00000000-0000-0000-0000-00000000000a', '\x0a', 'obs a', 'repeater'),
 ('00000000-0000-0000-0000-00000000000b', '\x0b', 'obs b', NULL);
INSERT INTO nodes (public_key, node_type, name) VALUES ('\xee', 2, 'node e');
INSERT INTO channels (id, channel_hash) VALUES (1, '\x11');
-- hash, payload type, route type, origin, scope
INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, origin_pubkey, scope_id, raw_payload, raw_header, first_heard_at, last_heard_at)
SELECT hash, pt, rt, 0, origin, scope, '\x00', '\x00', $1::timestamptz, $1::timestamptz FROM (VALUES
 ('\x01'::bytea, 4, 1, '\xee'::bytea, NULL::int),  -- flood advert, two IATAs
 ('\x02', 4, 2, '\xee', 1),                          -- direct advert, scoped, one IATA
 ('\x03', 5, 1, NULL, 1),                            -- scoped channel message, two IATAs
 ('\x04', 5, 1, NULL, 1),                            -- scoped channel message, one IATA
 ('\x05', 2, 1, NULL, NULL),                         -- straddles h0 and h0+1h
 ('\x06', 9, 1, NULL, NULL),                         -- TRACE
 ('\x07', 2, 1, NULL, NULL)                          -- invalid path
) v(hash, pt, rt, origin, scope);
INSERT INTO channel_messages (id, channel_id, packet_hash, sender_name, sent_at) VALUES
 (1, 1, '\x03', 'alice', $1::timestamptz + INTERVAL '5 minutes'),
 (2, 1, '\x04', 'alice', $1::timestamptz + INTERVAL '6 minutes');
SELECT setval(pg_get_serial_sequence('channel_messages', 'id'), 100);
INSERT INTO packet_observations (id, packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count, path_bytes, rssi, snr, airtime_ms, payload_type)
SELECT n, hash, obs::uuid, iata, $1::timestamptz + mins * INTERVAL '1 minute', plb, hs, hc, pb, rssi, snr, air, pt FROM (VALUES
 (1, '\x01'::bytea, '00000000-0000-0000-0000-00000000000a', 'YVR', 1, 65, 2, 1, '\xaabb'::bytea, -90, 5.5::real, 12.5::real, 4),
 (2, '\x01', '00000000-0000-0000-0000-00000000000b', 'YYZ', 2, 0, 1, 0, NULL, -100, -3.25, NULL, 4),
 (3, '\x02', '00000000-0000-0000-0000-00000000000a', 'YVR', 3, 2, 1, 2, '\xaabb', 0, 0, 10, 4),
 (4, '\x03', '00000000-0000-0000-0000-00000000000a', 'YVR', 4, 1, 1, 1, '\xaa', -80, 8, 20, 5),
 (5, '\x03', '00000000-0000-0000-0000-00000000000b', 'YYZ', 5, 1, 1, 1, '\xbb', -85, 7, NULL, 5),
 (6, '\x04', '00000000-0000-0000-0000-00000000000b', 'YYZ', 6, 0, 1, 0, NULL, NULL, NULL, NULL, 5),
 (7, '\x05', '00000000-0000-0000-0000-00000000000a', 'YVR', 59, 0, 1, 0, NULL, -120, -12, 5, 2),
 (8, '\x05', '00000000-0000-0000-0000-00000000000b', 'YYZ', 61, 0, 1, 0, NULL, -121, -13, 5, 2),
 (9, '\x06', '00000000-0000-0000-0000-00000000000a', 'YVR', 7, 3, 1, 3, '\x010203', -95, 2, 30, 9),
 (10, '\x07', '00000000-0000-0000-0000-00000000000a', 'YVR', 8, 3, 1, 3, '\xaa', -95, 2, 30, 2)
) v(n, hash, obs, iata, mins, plb, hs, hc, pb, rssi, snr, air, pt);`, pgx.QueryExecModeSimpleProtocol, h0)
	if err != nil {
		t.Fatal(err)
	}
	return h0
}

// Raw-scan oracles: the SQL the old live views ran, grouped per hour.
var rollupOracles = map[string][2]string{
	"iata observations": {
		`SELECT hour, iata, observation_count FROM analytics_hourly_iata_observations`,
		`SELECT date_trunc('hour', heard_at, 'UTC'), iata, count(*) FROM packet_observations GROUP BY 1, 2`},
	"payload breakdown": {
		`SELECT hour, iata, payload_type, count FROM analytics_hourly_payload_breakdown`,
		`SELECT date_trunc('hour', heard_at, 'UTC'), iata, payload_type, count(*) FROM packet_observations
		 WHERE payload_type IS NOT NULL GROUP BY 1, 2, 3`},
	"signal": {
		`SELECT hour, iata, kind, snr_bin, rssi_bin, receptions, snr_samples, snr_sum, rssi_samples, rssi_sum FROM analytics_hourly_signal`,
		`WITH samples AS (
		    SELECT iata, date_trunc('hour', heard_at, 'UTC') AS hour,
		           CASE WHEN NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0)
		                     AND snr > '-Infinity'::real AND snr < 'Infinity'::real THEN snr::double precision END AS snr,
		           CASE WHEN NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0) THEN rssi::double precision END AS rssi
		    FROM packet_observations
		), binned AS (
		    SELECT *, width_bucket(snr, -30, 30, 12) AS snr_bin, width_bucket(rssi, -140, 0, 14) AS rssi_bin FROM samples
		)
		SELECT hour, iata, grouping(snr_bin, rssi_bin)::integer, COALESCE(snr_bin, -1)::integer, COALESCE(rssi_bin, -1)::integer,
		       count(*)::bigint, count(snr)::bigint, COALESCE(sum(snr), 0)::double precision,
		       count(rssi)::bigint, COALESCE(sum(rssi), 0)::double precision
		FROM binned GROUP BY GROUPING SETS ((iata, hour), (iata, hour, snr_bin), (iata, hour, rssi_bin))`},
	"paths": {
		`SELECT hour, iata, category, hash_bytes, entries, receptions FROM analytics_hourly_paths`,
		`WITH classified AS (
		    SELECT iata, heard_at, hash_size, hop_count,
		           CASE WHEN payload_type = 9 THEN 2
		                WHEN payload_type IS NULL OR payload_type NOT BETWEEN 0 AND 15
		                  OR NOT (path_length_byte BETWEEN 0 AND 191
		                    AND hash_size BETWEEN 1 AND 3 AND hop_count BETWEEN 0 AND 63
		                    AND hash_size = (path_length_byte >> 6) + 1
		                    AND hop_count = (path_length_byte & 63)
		                    AND hash_size::integer * hop_count::integer <= 64
		                    AND COALESCE(octet_length(path_bytes), 0) = hash_size::integer * hop_count::integer)
		                THEN 3
		                WHEN hop_count = 0 THEN 1
		                ELSE 0 END::integer AS category
		    FROM packet_observations
		), buckets AS (
		    SELECT iata, date_trunc('hour', heard_at, 'UTC') AS hour, category,
		           CASE WHEN category = 0 THEN hash_size ELSE 0 END::integer AS hash_bytes,
		           CASE WHEN category = 0 THEN hop_count ELSE 0 END::integer AS entries
		    FROM classified
		)
		SELECT hour, iata, category, hash_bytes, entries, count(*)::bigint FROM buckets GROUP BY 1, 2, 3, 4, 5`},
	"observer activity": {
		`SELECT hour, observer_id, payload_type, observations, airtime_ms, airtime_n, snr_sum, snr_n, snr_min, rssi_sum, rssi_n
		 FROM analytics_hourly_observer_activity`,
		`SELECT date_trunc('hour', heard_at, 'UTC'), observer_id, COALESCE(payload_type, -1)::smallint,
		  COUNT(*)::bigint, SUM(airtime_ms)::real, COUNT(airtime_ms)::bigint,
		  SUM(snr)   FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::real,
		  COUNT(snr) FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint,
		  MIN(snr)   FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::real,
		  SUM(rssi)  FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint,
		  COUNT(rssi) FILTER (WHERE NOT (COALESCE(rssi, 0) = 0 AND COALESCE(snr, 0) = 0))::bigint
		FROM packet_observations GROUP BY 1, 2, 3`},
	"observer identity": {
		`SELECT hour, iata, observer_id, observation_count, display_name, observer_type FROM analytics_hourly_observer_identity`,
		`SELECT date_trunc('hour', po.heard_at, 'UTC'), po.iata, po.observer_id, count(*), o.display_name, o.observer_type
		 FROM packet_observations po JOIN observers o ON o.id = po.observer_id GROUP BY 1, 2, 3, 5, 6`},
	"scope observers": {
		`SELECT hour, iata, scope_id, observer_id FROM analytics_hourly_scope_observers`,
		`SELECT DISTINCT date_trunc('hour', po.heard_at, 'UTC'), po.iata, p.scope_id, po.observer_id
		 FROM packet_observations po JOIN packets p ON p.packet_hash = po.packet_hash WHERE p.scope_id IS NOT NULL`},
	"scope nodes": {
		`SELECT hour, iata, scope_id, origin_pubkey FROM analytics_hourly_scope_nodes`,
		`SELECT DISTINCT date_trunc('hour', po.heard_at, 'UTC'), po.iata, p.scope_id, p.origin_pubkey
		 FROM packet_observations po JOIN packets p ON p.packet_hash = po.packet_hash
		 WHERE po.payload_type = 4 AND p.origin_pubkey IS NOT NULL AND p.scope_id IS NOT NULL`},
	"advert hearings": {
		`SELECT hour, iata, origin_pubkey, observations, last_heard, name, node_type FROM analytics_hourly_advert_hearings`,
		`SELECT date_trunc('hour', po.heard_at, 'UTC'), po.iata, p.origin_pubkey, count(*), max(po.heard_at), n.name, n.node_type
		 FROM packet_observations po JOIN packets p ON p.packet_hash = po.packet_hash
		 LEFT JOIN nodes n ON n.public_key = p.origin_pubkey
		 WHERE po.payload_type = 4 AND p.origin_pubkey IS NOT NULL GROUP BY 1, 2, 3, 6, 7`},
}

// Set families must give exact distinct counts for any IATA set, summed per hour.
var rollupSetOracles = map[string][2]string{
	"packets": {
		`SELECT COALESCE(sum(packets), 0) FROM analytics_hourly_packet_sets WHERE iatas && $1::bpchar[]`,
		`SELECT count(*) FROM (SELECT DISTINCT date_trunc('hour', heard_at, 'UTC'), packet_hash
		 FROM packet_observations WHERE iata = ANY($1::bpchar[])) d`},
	"scoped packets": {
		`SELECT COALESCE(sum(packets), 0) FROM analytics_hourly_scope_sets WHERE iatas && $1::bpchar[] AND scope_id = 1`,
		`SELECT count(*) FROM (SELECT DISTINCT date_trunc('hour', po.heard_at, 'UTC'), po.packet_hash
		 FROM packet_observations po JOIN packets p ON p.packet_hash = po.packet_hash
		 WHERE po.iata = ANY($1::bpchar[]) AND p.scope_id = 1) d`},
	"adverts": {
		`SELECT COALESCE(sum(advert_packets), 0) FROM analytics_hourly_advert_sets WHERE iatas && $1::bpchar[]`,
		`SELECT count(*) FROM (SELECT DISTINCT date_trunc('hour', po.heard_at, 'UTC'), po.packet_hash
		 FROM packet_observations po JOIN packets p ON p.packet_hash = po.packet_hash
		 WHERE po.iata = ANY($1::bpchar[]) AND po.payload_type = 4) d`},
	"flood adverts": {
		`SELECT COALESCE(sum(flood_packets), 0) FROM analytics_hourly_advert_sets WHERE iatas && $1::bpchar[]`,
		`SELECT count(*) FROM (SELECT DISTINCT date_trunc('hour', po.heard_at, 'UTC'), po.packet_hash
		 FROM packet_observations po JOIN packets p ON p.packet_hash = po.packet_hash
		 WHERE po.iata = ANY($1::bpchar[]) AND po.payload_type = 4 AND p.route_type IN (0, 1)) d`},
	"messages": {
		`SELECT COALESCE(sum(messages), 0) FROM analytics_hourly_talker_sets WHERE iatas && $1::bpchar[] AND sender_name = 'alice'`,
		`SELECT count(*) FROM (SELECT DISTINCT date_trunc('hour', po.heard_at, 'UTC'), cm.id
		 FROM packet_observations po JOIN channel_messages cm ON cm.packet_hash = po.packet_hash
		 WHERE po.iata = ANY($1::bpchar[]) AND cm.sender_name = 'alice') d`},
}

// rollTxHours registers every hour holding observations (eligible or not) and rolls all
// registered hours inside tx, so tx-scoped fixtures can read rollup-backed stats.
func rollTxHours(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	if _, err := tx.Exec(ctx, `INSERT INTO analytics_rollup_hours (hour, status)
		SELECT DISTINCT date_trunc('hour', heard_at, 'UTC'), 'missing' FROM packet_observations
		ON CONFLICT (hour) DO NOTHING`); err != nil {
		t.Fatal(err)
	}
	rows, err := tx.Query(ctx, "SELECT hour FROM analytics_rollup_hours ORDER BY hour")
	if err != nil {
		t.Fatal(err)
	}
	hours, err := pgx.CollectRows(rows, pgx.RowTo[time.Time])
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hours {
		if outcome, _, err := rollHour(ctx, tx, h); err != nil || outcome != RollComplete {
			t.Fatalf("roll %s: outcome %d err %v", h, outcome, err)
		}
	}
}

// backfillMessage stores a decrypted-late message the way channel backfill does.
func backfillMessage(t *testing.T, ctx context.Context, store *Store, hash byte, sender string, at time.Time) {
	t.Helper()
	m, err := store.InsertChannelMessage(ctx, ingest.InsertChannelMessageParams{
		ChannelID: 1, PacketHash: []byte{hash}, SenderName: sender, Content: "late", SentAt: at, Historical: true,
	})
	if err != nil || m == nil {
		t.Fatalf("backfill message %x: %v %v", hash, m, err)
	}
}

func rowsText(t *testing.T, ctx context.Context, pool *pgxpool.Pool, query string) string {
	t.Helper()
	var s *string
	if err := pool.QueryRow(ctx, "SELECT string_agg(x::text, E'\\n' ORDER BY x::text) FROM ("+query+") x").Scan(&s); err != nil {
		t.Fatalf("%v\n%s", err, query)
	}
	if s == nil {
		return ""
	}
	return *s
}

func rollAll(t *testing.T, ctx context.Context, r *RollupSession) {
	t.Helper()
	hours, err := r.MissingHours(ctx, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, h := range hours {
		if outcome, _, err := r.RollHour(ctx, h); err != nil || outcome != RollComplete {
			t.Fatalf("roll %s: outcome %d, err %v", h, outcome, err)
		}
	}
}

func revision(t *testing.T, ctx context.Context, s *Store) int64 {
	t.Helper()
	rev, err := s.AnalyticsRevision(ctx)
	if err != nil {
		t.Fatal(err)
	}
	return rev
}

func TestRollupHourPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	h0 := rollupFixture(t, ctx, pool)
	store := New(pool, 0, 0)

	r, ok, err := store.BeginRollup(ctx)
	if err != nil || !ok {
		t.Fatalf("BeginRollup: ok=%v err=%v", ok, err)
	}
	defer r.Close(ctx)
	if other, ok, err := store.BeginRollup(ctx); err != nil || ok {
		if other != nil {
			other.Close(ctx)
		}
		t.Fatalf("second BeginRollup: ok=%v err=%v, want lock held", ok, err)
	}

	if err := r.RegisterHours(ctx, h0.Add(-10*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var first, last time.Time
	var eligibleLast bool
	if err := pool.QueryRow(ctx, `SELECT min(hour), max(hour), max(hour) = date_trunc('hour', now() - INTERVAL '95 minutes', 'UTC')
		FROM analytics_rollup_hours WHERE status = 'missing'`).Scan(&first, &last, &eligibleLast); err != nil {
		t.Fatal(err)
	}
	if !first.Equal(h0) || !eligibleLast {
		t.Fatalf("registered hours %s..%s, want %s..now-95m", first, last, h0)
	}

	rev0 := revision(t, ctx, store)
	rollAll(t, ctx, r)
	rev1 := revision(t, ctx, store)
	if rev1 == rev0 {
		t.Error("first roll did not bump the revision")
	}

	for name, q := range rollupOracles {
		if got, want := rowsText(t, ctx, pool, q[0]), rowsText(t, ctx, pool, q[1]); got != want {
			t.Errorf("%s differs from raw:\nrollup:\n%s\nraw:\n%s", name, got, want)
		}
	}
	for name, q := range rollupSetOracles {
		for _, set := range [][]string{{"YVR"}, {"YYZ"}, {"YVR", "YYZ"}, {"YOW"}, {"YOW", "YVR", "YYZ"}} {
			var got, want int64
			if err := pool.QueryRow(ctx, q[0], set).Scan(&got); err != nil {
				t.Fatal(err)
			}
			if err := pool.QueryRow(ctx, q[1], set).Scan(&want); err != nil {
				t.Fatal(err)
			}
			if got != want {
				t.Errorf("%s for %v: rollup %d, raw %d", name, set, got, want)
			}
		}
	}

	// Empty hour: complete, no family rows.
	var emptyStatus string
	var emptyRows int
	if err := pool.QueryRow(ctx, `SELECT status, (SELECT count(*) FROM analytics_hourly_iata_observations WHERE hour = $1)
		FROM analytics_rollup_hours WHERE hour = $1`, h0.Add(2*time.Hour)).Scan(&emptyStatus, &emptyRows); err != nil {
		t.Fatal(err)
	}
	if emptyStatus != "complete" || emptyRows != 0 {
		t.Errorf("empty hour status %q rows %d, want complete, 0", emptyStatus, emptyRows)
	}

	// Re-rolling unchanged data is a no-op for readers.
	if outcome, changed, err := r.RollHour(ctx, h0); err != nil || outcome != RollComplete || changed {
		t.Fatalf("idempotent re-roll: outcome %d changed %v err %v", outcome, changed, err)
	}
	if revision(t, ctx, store) != rev1 {
		t.Error("idempotent re-roll bumped the revision")
	}

	// A backfilled message dirties every hour its packet was heard in; re-rolling picks it up.
	backfillMessage(t, ctx, store, 0x05, "bob", h0)
	dirty, err := r.DirtyHours(ctx, 10)
	if err != nil || len(dirty) != 2 || !dirty[0].Equal(h0) || !dirty[1].Equal(h0.Add(time.Hour)) {
		t.Fatalf("dirty hours %v, %v; want h0 and h0+1h", dirty, err)
	}
	for _, h := range dirty {
		if outcome, changed, err := r.RollHour(ctx, h); err != nil || outcome != RollComplete || !changed {
			t.Fatalf("dirty re-roll %s: outcome %d changed %v err %v", h, outcome, changed, err)
		}
	}
	if revision(t, ctx, store) <= rev1 {
		t.Error("changed re-roll did not bump the revision")
	}
	if dirty, err := r.DirtyHours(ctx, 10); err != nil || len(dirty) != 0 {
		t.Errorf("dirty hours after re-roll %v, %v; want none", dirty, err)
	}
	var bob int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(sum(messages), 0) FROM analytics_hourly_talker_sets WHERE sender_name = 'bob'`).Scan(&bob); err != nil || bob != 2 {
		t.Errorf("bob messages across the straddled hours = %d, %v; want 2", bob, err)
	}
}

func TestRollupPartialHoursPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	h0 := rollupFixture(t, ctx, pool)
	store := New(pool, 0, 0)
	r, ok, err := store.BeginRollup(ctx)
	if err != nil || !ok {
		t.Fatalf("BeginRollup: ok=%v err=%v", ok, err)
	}
	defer r.Close(ctx)
	if err := r.RegisterHours(ctx, h0.Add(-10*time.Hour)); err != nil {
		t.Fatal(err)
	}

	// Rolling h0+1h first leaves h0 missing when cleanup reaches it.
	if outcome, _, err := r.RollHour(ctx, h0.Add(time.Hour)); err != nil || outcome != RollComplete {
		t.Fatalf("roll h0+1h: %d %v", outcome, err)
	}
	if _, err := pool.Exec(ctx, "UPDATE analytics_raw_state SET raw_deleted_before = $1", h0.Add(20*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if outcome, _, err := r.RollHour(ctx, h0); err != nil || outcome != RollPartial {
		t.Fatalf("roll h0 after raw deletion: outcome %d err %v, want partial", outcome, err)
	}
	if err := r.MarkPartialHours(ctx); err != nil {
		t.Fatal(err)
	}
	var statuses string
	if err := pool.QueryRow(ctx, `SELECT string_agg(status, ',' ORDER BY hour) FROM analytics_rollup_hours WHERE hour IN ($1, $2)`,
		h0, h0.Add(time.Hour)).Scan(&statuses); err != nil {
		t.Fatal(err)
	}
	if statuses != "partial,complete" {
		t.Errorf("statuses %q, want partial,complete", statuses)
	}

	// Only complete hours with intact raw rows are re-rolled; h0's entry is dropped.
	backfillMessage(t, ctx, store, 0x01, "carol", h0)
	backfillMessage(t, ctx, store, 0x05, "bob", h0)
	if dirty, err := r.DirtyHours(ctx, 10); err != nil || len(dirty) != 1 || !dirty[0].Equal(h0.Add(time.Hour)) {
		t.Errorf("dirty hours %v, %v; want only h0+1h", dirty, err)
	}
	if missing, err := r.MissingHours(ctx, 100); err != nil || len(missing) == 0 || !missing[0].After(h0.Add(time.Hour)) {
		t.Errorf("missing hours %v, %v; want only hours after h0+1h", missing, err)
	}
}

// A roll that dies mid-transaction leaves the hour missing for the next pass.
func TestRollupInterruptedHourPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	h0 := rollupFixture(t, ctx, pool)
	store := New(pool, 0, 0)
	r, ok, err := store.BeginRollup(ctx)
	if err != nil || !ok {
		t.Fatalf("BeginRollup: ok=%v err=%v", ok, err)
	}
	defer r.Close(ctx)
	if err := r.RegisterHours(ctx, h0); err != nil {
		t.Fatal(err)
	}
	// Breaking a family table makes the roll fail after earlier families were written.
	if _, err := pool.Exec(ctx, "ALTER TABLE analytics_hourly_scope_sets ADD CONSTRAINT boom CHECK (packets < 0) NOT VALID"); err != nil {
		t.Fatal(err)
	}
	if _, _, err := r.RollHour(ctx, h0); err == nil {
		t.Fatal("roll succeeded despite a failing family")
	}
	var status string
	var rows int
	if err := pool.QueryRow(ctx, `SELECT status, (SELECT count(*) FROM analytics_hourly_iata_observations WHERE hour = $1)
		FROM analytics_rollup_hours WHERE hour = $1`, h0).Scan(&status, &rows); err != nil {
		t.Fatal(err)
	}
	if status != "missing" || rows != 0 {
		t.Errorf("after failed roll: status %q, %d family rows; want missing, 0", status, rows)
	}
	if _, err := pool.Exec(ctx, "ALTER TABLE analytics_hourly_scope_sets DROP CONSTRAINT boom"); err != nil {
		t.Fatal(err)
	}
	if outcome, _, err := r.RollHour(ctx, h0); err != nil || outcome != RollComplete {
		t.Errorf("retry: outcome %d err %v", outcome, err)
	}
}

// A message backfilled after a first roll's snapshot (but before it commits) is not lost:
// its dirty entry is invisible to that roll, survives it, and triggers a re-roll.
func TestRollupBackfillDuringFirstRollPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	h0 := rollupFixture(t, ctx, pool)
	store := New(pool, 0, 0)
	r, ok, err := store.BeginRollup(ctx)
	if err != nil || !ok {
		t.Fatalf("BeginRollup: ok=%v err=%v", ok, err)
	}
	defer r.Close(ctx)
	if err := r.RegisterHours(ctx, h0); err != nil {
		t.Fatal(err)
	}

	roll, err := pool.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.RepeatableRead})
	if err != nil {
		t.Fatal(err)
	}
	defer roll.Rollback(ctx)
	if _, err := roll.Exec(ctx, "SELECT count(*) FROM channel_messages"); err != nil { // fixes the snapshot
		t.Fatal(err)
	}
	backfillMessage(t, ctx, store, 0x05, "bob", h0) // commits on another connection
	if outcome, _, err := rollHour(ctx, roll, h0); err != nil || outcome != RollComplete {
		t.Fatalf("first roll: %d %v", outcome, err)
	}
	if err := roll.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	talkers := func() int64 {
		t.Helper()
		var n int64
		if err := pool.QueryRow(ctx, "SELECT COALESCE(sum(messages), 0) FROM analytics_hourly_talker_sets WHERE hour = $1 AND sender_name = 'bob'", h0).Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	if talkers() != 0 {
		t.Fatal("first roll saw the message; the race was not reproduced")
	}
	dirty, err := r.DirtyHours(ctx, 10)
	if err != nil || len(dirty) == 0 || !dirty[0].Equal(h0) {
		t.Fatalf("dirty hours %v, %v; want h0 still queued", dirty, err)
	}
	if outcome, _, err := r.RollHour(ctx, h0); err != nil || outcome != RollComplete {
		t.Fatalf("re-roll: %d %v", outcome, err)
	}
	if talkers() != 1 {
		t.Error("re-roll did not pick up the backfilled message")
	}
}

// With no observations there is nothing to roll, so no empty "complete" hours.
func TestRollupRegisterEmptyDatabasePostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	r, ok, err := New(pool, 0, 0).BeginRollup(ctx)
	if err != nil || !ok {
		t.Fatalf("BeginRollup: ok=%v err=%v", ok, err)
	}
	defer r.Close(ctx)
	if err := r.RegisterHours(ctx, time.Now().Add(-7*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM analytics_rollup_hours").Scan(&n); err != nil || n != 0 {
		t.Errorf("registered %d hours on an empty database, err %v; want 0", n, err)
	}
}

// An observation may be heard up to 30 minutes after its packet's last_heard_at, so an hour
// starting exactly raw_deleted_before + 30 min may have lost rows too.
func TestRollupPartialBoundaryPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	h0 := rollupFixture(t, ctx, pool)
	r, ok, err := New(pool, 0, 0).BeginRollup(ctx)
	if err != nil || !ok {
		t.Fatalf("BeginRollup: ok=%v err=%v", ok, err)
	}
	defer r.Close(ctx)
	if err := r.RegisterHours(ctx, h0); err != nil {
		t.Fatal(err)
	}
	h1 := h0.Add(time.Hour)
	if _, err := pool.Exec(ctx, "UPDATE analytics_raw_state SET raw_deleted_before = $1", h1.Add(-30*time.Minute)); err != nil {
		t.Fatal(err)
	}
	if outcome, _, err := r.RollHour(ctx, h1); err != nil || outcome != RollPartial {
		t.Errorf("hour at the boundary: outcome %d err %v, want partial", outcome, err)
	}
	if outcome, _, err := r.RollHour(ctx, h1.Add(time.Hour)); err != nil || outcome != RollComplete {
		t.Errorf("next hour: outcome %d err %v, want complete", outcome, err)
	}
}
