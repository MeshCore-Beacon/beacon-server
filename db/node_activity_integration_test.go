// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"encoding/json"
	"errors"
	"math/rand/v2"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Rolled hours plus the raw tail must give a node's chart exactly what counting its raw rows gives.
func TestNodeActivityMatchesRawPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	nodeA, nodeB := uuid.MustParse("00000000-0000-0000-0000-0000000000a1"), uuid.MustParse("00000000-0000-0000-0000-0000000000b1")
	if _, err := tx.Exec(ctx, `INSERT INTO observers(id,public_key,display_name) VALUES ('00000000-0000-0000-0000-000000000001','\x01','obs');
 INSERT INTO nodes(id,public_key,node_type,name) VALUES ('00000000-0000-0000-0000-0000000000a1','\xa1',2,'a'),
  ('00000000-0000-0000-0000-0000000000b1','\xb1',2,'b'), ('00000000-0000-0000-0000-0000000000c1','\xc1',2,'quiet')`); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	rng := rand.New(rand.NewPCG(3, 7))
	hear := func(origin []byte, first, heard time.Time) {
		t.Helper()
		rssi, snr := int16(-40-rng.IntN(80)), float32(rng.IntN(140)-80)/4
		if rng.IntN(8) == 0 {
			rssi, snr = 0, 0 // no signal reported
		}
		if _, err := tx.Exec(ctx, `WITH packet AS (
 INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,origin_pubkey,raw_payload,raw_header,first_heard_at,last_heard_at)
 VALUES (decode(md5(gen_random_uuid()::text),'hex'),4,0,1,$1,'\x00','\x00',$2,$3) RETURNING packet_hash
 ) INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type,rssi,snr)
 SELECT packet_hash,'00000000-0000-0000-0000-000000000001','YVR',$3,0,1,$4,4,$5,$6 FROM packet`,
			origin, first, heard, int16(rng.IntN(6)), rssi, snr); err != nil {
			t.Fatal(err)
		}
	}
	for range 600 {
		at := now.Add(-time.Duration(rng.Int64N(int64(40 * time.Hour)))).Truncate(time.Microsecond)
		// Stored up to 30 minutes either side of when it was heard, as ingest's clamp allows.
		first := at.Add(time.Duration(rng.Int64N(int64(time.Hour))) - 30*time.Minute)
		hear([][]byte{{0xa1}, {0xb1}, nil}[rng.IntN(3)], first, at)
	}
	hear([]byte{0xa1}, now.Add(-5*time.Hour), now.Add(-10*time.Minute)) // a late repeat of an older packet
	for _, origin := range [][]byte{{0xa1}, {0xb1}} {
		hear(origin, now.Truncate(time.Microsecond), now.Truncate(time.Microsecond)) // inside even the shortest window
	}

	store := &Store{q: sqlc.New(tx)}
	type oracleRow struct {
		T                               time.Time
		N, SnrN, RssiSum, RssiN, HopSum int64
		SnrSum                          float64
	}
	// expected is the response's points and summary counted from raw rows heard in [since, until).
	expected := func(pubkey []byte, since, until time.Time) ([]api.NodeActivityPoint, api.NodeActivitySummary) {
		t.Helper()
		rows, err := tx.Query(ctx, `SELECT date_trunc('hour', po.heard_at, 'UTC'), count(*),
  count(po.snr) FILTER (WHERE NOT (COALESCE(po.rssi, 0) = 0 AND COALESCE(po.snr, 0) = 0)),
  COALESCE(sum(po.rssi) FILTER (WHERE NOT (COALESCE(po.rssi, 0) = 0 AND COALESCE(po.snr, 0) = 0)), 0)::bigint,
  count(po.rssi) FILTER (WHERE NOT (COALESCE(po.rssi, 0) = 0 AND COALESCE(po.snr, 0) = 0)),
  sum(po.hop_count)::bigint,
  COALESCE(sum(po.snr::double precision) FILTER (WHERE NOT (COALESCE(po.rssi, 0) = 0 AND COALESCE(po.snr, 0) = 0)), 0)
 FROM packet_observations po JOIN packets p ON p.packet_hash = po.packet_hash
 WHERE p.origin_pubkey = $1 AND po.heard_at >= $2 AND po.heard_at < $3 GROUP BY 1 ORDER BY 1`, pubkey, since, until)
		if err != nil {
			t.Fatal(err)
		}
		hours, err := pgx.CollectRows(rows, pgx.RowToStructByPos[oracleRow])
		if err != nil {
			t.Fatal(err)
		}
		var total nodeActivitySums
		points := make([]api.NodeActivityPoint, 0, len(hours))
		for _, h := range hours {
			b := nodeActivitySums{h.N, h.SnrN, h.RssiSum, h.RssiN, h.HopSum, h.N, h.SnrSum}
			p := api.NodeActivityPoint{T: h.T.UnixMilli(), Observations: h.N}
			p.SNRAvg, p.RSSIAvg, p.HopAvg = b.averages()
			points = append(points, p)
			total = nodeActivitySums{total.observations + h.N, total.snrN + h.SnrN, total.rssiSum + h.RssiSum,
				total.rssiN + h.RssiN, total.hopSum + h.HopSum, total.hopN + h.N, total.snrSum + h.SnrSum}
		}
		summary := api.NodeActivitySummary{Observations: total.observations}
		summary.SNRAvg, summary.RSSIAvg, summary.HopAvg = total.averages()
		return points, summary
	}
	// SNR comes in quarter dB, so its sums are exact in any order.
	text := func(v any) string {
		b, err := json.Marshal(v)
		if err != nil {
			t.Fatal(err)
		}
		return string(b)
	}
	check := func(label string, node uuid.UUID, pubkey []byte, window time.Duration, covered func(a *api.NodeActivity) time.Time) *api.NodeActivity {
		t.Helper()
		a, err := store.GetNodeActivity(ctx, node, window)
		if err != nil {
			t.Fatal(err)
		}
		points, summary := expected(pubkey, covered(a), time.UnixMilli(a.WindowEnd).Add(time.Millisecond))
		if got, want := text(a.Points), text(points); got != want {
			t.Fatalf("%s points:\n%s\nraw:\n%s", label, got, want)
		}
		if got, want := text(a.Summary), text(summary); got != want {
			t.Fatalf("%s summary: %s, raw %s", label, got, want)
		}
		if len(a.Points) == 0 {
			t.Fatalf("%s: empty fixture", label)
		}
		return a
	}
	windowStart := func(a *api.NodeActivity) time.Time { return time.UnixMilli(a.WindowStart) }

	// Nothing rolled yet: only the raw tail, the last 24 hours, is covered.
	a := check("unrolled", nodeA, []byte{0xa1}, 48*time.Hour, func(a *api.NodeActivity) time.Time { return time.UnixMilli(*a.RawFrom) })
	if a.RolledUntil != nil || *a.RawFrom != a.WindowEnd-(24*time.Hour).Milliseconds() {
		t.Fatalf("unrolled: rolledUntil %v rawFrom %d windowEnd %d", a.RolledUntil, *a.RawFrom, a.WindowEnd)
	}

	// Roll every eligible hour; the rest stays a raw tail.
	if _, err := tx.Exec(ctx, `INSERT INTO analytics_rollup_hours (hour, status)
		SELECT h, 'missing' FROM generate_series(date_trunc('hour', now() - INTERVAL '41 hours', 'UTC'),
		  date_trunc('hour', now() - INTERVAL '95 minutes', 'UTC'), INTERVAL '1 hour') h`); err != nil {
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
	for _, window := range []time.Duration{48 * time.Hour, 24 * time.Hour, 6 * time.Hour, time.Hour, 90 * time.Minute} {
		a = check("rolled a "+window.String(), nodeA, []byte{0xa1}, window, windowStart)
		check("rolled b "+window.String(), nodeB, []byte{0xb1}, window, windowStart)
	}
	if a.RolledUntil == nil || *a.RolledUntil != *a.RawFrom || *a.RolledUntil != hours[len(hours)-1].Add(time.Hour).UnixMilli() {
		t.Fatalf("rolled: rolledUntil %v rawFrom %v", a.RolledUntil, a.RawFrom)
	}
	if a.WindowStart%time.Hour.Milliseconds() != 0 || a.WindowStart < a.WindowEnd-(90*time.Minute).Milliseconds() {
		t.Fatalf("window start %d is not the first whole hour inside 90m of %d", a.WindowStart, a.WindowEnd)
	}
	var latestHash string
	var latestHeard time.Time
	if err := tx.QueryRow(ctx, `SELECT encode(packet_hash, 'hex'), GREATEST(first_heard_at, last_heard_at) FROM packets WHERE origin_pubkey = '\xa1'
		ORDER BY first_heard_at DESC LIMIT 1`).Scan(&latestHash, &latestHeard); err != nil {
		t.Fatal(err)
	}
	if a.Latest == nil || a.Latest.PacketHash != latestHash || a.Latest.HeardAt != latestHeard.UnixMilli() {
		t.Fatalf("latest %+v, want %s at %d", a.Latest, latestHash, latestHeard.UnixMilli())
	}

	// The tail reads raw rows and the rest does not: delete them all and only the tail's are lost.
	rolledUntil := time.UnixMilli(*a.RolledUntil)
	before := check("before deleting", nodeA, []byte{0xa1}, 48*time.Hour, windowStart).Summary.Observations
	var tailGone int64
	if err := tx.QueryRow(ctx, `WITH gone AS (DELETE FROM packet_observations po USING packets p
		WHERE p.packet_hash = po.packet_hash AND p.origin_pubkey = '\xa1' RETURNING po.heard_at)
		SELECT count(*) FILTER (WHERE heard_at >= $1) FROM gone`, rolledUntil).Scan(&tailGone); err != nil {
		t.Fatal(err)
	}
	rolledOnly, err := store.GetNodeActivity(ctx, nodeA, 48*time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if tailGone < 2 || tailGone == before || rolledOnly.Summary.Observations != before-tailGone {
		t.Fatalf("after deleting the raw rows, %d in the tail: %d observations, had %d", tailGone, rolledOnly.Summary.Observations, before)
	}

	quiet, err := store.GetNodeActivity(ctx, uuid.MustParse("00000000-0000-0000-0000-0000000000c1"), 24*time.Hour)
	if err != nil || quiet.Points == nil || len(quiet.Points) != 0 || quiet.Latest != nil || quiet.Summary != (api.NodeActivitySummary{}) {
		t.Fatalf("quiet node: %+v, %v", quiet, err)
	}
	if _, err := store.GetNodeActivity(ctx, uuid.New(), 24*time.Hour); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("unknown node: %v, want pgx.ErrNoRows", err)
	}
}
