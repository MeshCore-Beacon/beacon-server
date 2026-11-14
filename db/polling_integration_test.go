// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"sort"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

func pollKey(b byte) []byte { return bytes.Repeat([]byte{b}, 32) }

func pollStore(t *testing.T) (context.Context, *pgxpool.Pool, *Store) {
	t.Helper()
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO iata_codes (iata) VALUES ('YVR')`); err != nil {
		t.Fatal(err)
	}
	return ctx, pool, New(pool, pool, 0, 0)
}

func pollObserver(t *testing.T, ctx context.Context, s *Store, k []byte) uuid.UUID {
	t.Helper()
	id, err := s.ObserverID(ctx, k)
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func pollClaim(obs []byte, at time.Time, targets ...byte) polling.Claim {
	c := polling.Claim{Origin: "o", Observer: obs, Timestamp: at, Client: "fw", PolicyVersion: "p"}
	for _, b := range targets {
		c.Nodes = append(c.Nodes, polling.ClaimNode{Target: pollKey(b), KeyMode: "own"})
	}
	return c
}

func candidateTargets(t *testing.T, ctx context.Context, s *Store, after time.Time, target byte) int {
	t.Helper()
	cs, err := s.ListPollingCandidates(ctx, pollKey(target), after, after)
	if err != nil {
		t.Fatal(err)
	}
	return len(cs)
}

func targetSet(t *testing.T, ctx context.Context, s *Store, after time.Time) []int {
	t.Helper()
	ts, err := s.ListPollingTargets(ctx, after)
	if err != nil {
		t.Fatal(err)
	}
	var out []int
	for _, k := range ts {
		out = append(out, int(k[0]))
	}
	sort.Ints(out)
	return out
}

func sameInts(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestPollingClaimReplaceIsSnapshotPostgres(t *testing.T) {
	ctx, _, s := pollStore(t)
	o := pollKey(1)
	id := pollObserver(t, ctx, s, o)
	T := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	if ok, err := s.ReplacePollingClaim(ctx, pollClaim(o, T, 0xA, 0xB), id, T); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ok, err := s.ReplacePollingClaim(ctx, pollClaim(o, T.Add(60*time.Second), 0xB, 0xC), id, T); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if got := targetSet(t, ctx, s, T.Add(-time.Hour)); !sameInts(got, []int{0xB, 0xC}) {
		t.Fatal("targets", got)
	}
	if ok, err := s.HasLivePollingCandidate(ctx, o, pollKey(0xA), T.Add(-time.Hour)); err != nil || ok {
		t.Fatal("A should be gone", ok, err)
	}
}

func TestPollingClaimOlderIsIgnoredPostgres(t *testing.T) {
	ctx, _, s := pollStore(t)
	o := pollKey(1)
	id := pollObserver(t, ctx, s, o)
	T := time.Now().Add(-time.Minute).UTC().Truncate(time.Second)
	if ok, err := s.ReplacePollingClaim(ctx, pollClaim(o, T.Add(60*time.Second), 0xB), id, T); err != nil || !ok {
		t.Fatal(ok, err)
	}
	if ok, err := s.ReplacePollingClaim(ctx, pollClaim(o, T, 0xA, 0xB), id, T); err != nil || ok {
		t.Fatal("older claim applied", ok, err)
	}
	if got := targetSet(t, ctx, s, T.Add(-time.Hour)); !sameInts(got, []int{0xB}) {
		t.Fatal("targets", got)
	}
}

func TestPollingClaimTruncatesTextPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	o := pollKey(1)
	id := pollObserver(t, ctx, s, o)
	T := time.Now().UTC()
	c := pollClaim(o, T)
	c.Origin = string(bytes.Repeat([]byte("é"), 100)) // 200 bytes, cut must stay valid UTF-8
	c.Client = string(bytes.Repeat([]byte("c"), 300))
	c.PolicyVersion = string(bytes.Repeat([]byte("v"), 50))
	if _, err := s.ReplacePollingClaim(ctx, c, id, T); err != nil {
		t.Fatal(err)
	}
	var origin, client, pv int
	if err := pool.QueryRow(ctx, `SELECT octet_length(origin), octet_length(client), octet_length(policy_version) FROM polling_claims`).Scan(&origin, &client, &pv); err != nil {
		t.Fatal(err)
	}
	if origin != 128 || client != 128 || pv != 32 {
		t.Fatal(origin, client, pv)
	}
}

func pollReading(obs []byte, target byte, method string, at time.Time, tag *uint32) polling.Reading {
	return polling.Reading{Version: 2, Observer: obs, Client: "fw", Target: pollKey(target), Method: method, SampledAt: at, Tag: tag}
}

func TestRepeaterReadingDedupPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	o := pollKey(1)
	id := pollObserver(t, ctx, s, o)
	at := time.Now().UTC().Truncate(time.Second)
	tag := uint32(77)
	r := pollReading(o, 0xA, "status", at, &tag)
	for i, want := range []bool{true, false} {
		got, err := s.InsertRepeaterReading(ctx, r, id, at)
		if err != nil || got != want {
			t.Fatal(i, got, err)
		}
	}
	r.Method = "telemetry"
	if got, err := s.InsertRepeaterReading(ctx, r, id, at); err != nil || !got {
		t.Fatal(got, err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM repeater_readings`).Scan(&n); err != nil || n != 2 {
		t.Fatal(n, err)
	}
}

func TestRepeaterReadingWithoutTagUsesSampledAtPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	o := pollKey(1)
	id := pollObserver(t, ctx, s, o)
	at := time.Now().UTC().Truncate(time.Second)
	if _, err := s.InsertRepeaterReading(ctx, pollReading(o, 0xA, "status", at, nil), id, at); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO nodes (public_key, node_type) VALUES ($1, 2)`, pollKey(0xB)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.InsertRepeaterReading(ctx, pollReading(o, 0xB, "status", at, nil), id, at); err != nil {
		t.Fatal(err)
	}
	var tag int64
	var nodeA, nodeB *uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT tag, node_id FROM repeater_readings WHERE target_public_key = $1`, pollKey(0xA)).Scan(&tag, &nodeA); err != nil {
		t.Fatal(err)
	}
	if tag != at.Unix() || nodeA != nil {
		t.Fatal(tag, nodeA)
	}
	if err := pool.QueryRow(ctx, `SELECT node_id FROM repeater_readings WHERE target_public_key = $1`, pollKey(0xB)).Scan(&nodeB); err != nil || nodeB == nil {
		t.Fatal("node_id not linked", nodeB, err)
	}
}

func TestPollingCandidatesEvidencePostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	o1, o2 := pollKey(1), pollKey(2)
	id1, id2 := pollObserver(t, ctx, s, o1), pollObserver(t, ctx, s, o2)
	now := time.Now().UTC().Truncate(time.Second)
	for _, o := range []struct {
		k  []byte
		id uuid.UUID
	}{{o1, id1}, {o2, id2}} {
		if _, err := s.ReplacePollingClaim(ctx, pollClaim(o.k, now, 0xA), o.id, now); err != nil {
			t.Fatal(err)
		}
	}
	x := pollKey(0xA)
	hear := func(obs uuid.UUID, hops int, snr float32, rssi int16) {
		if _, err := pool.Exec(ctx, `WITH p AS (
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,origin_pubkey,raw_payload,raw_header,first_heard_at,last_heard_at)
VALUES (decode(md5(gen_random_uuid()::text),'hex'),4,0,1,$1,'\x00','\x00',$2,$2) RETURNING packet_hash)
INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type,rssi,snr)
SELECT packet_hash,$3,'YVR',$2,0,1,$4,4,$5,$6 FROM p`, x, now, obs, int16(hops), rssi, snr); err != nil {
			t.Fatal(err)
		}
	}
	hear(id1, 0, 6, -90)
	hear(id1, 0, 8, -90)
	hear(id1, 0, 10, -90)
	hear(id1, 2, 20, -90)
	hear(id2, 0, 0, 0)
	cs, err := s.ListPollingCandidates(ctx, x, now.Add(-time.Hour), now.Add(-time.Hour))
	if err != nil || len(cs) != 2 {
		t.Fatal(len(cs), err)
	}
	for _, c := range cs {
		switch {
		case bytes.Equal(c.Observer, o1):
			if c.EvidenceCount != 3 || c.EvidenceSNR == nil || *c.EvidenceSNR != 8 {
				t.Fatal("o1", c.EvidenceCount, c.EvidenceSNR)
			}
		case bytes.Equal(c.Observer, o2):
			if c.EvidenceCount != 0 || c.EvidenceSNR != nil {
				t.Fatal("o2", c.EvidenceCount, c.EvidenceSNR)
			}
		}
	}
	if err := s.SetPollingObserver(ctx, o2, true, false, "x"); err != nil {
		t.Fatal(err)
	}
	cs, _ = s.ListPollingCandidates(ctx, x, now.Add(-time.Hour), now.Add(-time.Hour))
	for _, c := range cs {
		if c.Blocked != bytes.Equal(c.Observer, o2) {
			t.Fatal("blocked flag", c.Observer[0], c.Blocked)
		}
	}
}

func TestPollingAssignmentsOnePollerPostgres(t *testing.T) {
	ctx, _, s := pollStore(t)
	at := time.Now().UTC()
	tgt := pollKey(0xA)
	good := []polling.Assignment{{Observer: pollKey(1), Rank: 0}, {Observer: pollKey(2), Rank: 1}}
	if err := s.ReplacePollingAssignments(ctx, tgt, good, at); err != nil {
		t.Fatal(err)
	}
	if p, err := s.CurrentPoller(ctx, tgt); err != nil || !bytes.Equal(p, pollKey(1)) {
		t.Fatal(p, err)
	}
	bad := []polling.Assignment{{Observer: pollKey(3), Rank: 0}, {Observer: pollKey(4), Rank: 0}}
	if err := s.ReplacePollingAssignments(ctx, tgt, bad, at); err == nil {
		t.Fatal("two rank-0 rows accepted")
	}
	if p, err := s.CurrentPoller(ctx, tgt); err != nil || !bytes.Equal(p, pollKey(1)) {
		t.Fatal("changed on error", p, err)
	}
	if err := s.ReplacePollingAssignments(ctx, tgt, nil, at); err != nil {
		t.Fatal(err)
	}
	if p, err := s.CurrentPoller(ctx, tgt); err != nil || p != nil {
		t.Fatal(p, err)
	}
}

func TestPollingOwnerRowsLatestTsPostgres(t *testing.T) {
	ctx, _, s := pollStore(t)
	o1, o2 := pollKey(1), pollKey(2)
	id1, id2 := pollObserver(t, ctx, s, o1), pollObserver(t, ctx, s, o2)
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := s.ReplacePollingClaim(ctx, pollClaim(o1, now, 0xA, 0xB), id1, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.ReplacePollingClaim(ctx, pollClaim(o2, now, 0xA), id2, now); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplacePollingAssignments(ctx, pollKey(0xA), []polling.Assignment{{Observer: o2, Rank: 0}, {Observer: o1, Rank: 1}}, now); err != nil {
		t.Fatal(err)
	}
	older, newer := now.Add(-20*time.Minute), now.Add(-5*time.Minute)
	if _, err := s.InsertRepeaterReading(ctx, pollReading(o1, 0xA, "status", older, nil), id1, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.InsertRepeaterReading(ctx, pollReading(o2, 0xA, "status", newer, nil), id2, now); err != nil {
		t.Fatal(err)
	}
	rows, err := s.PollingOwnerRows(ctx, o1, now.Add(-time.Hour))
	if err != nil || len(rows) != 2 {
		t.Fatal(len(rows), err)
	}
	for _, r := range rows {
		switch r.Target[0] {
		case 0xA:
			if r.Rank != 1 || r.LatestTS == nil || !r.LatestTS.Equal(newer) || r.Excluded || r.PollerHops != 0 {
				t.Fatal("A", r)
			}
		case 0xB:
			if r.Rank != -1 || r.LatestTS != nil {
				t.Fatal("B", r)
			}
		}
	}
}

func TestCorroborateReadingsPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	o1, o2, o3, o4 := pollKey(1), pollKey(2), pollKey(3), pollKey(4)
	id1, id2, id3, id4 := pollObserver(t, ctx, s, o1), pollObserver(t, ctx, s, o2), pollObserver(t, ctx, s, o3), pollObserver(t, ctx, s, o4)
	now := time.Now().UTC().Truncate(time.Second)
	hash := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	other := []byte{9, 9, 9, 9, 9, 9, 9, 9}
	for _, p := range []struct {
		h []byte
		t int
	}{{hash, 1}, {other, 2}} {
		if _, err := pool.Exec(ctx, `INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
VALUES ($1,$2,0,1,'\x00','\x00',$3,$3)`, p.h, p.t, now); err != nil {
			t.Fatal(err)
		}
	}
	hear := func(h []byte, id uuid.UUID, at time.Time) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
VALUES ($1,$2,'YVR',$3,0,1,0)`, h, id, at); err != nil {
			t.Fatal(err)
		}
	}
	hear(hash, id1, now) // the reading's own observer: never counted
	hear(hash, id2, now.Add(5*time.Minute))
	hear(other, id3, now) // other hash: not counted
	r := pollReading(o1, 0xA, "status", now, nil)
	r.PacketHash = hash
	if _, err := s.InsertRepeaterReading(ctx, r, id1, now); err != nil {
		t.Fatal(err)
	}
	get := func() int16 {
		t.Helper()
		var c *int16
		if err := pool.QueryRow(ctx, `SELECT corroborated FROM repeater_readings`).Scan(&c); err != nil || c == nil {
			t.Fatal(c, err)
		}
		return *c
	}
	run := func() {
		t.Helper()
		if n, err := s.CorroborateReadings(ctx, now.Add(-time.Hour), 100); err != nil || n != 1 {
			t.Fatal(n, err)
		}
	}
	run()
	if c := get(); c != 1 {
		t.Fatal("want 1, got", c)
	}
	// 11 minutes away is outside the window.
	hear(hash, id3, now.Add(11*time.Minute))
	run()
	if c := get(); c != 1 {
		t.Fatal("late hearing counted", c)
	}
	// A third observer inside the window raises the count on the next pass.
	hear(hash, id4, now.Add(-9*time.Minute))
	run()
	if c := get(); c != 2 {
		t.Fatal("recompute missed the new observer", c)
	}
	// The same hash on a packet of another payload type does not count.
	if _, err := pool.Exec(ctx, `UPDATE packets SET payload_type = 4 WHERE packet_hash = $1`, hash); err != nil {
		t.Fatal(err)
	}
	run()
	if c := get(); c != 0 {
		t.Fatal("non-reply payload counted", c)
	}
}

func TestPollingLivenessUsesReceiveTimePostgres(t *testing.T) {
	ctx, _, s := pollStore(t)
	oFuture, oOld := pollKey(1), pollKey(2)
	idF, idO := pollObserver(t, ctx, s, oFuture), pollObserver(t, ctx, s, oOld)
	now := time.Now().UTC().Truncate(time.Second)
	after := now.Add(-time.Hour)
	// Claimed an hour ahead by a fast clock but received 3 hours ago.
	if _, err := s.ReplacePollingClaim(ctx, pollClaim(oFuture, now.Add(time.Hour), 0xA), idF, now.Add(-3*time.Hour)); err != nil {
		t.Fatal(err)
	}
	// Claimed 3 hours ago by a slow clock but received just now.
	if _, err := s.ReplacePollingClaim(ctx, pollClaim(oOld, now.Add(-3*time.Hour), 0xB), idO, now); err != nil {
		t.Fatal(err)
	}
	if ok, err := s.HasLivePollingCandidate(ctx, oFuture, pollKey(0xA), after); err != nil || ok {
		t.Fatal("future-claimed stale claim is live", ok, err)
	}
	if ok, err := s.HasLivePollingCandidate(ctx, oOld, pollKey(0xB), after); err != nil || !ok {
		t.Fatal("fresh claim not live", ok, err)
	}
	if got := targetSet(t, ctx, s, after); !sameInts(got, []int{0xB}) {
		t.Fatal("targets", got)
	}
	if n := candidateTargets(t, ctx, s, after, 0xA); n != 0 {
		t.Fatal("candidates for A", n)
	}
	if n := candidateTargets(t, ctx, s, after, 0xB); n != 1 {
		t.Fatal("candidates for B", n)
	}
	if rows, err := s.PollingOwnerRows(ctx, oFuture, after); err != nil || len(rows) != 0 {
		t.Fatal("owner rows for stale", len(rows), err)
	}
	if rows, err := s.PollingOwnerRows(ctx, oOld, after); err != nil || len(rows) != 1 {
		t.Fatal("owner rows for fresh", len(rows), err)
	}
	// Expiry follows receive time too.
	if err := s.DeleteExpiredPolling(ctx, after, after); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.HasLivePollingCandidate(ctx, oOld, pollKey(0xB), after.Add(-24*time.Hour)); !ok {
		t.Fatal("fresh claim deleted")
	}
	if ok, _ := s.HasLivePollingCandidate(ctx, oFuture, pollKey(0xA), after.Add(-24*time.Hour)); ok {
		t.Fatal("stale claim kept")
	}
}

func TestPollingDeleteExpiredAndFlagsPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	o := pollKey(1)
	id := pollObserver(t, ctx, s, o)
	now := time.Now().UTC().Truncate(time.Second)
	old := now.Add(-2 * time.Hour)
	if _, err := s.ReplacePollingClaim(ctx, pollClaim(o, old, 0xA), id, old); err != nil {
		t.Fatal(err)
	}
	at := now
	if err := s.ReplacePollingAssignments(ctx, pollKey(0xA), []polling.Assignment{{Observer: o, Rank: 0}}, at); err != nil {
		t.Fatal(err)
	}
	if err := s.ReplacePollingAssignments(ctx, pollKey(0xB), []polling.Assignment{{Observer: pollKey(9), Rank: 0}}, at); err != nil {
		t.Fatal(err)
	}
	if _, err := s.InsertRepeaterReading(ctx, pollReading(o, 0xA, "status", old, nil), id, old); err != nil {
		t.Fatal(err)
	}
	if got := targetSet(t, ctx, s, now.Add(-time.Hour)); !sameInts(got, []int{0xA, 0xB}) {
		t.Fatal("expired claim target must stay while assigned", got)
	}
	if err := s.DeleteExpiredPolling(ctx, now.Add(-time.Hour), now.Add(-time.Hour)); err != nil {
		t.Fatal(err)
	}
	var claims, assigns, readings int
	if err := pool.QueryRow(ctx, `SELECT (SELECT count(*) FROM polling_claims), (SELECT count(*) FROM polling_assignments), (SELECT count(*) FROM repeater_readings)`).Scan(&claims, &assigns, &readings); err != nil {
		t.Fatal(err)
	}
	if claims != 0 || assigns != 0 || readings != 0 {
		t.Fatal(claims, assigns, readings)
	}
	bl, ap, err := s.PollingObserverFlags(ctx, o)
	if err != nil || bl || ap {
		t.Fatal(bl, ap, err)
	}
	if err := s.SetPollingObserver(ctx, o, false, true, "n"); err != nil {
		t.Fatal(err)
	}
	if bl, ap, err = s.PollingObserverFlags(ctx, o); err != nil || bl || !ap {
		t.Fatal(bl, ap, err)
	}
	until := now.Add(time.Hour)
	if err := s.SetPollingState(ctx, true, &until, [][]byte{pollKey(5)}); err != nil {
		t.Fatal(err)
	}
	paused, pu, ex, err := s.PollingState(ctx)
	if err != nil || !paused || pu == nil || !pu.Equal(until) || len(ex) != 1 || !bytes.Equal(ex[0], pollKey(5)) {
		t.Fatal(paused, pu, ex, err)
	}
}
