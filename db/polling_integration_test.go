// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
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
	var tag int64
	var obs []byte
	if err := pool.QueryRow(ctx, `SELECT tag, observer_public_key FROM repeater_readings WHERE target_public_key = $1`, pollKey(0xA)).Scan(&tag, &obs); err != nil {
		t.Fatal(err)
	}
	if tag != at.Unix() || !bytes.Equal(obs, o) {
		t.Fatal(tag, obs)
	}
}

func TestRepeaterReadingOutlivesObserverPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	o1, o2 := pollKey(1), pollKey(2)
	id1, id2 := pollObserver(t, ctx, s, o1), pollObserver(t, ctx, s, o2)
	now := time.Now().UTC().Truncate(time.Second)
	hash := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	if _, err := pool.Exec(ctx, `INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
VALUES ($1,1,0,1,'\x00','\x00',$2,$2)`, hash, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
VALUES ($1,$2,'YVR',$3,0,1,0)`, hash, id2, now); err != nil {
		t.Fatal(err)
	}
	r := pollReading(o1, 0xA, "status", now, nil)
	r.PacketHash = hash
	if _, err := s.InsertRepeaterReading(ctx, r, id1, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM observers WHERE id = $1`, id1); err != nil {
		t.Fatal(err)
	}
	var obs []byte
	var oid *uuid.UUID
	if err := pool.QueryRow(ctx, `SELECT observer_public_key, observer_id FROM repeater_readings`).Scan(&obs, &oid); err != nil {
		t.Fatal("reading gone with its observer", err)
	}
	if !bytes.Equal(obs, o1) || oid != nil {
		t.Fatal(obs, oid)
	}
	// The same tag again from the same key is still a duplicate.
	id1b := pollObserver(t, ctx, s, o1)
	if ok, err := s.InsertRepeaterReading(ctx, r, id1b, now); err != nil || ok {
		t.Fatal("duplicate after observer re-created", ok, err)
	}
	// With no observer id to exclude, every observer that heard the reply counts.
	if n, err := s.CorroborateReadings(ctx, now.Add(-time.Hour), 100); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	var c *int16
	if err := pool.QueryRow(ctx, `SELECT corroborated FROM repeater_readings`).Scan(&c); err != nil || c == nil || *c != 1 {
		t.Fatal(c, err)
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
	if err := s.ReplacePollingAssignments(ctx, pollKey(0xA), []polling.Assignment{{Observer: o2, Rank: 0, EvidenceCount: 1}, {Observer: o1, Rank: 1}}, now); err != nil {
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

func TestCorroborateReadingsNewestFirstPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	o1, o2 := pollKey(1), pollKey(2)
	id1, id2 := pollObserver(t, ctx, s, o1), pollObserver(t, ctx, s, o2)
	now := time.Now().UTC().Truncate(time.Second)
	hash := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	if _, err := pool.Exec(ctx, `INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,raw_payload,raw_header,first_heard_at,last_heard_at)
VALUES ($1,1,0,1,'\x00','\x00',$2,$2)`, hash, now); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count)
VALUES ($1,$2,'YVR',$3,0,1,0)`, hash, id2, now); err != nil {
		t.Fatal(err)
	}
	for _, tgt := range []byte{0xA, 0xB} { // older first, so the newer reading has the larger id
		r := pollReading(o1, tgt, "status", now, nil)
		r.PacketHash = hash
		if _, err := s.InsertRepeaterReading(ctx, r, id1, now); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := s.CorroborateReadings(ctx, now.Add(-time.Hour), 1); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	var older, newer *int16
	if err := pool.QueryRow(ctx, `SELECT (SELECT corroborated FROM repeater_readings ORDER BY id LIMIT 1),
(SELECT corroborated FROM repeater_readings ORDER BY id DESC LIMIT 1)`).Scan(&older, &newer); err != nil {
		t.Fatal(err)
	}
	if newer == nil || older != nil {
		t.Fatal("limit 1 should update the newer reading", older, newer)
	}
}

func TestListPollingObserversPostgres(t *testing.T) {
	ctx, _, s := pollStore(t)
	if got, err := s.ListPollingObservers(ctx); err != nil || len(got) != 0 {
		t.Fatal(got, err)
	}
	if err := s.SetPollingObserver(ctx, pollKey(2), true, false, "noisy"); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPollingObserver(ctx, pollKey(1), false, true, ""); err != nil {
		t.Fatal(err)
	}
	got, err := s.ListPollingObservers(ctx)
	if err != nil || len(got) != 2 {
		t.Fatal(got, err)
	}
	if got[0].PublicKey != hex.EncodeToString(pollKey(1)) || !got[0].Approved || got[0].Blocked {
		t.Fatal("first row wrong", got[0])
	}
	if got[1].PublicKey != hex.EncodeToString(pollKey(2)) || !got[1].Blocked || got[1].Note != "noisy" {
		t.Fatal("second row wrong", got[1])
	}
}

func TestPollingElectionLockPostgres(t *testing.T) {
	ctx, pool, _ := pollStore(t)
	s := New(pool, pool, 0, 0)
	l, ok, err := s.BeginPollingElection(ctx)
	if err != nil || !ok {
		t.Fatal(ok, err)
	}
	if other, ok, err := s.BeginPollingElection(ctx); err != nil || ok {
		if other != nil {
			other.Close(ctx)
		}
		t.Fatal("second lock should be refused", ok, err)
	}
	l.Close(ctx)
	l2, ok, err := s.BeginPollingElection(ctx)
	if err != nil || !ok {
		t.Fatal("lock not released", ok, err)
	}
	l2.Close(ctx)
}

func TestPollingObserverIndexesPostgres(t *testing.T) {
	ctx, pool, _ := pollStore(t)
	for _, name := range []string{"idx_polling_claims_observer", "idx_repeater_readings_observer"} {
		var ok bool
		if err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM pg_indexes WHERE schemaname = current_schema() AND indexname = $1)`, name).Scan(&ok); err != nil || !ok {
			t.Fatal(name, ok, err)
		}
	}
}

func TestPollingElectionConcurrentPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	svc := polling.NewService(s, config.ResolvedPolling{Enabled: true}, time.Now, slog.New(slog.DiscardHandler))
	x := pollKey(0xA)
	now := time.Now().UTC().Truncate(time.Second)
	claim := func(b byte) error {
		o := pollKey(b)
		id, err := s.ObserverID(ctx, o)
		if err != nil {
			return err
		}
		_, err = s.ReplacePollingClaim(ctx, pollClaim(o, now, 0xA), id, now)
		return err
	}
	for b := byte(1); b <= 4; b++ {
		if err := claim(b); err != nil {
			t.Fatal(err)
		}
	}
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for range 6 {
		wg.Go(func() {
			for range 15 {
				if err := svc.ElectTarget(ctx, x); err != nil {
					errs <- err
				}
			}
		})
	}
	for g := range 2 {
		wg.Go(func() {
			for i := range 4 {
				if err := claim(byte(10 + g*4 + i)); err != nil {
					errs <- err
					continue
				}
				if err := svc.ElectTarget(ctx, x); err != nil {
					errs <- err
				}
			}
		})
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Error(err)
	}
	rows, err := pool.Query(ctx, `SELECT rank FROM polling_assignments WHERE target_public_key = $1 ORDER BY rank`, x)
	if err != nil {
		t.Fatal(err)
	}
	var ranks []int
	for rows.Next() {
		var r int
		if err := rows.Scan(&r); err != nil {
			t.Fatal(err)
		}
		ranks = append(ranks, r)
	}
	if len(ranks) != 12 {
		t.Fatalf("want a row for each of 12 candidates, got ranks %v", ranks)
	}
	for i, r := range ranks {
		if r != i {
			t.Fatalf("ranks not consecutive from 0: %v", ranks)
		}
	}
}

func TestPollingOwnerRowsTrustedLatestPostgres(t *testing.T) {
	ctx, _, s := pollStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	keys := [][]byte{pollKey(1), pollKey(2), pollKey(3), pollKey(4)}
	ids := make([]uuid.UUID, len(keys))
	for i, k := range keys {
		ids[i] = pollObserver(t, ctx, s, k)
		if _, err := s.ReplacePollingClaim(ctx, pollClaim(k, now, 0xA), ids[i], now); err != nil {
			t.Fatal(err)
		}
	}
	// 1 polls, 2 stands by without evidence, 3 stands by with evidence, 4 has no row.
	if err := s.ReplacePollingAssignments(ctx, pollKey(0xA), []polling.Assignment{
		{Observer: keys[0], Rank: 0}, {Observer: keys[1], Rank: 1}, {Observer: keys[2], Rank: 2, EvidenceCount: 3},
	}, now); err != nil {
		t.Fatal(err)
	}
	latest := func(obs int) (*time.Time, polling.OwnerRow) {
		t.Helper()
		rows, err := s.PollingOwnerRows(ctx, keys[obs], now.Add(-time.Hour))
		if err != nil || len(rows) != 1 {
			t.Fatal(len(rows), err)
		}
		return rows[0].LatestTS, rows[0]
	}
	read := func(obs int, at time.Time) {
		t.Helper()
		if ok, err := s.InsertRepeaterReading(ctx, pollReading(keys[obs], 0xA, "status", at, nil), ids[obs], now); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	// Without evidence even the poller's readings hold nobody back.
	read(1, now.Add(-time.Minute))
	read(3, now.Add(-30*time.Second))
	read(0, now.Add(-20*time.Minute))
	if got, _ := latest(0); got != nil {
		t.Fatal("readings without evidence moved latest_ts", got)
	}
	read(2, now.Add(-10*time.Minute))
	if got, _ := latest(0); got == nil || !got.Equal(now.Add(-10*time.Minute)) {
		t.Fatal("standby with evidence did not count", got)
	}
	if _, r := latest(3); r.Rank != -1 || r.Assigned != 3 {
		t.Fatal("unassigned row", r.Rank, r.Assigned)
	}
}

func TestRepeaterReadingRateLimitPostgres(t *testing.T) {
	ctx, _, s := pollStore(t)
	o1, o2 := pollKey(1), pollKey(2)
	id1, id2 := pollObserver(t, ctx, s, o1), pollObserver(t, ctx, s, o2)
	now := time.Now().UTC().Truncate(time.Second)
	tag := func(n uint32) *uint32 { return &n }
	for i, c := range []struct {
		obs    []byte
		id     uuid.UUID
		target byte
		method string
		tag    uint32
		recv   time.Duration
		want   bool
	}{
		{o1, id1, 0xA, "status", 1, 0, true},
		{o1, id1, 0xA, "status", 2, 4 * time.Minute, false}, // inside the window
		{o1, id1, 0xA, "status", 1, time.Minute, false},     // duplicate
		{o1, id1, 0xA, "telemetry", 1, time.Minute, true},   // other method
		{o2, id2, 0xA, "status", 1, time.Minute, true},      // other observer
		{o1, id1, 0xB, "status", 1, time.Minute, true},      // other target
		{o1, id1, 0xA, "status", 3, polling.ReadingMinInterval + time.Second, true},
	} {
		r := pollReading(c.obs, c.target, c.method, now, tag(c.tag))
		if got, err := s.InsertRepeaterReading(ctx, r, c.id, now.Add(c.recv)); err != nil || got != c.want {
			t.Fatal(i, got, err)
		}
	}
}

func TestPollingClaimReceivedAtPostgres(t *testing.T) {
	ctx, _, s := pollStore(t)
	o := pollKey(1)
	id := pollObserver(t, ctx, s, o)
	if got, err := s.PollingClaimReceivedAt(ctx, o); err != nil || got != nil {
		t.Fatal(got, err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := s.ReplacePollingClaim(ctx, pollClaim(o, now.Add(-time.Hour), 0xA), id, now); err != nil {
		t.Fatal(err)
	}
	if got, err := s.PollingClaimReceivedAt(ctx, o); err != nil || got == nil || !got.Equal(now) {
		t.Fatal(got, err)
	}
}

func TestPollingTargetsAdminPostgres(t *testing.T) {
	ctx, _, s := pollStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	claim := func(b byte, recv time.Time, targets ...byte) uuid.UUID {
		t.Helper()
		id := pollObserver(t, ctx, s, pollKey(b))
		if _, err := s.ReplacePollingClaim(ctx, pollClaim(pollKey(b), recv, targets...), id, recv); err != nil {
			t.Fatal(err)
		}
		return id
	}
	id1 := claim(1, now.Add(-5*time.Minute), 0xA)
	id2 := claim(2, now, 0xA)
	claim(3, now.Add(-10*time.Minute), 0xB)
	claim(4, now.Add(-2*time.Hour), 0xB) // expired: not listed
	snr := 7.5
	if err := s.ReplacePollingAssignments(ctx, pollKey(0xA), []polling.Assignment{{Observer: pollKey(1), Rank: 0, EvidenceSNR: &snr, EvidenceCount: 4}}, now); err != nil {
		t.Fatal(err)
	}
	claim(5, now.Add(-2*time.Hour), 0xC) // expired, but its candidate row is kept until cleanup
	if err := s.ReplacePollingAssignments(ctx, pollKey(0xC), []polling.Assignment{{Observer: pollKey(9), Rank: 0}, {Observer: pollKey(5), Rank: 1}}, now); err != nil {
		t.Fatal(err)
	}
	if err := s.SetPollingObserver(ctx, pollKey(1), false, true, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := s.InsertRepeaterReading(ctx, pollReading(pollKey(1), 0xA, "status", now.Add(-10*time.Minute), nil), id1, now); err != nil {
		t.Fatal(err)
	}
	if _, err := s.InsertRepeaterReading(ctx, pollReading(pollKey(2), 0xA, "status", now.Add(-time.Minute), nil), id2, now); err != nil {
		t.Fatal(err)
	}
	got, err := s.PollingTargetsAdmin(ctx, now.Add(-time.Hour), 500)
	if err != nil || len(got) != 3 {
		t.Fatal(len(got), err)
	}
	if got[0].PublicKey != hex.EncodeToString(pollKey(0xA)) || got[1].PublicKey != hex.EncodeToString(pollKey(0xB)) || got[2].PublicKey != hex.EncodeToString(pollKey(0xC)) {
		t.Fatal("order", got[0].PublicKey[:2], got[1].PublicKey[:2], got[2].PublicKey[:2])
	}
	a := got[0]
	if a.LatestTrustedReading == nil || *a.LatestTrustedReading != now.Add(-10*time.Minute).UnixMilli() ||
		a.LatestReading == nil || *a.LatestReading != now.Add(-time.Minute).UnixMilli() {
		t.Fatal("A reading times", a.LatestTrustedReading, a.LatestReading)
	}
	if len(a.Candidates) != 2 {
		t.Fatal("A candidates", a.Candidates)
	}
	c0, c1 := a.Candidates[0], a.Candidates[1]
	if c0.ObserverPublicKey != hex.EncodeToString(pollKey(1)) || c0.Rank == nil || *c0.Rank != 0 || c0.EvidenceSnr == nil ||
		*c0.EvidenceSnr != 7.5 || c0.EvidenceCount != 4 || c0.KeyMode != "own" || !c0.Approved {
		t.Fatalf("A poller %+v", c0)
	}
	if c1.ObserverPublicKey != hex.EncodeToString(pollKey(2)) || c1.Rank != nil || c1.EvidenceSnr != nil || c1.Approved {
		t.Fatalf("A unassigned %+v", c1)
	}
	if b := got[1]; len(b.Candidates) != 1 || b.Candidates[0].ObserverPublicKey != hex.EncodeToString(pollKey(3)) || b.LatestReading != nil {
		t.Fatalf("B %+v", b)
	}
	if c := got[2]; len(c.Candidates) != 2 || c.Candidates[0].Rank == nil || c.Candidates[0].KeyMode != "" {
		t.Fatalf("C %+v", c)
	}
	// An expired claim's state is not shown as current.
	if c := got[2].Candidates[1]; c.ObserverPublicKey != hex.EncodeToString(pollKey(5)) || c.KeyMode != "" || c.State != "" {
		t.Fatalf("C expired claimant %+v", c)
	}
	if got, err := s.PollingTargetsAdmin(ctx, now.Add(-time.Hour), 2); err != nil || len(got) != 2 || got[1].PublicKey != hex.EncodeToString(pollKey(0xB)) {
		t.Fatal("limit", len(got), err)
	}
}

func TestPollingCandidatesEvidenceSincePostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	o := pollKey(1)
	id := pollObserver(t, ctx, s, o)
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := s.ReplacePollingClaim(ctx, pollClaim(o, now, 0xA), id, now); err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		at  time.Time
		snr float32
	}{{now.Add(-25 * time.Hour), 20}, {now.Add(-time.Hour), 4}} {
		if _, err := pool.Exec(ctx, `WITH p AS (
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,origin_pubkey,raw_payload,raw_header,first_heard_at,last_heard_at)
VALUES (decode(md5(gen_random_uuid()::text),'hex'),4,0,1,$1,'\x00','\x00',$2,$2) RETURNING packet_hash)
INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type,rssi,snr)
SELECT packet_hash,$3,'YVR',$2,0,1,0,4,-90,$4 FROM p`, pollKey(0xA), c.at, id, c.snr); err != nil {
			t.Fatal(err)
		}
	}
	cs, err := s.ListPollingCandidates(ctx, pollKey(0xA), now.Add(-time.Hour), now.Add(-24*time.Hour))
	if err != nil || len(cs) != 1 {
		t.Fatal(len(cs), err)
	}
	if cs[0].EvidenceCount != 1 || cs[0].EvidenceSNR == nil || *cs[0].EvidenceSNR != 4 {
		t.Fatal("old advert counted", cs[0].EvidenceCount, cs[0].EvidenceSNR)
	}
}

// pollHearAdvert records a zero-hop advert of target heard by the observer, which is election evidence.
func pollHearAdvert(t *testing.T, ctx context.Context, pool *pgxpool.Pool, target []byte, observer uuid.UUID, at time.Time, snr float32) {
	t.Helper()
	if _, err := pool.Exec(ctx, `WITH p AS (
INSERT INTO packets(packet_hash,payload_type,payload_version,route_type,origin_pubkey,raw_payload,raw_header,first_heard_at,last_heard_at)
VALUES (decode(md5(gen_random_uuid()::text),'hex'),4,0,1,$1,'\x00','\x00',$2,$2) RETURNING packet_hash)
INSERT INTO packet_observations(packet_hash,observer_id,iata,heard_at,path_length_byte,hash_size,hop_count,payload_type,rssi,snr)
SELECT packet_hash,$3,'YVR',$2,0,1,0,4,-90,$4 FROM p`, target, at, observer, snr); err != nil {
		t.Fatal(err)
	}
}

func pollClaimJSON(obs, target []byte, at time.Time, misses int) []byte {
	return fmt.Appendf(nil, `{"v":2,"origin":"o","origin_id":%q,"timestamp":%q,"plugin":"fw","nodes":[{"node_id":%q,"key":"own","misses":%d}]}`,
		hex.EncodeToString(obs), at.Format(time.RFC3339), hex.EncodeToString(target), misses)
}

func pollReadingJSON(obs, target []byte, at time.Time, tag int) []byte {
	return fmt.Appendf(nil, `{"v":2,"origin_id":%q,"plugin":"fw","readings":[{"node_id":%q,"method":"status","sampled_at":%q,"tag":%d}]}`,
		hex.EncodeToString(obs), hex.EncodeToString(target), at.Format(time.RFC3339), tag)
}

func pollEntry(t *testing.T, ctx context.Context, svc *polling.Service, obs, target []byte) polling.OwnerEntry {
	t.Helper()
	a, err := svc.Owners(ctx, polling.Caller{Observer: obs})
	if err != nil {
		t.Fatal(err)
	}
	return a.Nodes[hex.EncodeToString(target)]
}

func TestPollingEvidenceOutranksHealthyStrangerPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	svc := polling.NewService(s, config.ResolvedPolling{Enabled: true}, time.Now, slog.New(slog.DiscardHandler))
	real, stranger, x := pollKey(1), pollKey(2), pollKey(0xA)
	now := time.Now().UTC().Truncate(time.Second)
	pollHearAdvert(t, ctx, pool, x, pollObserver(t, ctx, s, real), now.Add(-time.Hour), 3)
	svc.HandleClaim(ctx, hex.EncodeToString(real), pollClaimJSON(real, x, now, 3))
	svc.HandleClaim(ctx, hex.EncodeToString(stranger), pollClaimJSON(stranger, x, now, 0))
	if e := pollEntry(t, ctx, svc, real, x); e.Role != "polling" || !e.Mine {
		t.Fatalf("evidenced observer with misses: %+v", e)
	}
	if e := pollEntry(t, ctx, svc, stranger, x); e.Role != "standby" || e.StandbyRank != 1 {
		t.Fatalf("stranger: %+v", e)
	}
	svc.HandleReading(ctx, hex.EncodeToString(stranger), pollReadingJSON(stranger, x, now, 1))
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM repeater_readings`).Scan(&n); err != nil || n != 1 {
		t.Fatal("stranger's reading not stored", n, err)
	}
	if e := pollEntry(t, ctx, svc, real, x); e.LatestTS != nil {
		t.Fatal("stranger's reading moved latest_ts", *e.LatestTS)
	}
}

func TestPollingApprovedAuthorityPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	svc := polling.NewService(s, config.ResolvedPolling{Enabled: true, Authority: config.PollingAuthorityApproved},
		time.Now, slog.New(slog.DiscardHandler))
	good, best, x := pollKey(1), pollKey(2), pollKey(0xA)
	now := time.Now().UTC().Truncate(time.Second)
	pollHearAdvert(t, ctx, pool, x, pollObserver(t, ctx, s, good), now.Add(-time.Hour), 2)
	pollHearAdvert(t, ctx, pool, x, pollObserver(t, ctx, s, best), now.Add(-time.Hour), 12)
	if err := s.SetPollingObserver(ctx, good, false, true, ""); err != nil {
		t.Fatal(err)
	}
	svc.HandleClaim(ctx, hex.EncodeToString(good), pollClaimJSON(good, x, now, 0))
	svc.HandleClaim(ctx, hex.EncodeToString(best), pollClaimJSON(best, x, now, 0))
	if e := pollEntry(t, ctx, svc, good, x); e.Role != "polling" || !e.Mine {
		t.Fatalf("approved: %+v", e)
	}
	if e := pollEntry(t, ctx, svc, best, x); e.Role != "standby" || e.Mine || e.StandbyRank != 1 {
		t.Fatalf("unapproved: %+v", e)
	}
	cands, err := s.ListPollingCandidates(ctx, x, now.Add(-time.Hour), now.Add(-24*time.Hour))
	if err != nil || len(cands) != 2 {
		t.Fatal(len(cands), err)
	}
	for _, c := range cands {
		if c.Approved != bytes.Equal(c.Observer, good) {
			t.Fatalf("approved flag for %x: %v", c.Observer[0], c.Approved)
		}
	}
	svc.HandleReading(ctx, hex.EncodeToString(best), pollReadingJSON(best, x, now, 1))
	if e := pollEntry(t, ctx, svc, good, x); e.LatestTS != nil {
		t.Fatal("unapproved reading moved latest_ts", *e.LatestTS)
	}
	// Approval makes it eligible at the next election, where its evidence wins.
	if err := s.SetPollingObserver(ctx, best, false, true, ""); err != nil {
		t.Fatal(err)
	}
	if err := svc.ElectTarget(ctx, x); err != nil {
		t.Fatal(err)
	}
	if e := pollEntry(t, ctx, svc, best, x); e.Role != "polling" || !e.Mine || e.LatestTS == nil {
		t.Fatalf("after approval: %+v", e)
	}
}
