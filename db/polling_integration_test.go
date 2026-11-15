// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	if ok, err := s.ReplacePollingClaim(ctx, pollClaim(o, T.Add(60*time.Second), 0xB, 0xC), id, T.Add(polling.ClaimMinInterval)); err != nil || !ok {
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

func TestCorroborateReadingsUncountedFirstPostgres(t *testing.T) {
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
	for _, tgt := range []byte{0xA, 0xB, 0xC} { // in id order
		r := pollReading(o1, tgt, "status", now, nil)
		r.PacketHash = hash
		if _, err := s.InsertRepeaterReading(ctx, r, id1, now); err != nil {
			t.Fatal(err)
		}
	}
	counted := func() string {
		t.Helper()
		var out string
		if err := pool.QueryRow(ctx, `SELECT string_agg(COALESCE(corroborated::text, '-'), '' ORDER BY id) FROM repeater_readings`).Scan(&out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	pass := func() {
		t.Helper()
		if n, err := s.CorroborateReadings(ctx, now.Add(-time.Hour), 1); err != nil || n != 1 {
			t.Fatal(n, err)
		}
	}
	// Never-counted readings go first, oldest first.
	for _, want := range []string{"1--", "11-", "111"} {
		pass()
		if got := counted(); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	// Once all are counted, the newest is recounted first.
	if _, err := pool.Exec(ctx, `UPDATE repeater_readings SET corroborated = 9`); err != nil {
		t.Fatal(err)
	}
	pass()
	if got := counted(); got != "991" {
		t.Fatalf("recount order %q", got)
	}
}

func TestCorroborateReadingsReachesEveryReadingPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := pool.Exec(ctx, `INSERT INTO repeater_readings (target_public_key, observer_public_key, sampled_at, received_at, method, tag, packet_hash)
SELECT $1, $2, $3, $3, 'status', g, '\x0102030405060708' FROM generate_series(1, 501) g`, pollKey(0xA), pollKey(1), now); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := s.CorroborateReadings(ctx, now.Add(-time.Hour), 500); err != nil {
			t.Fatal(err)
		}
	}
	var left int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM repeater_readings WHERE corroborated IS NULL`).Scan(&left); err != nil || left != 0 {
		t.Fatal("readings never counted", left, err)
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
	for _, name := range []string{"idx_polling_claims_observer", "idx_repeater_readings_observer", "idx_repeater_readings_observer_received"} {
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

func TestPollingClaimIntervalPostgres(t *testing.T) {
	ctx, _, s := pollStore(t)
	o := pollKey(1)
	id := pollObserver(t, ctx, s, o)
	now := time.Now().UTC().Truncate(time.Second)
	for i, c := range []struct {
		recv time.Duration
		want bool
	}{
		{0, true},
		{polling.ClaimMinInterval - time.Second, false},
		{polling.ClaimMinInterval, true},
	} {
		ok, err := s.ReplacePollingClaim(ctx, pollClaim(o, now.Add(time.Duration(i)*time.Second), 0xA), id, now.Add(c.recv))
		if err != nil || ok != c.want {
			t.Fatal(i, ok, err)
		}
	}
}

// Distinct claims for one observer racing inside one interval: exactly one applies, with or without a stored claim.
func TestPollingClaimIntervalConcurrentPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	for _, stored := range []bool{false, true} {
		o := pollKey(1)
		if stored {
			o = pollKey(2)
		}
		id := pollObserver(t, ctx, s, o)
		if stored {
			if ok, err := s.ReplacePollingClaim(ctx, pollClaim(o, now.Add(-time.Hour), 0xA), id, now.Add(-time.Hour)); err != nil || !ok {
				t.Fatal(ok, err)
			}
		}
		var wg sync.WaitGroup
		var mu sync.Mutex
		applied := 0
		start := make(chan struct{})
		for i := range 20 {
			wg.Go(func() {
				<-start
				ok, err := s.ReplacePollingClaim(ctx, pollClaim(o, now.Add(time.Duration(i)*time.Second), byte(0x10+i)), id,
					now.Add(time.Duration(i)*time.Millisecond))
				if err != nil {
					t.Error(err)
				}
				if ok {
					mu.Lock()
					applied++
					mu.Unlock()
				}
			})
		}
		close(start)
		wg.Wait()
		var n int
		if err := pool.QueryRow(ctx, `SELECT count(*) FROM polling_candidates WHERE observer_public_key = $1`, o).Scan(&n); err != nil {
			t.Fatal(err)
		}
		if applied != 1 || n != 1 {
			t.Fatalf("stored=%v: %d claims applied, %d candidates", stored, applied, n)
		}
	}
}

func TestRepeaterReadingRateLimitConcurrentPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	o := pollKey(1)
	id := pollObserver(t, ctx, s, o)
	now := time.Now().UTC().Truncate(time.Second)
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := range 40 {
		wg.Go(func() {
			tag := uint32(i + 1)
			<-start
			if _, err := s.InsertRepeaterReading(ctx, pollReading(o, 0xA, "status", now, &tag), id, now); err != nil {
				t.Error(err)
			}
		})
	}
	close(start)
	wg.Wait()
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM repeater_readings`).Scan(&n); err != nil || n != 1 {
		t.Fatal("rows", n, err)
	}
}

func TestRepeaterReadingObserverBudgetPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	o := pollKey(1)
	id := pollObserver(t, ctx, s, o)
	now := time.Now().UTC().Truncate(time.Second)
	insert := func(target byte, recv time.Time) bool {
		t.Helper()
		ok, err := s.InsertRepeaterReading(ctx, pollReading(o, target, "status", now, nil), id, recv)
		if err != nil {
			t.Fatal(err)
		}
		return ok
	}
	// Each reading names a new target, so only the observer-wide budget can stop them.
	for i := range polling.ReadingsPerObserverPerHour {
		if !insert(byte(i+1), now.Add(time.Duration(i)*time.Second)) {
			t.Fatalf("reading %d dropped under the budget", i+1)
		}
	}
	if insert(byte(polling.ReadingsPerObserverPerHour+1), now.Add(30*time.Minute)) {
		t.Fatal("reading over the hourly budget stored")
	}
	// Another observer has its own budget.
	o2 := pollKey(2)
	if ok, err := s.InsertRepeaterReading(ctx, pollReading(o2, 0xFE, "status", now, nil), pollObserver(t, ctx, s, o2), now.Add(30*time.Minute)); err != nil || !ok {
		t.Fatal("other observer throttled", ok, err)
	}
	// An hour after the first ones were received there is room again.
	if !insert(0xFF, now.Add(time.Hour+time.Second)) {
		t.Fatal("budget did not roll")
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM repeater_readings WHERE observer_public_key = $1`, o).Scan(&n); err != nil || n != polling.ReadingsPerObserverPerHour+1 {
		t.Fatal(n, err)
	}
}

func TestRepeaterReadingConstraintsPostgres(t *testing.T) {
	ctx, pool, _ := pollStore(t)
	for name, c := range map[string]struct {
		method, client  string
		lpp, packetHash []byte
	}{
		"hash 7 bytes":    {"status", "", nil, make([]byte, 7)},
		"hash 7000 bytes": {"status", "", nil, make([]byte, 7000)},
		"lpp 161 bytes":   {"status", "", make([]byte, 161), nil},
		"method":          {"history", "", nil, nil},
		"client":          {"status", string(bytes.Repeat([]byte("c"), 129)), nil, nil},
	} {
		_, err := pool.Exec(ctx, `INSERT INTO repeater_readings (target_public_key, observer_public_key, sampled_at, received_at, method, tag, lpp, packet_hash, client)
VALUES ($1, $2, now(), now(), $3, 1, $4, $5, $6)`, pollKey(0xA), pollKey(1), c.method, c.lpp, c.packetHash, c.client)
		if err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if _, err := pool.Exec(ctx, `INSERT INTO repeater_readings (target_public_key, observer_public_key, sampled_at, received_at, method, tag, lpp, packet_hash, client)
VALUES ($1, $2, now(), now(), 'telemetry', 1, $3, $4, $5)`, pollKey(0xA), pollKey(1), make([]byte, 160), make([]byte, 8), string(bytes.Repeat([]byte("c"), 128))); err != nil {
		t.Fatal("limits themselves rejected", err)
	}
}

// A reading whose insert transaction is still open holds back a racing one from the same observer until it commits.
func TestRepeaterReadingWaitsForOpenInsertPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	o := pollKey(1)
	id := pollObserver(t, ctx, s, o)
	now := time.Now().UTC().Truncate(time.Second)
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	tag1, tag2 := uint32(1), uint32(2)
	if ok, err := insertRepeaterReading(ctx, sqlc.New(tx), pollReading(o, 0xA, "status", now, &tag1), id, now); err != nil || !ok {
		t.Fatal(ok, err)
	}
	done := make(chan bool, 1)
	go func() {
		ok, err := s.InsertRepeaterReading(ctx, pollReading(o, 0xA, "status", now, &tag2), id, now)
		if err != nil {
			t.Error(err)
		}
		done <- ok
	}()
	select {
	case ok := <-done:
		t.Fatalf("second insert did not wait for the first (stored=%v)", ok)
	case <-time.After(300 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if <-done {
		t.Fatal("second reading stored inside the window")
	}
}

// Orphan cleanup takes the target's election lock, so it waits for an election in progress.
func TestPollingOrphanCleanupWaitsForElectionPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	if err := s.ReplacePollingAssignments(ctx, pollKey(0xA), []polling.Assignment{{Observer: pollKey(9), Rank: 0}}, now); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	if err := sqlc.New(tx).LockPollingTarget(ctx, pollKey(0xA)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- s.DeleteExpiredPolling(ctx, now.Add(-time.Hour), now.Add(-time.Hour)) }()
	select {
	case err := <-done:
		t.Fatal("cleanup did not wait for the election lock", err)
	case <-time.After(300 * time.Millisecond):
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM polling_assignments`).Scan(&n); err != nil || n != 0 {
		t.Fatal("orphan kept", n, err)
	}
}

// The target and observer locks are separate classes and scoped to the schema: only the same lock in the same
// schema waits.
func TestPollingLockKeysPostgres(t *testing.T) {
	ctx, poolA, _ := pollStore(t)
	_, poolB, _ := pollStore(t)
	k := pollKey(0xA)
	held, err := poolA.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer held.Rollback(ctx)
	if err := sqlc.New(held).LockPollingTarget(ctx, k); err != nil {
		t.Fatal(err)
	}
	try := func(pool *pgxpool.Pool, lock func(q *sqlc.Queries) error) error {
		t.Helper()
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer tx.Rollback(ctx)
		if _, err := tx.Exec(ctx, `SET LOCAL lock_timeout = '200ms'`); err != nil {
			t.Fatal(err)
		}
		return lock(sqlc.New(tx))
	}
	target := func(q *sqlc.Queries) error { return q.LockPollingTarget(ctx, k) }
	observer := func(q *sqlc.Queries) error { return q.LockPollingObserver(ctx, k) }
	if err := try(poolA, target); err == nil {
		t.Fatal("same target lock in the same schema did not wait")
	}
	if err := try(poolA, observer); err != nil {
		t.Fatal("observer lock collided with the target lock", err)
	}
	if err := try(poolB, target); err != nil {
		t.Fatal("another schema waited for this schema's lock", err)
	}
}

func readingTrusted(t *testing.T, ctx context.Context, pool *pgxpool.Pool, obs []byte) bool {
	t.Helper()
	var trusted bool
	if err := pool.QueryRow(ctx, `SELECT trusted FROM repeater_readings WHERE observer_public_key = $1`, obs).Scan(&trusted); err != nil {
		t.Fatal(err)
	}
	return trusted
}

func TestRepeaterReadingTrustedAtInsertPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	now := time.Now().UTC().Truncate(time.Second)
	a, b, c := pollKey(1), pollKey(2), pollKey(3)
	ids := map[byte]uuid.UUID{}
	for _, k := range [][]byte{a, b, c} {
		ids[k[0]] = pollObserver(t, ctx, s, k)
		if _, err := s.ReplacePollingClaim(ctx, pollClaim(k, now, 0xA), ids[k[0]], now); err != nil {
			t.Fatal(err)
		}
	}
	// A has evidence, B is assigned without it, C has a live claim and no assignment row.
	if err := s.ReplacePollingAssignments(ctx, pollKey(0xA), []polling.Assignment{
		{Observer: a, Rank: 0, EvidenceCount: 3}, {Observer: b, Rank: 1},
	}, now); err != nil {
		t.Fatal(err)
	}
	for _, k := range [][]byte{a, b, c} {
		if ok, err := s.InsertRepeaterReading(ctx, pollReading(k, 0xA, "status", now, nil), ids[k[0]], now); err != nil || !ok {
			t.Fatal(ok, err)
		}
	}
	for k, want := range map[byte]bool{1: true, 2: false, 3: false} {
		if got := readingTrusted(t, ctx, pool, pollKey(k)); got != want {
			t.Fatalf("observer %d trusted = %v, want %v", k, got, want)
		}
	}
	// Removing the assignment later does not change what was stored.
	if err := s.ReplacePollingAssignments(ctx, pollKey(0xA), nil, now); err != nil {
		t.Fatal(err)
	}
	if !readingTrusted(t, ctx, pool, a) {
		t.Fatal("stored reading lost its trust when the assignment went")
	}
}

func TestRepeaterReadingTrustMigrationBackfillPostgres(t *testing.T) {
	ctx, pool, _ := pollStore(t)
	const name, indexName = "065_repeater_reading_trust.sql", "066_repeater_reading_trust_index.sql"
	// The helper cannot stop at a migration, so undo 065 (dropping the column takes its index too) and run it again over existing rows.
	if _, err := pool.Exec(ctx, `ALTER TABLE repeater_readings DROP COLUMN trusted`); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM schema_migrations WHERE filename = $1`, name); err != nil {
		t.Fatal(err)
	}
	a, b := pollKey(1), pollKey(2)
	now := time.Now().UTC().Truncate(time.Second)
	if _, err := pool.Exec(ctx, `INSERT INTO polling_assignments (target_public_key, observer_public_key, rank, evidence_count, assigned_at)
VALUES ($1, $2, 0, 2, $3)`, pollKey(0xA), a, now); err != nil {
		t.Fatal(err)
	}
	for i, k := range [][]byte{a, b} {
		if _, err := pool.Exec(ctx, `INSERT INTO repeater_readings (target_public_key, observer_public_key, sampled_at, received_at, method, tag)
VALUES ($1, $2, $3, $3, 'status', $4)`, pollKey(0xA), k, now, i); err != nil {
			t.Fatal(err)
		}
	}
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if !readingTrusted(t, ctx, pool, a) || readingTrusted(t, ctx, pool, b) {
		t.Fatal("backfill marked the wrong readings")
	}
	// 065 only backfills: the index is 066's, built concurrently so it never holds the table lock.
	if trustedIndexValid(t, ctx, pool) {
		t.Fatal("065 built the trusted index")
	}
	if _, err := pool.Exec(ctx, `DELETE FROM schema_migrations WHERE filename = $1`, indexName); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	if !trustedIndexValid(t, ctx, pool) {
		t.Fatal("066 did not build a valid trusted index")
	}
}

func trustedIndexValid(t *testing.T, ctx context.Context, pool *pgxpool.Pool) bool {
	t.Helper()
	var valid bool
	err := pool.QueryRow(ctx, `SELECT coalesce(bool_and(indisvalid AND indisready), false) FROM pg_index WHERE indexrelid = to_regclass('idx_repeater_readings_trusted')`).Scan(&valid)
	if err != nil {
		t.Fatal(err)
	}
	return valid
}

func TestNodeReadingsPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	obs := pollKey(1)
	oid := pollObserver(t, ctx, s, obs)
	target := pollKey(0xA)
	nodeID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO nodes (id, public_key, node_type, name) VALUES ($1, $2, 2, 'rep')`, nodeID, target); err != nil {
		t.Fatal(err)
	}
	bare := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO nodes (id, public_key, node_type, name) VALUES ($1, $2, 2, 'bare')`, bare, pollKey(0xB)); err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC().Truncate(time.Second)
	since := now.Add(-24 * time.Hour)
	add := func(at time.Time, method string, trusted bool, tag int, battery int, recv *int64) {
		t.Helper()
		_, err := pool.Exec(ctx, `INSERT INTO repeater_readings (target_public_key, observer_public_key, observer_id, sampled_at, received_at, method, tag, battery_mv, packets_recv, uptime_secs, trusted)
			VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $8, $9, $10)`,
			target, obs, oid, at, method, tag, battery, recv, recv, trusted)
		if err != nil {
			t.Fatal(err)
		}
	}
	n := func(v int64) *int64 { return &v }
	add(since.Add(-30*time.Hour), "status", true, 1, 3900, n(10)) // older baseline, not the newest
	add(since.Add(-2*time.Hour), "status", true, 2, 3950, n(100)) // the baseline
	add(since.Add(-1*time.Hour), "telemetry", true, 3, 3960, nil) // telemetry never a baseline
	add(since.Add(1*time.Hour), "status", true, 4, 4000, n(200))
	add(since.Add(2*time.Hour), "telemetry", false, 5, 4100, nil) // untrusted
	add(since.Add(3*time.Hour), "status", false, 6, 4200, n(300)) // untrusted
	add(since.Add(4*time.Hour), "telemetry", true, 7, 4300, nil)

	got, polled, untrusted, err := s.GetNodeReadings(ctx, nodeID, since, time.Time{}, false)
	if err != nil || !polled || untrusted != 2 {
		t.Fatalf("default: polled=%v untrusted=%d err=%v", polled, untrusted, err)
	}
	var batt []int32
	for _, r := range got {
		if !r.Trusted {
			t.Errorf("untrusted reading returned by default: %+v", r)
		}
		batt = append(batt, *r.BatteryMV)
	}
	if fmt.Sprint(batt) != "[3950 4000 4300]" {
		t.Errorf("batteries = %v, want baseline then the two trusted in-range readings", batt)
	}

	got, _, untrusted, err = s.GetNodeReadings(ctx, nodeID, since, time.Time{}, true)
	if err != nil || untrusted != 0 || len(got) != 5 {
		t.Fatalf("include: n=%d untrusted=%d err=%v", len(got), untrusted, err)
	}
	if !sort.SliceIsSorted(got, func(i, j int) bool { return got[i].SampledAt.Before(got[j].SampledAt) }) {
		t.Error("readings not ascending")
	}

	// until bounds the range
	got, _, _, err = s.GetNodeReadings(ctx, nodeID, since, since.Add(2*time.Hour), true)
	if err != nil || len(got) != 2 {
		t.Fatalf("until: n=%d err=%v", len(got), err)
	}

	// polled counts readings outside the range too
	_, polled, _, err = s.GetNodeReadings(ctx, nodeID, now.Add(time.Hour), time.Time{}, false)
	if err != nil || !polled {
		t.Fatalf("out of range: polled=%v err=%v", polled, err)
	}

	got, polled, untrusted, err = s.GetNodeReadings(ctx, bare, since, time.Time{}, false)
	if err != nil || polled || untrusted != 0 || len(got) != 0 {
		t.Fatalf("never polled: %v %v %d %v", got, polled, untrusted, err)
	}

	if _, _, _, err = s.GetNodeReadings(ctx, uuid.New(), since, time.Time{}, false); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("unknown node err = %v, want pgx.ErrNoRows", err)
	}
}

func TestNodePollingPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	target := pollKey(0xA)
	nodeID, bare := uuid.New(), uuid.New()
	for id, k := range map[uuid.UUID][]byte{nodeID: target, bare: pollKey(0xB)} {
		if _, err := pool.Exec(ctx, `INSERT INTO nodes (id, public_key, node_type, name) VALUES ($1, $2, 2, 'rep')`, id, k); err != nil {
			t.Fatal(err)
		}
	}
	o1, o2, o3, o4 := pollKey(1), pollKey(2), pollKey(3), pollKey(4)
	id1, id2 := pollObserver(t, ctx, s, o1), pollObserver(t, ctx, s, o2)
	id3 := pollObserver(t, ctx, s, o3)
	if _, err := pool.Exec(ctx, `UPDATE observers SET display_name = 'HOWL V4.2' WHERE id = $1`, id1); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE observers SET display_name = '' WHERE id = $1`, id3); err != nil {
		t.Fatal(err)
	}
	// o2 keeps a NULL name; o4 has an assignment but no observers row.
	now := time.Now().UTC().Truncate(time.Second)
	for i, k := range [][]byte{o1, o2, o3, o4} {
		rank := []int{0, 1, 2, 3}[i]
		if _, err := pool.Exec(ctx, `INSERT INTO polling_assignments (target_public_key, observer_public_key, rank, assigned_at) VALUES ($1, $2, $3, $4)`,
			target, k, rank, now); err != nil {
			t.Fatal(err)
		}
	}
	add := func(at time.Time, tag int, trusted bool) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO repeater_readings (target_public_key, observer_public_key, observer_id, sampled_at, received_at, method, tag, battery_mv, trusted)
			VALUES ($1, $2, $3, $4, $4, 'status', $5, 4000, $6)`, target, o1, id1, at, tag, trusted); err != nil {
			t.Fatal(err)
		}
	}
	trustedAt := now.Add(-2 * time.Hour)
	add(trustedAt, 1, true)
	add(now.Add(-time.Hour), 2, false) // newer but untrusted

	got, err := s.GetNodePolling(ctx, nodeID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Polled || got.LatestReadingAt == nil || *got.LatestReadingAt != trustedAt.UnixMilli() {
		t.Errorf("polled=%v latest=%v, want the trusted reading %d", got.Polled, got.LatestReadingAt, trustedAt.UnixMilli())
	}
	if got.Poller == nil || got.Poller.ObserverID != id1 || got.Poller.Name != "HOWL V4.2" || got.Poller.Rank != 0 {
		t.Errorf("poller = %+v", got.Poller)
	}
	if len(got.Standbys) != 2 || got.Standbys[0].ObserverID != id2 || got.Standbys[0].Rank != 1 || got.Standbys[0].Name != "02020202" ||
		got.Standbys[1].ObserverID != id3 || got.Standbys[1].Rank != 2 || got.Standbys[1].Name != "03030303" {
		t.Errorf("standbys = %+v (missing observer row must be left out, empty names fall back to key hex)", got.Standbys)
	}

	// Only untrusted readings: polled, but no latest reading.
	if _, err := pool.Exec(ctx, `DELETE FROM repeater_readings WHERE tag = 1`); err != nil {
		t.Fatal(err)
	}
	if got, err = s.GetNodePolling(ctx, nodeID); err != nil || !got.Polled || got.LatestReadingAt != nil {
		t.Errorf("untrusted only: %+v %v", got, err)
	}

	// A deleted poller row leaves the poller out.
	if _, err := pool.Exec(ctx, `DELETE FROM observers WHERE id = $1`, id1); err != nil {
		t.Fatal(err)
	}
	if got, err = s.GetNodePolling(ctx, nodeID); err != nil || got.Poller != nil || len(got.Standbys) != 2 {
		t.Errorf("deleted poller: %+v %v", got, err)
	}

	got, err = s.GetNodePolling(ctx, bare)
	if err != nil || got.Polled || got.LatestReadingAt != nil || got.Poller != nil || got.Standbys == nil || len(got.Standbys) != 0 {
		t.Fatalf("bare node: %+v %v", got, err)
	}
	if _, err = s.GetNodePolling(ctx, uuid.New()); !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("unknown node err = %v, want pgx.ErrNoRows", err)
	}
}

func TestNodeReadingsKeepTheMethodPostgres(t *testing.T) {
	ctx, pool, s := pollStore(t)
	obs := pollKey(1)
	oid := pollObserver(t, ctx, s, obs)
	target := pollKey(0xA)
	nodeID := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO nodes (id, public_key, node_type, name) VALUES ($1, $2, 2, 'rep')`, nodeID, target); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
	add := func(at time.Time, method string, tag int, recv *int64) {
		t.Helper()
		if _, err := pool.Exec(ctx, `INSERT INTO repeater_readings (target_public_key, observer_public_key, observer_id, sampled_at, received_at, method, tag, packets_recv, trusted)
			VALUES ($1, $2, $3, $4, $4, $5, $6, $7, true)`, target, obs, oid, at, method, tag, recv); err != nil {
			t.Fatal(err)
		}
	}
	n := func(v int64) *int64 { return &v }
	add(now.Add(-6*time.Hour), "status", 1, n(100))
	add(now.Add(-2*time.Hour), "status", 2, nil) // a status reading whose reply carried no counters
	add(now.Add(-time.Hour), "telemetry", 3, nil)
	add(now.Add(-time.Minute), "status", 4, n(150))
	for name, since := range map[string]time.Time{"wide": now.Add(-8 * time.Hour), "narrow": now.Add(-90 * time.Minute)} {
		got, _, _, err := s.GetNodeReadings(ctx, nodeID, since, time.Time{}, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range got {
			if r.Method != "status" && r.Method != "telemetry" {
				t.Fatalf("%s: method not carried: %+v", name, r)
			}
		}
		pts := api.BucketNodeReadings(got, since, 24*time.Hour)
		last := pts[len(pts)-1]
		if last.PacketsReceived != nil || last.CounterSpanSecs != nil {
			t.Errorf("%s: the counterless status reading was bridged: %+v", name, last)
		}
	}
}
