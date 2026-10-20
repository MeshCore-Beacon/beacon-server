// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/jackc/pgx/v5/pgconn"
)

// unsited stores observations the way an older binary does: without their site rows.
func (f *psFixture) unsited(obs ...ingest.InsertObservationParams) {
	f.t.Helper()
	q := sqlc.New(f.pool)
	for _, o := range obs {
		if _, err := q.InsertObservationUnsited(f.ctx, sqlc.InsertObservationUnsitedParams(observationParams(o))); err != nil {
			f.t.Fatal(err)
		}
	}
}

// sited stores observations through the store's writer.
func (f *psFixture) sited(obs ...ingest.InsertObservationParams) {
	f.t.Helper()
	for _, o := range obs {
		if _, _, err := f.store.InsertObservation(f.ctx, o); err != nil {
			f.t.Fatal(err)
		}
	}
}

func (f *psFixture) maxID() int64 {
	f.t.Helper()
	id, err := sqlc.New(f.pool).MaxObservationID(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	return id
}

// check runs the exact check over (0, max id].
func (f *psFixture) check() sqlc.CheckPacketSitesSliceRow {
	f.t.Helper()
	row, err := sqlc.New(f.pool).CheckPacketSitesSlice(f.ctx, sqlc.CheckPacketSitesSliceParams{AfterID: 0, ThroughID: f.maxID()})
	if err != nil {
		f.t.Fatal(err)
	}
	return row
}

func (f *psFixture) wantClean(label string) {
	f.t.Helper()
	if c := f.check(); c != (sqlc.CheckPacketSitesSliceRow{}) {
		f.t.Fatalf("%s: %+v", label, c)
	}
}

func (f *psFixture) begin() *PacketSitesSession {
	f.t.Helper()
	p, ok, err := f.store.BeginPacketSites(f.ctx)
	if err != nil || !ok {
		f.t.Fatalf("BeginPacketSites = %v, %v", ok, err)
	}
	f.t.Cleanup(func() { p.Close(context.Background()) })
	p.pause = 0
	return p
}

// packets stores packets 0..n-1 and returns the generated stream for them.
func (f *psFixture) packets(n int) []ingest.InsertObservationParams {
	f.t.Helper()
	for i := range n {
		f.psPacket(i)
	}
	return psStream(n)
}

func TestPacketSitesInitAndBackfillPostgres(t *testing.T) {
	empty := newPsFixture(t)
	p := empty.begin()
	st, err := p.Init(empty.ctx)
	if err != nil || st != (PacketSitesState{Complete: true}) {
		t.Fatalf("empty Init = %+v, %v", st, err)
	}
	empty.wantClean("empty")

	f := newPsFixture(t)
	stream := f.packets(100)
	f.unsited(stream[:300]...)
	f.exec("SELECT setval('packet_observations_id_seq', 3001199700)")
	f.unsited(stream[300:600]...)
	hi := f.maxID()
	if hi != 3_001_200_000 {
		t.Fatalf("max id %d", hi)
	}
	p = f.begin()
	st, err = p.Init(f.ctx)
	want := PacketSitesState{WriterFromID: hi, BackfilledDownToID: hi, ReconPrevHi: hi, ReconHi: hi}
	if err != nil || st != want {
		t.Fatalf("Init = %+v, %v; want %+v", st, err, want)
	}
	var saved PacketSitesState
	if found, err := f.store.LoadState(f.ctx, "packet_sites", &saved); err != nil || !found || saved != want {
		t.Fatalf("persisted %+v, %v, %v", saved, found, err)
	}
	// The writer maintains everything above WriterFromID; the backfill leaves its rows as they are.
	f.sited(stream[600:]...)
	var above int
	if err := f.pool.QueryRow(f.ctx, "SELECT count(*) FROM packet_sites").Scan(&above); err != nil || above == 0 {
		t.Fatalf("writer rows %d, %v", above, err)
	}
	st, err = p.Backfill(f.ctx, time.Minute)
	if err != nil || !st.Complete {
		t.Fatalf("Backfill = %+v, %v", st, err)
	}
	// 1.2M ids from the oldest: three slices, none below the oldest observation.
	if fmt.Sprint(p.fills) != "[[3000700000 3001200000] [3000200000 3000700000] [3000000000 3000200000]]" {
		t.Fatalf("fills %v", p.fills)
	}
	f.wantClean("after backfill")
	if n, err := sqlc.New(f.pool).FillPacketSitesSlice(f.ctx, sqlc.FillPacketSitesSliceParams{AfterID: 0, ThroughID: f.maxID()}); err != nil || n != 0 {
		t.Fatalf("a second fill wrote %d, %v", n, err)
	}
	if st, err := p.Backfill(f.ctx, time.Minute); err != nil || len(p.fills) != 3 || !st.Complete {
		t.Fatalf("a complete backfill ran again: %+v, %v, %v", st, err, p.fills)
	}

	// One session per schema.
	if _, ok, err := f.store.BeginPacketSites(f.ctx); err != nil || ok {
		t.Fatalf("second session in one schema: %v, %v", ok, err)
	}
	if other, ok, err := empty.store.BeginPacketSites(empty.ctx); err != nil || ok {
		t.Fatalf("empty schema's lock still held by its first session: %v, %v", ok, err)
	} else {
		_ = other
	}
	p.Close(f.ctx)
	if q, ok, err := newPsFixture(t).store.BeginPacketSites(f.ctx); err != nil || !ok {
		t.Fatalf("session in another schema: %v, %v", ok, err)
	} else {
		q.Close(f.ctx)
	}

	// A session begun after another advanced the watermarks reads the newer state.
	other := f.begin()
	f.unsited(psObs(psHash(0), 60, "AAA", psT0.Add(time.Hour)))
	if _, err := other.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := other.Reconcile(f.ctx); err != nil {
		t.Fatal(err)
	}
	advanced := other.State()
	other.Close(f.ctx)
	if later := f.begin(); later.State().ReconHi != advanced.ReconHi || later.State().ReconHi != f.maxID() {
		t.Fatalf("later session read %+v, want %+v", later.State(), advanced)
	}
}

func TestFillPacketSitesSliceIdempotentPostgres(t *testing.T) {
	f := newPsFixture(t)
	f.unsited(f.packets(100)...)
	q := sqlc.New(f.pool)
	if n, err := q.FillPacketSitesSlice(f.ctx, sqlc.FillPacketSitesSliceParams{AfterID: 0, ThroughID: f.maxID()}); err != nil || n == 0 {
		t.Fatalf("first fill %d, %v", n, err)
	}
	f.wantClean("filled")

	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	n, err := sqlc.New(tx).FillPacketSitesSlice(f.ctx, sqlc.FillPacketSitesSliceParams{AfterID: 0, ThroughID: f.maxID()})
	var noXid bool
	if err == nil {
		err = tx.QueryRow(f.ctx, "SELECT pg_current_xact_id_if_assigned() IS NULL").Scan(&noXid)
	}
	_ = tx.Rollback(f.ctx)
	if err != nil || n != 0 || !noXid {
		t.Fatalf("clean re-fill wrote %d (no xid %v), %v", n, noXid, err)
	}

	// Damage: 5 rows gone, every row of 3 two-site packets moved back 1 s, 1 row no observation backs.
	f.exec(`DELETE FROM packet_sites WHERE (packet_hash, iata) IN (
  SELECT packet_hash, iata FROM packet_sites WHERE packet_hash IN (
    SELECT packet_hash FROM packet_sites GROUP BY packet_hash HAVING count(*) <> 2) ORDER BY packet_hash, iata LIMIT 5)`)
	f.exec(`UPDATE packet_sites SET site_heard_at = site_heard_at - interval '1 second' WHERE packet_hash IN (
  SELECT packet_hash FROM packet_sites GROUP BY packet_hash HAVING count(*) = 2 ORDER BY packet_hash LIMIT 3)`)
	f.exec(`INSERT INTO packet_sites (packet_hash, iata, site_heard_at, first_heard_at, payload_type, route_type, scope_id)
SELECT packet_hash, 'ZZZ', now(), first_heard_at, payload_type, route_type, scope_id FROM packets ORDER BY packet_hash LIMIT 1`)
	if c := f.check(); c != (sqlc.CheckPacketSitesSliceRow{Missing: 5, Wrong: 6, Extra: 1}) {
		t.Fatalf("damage check %+v", c)
	}
	if n, err := q.FillPacketSitesSlice(f.ctx, sqlc.FillPacketSitesSliceParams{AfterID: 0, ThroughID: f.maxID()}); err != nil || n != 11 {
		t.Fatalf("repair wrote %d, %v; want 11", n, err)
	}
	// An extra row is reported, never deleted.
	if c := f.check(); c != (sqlc.CheckPacketSitesSliceRow{Extra: 1}) {
		t.Fatalf("after repair %+v", c)
	}
}

// waitForLock waits until pid is blocked on a lock.
func (f *psFixture) waitForLock(pid uint32, done <-chan error) {
	f.t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting bool
		if err := f.pool.QueryRow(f.ctx, "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted)", pid).Scan(&waiting); err != nil {
			f.t.Fatal(err)
		}
		if waiting {
			return
		}
		select {
		case err := <-done:
			f.t.Fatalf("finished without waiting: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			f.t.Fatal("never waited")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestFillPacketSitesSliceRetries23503Postgres(t *testing.T) {
	f := newPsFixture(t)
	for i := range 3 {
		f.psPacket(i)
		f.unsited(psObs(psHash(i), 1, "AAA", psT0), psObs(psHash(i), 5, "BBB", psT0.Add(time.Second)))
	}
	p := f.begin()
	if _, err := p.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	tx, err := f.pool.Begin(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background()) //nolint:errcheck // committed below
	if _, err := tx.Exec(f.ctx, "DELETE FROM packets WHERE packet_hash = $1", psHash(0)); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := p.Backfill(f.ctx, time.Minute)
		done <- err
	}()
	f.waitForLock(p.conn.Conn().PgConn().PID(), done)
	if err := tx.Commit(f.ctx); err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil || !p.State().Complete || p.retries != 1 {
		t.Fatalf("Backfill = %v, %+v, %d retries", err, p.State(), p.retries)
	}
	var got []string
	rows, err := f.pool.Query(f.ctx, "SELECT DISTINCT packet_hash FROM packet_sites")
	if err != nil {
		t.Fatal(err)
	}
	for rows.Next() {
		var h []byte
		if err := rows.Scan(&h); err != nil {
			t.Fatal(err)
		}
		got = append(got, string(h))
	}
	if len(got) != 2 {
		t.Fatalf("packets with rows %d, want 2", len(got))
	}
	f.wantClean("after the retry")

	// A slice that fails with 23503 on every attempt: the error after 3 attempts, and no progress past it.
	g := newPsFixture(t)
	g.unsited(g.packets(10)...)
	s := g.begin()
	if _, err := s.Init(g.ctx); err != nil {
		t.Fatal(err)
	}
	attempts := 0
	s.fillStmt = func(context.Context, int64, int64) (int64, error) {
		attempts++
		return 0, &pgconn.PgError{Code: "23503"}
	}
	before := s.State().BackfilledDownToID
	st, err := s.Backfill(g.ctx, time.Minute)
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" || attempts != 3 || st.BackfilledDownToID != before || st.Complete {
		t.Fatalf("Backfill = %+v, %v after %d attempts", st, err, attempts)
	}
}

func TestPacketSitesReconcilePostgres(t *testing.T) {
	f := newPsFixture(t)
	stream := f.packets(200)
	f.sited(stream[:400]...)
	p := f.begin()
	if _, err := p.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Backfill(f.ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	// Ingest through the writer: the reconcile repairs nothing.
	f.sited(stream[400:800]...)
	if n, err := p.Reconcile(f.ctx); err != nil || n != 0 {
		t.Fatalf("reconcile after a clean ingest = %d, %v", n, err)
	}
	// An older binary: new sites and later hearings at existing sites, without site rows.
	var older []ingest.InsertObservationParams
	for i := range 20 {
		older = append(older, psObs(psHash(i), 41+i%20, "FFF", psT0.Add(time.Hour)))
		older = append(older, psObs(psHash(100+i), 61, stream[0].IATA, psT0.Add(2*time.Hour)))
	}
	f.unsited(older...)
	damage := f.check()
	if damage.Missing+damage.Wrong == 0 || damage.Extra != 0 {
		t.Fatalf("older binary left %+v", damage)
	}
	prev := p.State()
	n, err := p.Reconcile(f.ctx)
	if err != nil || n != damage.Missing+damage.Wrong {
		t.Fatalf("reconcile repaired %d, %v; want %d", n, err, damage.Missing+damage.Wrong)
	}
	f.wantClean("reconciled")
	st := p.State()
	if st.ReconPrevHi != prev.ReconHi || st.ReconHi != f.maxID() || st.ReconAt == nil {
		t.Fatalf("watermarks %+v after %+v", st, prev)
	}
	for range 2 {
		if n, err := p.Reconcile(f.ctx); err != nil || n != 0 {
			t.Fatalf("idle reconcile %d, %v", n, err)
		}
	}
	// A smaller maximum (the newest packet cascaded away) never moves a watermark back.
	hi := p.State().ReconHi
	f.exec("DELETE FROM packets WHERE packet_hash = (SELECT packet_hash FROM packet_observations ORDER BY id DESC LIMIT 1)")
	if _, err := p.Reconcile(f.ctx); err != nil || p.State().ReconHi != hi || p.State().ReconPrevHi != hi {
		t.Fatalf("watermarks moved back: %+v, %v", p.State(), err)
	}
	// A gap wider than three slices is filled in slices, newest first.
	f.exec("SELECT setval('packet_observations_id_seq', $1::bigint)", hi+1_600_000)
	f.unsited(psObs(psHash(5), 62, "EEE", psT0.Add(3*time.Hour)))
	p.fills = nil
	if n, err := p.Reconcile(f.ctx); err != nil || n != 1 {
		t.Fatalf("gap reconcile %d, %v", n, err)
	}
	top := hi + 1_600_001
	want := fmt.Sprint([][2]int64{{top - 500_000, top}, {top - 1_000_000, top - 500_000}, {top - 1_500_000, top - 1_000_000}, {hi, top - 1_500_000}})
	if fmt.Sprint(p.fills) != want {
		t.Fatalf("fills %v, want %s", p.fills, want)
	}
	f.wantClean("after the gap")
}

func TestPacketSitesAuditPostgres(t *testing.T) {
	f := newPsFixture(t)
	stream := f.packets(100)
	f.sited(stream[:300]...)
	f.exec("SELECT setval('packet_observations_id_seq', 3001199700)")
	f.sited(stream[300:600]...)
	p := f.begin()
	if _, err := p.Init(f.ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Backfill(f.ctx, time.Minute); err != nil {
		t.Fatal(err)
	}
	lowest := func(lo, _ int64) int64 { return lo }
	if n, err := p.Audit(f.ctx, lowest); err != nil || n != 0 || fmt.Sprint(p.fills[len(p.fills)-1]) != "[3000000000 3000500000]" {
		t.Fatalf("clean audit %d, %v, %v", n, err, p.fills)
	}
	// A row lost below the watermark (manual SQL) comes back from the audit of its slice.
	f.exec(`DELETE FROM packet_sites WHERE (packet_hash, iata) = (
  SELECT packet_hash, iata FROM packet_observations WHERE id = 3000000001)`)
	if n, err := p.Audit(f.ctx, lowest); err != nil || n != 1 {
		t.Fatalf("audit repaired %d, %v; want 1", n, err)
	}
	f.wantClean("audited")

	// Less than one slice below the watermark: the audit covers (min - 1, ReconPrevHi].
	g := newPsFixture(t)
	g.sited(g.packets(20)...)
	s := g.begin()
	st, err := s.Init(g.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Audit(g.ctx, func(int64, int64) int64 { t.Fatal("start called for a short range"); return 0 }); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(s.fills) != fmt.Sprint([][2]int64{{3_000_000_000, st.ReconPrevHi}}) {
		t.Fatalf("short audit fills %v", s.fills)
	}
}
