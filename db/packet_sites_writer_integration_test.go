// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// psSites are the sites the packet_sites fixtures hear packets at; ZZZ and NEW are known codes the stream never uses.
var psSites = []string{"AAA", "BBB", "CCC", "DDD", "EEE", "FFF"}

// psT0 is the fixtures' first hearing time.
var psT0 = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

// psFixture is a migrated schema whose observation ids start above 3,000,000,000, with the psSites (plus ZZZ, NEW
// and BAD) as known codes, scopes #a (1), #b (2) and #none (3), and four observers per site.
type psFixture struct {
	t      *testing.T
	ctx    context.Context
	pool   *pgxpool.Pool
	store  *Store
	schema string
}

func newPsFixture(t *testing.T) *psFixture {
	t.Helper()
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	f := &psFixture{t: t, ctx: ctx, pool: pool, store: New(pool, pool, 0, 0)}
	f.exec("INSERT INTO iata_codes (iata) SELECT unnest($1::text[]) ON CONFLICT DO NOTHING", psSites)
	f.exec(`SELECT setval('packet_observations_id_seq', 3000000000);
INSERT INTO iata_codes (iata) VALUES ('ZZZ'), ('NEW'), ('BAD') ON CONFLICT DO NOTHING;
INSERT INTO transport_scopes (id, name, transport_key, key_fingerprint) VALUES (1, '#a', '\x01', '\x01'), (2, '#b', '\x02', '\x02'), (3, '#none', '\x03', '\x03');`)
	f.exec(`INSERT INTO observers (id, public_key)
SELECT ('00000000-0000-0000-0000-' || lpad(to_hex(n), 12, '0'))::uuid, decode(lpad(to_hex(n), 8, '0'), 'hex') FROM generate_series(1, 64) n`)
	if err := pool.QueryRow(ctx, "SELECT current_schema()").Scan(&f.schema); err != nil {
		t.Fatal(err)
	}
	return f
}

func (f *psFixture) exec(sql string, args ...any) {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, sql, args...); err != nil {
		f.t.Fatal(err)
	}
}

// psObserver is the k-th observer (1-based, up to 64).
func psObserver(k int) uuid.UUID {
	var id uuid.UUID
	binary.BigEndian.PutUint64(id[8:], uint64(k))
	return id
}

// psHash is packet i's hash.
func psHash(i int) []byte {
	h := sha256.Sum256([]byte(fmt.Sprintf("packet %d", i)))
	return h[:]
}

// psPacket stores packet i: payload type (i*7) % 16, route type (i*3) % 4, scope NULL for i % 4 = 0 else #a or #b,
// first heard at psT0 + i * 10 s.
func (f *psFixture) psPacket(i int) []byte {
	f.t.Helper()
	var scope *int32
	if i%4 != 0 {
		v := int32(1 + i%2)
		scope = &v
	}
	at := psT0.Add(time.Duration(i) * 10 * time.Second)
	f.exec(`INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, scope_id, raw_payload, raw_header, first_heard_at, last_heard_at, observation_count)
VALUES ($1, $2, 0, $3, $4, '\x00', '\x11', $5, $5, 0)`, psHash(i), int16(i*7%16), int16(i*3%4), scope, at)
	return psHash(i)
}

// psObs is one observation of the generated stream.
func psObs(hash []byte, observer int, site string, at time.Time) ingest.InsertObservationParams {
	return ingest.InsertObservationParams{
		PacketHash: hash, ObserverID: psObserver(observer), IATA: site, HeardAt: at,
		PathLengthByte: 1, HashSize: 1, HopCount: 1, PathBytes: []byte{byte(observer)},
		RSSI: -90, SNR: 5, SourceBroker: "test", PayloadType: 4,
	}
}

// psStream generates n packets' observations: 1-6 sites each, 1-4 observers per site, hearings up to 30 s after
// the packet's first hearing and out of order, 10 packets with two observers at one site in the same microsecond,
// and 15 % re-sends of an earlier (packet, observer) with a later hearing at the end.
func psStream(n int) []ingest.InsertObservationParams {
	rng := rand.New(rand.NewPCG(7, 11))
	var stream, resends []ingest.InsertObservationParams
	for i := range n {
		first := psT0.Add(time.Duration(i) * 10 * time.Second)
		sites := rng.Perm(len(psSites))[:1+rng.IntN(len(psSites))]
		for _, s := range sites {
			observers := rng.Perm(4)[:1+rng.IntN(4)]
			for k, o := range observers {
				at := first.Add(time.Duration(rng.Int64N(int64(30*time.Second))) / time.Microsecond * time.Microsecond)
				if k == 1 && i%40 == 0 {
					at = stream[len(stream)-1].HeardAt // two observers at one site in the same microsecond
				}
				obs := psObs(psHash(i), 1+s*4+o, psSites[s], at)
				stream = append(stream, obs)
				if rng.IntN(100) < 15 {
					re := obs
					re.HeardAt = at.Add(time.Duration(1+rng.IntN(60)) * time.Second)
					resends = append(resends, re)
				}
			}
		}
	}
	rng.Shuffle(len(resends), func(a, b int) { resends[a], resends[b] = resends[b], resends[a] })
	return append(stream, resends...)
}

// psInvariant counts, over every packet, the site rows the observations imply and the table lacks, the rows whose
// hearing time or packet columns differ, and the rows no observation backs.
func psInvariant(t *testing.T, ctx context.Context, q sqlc.DBTX) (missing, wrong, extra int64) {
	t.Helper()
	if err := q.QueryRow(ctx, `WITH def AS (
  SELECT po.packet_hash, po.iata, max(po.heard_at) AS site_heard_at, p.first_heard_at, p.payload_type, p.route_type, p.scope_id
  FROM packet_observations po JOIN packets p ON p.packet_hash = po.packet_hash
  GROUP BY po.packet_hash, po.iata, p.first_heard_at, p.payload_type, p.route_type, p.scope_id
), have AS (SELECT packet_hash, iata, site_heard_at, first_heard_at, payload_type, route_type, scope_id FROM packet_sites)
SELECT
  (SELECT count(*) FROM def d WHERE NOT EXISTS (SELECT 1 FROM have h WHERE h.packet_hash = d.packet_hash AND h.iata = d.iata)),
  (SELECT count(*) FROM def d JOIN have h USING (packet_hash, iata)
    WHERE (h.site_heard_at, h.first_heard_at, h.payload_type, h.route_type, h.scope_id)
          IS DISTINCT FROM (d.site_heard_at, d.first_heard_at, d.payload_type, d.route_type, d.scope_id)),
  (SELECT count(*) FROM have h WHERE NOT EXISTS (SELECT 1 FROM def d WHERE d.packet_hash = h.packet_hash AND d.iata = h.iata))`).Scan(&missing, &wrong, &extra); err != nil {
		t.Fatal(err)
	}
	return missing, wrong, extra
}

func psCheckInvariant(t *testing.T, ctx context.Context, q sqlc.DBTX, label string) {
	t.Helper()
	if m, w, x := psInvariant(t, ctx, q); m != 0 || w != 0 || x != 0 {
		t.Fatalf("%s: missing %d, wrong %d, extra %d", label, m, w, x)
	}
}

// The extended statement returns what today's returns for every call, in stream order and shuffled, stores the same
// observations and counts, and leaves exactly one row per (packet, site) at its latest hearing.
func TestInsertObservationMaintainsPacketSitesPostgres(t *testing.T) {
	stream := psStream(400)
	shuffled := append([]ingest.InsertObservationParams(nil), stream...)
	rand.New(rand.NewPCG(1, 2)).Shuffle(len(shuffled), func(a, b int) { shuffled[a], shuffled[b] = shuffled[b], shuffled[a] })
	for name, calls := range map[string][]ingest.InsertObservationParams{"stream order": stream, "shuffled": shuffled} {
		t.Run(name, func(t *testing.T) {
			a, b := newPsFixture(t), newPsFixture(t)
			for i := range 400 {
				a.psPacket(i)
				b.psPacket(i)
			}
			qb := sqlc.New(b.pool)
			for i, o := range calls {
				insA, nA, err := a.store.InsertObservation(a.ctx, o)
				if err != nil {
					t.Fatal(err)
				}
				rowB, err := qb.InsertObservationUnsited(b.ctx, sqlc.InsertObservationUnsitedParams(observationParams(o)))
				if err != nil {
					t.Fatal(err)
				}
				if insA != rowB.Inserted || nA != rowB.ObservationCount {
					t.Fatalf("call %d: extended (%v, %d), today's (%v, %d)", i, insA, nA, rowB.Inserted, rowB.ObservationCount)
				}
			}
			ia, ib := pgx.Identifier{a.schema}.Sanitize(), pgx.Identifier{b.schema}.Sanitize()
			for _, q := range []string{
				"SELECT count(*) FROM (SELECT * FROM " + ia + ".packet_observations EXCEPT ALL SELECT * FROM " + ib + ".packet_observations) x",
				"SELECT count(*) FROM (SELECT * FROM " + ib + ".packet_observations EXCEPT ALL SELECT * FROM " + ia + ".packet_observations) x",
				"SELECT count(*) FROM (SELECT packet_hash, observation_count FROM " + ia + ".packets EXCEPT ALL SELECT packet_hash, observation_count FROM " + ib + ".packets) x",
				"SELECT count(*) FROM (SELECT packet_hash, observation_count FROM " + ib + ".packets EXCEPT ALL SELECT packet_hash, observation_count FROM " + ia + ".packets) x",
			} {
				var n int
				if err := a.pool.QueryRow(a.ctx, q).Scan(&n); err != nil || n != 0 {
					t.Fatalf("%s = %d, %v", q, n, err)
				}
			}
			var rows int
			if err := a.pool.QueryRow(a.ctx, "SELECT count(*) FROM packet_sites").Scan(&rows); err != nil || rows < 400 {
				t.Fatalf("packet_sites rows %d, %v", rows, err)
			}
			psCheckInvariant(t, a.ctx, a.pool, name)
		})
	}
}

// A re-sent (packet, observer) with a later hearing returns the current count and writes nothing: no inserted or
// updated tuple in any of the three tables, and no xid beyond the sequence's own.
func TestInsertObservationResendWritesNothingPostgres(t *testing.T) {
	f := newPsFixture(t)
	for i := range 100 {
		f.psPacket(i)
	}
	for _, o := range psStream(100) {
		if _, _, err := f.store.InsertObservation(f.ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	rows, err := f.pool.Query(f.ctx, `SELECT po.packet_hash, po.observer_id, po.iata, po.heard_at, p.observation_count
FROM packet_observations po JOIN packets p USING (packet_hash) ORDER BY po.id LIMIT 50`)
	if err != nil {
		t.Fatal(err)
	}
	type sent struct {
		hash     []byte
		observer uuid.UUID
		iata     string
		at       time.Time
		count    int64
	}
	pairs, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (sent, error) {
		var s sent
		err := r.Scan(&s.hash, &s.observer, &s.iata, &s.at, &s.count)
		return s, err
	})
	if err != nil || len(pairs) != 50 {
		t.Fatalf("%d pairs, %v", len(pairs), err)
	}
	// The backend's pending table counters before and after the call, inside one transaction (no flush between).
	const writesSQL = `SELECT coalesce(sum(n_tup_ins + n_tup_upd), 0)::bigint FROM pg_stat_xact_user_tables
WHERE schemaname = current_schema() AND relname IN ('packet_observations', 'packets', 'packet_sites')`
	xids := 0
	for _, p := range pairs {
		func() {
			tx, err := f.pool.Begin(f.ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer tx.Rollback(context.Background()) //nolint:errcheck // nothing to keep
			var before, after int64
			if err := tx.QueryRow(f.ctx, writesSQL).Scan(&before); err != nil {
				t.Fatal(err)
			}
			o := psObs(p.hash, 0, p.iata, p.at.Add(time.Hour))
			o.ObserverID = p.observer
			ins, n, err := New(tx, nil, 0, 0).InsertObservation(f.ctx, o)
			if err != nil || ins || n != p.count {
				t.Fatalf("re-send = %v, %d, %v; want false, %d", ins, n, err, p.count)
			}
			var noXid bool
			if err := tx.QueryRow(f.ctx, "SELECT pg_current_xact_id_if_assigned() IS NULL").Scan(&noXid); err != nil {
				t.Fatal(err)
			}
			if err := tx.QueryRow(f.ctx, writesSQL).Scan(&after); err != nil {
				t.Fatal(err)
			}
			if after != before {
				t.Fatalf("re-send wrote %d tuple(s)", after-before)
			}
			if !noXid {
				xids++
			}
		}()
	}
	// The id default's nextval WAL-logs the sequence once per 32 values and takes an xid for it, as today's
	// statement does; nothing else may.
	if xids > 2 {
		t.Fatalf("%d of 50 re-sends took an xid; the sequence accounts for at most 2", xids)
	}
	psCheckInvariant(t, f.ctx, f.pool, "after re-sends")
}

// psLane is one ingest connection; open runs its insert inside a transaction left open.
type psLane struct {
	f    *psFixture
	conn *pgxpool.Conn
	tx   pgx.Tx
}

func (f *psFixture) lane() *psLane {
	f.t.Helper()
	conn, err := f.pool.Acquire(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(conn.Release)
	return &psLane{f: f, conn: conn}
}

func (l *psLane) db() sqlc.DBTX {
	if l.tx != nil {
		return l.tx
	}
	return l.conn
}

func (l *psLane) insert(o ingest.InsertObservationParams) bool {
	l.f.t.Helper()
	ins, _, err := New(l.db(), nil, 0, 0).InsertObservation(l.f.ctx, o)
	if err != nil {
		l.f.t.Fatal(err)
	}
	return ins
}

func (l *psLane) begin() {
	l.f.t.Helper()
	tx, err := l.conn.Begin(l.f.ctx)
	if err != nil {
		l.f.t.Fatal(err)
	}
	l.tx = tx
}

func (l *psLane) open(o ingest.InsertObservationParams) {
	l.begin()
	l.insert(o)
}

func (l *psLane) end(commit bool) {
	l.f.t.Helper()
	var err error
	if commit {
		err = l.tx.Commit(l.f.ctx)
	} else {
		err = l.tx.Rollback(l.f.ctx)
	}
	l.tx = nil
	if err != nil {
		l.f.t.Fatal(err)
	}
}

// waiting starts run on the lane's connection, waits until Postgres shows it blocked on a lock, and returns a
// function that waits for it to finish and returns its error.
func (l *psLane) waiting(run func(db sqlc.DBTX) error) func() error {
	l.f.t.Helper()
	pid := l.conn.Conn().PgConn().PID()
	done := make(chan error, 1)
	go func() { done <- run(l.conn) }()
	deadline := time.Now().Add(10 * time.Second)
	for {
		var waiting bool
		if err := l.f.pool.QueryRow(l.f.ctx, "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE pid = $1 AND NOT granted)", pid).Scan(&waiting); err != nil {
			l.f.t.Fatal(err)
		}
		if waiting {
			break
		}
		select {
		case err := <-done:
			l.f.t.Fatalf("statement did not wait: %v", err)
		default:
		}
		if time.Now().After(deadline) {
			l.f.t.Fatal("statement never waited")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return func() error {
		select {
		case err := <-done:
			return err
		case <-time.After(10 * time.Second):
			l.f.t.Fatal("statement never finished")
		}
		return nil
	}
}

// insertWaiting runs an observation insert through the store on the lane's connection, expecting it to wait.
func (l *psLane) insertWaiting(o ingest.InsertObservationParams) func() error {
	return l.waiting(func(db sqlc.DBTX) error {
		_, _, err := New(db, nil, 0, 0).InsertObservation(l.f.ctx, o)
		return err
	})
}

// psSiteRow checks that hash's row at site holds the latest stored hearing there (or that both are absent).
func (f *psFixture) psSiteRow(label string, hash []byte, site string) {
	f.t.Helper()
	var row, truth *time.Time
	if err := f.pool.QueryRow(f.ctx, `SELECT (SELECT site_heard_at FROM packet_sites WHERE packet_hash = $1 AND iata = $2),
  (SELECT max(heard_at) FROM packet_observations WHERE packet_hash = $1 AND iata = $2)`, hash, site).Scan(&row, &truth); err != nil {
		f.t.Fatal(err)
	}
	if (row == nil) != (truth == nil) || (row != nil && !row.Equal(*truth)) {
		f.t.Fatalf("%s: row %v, latest stored hearing %v", label, row, truth)
	}
	psCheckInvariant(f.t, f.ctx, f.pool, label)
}

// Two writers of one packet at one site, in every order and lock interleaving, leave the row at the latest stored
// hearing: the conflict update only moves it forward and the packet row lock orders the lanes.
func TestPacketSitesRaceMaxMergePostgres(t *testing.T) {
	f := newPsFixture(t)
	l1, l2 := f.lane(), f.lane()
	t1 := psT0.Add(time.Second)
	t2, t3 := t1.Add(time.Second), t1.Add(2*time.Second)
	next := 0
	packet := func() []byte { next++; return f.psPacket(next) }

	p := packet() // the later hearing commits first, then the earlier one
	l2.insert(psObs(p, 2, "AAA", t2))
	l1.insert(psObs(p, 1, "AAA", t1))
	f.psSiteRow("later first", p, "AAA")

	p = packet() // the earlier lane open, the later one waits, the earlier commits
	l1.open(psObs(p, 1, "AAA", t1))
	wait := l2.insertWaiting(psObs(p, 2, "AAA", t2))
	l1.end(true)
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	f.psSiteRow("later waits on earlier", p, "AAA")

	p = packet() // the later lane open, the earlier one waits, the later commits
	l2.open(psObs(p, 2, "AAA", t2))
	wait = l1.insertWaiting(psObs(p, 1, "AAA", t1))
	l2.end(true)
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	f.psSiteRow("earlier waits on later", p, "AAA")

	fill := func(db sqlc.DBTX, p []byte) error {
		_, err := sqlc.New(db).FillPacketSitesSlice(f.ctx, sqlc.FillPacketSitesSliceParams{AfterID: 0, ThroughID: 1 << 62})
		return err
	}
	p = packet() // a fill over the packet's ids open, a later hearing waits, the fill commits
	f.unsited(psObs(p, 1, "AAA", t1))
	l1.begin()
	if err := fill(l1.tx, p); err != nil {
		t.Fatal(err)
	}
	wait = l2.insertWaiting(psObs(p, 2, "AAA", t2))
	l1.end(true)
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	f.psSiteRow("later waits on a fill", p, "AAA")

	p = packet() // a later hearing open, a fill over the packet's ids waits, the hearing commits
	f.unsited(psObs(p, 1, "AAA", t1))
	l2.open(psObs(p, 2, "AAA", t2))
	wait = l1.waiting(func(db sqlc.DBTX) error { return fill(db, p) })
	l2.end(true)
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	f.psSiteRow("fill waits on a later hearing", p, "AAA")

	p = packet() // two observers at one site in the same microsecond
	l1.open(psObs(p, 1, "AAA", t2))
	wait = l2.insertWaiting(psObs(p, 2, "AAA", t2))
	l1.end(true)
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	f.psSiteRow("same microsecond", p, "AAA")

	p = packet() // one packet heard at two sites at once
	l1.open(psObs(p, 1, "AAA", t1))
	wait = l2.insertWaiting(psObs(p, 5, "BBB", t2))
	l1.end(true)
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	f.psSiteRow("two sites, AAA", p, "AAA")
	f.psSiteRow("two sites, BBB", p, "BBB")

	// a re-send with a later hearing than any stored one never moves the row
	if l1.insert(psObs(p, 1, "AAA", t3)) {
		t.Fatal("a re-send was inserted")
	}
	f.psSiteRow("re-send", p, "AAA")

	p = packet() // the later lane rolls back while the earlier one waits
	l2.open(psObs(p, 2, "AAA", t2))
	wait = l1.insertWaiting(psObs(p, 1, "AAA", t1))
	l2.end(false)
	if err := wait(); err != nil {
		t.Fatal(err)
	}
	f.psSiteRow("rolled back", p, "AAA")

	p = packet() // the packet is deleted while an observation of it waits: 23503, nothing stored
	l1.insert(psObs(p, 1, "BBB", t1))
	l2.begin()
	if _, err := l2.tx.Exec(f.ctx, "DELETE FROM packets WHERE packet_hash = $1", p); err != nil {
		t.Fatal(err)
	}
	wait = l1.insertWaiting(psObs(p, 2, "AAA", t1))
	l2.end(true)
	var pgErr *pgconn.PgError
	if err := wait(); !errors.As(err, &pgErr) || pgErr.Code != "23503" {
		t.Fatalf("insert after the packet's delete = %v, want 23503", err)
	}
	var left int
	if err := f.pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM packet_observations WHERE packet_hash = $1)
  + (SELECT count(*) FROM packet_sites WHERE packet_hash = $1)`, p).Scan(&left); err != nil || left != 0 {
		t.Fatalf("rows left for the deleted packet: %d, %v", left, err)
	}
	f.psSiteRow("deleted packet", p, "AAA")
}

// A site write that fails stores the observation with today's statement and drops trust at once; a missing packet
// and a cancelled request return their error without the fallback.
func TestInsertObservationSiteFallbackPostgres(t *testing.T) {
	f := newPsFixture(t)
	f.exec("ALTER TABLE packet_sites ADD CONSTRAINT reject_bad CHECK (iata <> 'BAD')")
	tr := NewTrust("packet_sites_fallback_test", true, PacketSitesTrustPolicy)
	tr.Earn()
	rec := &recordingDBTX{DBTX: f.pool}
	s := New(rec, f.pool, 0, 0, WithReadModels(&ReadModels{PacketSites: tr}))
	p := f.psPacket(1)

	ins, n, err := s.InsertObservation(f.ctx, psObs(p, 1, "BAD", psT0))
	if err != nil || !ins || n != 1 {
		t.Fatalf("fallback = %v, %d, %v; want true, 1, nil", ins, n, err)
	}
	if tr.Trusted() {
		t.Fatal("trust survived an observation stored without its site row")
	}
	var obs, sites int
	if err := f.pool.QueryRow(f.ctx, `SELECT (SELECT count(*) FROM packet_observations WHERE packet_hash = $1),
  (SELECT count(*) FROM packet_sites WHERE packet_hash = $1)`, p).Scan(&obs, &sites); err != nil || obs != 1 || sites != 0 {
		t.Fatalf("observations %d, site rows %d, %v; want 1, 0", obs, sites, err)
	}
	if fmt.Sprint(rec.names) != "[InsertObservation InsertObservationUnsited]" {
		t.Fatalf("statements %v", rec.names)
	}

	tr.Repaired(tr.Generation())
	rec.names = nil
	var pgErr *pgconn.PgError
	if _, _, err := s.InsertObservation(f.ctx, psObs([]byte("no such packet"), 2, "AAA", psT0)); !errors.As(err, &pgErr) || pgErr.Code != "23503" {
		t.Fatalf("missing packet = %v, want 23503", err)
	}
	if fmt.Sprint(rec.names) != "[InsertObservation]" {
		t.Fatalf("missing packet ran %v", rec.names)
	}
	if tr.Trusted() {
		t.Fatal("a failed site-writing insert left trust on")
	}

	rec.names = nil
	ctx, cancel := context.WithCancel(f.ctx)
	cancel()
	if _, _, err := s.InsertObservation(ctx, psObs(p, 3, "AAA", psT0)); err == nil {
		t.Fatal("a cancelled insert returned no error")
	}
	if fmt.Sprint(rec.names) != "[InsertObservation]" {
		t.Fatalf("cancelled insert ran %v", rec.names)
	}
}
