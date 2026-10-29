// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// cmiFixture is a migrated schema whose message and observation ids start above 3,000,000,000, with sites,
// scopes #fr (3) and #none (4), and channels 7, 8 and 9.
type cmiFixture struct {
	t     *testing.T
	ctx   context.Context
	pool  *pgxpool.Pool
	store *Store
}

func newCmiFixture(t *testing.T) *cmiFixture {
	t.Helper()
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	f := &cmiFixture{t: t, ctx: ctx, pool: pool, store: New(pool, pool, 0, 0)}
	f.exec(`SELECT setval('channel_messages_id_seq', 3000000000);
SELECT setval('packet_observations_id_seq', 3000000000);
INSERT INTO iata_codes (iata) VALUES ('YUL'),('YOW'),('YYZ'),('YVR'),('YQB'),('BAD'),('ZZZ') ON CONFLICT DO NOTHING;
INSERT INTO transport_scopes (id, name, transport_key, key_fingerprint) VALUES (3, '#fr', '\x00', '\x01'), (4, '#none', '\x02', '\x03');
INSERT INTO channels (id, channel_hash, name) VALUES (7, '\x11', 'ch7'), (8, '\x12', 'ch8'), (9, '\x13', 'ch9');`)
	return f
}

func (f *cmiFixture) exec(sql string, args ...any) {
	f.t.Helper()
	if _, err := f.pool.Exec(f.ctx, sql, args...); err != nil {
		f.t.Fatal(err)
	}
}

// packet stores a group-text packet with the given scope id (0 = none).
func (f *cmiFixture) packet(hash string, scope int32) []byte {
	f.t.Helper()
	h := []byte(hash)
	var scopeID *int32
	if scope != 0 {
		scopeID = &scope
	}
	f.exec(`INSERT INTO packets (packet_hash, payload_type, payload_version, route_type, scope_id, raw_payload, raw_header, first_heard_at, last_heard_at, observation_count)
VALUES ($1, 5, 0, 1, $2, '\x00', '\x15', now(), now(), 0)`, h, scopeID)
	return h
}

// obs stores one observation of hash at iata by a new observer and returns its id; typed=false leaves payload_type NULL.
func (f *cmiFixture) obs(db execQuerier, hash []byte, iata string, typed bool) int64 {
	f.t.Helper()
	var pt *int16
	if typed {
		v := int16(5)
		pt = &v
	}
	var id int64
	if err := db.QueryRow(f.ctx, `WITH o AS (INSERT INTO observers (public_key) VALUES (decode(md5(random()::text), 'hex')) RETURNING id)
INSERT INTO packet_observations (packet_hash, observer_id, iata, heard_at, path_length_byte, hash_size, hop_count, payload_type)
SELECT $1, o.id, $2, now(), 0, 1, 0, $3 FROM o RETURNING id`, hash, iata, pt).Scan(&id); err != nil {
		f.t.Fatal(err)
	}
	return id
}

func (f *cmiFixture) message(s *Store, channel int, hash []byte, historical bool) *ingest.InsertedChannelMessage {
	f.t.Helper()
	m, err := s.InsertChannelMessage(f.ctx, ingest.InsertChannelMessageParams{ChannelID: channel, PacketHash: hash, SenderName: "s", Content: "c", SentAt: time.Now(), Historical: historical})
	if err != nil {
		f.t.Fatal(err)
	}
	return m
}

// rows lists the cmi rows as "message/iata/channel/scope", in key order.
func (f *cmiFixture) rows() []string {
	f.t.Helper()
	rows, err := f.pool.Query(f.ctx, "SELECT message_id, iata, channel_id, coalesce(scope_id, 0) FROM channel_message_iatas ORDER BY 1, 2")
	if err != nil {
		f.t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, func(r pgx.CollectableRow) (string, error) {
		var m int64
		var iata string
		var ch, sc int32
		err := r.Scan(&m, &iata, &ch, &sc)
		return fmt.Sprintf("%d/%s/%d/%d", m, iata, ch, sc), err
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return out
}

// definition counts the pairs the read model's definition has and the table lacks, and table pairs no stored
// observation backs, over every message.
func (f *cmiFixture) definition() (missing, extra int64) {
	f.t.Helper()
	if err := f.pool.QueryRow(f.ctx, `WITH def AS (
  SELECT DISTINCT cm.id AS message_id, po.iata, cm.channel_id, p.scope_id
  FROM channel_messages cm JOIN packets p ON p.packet_hash = cm.packet_hash JOIN packet_observations po ON po.packet_hash = cm.packet_hash
), have AS (SELECT message_id, iata, channel_id, scope_id FROM channel_message_iatas)
SELECT (SELECT count(*) FROM (SELECT * FROM def EXCEPT SELECT * FROM have) m), (SELECT count(*) FROM (SELECT * FROM have EXCEPT SELECT * FROM def) x)`).Scan(&missing, &extra); err != nil {
		f.t.Fatal(err)
	}
	return missing, extra
}

func (f *cmiFixture) checkDefinition(label string) {
	f.t.Helper()
	if missing, extra := f.definition(); missing != 0 || extra != 0 {
		f.t.Fatalf("%s: missing %d, extra %d; rows %v", label, missing, extra, f.rows())
	}
}

func TestInsertChannelMessageSeedsSitesPostgres(t *testing.T) {
	f := newCmiFixture(t)
	p := f.packet("P", 3)
	f.obs(f.pool, p, "YUL", true)
	f.obs(f.pool, p, "YOW", true)
	f.obs(f.pool, p, "YUL", true)
	m := f.message(f.store, 7, p, true)
	if m == nil || m.ID <= 3000000000 {
		t.Fatalf("message = %+v", m)
	}
	want := []string{fmt.Sprintf("%d/YOW/7/3", m.ID), fmt.Sprintf("%d/YUL/7/3", m.ID)}
	if got := f.rows(); !slices.Equal(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	if again := f.message(f.store, 7, p, true); again != nil {
		t.Fatalf("duplicate returned %+v", again)
	}
	if got := f.rows(); !slices.Equal(got, want) {
		t.Fatalf("rows after the duplicate = %v, want %v", got, want)
	}
	var dirty int
	if err := f.pool.QueryRow(f.ctx, "SELECT count(*) FROM analytics_dirty_hours WHERE hour = date_trunc('hour', now(), 'UTC')").Scan(&dirty); err != nil || dirty != 1 {
		t.Fatalf("historical message queued %d hours: %v", dirty, err)
	}

	// A packet with no observation: the message is stored, with no site.
	q := f.packet("Q", 0)
	if mq := f.message(f.store, 8, q, false); mq == nil {
		t.Fatal("message without observations not stored")
	}
	if got := f.rows(); !slices.Equal(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
	f.checkDefinition("seed")
}

func TestInsertChannelMessageSeedFallbackPostgres(t *testing.T) {
	f := newCmiFixture(t)
	f.exec("ALTER TABLE channel_message_iatas ADD CONSTRAINT reject_bad CHECK (iata <> 'BAD')")
	p := f.packet("P", 0)
	f.obs(f.pool, p, "BAD", true)
	m := f.message(f.store, 7, p, false)
	if m == nil {
		t.Fatal("the unseeded statement did not store the message")
	}
	if got := f.rows(); len(got) != 0 {
		t.Fatalf("rows = %v, want none", got)
	}
	ch, err := f.store.GetChannel(f.ctx, 7)
	if err != nil || ch.MessageCount != 1 {
		t.Fatalf("message count = %+v, %v; want 1", ch, err)
	}
	if again := f.message(f.store, 7, p, false); again != nil {
		t.Fatalf("duplicate through the fallback returned %+v", again)
	}

	// A packet missing from packets: both statements return today's foreign key error.
	_, err = f.store.InsertChannelMessage(f.ctx, ingest.InsertChannelMessageParams{ChannelID: 7, PacketHash: []byte("missing"), SentAt: time.Now()})
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
		t.Fatalf("missing packet: %v, want 23503", err)
	}
	_, err = f.store.q.InsertChannelMessageUnseeded(f.ctx, sqlc.InsertChannelMessageUnseededParams{ChannelID: 7, PacketHash: []byte("missing"), SentAt: pgtype.Timestamptz{Time: time.Now(), Valid: true}})
	if !errors.As(err, &pgErr) || pgErr.Code != "23503" {
		t.Fatalf("missing packet, unseeded: %v, want 23503", err)
	}
}

func TestAddChannelMessageIATAPostgres(t *testing.T) {
	f := newCmiFixture(t)
	p := f.packet("P", 3)
	if found, err := f.store.AddChannelMessageIATA(f.ctx, p, "YOW"); err != nil || found {
		t.Fatalf("Add with no message: found %v, %v", found, err)
	}
	if got := f.rows(); len(got) != 0 {
		t.Fatalf("Add with no message wrote %v", got)
	}
	m := f.message(f.store, 9, p, false)
	for range 2 {
		if found, err := f.store.AddChannelMessageIATA(f.ctx, p, "YOW"); err != nil || !found {
			t.Fatalf("Add with a message: found %v, %v", found, err)
		}
	}
	if got, want := f.rows(), []string{fmt.Sprintf("%d/YOW/9/3", m.ID)}; !slices.Equal(got, want) {
		t.Fatalf("rows = %v, want %v", got, want)
	}
}

// cmiLane is one ingest worker on its own connection: an observation commit, then the message insert, then the
// Add when the insert returned no row.
type cmiLane struct {
	f    *cmiFixture
	conn *pgxpool.Conn
	tx   pgx.Tx
	site string
	hash []byte
}

func (f *cmiFixture) lane(site string, hash []byte) *cmiLane {
	f.t.Helper()
	conn, err := f.pool.Acquire(f.ctx)
	if err != nil {
		f.t.Fatal(err)
	}
	f.t.Cleanup(conn.Release)
	return &cmiLane{f: f, conn: conn, site: site, hash: hash}
}

func (l *cmiLane) obs() { l.f.obs(l.conn, l.hash, l.site, true) }

// icm runs the insert (inside the open transaction when there is one) and, when it returned no row outside a
// transaction, the Add.
func (l *cmiLane) icm(historical bool) *ingest.InsertedChannelMessage {
	l.f.t.Helper()
	var db sqlc.DBTX = l.conn
	if l.tx != nil {
		db = l.tx
	}
	m, err := New(db, nil, 0, 0).InsertChannelMessage(l.f.ctx, ingest.InsertChannelMessageParams{ChannelID: 7, PacketHash: l.hash, SenderName: "s", Content: "c", SentAt: time.Now(), Historical: historical})
	if err != nil {
		l.f.t.Fatal(err)
	}
	if m == nil && l.tx == nil {
		l.add()
	}
	return m
}

func (l *cmiLane) add() {
	l.f.t.Helper()
	if _, err := New(l.conn, nil, 0, 0).AddChannelMessageIATA(l.f.ctx, l.hash, l.site); err != nil {
		l.f.t.Fatal(err)
	}
}

func (l *cmiLane) open(historical bool) {
	l.f.t.Helper()
	tx, err := l.conn.Begin(l.f.ctx)
	if err != nil {
		l.f.t.Fatal(err)
	}
	l.tx = tx
	l.icm(historical)
}

func (l *cmiLane) end(commit bool) {
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

// icmWaiting starts the lane's insert, waits until Postgres shows it blocked on a lock, and returns a function
// that waits for it (and its Add) to finish.
func (l *cmiLane) icmWaiting() func() *ingest.InsertedChannelMessage {
	l.f.t.Helper()
	pid := l.conn.Conn().PgConn().PID()
	done := make(chan *ingest.InsertedChannelMessage, 1)
	failed := make(chan error, 1)
	go func() {
		m, err := New(l.conn, nil, 0, 0).InsertChannelMessage(l.f.ctx, ingest.InsertChannelMessageParams{ChannelID: 7, PacketHash: l.hash, SenderName: "s", Content: "c", SentAt: time.Now()})
		if err == nil && m == nil {
			_, err = New(l.conn, nil, 0, 0).AddChannelMessageIATA(l.f.ctx, l.hash, l.site)
		}
		if err != nil {
			failed <- err
			return
		}
		done <- m
	}()
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
		case err := <-failed:
			l.f.t.Fatalf("insert did not wait: %v", err)
		case m := <-done:
			l.f.t.Fatalf("insert did not wait: returned %+v", m)
		default:
		}
		if time.Now().After(deadline) {
			l.f.t.Fatal("insert never waited")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return func() *ingest.InsertedChannelMessage {
		select {
		case err := <-failed:
			l.f.t.Fatal(err)
		case m := <-done:
			return m
		case <-time.After(10 * time.Second):
			l.f.t.Fatal("insert never finished")
		}
		return nil
	}
}

// Every interleaving of two or three writers of one packet leaves exactly the definition's pairs, with no
// reconcile: the seed covers what its snapshot saw, and every lane that did not create the message adds its site.
func TestCmiSeedAndAddRacePostgres(t *testing.T) {
	type sched struct {
		name string
		run  func(f *cmiFixture, hash []byte)
	}
	schedules := []sched{
		{"sequential", func(f *cmiFixture, h []byte) {
			a, b := f.lane("YUL", h), f.lane("YOW", h)
			a.obs()
			a.icm(false)
			b.obs()
			b.icm(false)
		}},
		{"both_heard_then_a_inserts", func(f *cmiFixture, h []byte) {
			a, b := f.lane("YUL", h), f.lane("YOW", h)
			a.obs()
			b.obs()
			a.icm(false)
			b.icm(false)
		}},
		{"both_heard_then_b_inserts", func(f *cmiFixture, h []byte) {
			a, b := f.lane("YUL", h), f.lane("YOW", h)
			a.obs()
			b.obs()
			b.icm(false)
			a.icm(false)
		}},
		{"b_heard_during_a_insert_and_waits", func(f *cmiFixture, h []byte) {
			a, b := f.lane("YUL", h), f.lane("YOW", h)
			a.obs()
			a.open(false)
			b.obs()
			wait := b.icmWaiting()
			a.end(true)
			if m := wait(); m != nil {
				f.t.Fatalf("B created %+v", m)
			}
		}},
		{"b_heard_during_a_insert_inserts_after", func(f *cmiFixture, h []byte) {
			a, b := f.lane("YUL", h), f.lane("YOW", h)
			a.obs()
			a.open(false)
			b.obs()
			a.end(true)
			b.icm(false)
		}},
		{"both_heard_b_waits_on_a", func(f *cmiFixture, h []byte) {
			a, b := f.lane("YUL", h), f.lane("YOW", h)
			a.obs()
			b.obs()
			a.open(false)
			wait := b.icmWaiting()
			a.end(true)
			wait()
		}},
		{"live_waits_on_historical_insert", func(f *cmiFixture, h []byte) {
			old, b := f.lane("YYZ", h), f.lane("YOW", h)
			old.obs()
			old.open(true)
			b.obs()
			wait := b.icmWaiting()
			old.end(true)
			wait()
		}},
		{"a_rolls_back_while_b_waits", func(f *cmiFixture, h []byte) {
			a, b := f.lane("YUL", h), f.lane("YOW", h)
			a.obs()
			a.open(false)
			b.obs()
			wait := b.icmWaiting()
			a.end(false)
			if m := wait(); m == nil {
				f.t.Fatal("B did not create the message after A rolled back")
			}
		}},
		{"two_wait_on_a", func(f *cmiFixture, h []byte) {
			a, b, c := f.lane("YUL", h), f.lane("YOW", h), f.lane("YYZ", h)
			a.obs()
			a.open(false)
			b.obs()
			c.obs()
			waitB := b.icmWaiting()
			waitC := c.icmWaiting()
			a.end(true)
			waitB()
			waitC()
		}},
		{"later_hearings_same_and_new_site", func(f *cmiFixture, h []byte) {
			a := f.lane("YUL", h)
			a.obs()
			a.icm(false)
			a.obs()
			a.icm(false)
			a.site = "YQB"
			a.obs()
			a.icm(false)
		}},
	}
	for _, s := range schedules {
		t.Run(s.name, func(t *testing.T) {
			f := newCmiFixture(t)
			s.run(f, f.packet("race", 3))
			var messages int
			if err := f.pool.QueryRow(f.ctx, "SELECT count(*) FROM channel_messages").Scan(&messages); err != nil || messages != 1 {
				t.Fatalf("%s: %d messages, %v", s.name, messages, err)
			}
			f.checkDefinition(s.name)
		})
	}
}
