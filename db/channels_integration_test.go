// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"fmt"
	"reflect"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/jackc/pgx/v5"
)

// channelFixture inserts 10 keyed channels (0x01..0x0a, fingerprint = the hash byte
// repeated 8 times, name "ch-N") and 5 hash-only channels (0x21..0x25) through the
// sequence default, all last seen long before the test transaction's NOW().
func channelFixture(t *testing.T, ctx context.Context, tx pgx.Tx) {
	t.Helper()
	mustExec(t, ctx, tx, `
INSERT INTO channels (channel_hash, key_fingerprint, name, key_known, first_seen, last_seen)
SELECT set_byte('\x00'::bytea, 0, h), decode(repeat(lpad(to_hex(h), 2, '0'), 8), 'hex'), 'ch-' || h, true,
       '2026-01-01'::timestamptz, '2026-01-02'::timestamptz
FROM generate_series(1, 10) h ORDER BY h`)
	mustExec(t, ctx, tx, `
INSERT INTO channels (channel_hash, first_seen, last_seen)
SELECT set_byte('\x00'::bytea, 0, h), '2026-01-01'::timestamptz, '2026-01-02'::timestamptz
FROM generate_series(33, 37) h ORDER BY h`)
}

func fp8(h byte) []byte { return bytes.Repeat([]byte{h}, 8) }

func channelSnap(t *testing.T, ctx context.Context, tx pgx.Tx) []sqlc.Channel {
	t.Helper()
	rows, err := tx.Query(ctx, `SELECT * FROM channels ORDER BY channel_hash, key_fingerprint NULLS FIRST`)
	if err != nil {
		t.Fatal(err)
	}
	out, err := pgx.CollectRows(rows, pgx.RowToStructByPos[sqlc.Channel])
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func channelSeq(t *testing.T, ctx context.Context, tx pgx.Tx) int64 {
	t.Helper()
	var v int64
	if err := tx.QueryRow(ctx, `SELECT last_value FROM channels_id_seq`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

func maxChannelID(t *testing.T, ctx context.Context, tx pgx.Tx) int32 {
	t.Helper()
	var v int32
	if err := tx.QueryRow(ctx, `SELECT max(id) FROM channels`).Scan(&v); err != nil {
		t.Fatal(err)
	}
	return v
}

// legacyWant runs hear through the legacy store inside a savepoint and returns the
// table it leaves, then rolls the rows back (the sequence keeps what it burned).
func legacyWant(t *testing.T, ctx context.Context, tx pgx.Tx, hear func(*Store)) []sqlc.Channel {
	t.Helper()
	mustExec(t, ctx, tx, `SAVEPOINT ref`)
	hear(&Store{q: sqlc.New(tx)})
	want := channelSnap(t, ctx, tx)
	mustExec(t, ctx, tx, `ROLLBACK TO SAVEPOINT ref`)
	return want
}

// sameChannels compares every column of every row; newRow (if set) matches its row on
// every column but id, and that id must be above floor.
func sameChannels(t *testing.T, got, want []sqlc.Channel, newRow func(sqlc.Channel) bool, floor int32) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("got %d rows, want %d", len(got), len(want))
	}
	for i := range got {
		g, w := got[i], want[i]
		if newRow != nil && newRow(g) {
			if g.ID <= floor {
				t.Errorf("new row id %d not above %d", g.ID, floor)
			}
			g.ID, w.ID = 0, 0
		}
		if !reflect.DeepEqual(g, w) {
			t.Errorf("row %d:\n got %+v\nwant %+v", i, g, w)
		}
	}
}

func isChannel(hash byte, fp []byte) func(sqlc.Channel) bool {
	return func(c sqlc.Channel) bool {
		return bytes.Equal(c.ChannelHash, []byte{hash}) && bytes.Equal(c.KeyFingerprint, fp) && (fp == nil) == (c.KeyFingerprint == nil)
	}
}

// After the boot preload, known-key hearings burn no sequence value and leave today's rows.
func TestUpsertChannelWriteThroughPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	channelFixture(t, ctx, tx)
	fixtureMax := maxChannelID(t, ctx, tx)
	cache := NewChannelIDCache()
	wt := &Store{q: sqlc.New(tx), channelIDs: cache}
	if err := wt.PreloadChannelIDs(ctx); err != nil {
		t.Fatal(err)
	}
	if cache.Len() != 15 {
		t.Fatalf("preloaded %d keys, want 15", cache.Len())
	}

	hear := func(s *Store, h byte, name string, n int) {
		for i := 0; i < n; i++ {
			if _, err := s.UpsertChannel(ctx, []byte{h}, fp8(h), name, ""); err != nil {
				t.Fatal(err)
			}
		}
	}
	stream := func(s *Store) {
		for i := 0; i < 1000; i++ {
			name := ""
			if i%3 == 0 {
				name = fmt.Sprintf("name-%d", i%7)
			}
			hear(s, byte(1+i%10), name, 1)
		}
	}

	want := legacyWant(t, ctx, tx, stream)
	seq0 := channelSeq(t, ctx, tx)
	stream(wt)
	if seq1 := channelSeq(t, ctx, tx); seq1 != seq0 {
		t.Fatalf("known keys burned %d sequence values", seq1-seq0)
	}
	sameChannels(t, channelSnap(t, ctx, tx), want, nil, 0)

	t.Run("unknown key", func(t *testing.T) {
		want := legacyWant(t, ctx, tx, func(s *Store) { hear(s, 0x0b, "new", 5) })
		seq0 := channelSeq(t, ctx, tx)
		hear(wt, 0x0b, "new", 5)
		if d := channelSeq(t, ctx, tx) - seq0; d != 1 {
			t.Fatalf("sequence moved %d, want 1", d)
		}
		if cache.Len() != 16 {
			t.Fatalf("cache.Len() = %d, want 16", cache.Len())
		}
		sameChannels(t, channelSnap(t, ctx, tx), want, isChannel(0x0b, fp8(0x0b)), fixtureMax)
	})

	t.Run("deleted", func(t *testing.T) {
		mustExec(t, ctx, tx, `DELETE FROM channels WHERE channel_hash = '\x03'`)
		want := legacyWant(t, ctx, tx, func(s *Store) { hear(s, 0x03, "back", 5) })
		seq0 := channelSeq(t, ctx, tx)
		hear(wt, 0x03, "back", 5)
		if d := channelSeq(t, ctx, tx) - seq0; d != 1 {
			t.Fatalf("sequence moved %d, want 1", d)
		}
		got := channelSnap(t, ctx, tx)
		sameChannels(t, got, want, isChannel(0x03, fp8(0x03)), fixtureMax)
		for _, c := range got {
			if isChannel(0x03, fp8(0x03))(c) {
				if id, ok := cache.get(channelKey{hash: "\x03", fp: string(fp8(0x03))}); !ok || id != c.ID {
					t.Fatalf("cache holds %d, %v; want new id %d", id, ok, c.ID)
				}
			}
		}
	})

	t.Run("re-keyed", func(t *testing.T) {
		mustExec(t, ctx, tx, `UPDATE channels SET key_fingerprint = '\xffffffffffffffff', name = 'rekeyed',
			last_seen = '2000-01-01' WHERE channel_hash = '\x04'`)
		before := channelSnap(t, ctx, tx)
		want := legacyWant(t, ctx, tx, func(s *Store) { hear(s, 0x04, "fresh", 5) })
		seq0 := channelSeq(t, ctx, tx)
		hear(wt, 0x04, "fresh", 5)
		if d := channelSeq(t, ctx, tx) - seq0; d != 1 {
			t.Fatalf("sequence moved %d, want 1", d)
		}
		got := channelSnap(t, ctx, tx)
		if len(got) != len(before)+1 {
			t.Fatalf("got %d rows, want %d (a new (0x04, fp4) row)", len(got), len(before)+1)
		}
		sameChannels(t, got, want, isChannel(0x04, fp8(0x04)), fixtureMax)
		rekeyed := isChannel(0x04, fp8(0xff))
		for i := range before {
			if rekeyed(before[i]) {
				for _, c := range got {
					if rekeyed(c) && !reflect.DeepEqual(c, before[i]) {
						t.Fatalf("re-keyed row changed:\n got %+v\nwant %+v", c, before[i])
					}
				}
			}
		}
	})
}

func TestUpsertChannelHashOnlyWriteThroughPostgres(t *testing.T) {
	ctx, tx := retentionTx(t)
	applyBaseline(t, ctx, tx)
	channelFixture(t, ctx, tx)
	// A keyed row beside the hash-only 0x21 that the hash-only path must never touch.
	mustExec(t, ctx, tx, `INSERT INTO channels (channel_hash, key_fingerprint, name, key_known, first_seen, last_seen)
		VALUES ('\x21', '\x2121212121212121', 'keyed-21', true, '2026-01-01', '2026-01-02')`)
	fixtureMax := maxChannelID(t, ctx, tx)
	keyedBefore := channelSnap(t, ctx, tx)
	cache := NewChannelIDCache()
	wt := &Store{q: sqlc.New(tx), channelIDs: cache}
	if err := wt.PreloadChannelIDs(ctx); err != nil {
		t.Fatal(err)
	}
	if cache.Len() != 16 {
		t.Fatalf("preloaded %d keys, want 16", cache.Len())
	}

	hear := func(s *Store, h byte, n int) {
		for i := 0; i < n; i++ {
			if _, err := s.UpsertChannelHashOnly(ctx, []byte{h}); err != nil {
				t.Fatal(err)
			}
		}
	}
	stream := func(s *Store) {
		for i := 0; i < 1000; i++ {
			hear(s, byte(0x21+i%5), 1)
		}
	}

	want := legacyWant(t, ctx, tx, stream)
	seq0 := channelSeq(t, ctx, tx)
	stream(wt)
	if seq1 := channelSeq(t, ctx, tx); seq1 != seq0 {
		t.Fatalf("known keys burned %d sequence values", seq1-seq0)
	}
	got := channelSnap(t, ctx, tx)
	sameChannels(t, got, want, nil, 0)
	keyed21 := isChannel(0x21, fp8(0x21))
	for _, b := range keyedBefore {
		if keyed21(b) {
			for _, c := range got {
				if keyed21(c) && !reflect.DeepEqual(c, b) {
					t.Fatalf("keyed 0x21 row touched:\n got %+v\nwant %+v", c, b)
				}
			}
		}
	}

	t.Run("unknown key", func(t *testing.T) {
		want := legacyWant(t, ctx, tx, func(s *Store) { hear(s, 0x26, 5) })
		seq0 := channelSeq(t, ctx, tx)
		hear(wt, 0x26, 5)
		if d := channelSeq(t, ctx, tx) - seq0; d != 1 {
			t.Fatalf("sequence moved %d, want 1", d)
		}
		if cache.Len() != 17 {
			t.Fatalf("cache.Len() = %d, want 17", cache.Len())
		}
		sameChannels(t, channelSnap(t, ctx, tx), want, isChannel(0x26, nil), fixtureMax)
	})

	t.Run("deleted", func(t *testing.T) {
		mustExec(t, ctx, tx, `DELETE FROM channels WHERE channel_hash = '\x23' AND key_fingerprint IS NULL`)
		want := legacyWant(t, ctx, tx, func(s *Store) { hear(s, 0x23, 5) })
		seq0 := channelSeq(t, ctx, tx)
		hear(wt, 0x23, 5)
		if d := channelSeq(t, ctx, tx) - seq0; d != 1 {
			t.Fatalf("sequence moved %d, want 1", d)
		}
		got := channelSnap(t, ctx, tx)
		sameChannels(t, got, want, isChannel(0x23, nil), fixtureMax)
		for _, c := range got {
			if isChannel(0x23, nil)(c) {
				if id, ok := cache.get(channelKey{hash: "\x23", hashOnly: true}); !ok || id != c.ID {
					t.Fatalf("cache holds %d, %v; want new id %d", id, ok, c.ID)
				}
			}
		}
	})
}
