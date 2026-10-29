// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"testing"
)

// A hash-only channel (key unknown) heard again, through either upsert and with or without the id cache, updates its
// row: no error and no second row. With the id cache (as ingest runs), no sequence value is burned once the row exists.
func TestChannelHashOnlyRehearPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	seq := func() int64 {
		t.Helper()
		var v int64
		if err := pool.QueryRow(ctx, `SELECT last_value FROM channels_id_seq`).Scan(&v); err != nil {
			t.Fatal(err)
		}
		return v
	}
	rows := func(hash byte) (n int, ids []int64) {
		t.Helper()
		if err := pool.QueryRow(ctx, `SELECT count(*), coalesce(array_agg(id ORDER BY id), '{}') FROM channels
			WHERE channel_hash = $1 AND key_fingerprint IS NULL`, []byte{hash}).Scan(&n, &ids); err != nil {
			t.Fatal(err)
		}
		return n, ids
	}
	for _, c := range []struct {
		name  string
		cache bool
	}{{"no cache", false}, {"id cache", true}} {
		t.Run(c.name, func(t *testing.T) {
			s := New(pool, pool, 0, 0)
			if c.cache {
				s = New(pool, pool, 0, 0, WithChannelIDs(NewChannelIDCache()))
			}
			hash := byte(0x40)
			if c.cache {
				hash = 0x41
			}
			first, err := s.UpsertChannelHashOnly(ctx, []byte{hash})
			if err != nil {
				t.Fatal(err)
			}
			seq0 := seq()
			for i := 0; i < 3; i++ {
				if id, err := s.UpsertChannelHashOnly(ctx, []byte{hash}); err != nil || id != first {
					t.Fatalf("UpsertChannelHashOnly again: id %d err %v, want %d", id, err, first)
				}
				if id, err := s.UpsertChannel(ctx, []byte{hash}, nil, "", ""); err != nil || id != first {
					t.Fatalf("UpsertChannel with a nil fingerprint: id %d err %v, want %d", id, err, first)
				}
			}
			if n, ids := rows(hash); n != 1 || ids[0] != int64(first) {
				t.Errorf("hash-only rows %d %v, want one, id %d", n, ids, first)
			}
			if d := seq() - seq0; c.cache && d != 0 {
				t.Errorf("re-hearing a known hash-only channel burned %d sequence values", d)
			}
		})
	}

	// Heard with a name and hashtag: the same row, the name kept as UpsertChannel keeps it.
	s := New(pool, pool, 0, 0, WithChannelIDs(NewChannelIDCache()))
	first, err := s.UpsertChannel(ctx, []byte{0x42}, nil, "named", "#tag")
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"", "renamed", ""} {
		if id, err := s.UpsertChannel(ctx, []byte{0x42}, nil, name, "#tag"); err != nil || id != first {
			t.Fatalf("named hash-only again (%q): id %d err %v, want %d", name, id, err, first)
		}
	}
	var name, hashtag string
	var isHashtag, keyKnown bool
	if err := pool.QueryRow(ctx, `SELECT name, hashtag, is_hashtag, key_known FROM channels WHERE id = $1`, first).
		Scan(&name, &hashtag, &isHashtag, &keyKnown); err != nil {
		t.Fatal(err)
	}
	if name != "renamed" || hashtag != "#tag" || !isHashtag || keyKnown {
		t.Errorf("named hash-only row: %q %q %v %v, want renamed #tag true false", name, hashtag, isHashtag, keyKnown)
	}
	if n, _ := rows(0x42); n != 1 {
		t.Errorf("named hash-only rows %d, want 1", n)
	}
}
