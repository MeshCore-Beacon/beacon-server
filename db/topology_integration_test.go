// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package db

import (
	"context"
	"fmt"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestTopologyLinksPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	if _, err = tx.Exec(ctx, "CREATE TEMP TABLE known_routes (LIKE known_routes INCLUDING ALL) ON COMMIT DROP"); err != nil {
		t.Fatal(err)
	}
	ids := []uuid.UUID{uuid.New(), uuid.New(), uuid.New()}
	now := time.Now().UTC().Truncate(time.Minute)
	// Thousands of duplicate/reversed routes transfer only their unique adjacent pairs.
	_, err = tx.Exec(ctx, `INSERT INTO known_routes (path_key,node_ids,hash_prefix,iata,hop_count,first_seen,last_seen)
 SELECT decode(md5(i::text),'hex'),CASE WHEN i%2=0 THEN $1::uuid[] ELSE $2::uuid[] END,ARRAY['\xaa'::bytea,'\xbb'::bytea],
 'YKF',2,$3,$3 FROM generate_series(1,1000) i`, ids[:2], []uuid.UUID{ids[1], ids[0]}, now)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO known_routes(path_key,node_ids,hash_prefix,iata,hop_count,first_seen,last_seen) VALUES
 (decode(md5('other'),'hex'),$1,ARRAY['\xbb'::bytea,'\xcc'::bytea],'YOW',2,$3,$3),
 (decode(md5('gap'),'hex'),$2,ARRAY['\xaa'::bytea,'\x00'::bytea,'\xcc'::bytea],'YKF',3,$3,$3),
 (decode(md5('old'),'hex'),$1,ARRAY['\xbb'::bytea,'\xcc'::bytea],'YKF',2,$3-interval '25 hours',$3-interval '25 hours')`, ids[1:], []*uuid.UUID{&ids[0], nil, &ids[2]}, now)
	if err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(tx)}
	result, err := store.GetTopologyLinks(ctx, []string{"YKF"}, now.Add(-24*time.Hour), now.Add(time.Minute))
	if err != nil || result.Capped || len(result.Links) != 1 {
		t.Fatalf("scoped: %+v %v", result, err)
	}
	all, err := store.GetTopologyLinks(ctx, nil, now.Add(-24*time.Hour), now.Add(time.Minute))
	if err != nil || len(all.Links) != 2 {
		t.Fatalf("all regions: %+v %v", all, err)
	}
	if result.Since != now.Add(-24*time.Hour).UnixMilli() {
		t.Fatal("window changed")
	}
	// The coarse millisecond index condition must not widen exact boundaries.
	if _, err := tx.Exec(ctx, "TRUNCATE pg_temp.known_routes"); err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(ctx, `INSERT INTO known_routes(path_key,node_ids,hash_prefix,iata,hop_count,first_seen,last_seen) VALUES
 (decode(md5('before'),'hex'),$1,ARRAY['\xaa'::bytea,'\xbb'::bytea],'YKF',2,$4::timestamptz,$4::timestamptz+interval '100 microseconds'),
 (decode(md5('inside'),'hex'),$2,ARRAY['\xbb'::bytea,'\xcc'::bytea],'YKF',2,$4::timestamptz,$4::timestamptz+interval '500 microseconds'),
 (decode(md5('after'),'hex'),$3,ARRAY['\xaa'::bytea,'\xcc'::bytea],'YKF',2,$4::timestamptz,$4::timestamptz+interval '900 microseconds')`,
		ids[:2], ids[1:], []uuid.UUID{ids[0], ids[2]}, now)
	if err != nil {
		t.Fatal(err)
	}
	exact, err := store.GetTopologyLinks(ctx, []string{"YKF"}, now.Add(250*time.Microsecond), now.Add(750*time.Microsecond))
	if err != nil || len(exact.Links) != 1 {
		t.Fatalf("exact boundary: %+v, %v", exact, err)
	}
	want := [2]uuid.UUID{ids[1], ids[2]}
	if want[0].String() > want[1].String() {
		want[0], want[1] = want[1], want[0]
	}
	if exact.Links[0] != want {
		t.Fatalf("boundary widened: %v, want %v", exact.Links[0], want)
	}
	// Adjacent values follow array storage order, not an assumed lower bound.
	// A NULL in a non-one-based array must never manufacture a bridge.
	for _, tc := range []struct {
		path string
		want int
	}{
		{fmt.Sprintf("[0:2]={%s,%s,%s}", ids[0], ids[1], ids[2]), 2},
		{fmt.Sprintf("[5:7]={%s,%s,%s}", ids[0], ids[1], ids[2]), 2},
		{fmt.Sprintf("[0:2]={%s,NULL,%s}", ids[0], ids[2]), 0},
	} {
		if _, err := tx.Exec(ctx, "TRUNCATE pg_temp.known_routes"); err != nil {
			t.Fatal(err)
		}
		_, err := tx.Exec(ctx, `INSERT INTO known_routes(path_key,node_ids,hash_prefix,iata,hop_count,first_seen,last_seen)
		 VALUES ('\xaa',$1::uuid[],ARRAY['\xaa'::bytea],'YKF',3,$2,$2)`, tc.path, now)
		if err != nil {
			t.Fatal(err)
		}
		got, err := store.GetTopologyLinks(ctx, nil, now, now.Add(time.Minute))
		if err != nil || len(got.Links) != tc.want {
			t.Fatalf("non-default array bound: %+v, want %d links, error %v", got, tc.want, err)
		}
	}
}
