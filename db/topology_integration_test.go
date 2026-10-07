// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package db

import (
	"context"
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
	if _, err = tx.Exec(ctx, "CREATE TEMP TABLE known_routes (LIKE public.known_routes INCLUDING ALL) ON COMMIT DROP"); err != nil {
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
}
