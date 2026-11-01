// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

// The region list carries each region's detail, so a client needs one call instead of one per region.
func TestListRegionsMatchesDetailPostgres(t *testing.T) {
	dsn := os.Getenv("BEACON_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("set BEACON_TEST_POSTGRES_DSN")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	if err := RunMigrations(ctx, pool); err != nil {
		pool.Close()
		t.Fatal(err)
	}
	pool.Close()
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close(context.Background())
	tx, err := conn.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(context.Background())
	for _, table := range []string{"regions", "region_iatas"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	store := &Store{q: sqlc.New(tx)}
	lat, lng, zoom := 49.2, -123.1, 7
	seed := []struct {
		slug, name string
		order      int
		iatas      []string
	}{
		{"west", "West", 2, []string{"YYJ", "YVR"}},
		{"empty", "Empty", 1, nil},
		{"alpha", "Alpha", 2, []string{"YOW"}},
	}
	for _, r := range seed {
		var la, ln *float64
		var z *int
		if r.slug == "west" {
			la, ln, z = &lat, &lng, &zoom
		}
		id, err := store.UpsertRegion(ctx, r.slug, r.name, "about "+r.name, r.order, la, ln, z)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.SetRegionIATAs(ctx, id, r.iatas); err != nil {
			t.Fatal(err)
		}
	}

	list, err := store.ListRegions(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var slugs []string
	for _, r := range list {
		slugs = append(slugs, r.Slug)
		detail, err := store.GetRegion(ctx, int32(r.ID))
		if err != nil {
			t.Fatal(err)
		}
		if detail.IATAs == nil {
			detail.IATAs = []string{} // the list never sends null
		}
		if !reflect.DeepEqual(r, *detail) {
			t.Errorf("list item %+v != detail %+v", r, *detail)
		}
	}
	if want := []string{"empty", "alpha", "west"}; !reflect.DeepEqual(slugs, want) {
		t.Errorf("order = %v, want %v", slugs, want)
	}
	if list[2].IATAs[0] != "YVR" || list[2].IATAs[1] != "YYJ" || *list[2].ZoomLevel != 7 {
		t.Errorf("west = %+v", list[2])
	}
}
