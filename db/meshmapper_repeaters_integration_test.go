// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"os"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/meshmapper"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestMeshMapperRepeatersPostgres(t *testing.T) {
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
	for _, table := range []string{"nodes", "transport_scopes", "observers", "node_iatas", "node_neighbors",
		"meshmapper_repeater_sources", "meshmapper_repeaters", "meshmapper_repeater_access"} {
		if _, err := tx.Exec(ctx, "CREATE TEMP TABLE "+table+" (LIKE public."+table+" INCLUDING ALL) ON COMMIT DROP"); err != nil {
			t.Fatal(err)
		}
	}
	store := &Store{q: sqlc.New(tx)}
	keyA, keyB := bytes.Repeat([]byte{0xa1}, 32), bytes.Repeat([]byte{0xb2}, 32)
	nodeID := uuid.New()
	if _, err := tx.Exec(ctx, "INSERT INTO nodes (id, public_key, node_type) VALUES ($1, $2, 2)", nodeID, keyA); err != nil {
		t.Fatal(err)
	}
	hw := "RAK4631"
	now := time.Now().UTC().Truncate(time.Second)
	save := func(iata string, at time.Time, list []meshmapper.Repeater) {
		t.Helper()
		c := meshmapper.Cache{CheckedAt: at, AttemptedAt: at, NextAttempt: at.Add(time.Hour)}
		if err := store.SaveRepeaterSource(ctx, iata, "https://"+iata+".meshmapper.net/get_repeaters.php", c, list); err != nil {
			t.Fatal(err)
		}
	}
	rows := func() (n int) {
		t.Helper()
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM meshmapper_repeaters").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}

	node, err := store.GetNode(ctx, nodeID)
	if err != nil || node.MeshMapper != nil {
		t.Fatal("unlisted node got MeshMapper data", node.MeshMapper, err)
	}
	save("YOW", now, []meshmapper.Repeater{
		{Key: keyA, Data: api.MeshMapperRepeater{IATA: "YOW", Site: api.MeshMapperSite{Hardware: &hw}, Admins: []string{"op"}, Scopes: []api.MeshMapperScope{}}},
		{Key: keyB, Data: api.MeshMapperRepeater{Admins: []string{}, Scopes: []api.MeshMapperScope{}}},
	})
	node, err = store.GetNode(ctx, nodeID)
	if err != nil || node.MeshMapper == nil || node.MeshMapper.Site.Hardware == nil || *node.MeshMapper.Site.Hardware != hw ||
		node.MeshMapper.FetchedAt != now.UnixMilli() || node.MeshMapper.IATA != "YOW" {
		t.Fatalf("node detail: %+v %v", node.MeshMapper, err)
	}

	// A failed check keeps the list; a newer check of the same key from another source wins.
	if err := store.SaveRepeaterSource(ctx, "YOW", "https://YOW.meshmapper.net/get_repeaters.php",
		meshmapper.Cache{AttemptedAt: now, NextAttempt: now.Add(time.Hour), LastError: "HTTP 503"}, nil); err != nil {
		t.Fatal(err)
	}
	save("ONQC", now.Add(time.Minute), []meshmapper.Repeater{{Key: keyA, Data: api.MeshMapperRepeater{IATA: "YUL", Admins: []string{}, Scopes: []api.MeshMapperScope{}}}})
	if node, err = store.GetNode(ctx, nodeID); err != nil || node.MeshMapper.IATA != "YUL" || rows() != 3 {
		t.Fatalf("newest source not preferred: %+v %v rows=%d", node.MeshMapper, err, rows())
	}
	var stored string
	if err := tx.QueryRow(ctx, "SELECT data::text FROM meshmapper_repeaters WHERE source = 'ONQC'").Scan(&stored); err != nil || strings.Contains(stored, "fetchedAt") {
		t.Fatal("fetchedAt stored", stored, err)
	}

	save("YOW", now.Add(2*time.Minute), []meshmapper.Repeater{{Key: keyB, Data: api.MeshMapperRepeater{Admins: []string{}, Scopes: []api.MeshMapperScope{}}}})
	var left int
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM meshmapper_repeaters WHERE source = 'YOW' AND public_key = $1", keyA).Scan(&left); err != nil || left != 0 {
		t.Fatal("dropped repeater kept", left, err)
	}
	save("YOW", now.Add(3*time.Minute), []meshmapper.Repeater{})
	if err := tx.QueryRow(ctx, "SELECT count(*) FROM meshmapper_repeaters WHERE source = 'YOW'").Scan(&left); err != nil || left != 0 {
		t.Fatal("empty list kept repeaters", left, err)
	}

	// A new site URL drops the old site's ETag even before the next successful check.
	c := meshmapper.Cache{ETag: `"v1"`, CheckedAt: now, AttemptedAt: now, NextAttempt: now}
	if err := store.SaveRepeaterSource(ctx, "YYZ", "https://yyz.meshmapper.net/get_repeaters.php", c, []meshmapper.Repeater{}); err != nil {
		t.Fatal(err)
	}
	if err := store.SaveRepeaterSource(ctx, "YYZ", "https://other.meshmapper.net/get_repeaters.php",
		meshmapper.Cache{AttemptedAt: now, NextAttempt: now, LastError: "no response"}, nil); err != nil {
		t.Fatal(err)
	}
	sources, err := store.ListRepeaterSources(ctx)
	if err != nil || len(sources) != 3 || sources[1].IATA != "YOW" || sources[1].LastError != "" || sources[2].ETag != "" {
		t.Fatal("sources", sources, err)
	}

	if err := store.DeleteRepeaterSource(ctx, "ONQC"); err != nil {
		t.Fatal(err)
	}
	if node, err = store.GetNode(ctx, nodeID); err != nil || node.MeshMapper != nil {
		t.Fatal("retired source still served", node.MeshMapper, err)
	}

	save("YOW", now.Add(4*time.Minute), []meshmapper.Repeater{{Key: keyB, Data: api.MeshMapperRepeater{Admins: []string{}, Scopes: []api.MeshMapperScope{}}}})
	if cleared, err := store.ClearRepeaters(ctx); err != nil || !cleared || rows() != 0 {
		t.Fatal("clear", cleared, err)
	}
	sources, _ = store.ListRepeaterSources(ctx)
	if len(sources) != 2 || !sources[0].CheckedAt.IsZero() || sources[0].ETag != "" || sources[0].NextAttempt.IsZero() {
		t.Fatal("clear should keep timing and drop list state", sources)
	}
	if cleared, err := store.ClearRepeaters(ctx); err != nil || cleared {
		t.Fatal("second clear reported a change", cleared, err)
	}
	if _, err := prepareRepeaterAccess(ctx, tx, "test-access-one"); err != nil {
		t.Fatal(err)
	}
	save("YOW", now.Add(5*time.Minute), []meshmapper.Repeater{{Key: keyB, Data: api.MeshMapperRepeater{Admins: []string{}, Scopes: []api.MeshMapperScope{}}}})
	before, _ := store.ListRepeaterSources(ctx)
	if changed, err := prepareRepeaterAccess(ctx, tx, "test-access-one"); err != nil || changed || rows() != 1 {
		t.Fatal("same access invalidated cache", changed, err)
	}
	if changed, err := prepareRepeaterAccess(ctx, tx, "test-access-two"); err != nil || !changed || rows() != 0 {
		t.Fatal("changed access retained cache", changed, err)
	}
	after, _ := store.ListRepeaterSources(ctx)
	for i := range after {
		if after[i].ETag != "" || !after[i].CheckedAt.IsZero() || !after[i].NextAttempt.Equal(before[i].NextAttempt) {
			t.Fatal("cache binding reset cooldown or retained validators")
		}
	}
}
