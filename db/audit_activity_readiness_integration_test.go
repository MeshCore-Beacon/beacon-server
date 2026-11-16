// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package db

import (
	"errors"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"testing"
	"time"
)

func TestAuditNodeActivityPendingPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	if _, err := pool.Exec(ctx, "INSERT INTO nodes(id,public_key,node_type) VALUES($1,'\\xa1',2)", id); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO analytics_rollup_hours(hour,status)
 VALUES (date_trunc('hour',now())-interval '1 hour','complete');
 INSERT INTO analytics_dirty_hours(hour) VALUES(date_trunc('hour',now())-interval '2 hours');`); err != nil {
		t.Fatal(err)
	}
	store := &Store{q: sqlc.New(pool)}
	if _, err := store.GetNodeActivity(ctx, id, 24*time.Hour); !errors.Is(err, api.ErrPagesNotReady) {
		t.Fatalf("pending history: %v", err)
	}
	if _, err := pool.Exec(ctx, `DELETE FROM analytics_dirty_hours;
 INSERT INTO analytics_dirty_hours(hour) VALUES(date_trunc('hour',now()));`); err != nil {
		t.Fatal(err)
	}
	if _, err := store.GetNodeActivity(ctx, id, 24*time.Hour); err != nil {
		t.Fatalf("raw current hour should remain readable: %v", err)
	}
}
