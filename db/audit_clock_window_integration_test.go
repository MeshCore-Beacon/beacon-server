// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package db

import (
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
	"testing"
)

func TestAuditClockWindowPostgres(t *testing.T) {
	ctx, pool := schemaPool(t)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	id := uuid.New()
	if _, err := pool.Exec(ctx, `INSERT INTO nodes(id,public_key,node_type) VALUES($1,'\xa1',2);
 `, id); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO node_clock_drifts(node_id,advert_at,drift_seconds)
 SELECT $1,now()-i*interval '1 minute',i FROM generate_series(1,30) i`, id); err != nil {
		t.Fatal(err)
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback(ctx)
	q := sqlc.New(tx)
	for range 2 { // now() repeats within the transaction: the conflict must not trim an extra row.
		if err := q.RecordClockDrift(ctx, sqlc.RecordClockDriftParams{NodeID: id, DriftSeconds: 99}); err != nil {
			t.Fatal(err)
		}
		var count int
		if err := tx.QueryRow(ctx, "SELECT count(*) FROM node_clock_drifts WHERE node_id=$1", id).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 16 {
			t.Fatalf("retained %d readings, want the newest 16", count)
		}
	}
}
