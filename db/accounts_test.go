// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func TestAccountFromRow(t *testing.T) {
	when := time.Date(2026, 1, 1, 12, 0, 0, 0, time.FixedZone("test", 3600))
	row := sqlc.Account{ID: uuid.New(), Name: "test", CreatedAt: pgtype.Timestamptz{Time: when, Valid: true}}
	active := accountFromRow(row)
	if active.ID != row.ID || active.Name != row.Name || !active.Active || active.DeactivatedAt != nil || !active.CreatedAt.Equal(when) || active.CreatedAt.Location() != time.UTC {
		t.Fatal("active account mapping")
	}
	row.DeactivatedAt = pgtype.Timestamptz{Time: when.Add(time.Hour), Valid: true}
	inactive := accountFromRow(row)
	if inactive.Active || inactive.DeactivatedAt == nil || !inactive.DeactivatedAt.Equal(row.DeactivatedAt.Time) {
		t.Fatal("inactive account mapping")
	}
}
