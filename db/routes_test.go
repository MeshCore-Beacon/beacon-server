// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"encoding/hex"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/google/uuid"
	"go.uber.org/mock/gomock"
)

func TestRoutePathKey_MatchesPostgresDigest(t *testing.T) {
	// Golden vector: Postgres computes
	//   decode(md5(array_to_string(node_ids, ',')), 'hex')
	// over lowercase-hyphenated UUIDs. Migration 024 backfilled with that
	// expression; this pins the Go side to the identical bytes.
	a := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	b := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	got := hex.EncodeToString(routePathKey([]uuid.UUID{a, b}))
	want := "f097439148601d9f3291c474f82fa64c"
	if got != want {
		t.Errorf("routePathKey = %s, want %s", got, want)
	}
}

func TestUpsertKnownRoute_ComputesPathKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := mockdb.NewMockQuerier(ctrl)
	store := &Store{q: mock}

	a := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	b := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	wantKey, _ := hex.DecodeString("f097439148601d9f3291c474f82fa64c")

	mock.EXPECT().UpsertKnownRoute(gomock.Any(), gomock.Cond(func(p sqlc.UpsertKnownRouteParams) bool {
		return bytes.Equal(p.PathKey, wantKey)
	})).Return(nil)

	if err := store.UpsertKnownRoute(context.Background(), []uuid.UUID{a, b}, [][]byte{{0x37}, {0xd8}}, "PRG", 2); err != nil {
		t.Fatal(err)
	}
}

func TestDeleteOldRoutes_PassesCutoffs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mock := mockdb.NewMockQuerier(ctrl)
	store := &Store{q: mock}

	retention := time.Date(2026, 7, 23, 0, 0, 0, 0, time.UTC)
	grace := time.Date(2026, 7, 30, 0, 0, 0, 0, time.UTC)

	mock.EXPECT().DeleteOldRoutes(gomock.Any(), gomock.Cond(func(p sqlc.DeleteOldRoutesParams) bool {
		return p.RetentionCutoff.Time.Equal(retention) && p.MinObservations == 3 && p.GraceCutoff.Time.Equal(grace) &&
			p.BatchSize == routeDeleteBatch
	})).Return(int64(0), nil)

	if err := store.DeleteOldRoutes(context.Background(), retention, 3, grace); err != nil {
		t.Fatal(err)
	}
}
