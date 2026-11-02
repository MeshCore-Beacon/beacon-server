// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/MeshCore-Beacon/beacon-server/internal/obscount"
	"github.com/google/uuid"
	"go.uber.org/mock/gomock"
)

// Only a row that was really stored is counted: a duplicate hearing writes nothing.
func TestInsertObservationCountsStoredRows(t *testing.T) {
	ctx := context.Background()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	clock := start
	counts := obscount.New(func() time.Time { return clock })
	mock := mockdb.NewMockQuerier(gomock.NewController(t))
	s := &Store{q: mock}
	WithObservationCounts(counts)(s)
	observer := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	at := start.Add(90 * time.Minute)
	o := ingest.InsertObservationParams{PacketHash: []byte{1}, ObserverID: observer, IATA: "YVR", HeardAt: at}
	mock.EXPECT().InsertObservation(gomock.Any(), gomock.Any()).Return(sqlc.InsertObservationRow{Inserted: true, ObservationCount: 1}, nil)
	mock.EXPECT().InsertObservation(gomock.Any(), gomock.Any()).Return(sqlc.InsertObservationRow{Inserted: false, ObservationCount: 1}, nil)
	mock.EXPECT().InsertObservation(gomock.Any(), gomock.Any()).Return(sqlc.InsertObservationRow{}, errors.New("boom"))
	mock.EXPECT().InsertObservationUnsited(gomock.Any(), gomock.Any()).Return(sqlc.InsertObservationUnsitedRow{Inserted: true, ObservationCount: 2}, nil)
	for range 3 {
		if _, _, err := s.InsertObservation(ctx, o); err != nil {
			t.Fatal(err)
		}
	}
	clock = start.Add(3 * time.Hour)
	snap := counts.Snapshot(clock)
	var total int64
	for i := range snap.Counts {
		if snap.Observers[i] != observer || snap.IATAs[i] != "YVR" || !snap.Hours[i].Equal(start.Add(time.Hour)) {
			t.Fatalf("unexpected count row %v %v %v", snap.Observers[i], snap.IATAs[i], snap.Hours[i])
		}
		total += snap.Counts[i]
	}
	if total != 2 {
		t.Fatalf("counted %d rows, want the 2 that were stored", total)
	}
}
