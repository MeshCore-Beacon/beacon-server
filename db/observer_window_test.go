// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/mock/gomock"
)

func TestObserverActivityLiveAndFixedWindow(t *testing.T) {
	for _, interval := range []time.Duration{15 * time.Minute, time.Hour, 6 * time.Hour} {
		for _, fixed := range []bool{false, true} {
			t.Run(interval.String()+"/"+map[bool]string{false: "live", true: "fixed"}[fixed], func(t *testing.T) {
				mock := mockdb.NewMockQuerier(gomock.NewController(t))
				id := uuid.New()
				mock.EXPECT().GetObserverByID(gomock.Any(), id).Return(sqlc.Observer{}, nil)
				mock.EXPECT().GetObserverActivityLiveSummary(gomock.Any(), gomock.Any()).Return(sqlc.GetObserverActivityLiveSummaryRow{}, nil)
				if interval < time.Hour {
					mock.EXPECT().GetObserverActivityRaw(gomock.Any(), gomock.Any()).Return(nil, nil)
					mock.EXPECT().GetObserverActivityRawPayloadTypes(gomock.Any(), gomock.Any()).Return(nil, nil)
				} else {
					mock.EXPECT().GetLatestCompleteRollupHour(gomock.Any()).Return(pgtype.Timestamptz{}, nil)
					mock.EXPECT().GetObserverActivityHourly(gomock.Any(), gomock.Any()).Return(nil, nil)
					mock.EXPECT().GetObserverActivityHourlyPayloadTypes(gomock.Any(), gomock.Any()).Return(nil, nil)
				}
				before := time.Now().UTC()
				var until time.Time
				if fixed {
					until = before.Add(-24*time.Hour + 123*time.Millisecond)
				}
				got, err := (&Store{q: mock}).GetObserverActivity(context.Background(), id, 48*time.Hour, interval, until)
				if err != nil {
					t.Fatal(err)
				}
				if fixed {
					if got.WindowEnd != until.Truncate(interval).UnixMilli() {
						t.Fatalf("fixed end was not aligned: %d", got.WindowEnd)
					}
				} else if got.WindowEnd < before.UnixMilli() || got.WindowEnd > time.Now().UnixMilli() {
					t.Fatalf("live end dropped the partial bucket: %d", got.WindowEnd)
				}
				// Freshness is always live, even when the activity window is historical.
				if got.Summary.LastCompleteHourEnd != time.UnixMilli(got.GeneratedAt).UTC().Truncate(time.Hour).UnixMilli() {
					t.Fatal("freshness was anchored to the historical activity window")
				}
			})
		}
	}
}
