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

func TestGetStatsOverview(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().GetAnalyticsRevision(gomock.Any()).Return(int64(7), nil)
	mock.EXPECT().GetEarliestCompleteRollupHour(gomock.Any()).Return(pgtype.Timestamptz{}, nil)
	var params sqlc.GetStatsSeriesParams
	mock.EXPECT().
		GetStatsSeries(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, p sqlc.GetStatsSeriesParams) ([]sqlc.GetStatsSeriesRow, error) {
			params = p
			rows := []sqlc.GetStatsSeriesRow{{Status: "summary", Observations: 500, UniquePackets: 100, ActiveObservers: 10, ActiveIatas: 3}}
			// The newest hour isn't rolled yet; the summary covers the other 23.
			for h := p.Since.Time; h.Before(p.Until.Time); h = h.Add(time.Hour) {
				status := "complete"
				if h.Add(time.Hour).Equal(p.Until.Time) {
					status = "missing"
				}
				rows = append(rows, sqlc.GetStatsSeriesRow{Hour: pgtype.Timestamptz{Time: h, Valid: true}, Status: status})
			}
			return rows, nil
		})

	store := &Store{q: mock}
	result, err := store.GetStatsOverview(context.Background(), []string{"YVR"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalPackets != 100 || result.TotalObservations != 500 || result.ActiveObservers != 10 || result.ActiveIATAs != 3 {
		t.Errorf("unexpected summary mapping: %+v", result)
	}
	if result.WindowHours != 23 || result.Until-result.Since != (24*time.Hour).Milliseconds() {
		t.Errorf("window %d..%d (%d hours)", result.Since, result.Until, result.WindowHours)
	}
	// The window ends at the last hour that can have been rolled.
	wantUntil := time.Now().UTC().Add(-rollupDelay).Truncate(time.Hour).Add(time.Hour)
	if !params.Until.Time.Equal(wantUntil) || len(params.Iatas) != 1 {
		t.Errorf("series params %+v, want until %s", params, wantUntil)
	}
}

func TestGetStatsTopNodes_MissingNodeRow(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	lastHeard := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	since := time.Date(2026, 1, 1, 12, 34, 0, 0, time.UTC)

	mock.EXPECT().
		GetTopNodes(gomock.Any(), sqlc.GetTopNodesParams{
			Since:    pgtype.Timestamptz{Time: since.Truncate(time.Hour), Valid: true},
			Iatas:    []string{"YVR"},
			RowLimit: 5,
		}).
		Return([]sqlc.GetTopNodesRow{
			{NodeID: pgtype.UUID{Bytes: nodeID, Valid: true}, PublicKey: "aa", Name: "test-node", NodeType: 1, Iata: "YVR", ObservationCount: 9, LastHeard: lastHeard},
			{PublicKey: "bb", Iata: "YVR", ObservationCount: 3, LastHeard: lastHeard},
		}, nil)

	store := &Store{q: mock}
	items, err := store.GetStatsTopNodes(context.Background(), []string{"YVR"}, since, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 2 || items[0].NodeID == nil || *items[0].NodeID != nodeID || *items[0].NodeName != "test-node" {
		t.Fatalf("unexpected first node: %+v", items)
	}
	if items[1].NodeID != nil || items[1].NodeName != nil || items[1].PublicKey != "bb" {
		t.Errorf("deleted node should keep its key and drop id/name: %+v", items[1])
	}
}

func TestGetStatsTopObservers_IATATypeAssertion(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000002")
	displayName := "test-observer"

	obsType := "fixed"
	mock.EXPECT().
		GetStatsTopObservers(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetStatsTopObserversRow{
			{
				ID:               observerID,
				DisplayName:      displayName,
				ObserverType:     obsType,
				Iata:             "YVR",
				ObservationCount: 42,
			},
		}, nil)

	store := &Store{q: mock}
	items, err := store.GetStatsTopObservers(context.Background(), []string{"YVR"}, time.Now().Add(-time.Hour), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].IATA != "YVR" {
		t.Errorf("expected IATA YVR, got %s", items[0].IATA)
	}
	if *items[0].ObserverType != "fixed" {
		t.Errorf("expected ObserverType fixed, got %s", *items[0].ObserverType)
	}
}

func TestGetStatsTopAdvertisers_FloodDirectSplit(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000003")
	name := "test-node"
	heardAt := pgtype.Timestamptz{Time: time.Now(), Valid: true}

	mock.EXPECT().
		GetStatsTopAdvertisers(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetStatsTopAdvertisersRow{
			{
				NodeID:            pgtype.UUID{Bytes: nodeID, Valid: true},
				PublicKey:         "cc",
				Name:              name,
				NodeType:          2, // repeater
				AdvertCount:       10,
				FloodAdvertCount:  7,
				DirectAdvertCount: 3,
				LastHeard:         heardAt,
				Iata:              "YVR",
			},
		}, nil)

	store := &Store{q: mock}
	items, err := store.GetStatsTopAdvertisers(context.Background(), []string{"YVR"}, time.Now().Add(-time.Hour), 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].AdvertCount != 10 {
		t.Errorf("expected AdvertCount 10, got %d", items[0].AdvertCount)
	}
	if items[0].FloodAdvertCount != 7 {
		t.Errorf("expected FloodAdvertCount 7, got %d", items[0].FloodAdvertCount)
	}
	if items[0].DirectAdvertCount != 3 {
		t.Errorf("expected DirectAdvertCount 3, got %d", items[0].DirectAdvertCount)
	}
	if items[0].FloodAdvertCount+items[0].DirectAdvertCount != items[0].AdvertCount {
		t.Error("expected FloodAdvertCount + DirectAdvertCount to equal AdvertCount")
	}
}

func TestGetStatsClockDrift_Mapping(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000004")
	name := "drifty-repeater"
	drift := int32(-600)
	checkedAt := pgtype.Timestamptz{Time: time.Now(), Valid: true}
	iatasJSON := []byte(`[{"iata":"YVR","lastHeard":1700000000000}]`)

	mock.EXPECT().
		GetStatsClockDrift(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetStatsClockDriftRow{
			{
				ID:                      nodeID,
				Name:                    &name,
				NodeType:                2, // repeater
				DeviceClockDriftSeconds: &drift,
				LastAdvertAt:            checkedAt,
				Iatas:                   iatasJSON,
			},
		}, nil)

	store := &Store{q: mock, clockDriftThreshold: 5 * time.Minute}
	items, err := store.GetStatsClockDrift(context.Background(), []string{"YVR"}, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].ClockDriftSeconds != -600 {
		t.Errorf("expected ClockDriftSeconds -600, got %d", items[0].ClockDriftSeconds)
	}
	if len(items[0].IATAs) != 1 || items[0].IATAs[0].IATA != "YVR" {
		t.Errorf("expected 1 IATA entry for YVR, got %v", items[0].IATAs)
	}
}

func TestGetStatsNodeTypes_Mapping(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		GetStatsNodeTypes(gomock.Any(), []string{"YVR"}).
		Return([]sqlc.GetStatsNodeTypesRow{
			{NodeType: 1, Count: 10},
			{NodeType: 2, Count: 5},
		}, nil)

	store := &Store{q: mock}
	result, err := store.GetStatsNodeTypes(context.Background(), []string{"YVR"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 2 {
		t.Fatalf("expected 2 items, got %d", len(result))
	}
	if result[0].NodeTypeName == "" {
		t.Error("expected NodeTypeName to be set")
	}
}

func TestGetScopeStats(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		GetScopeStats(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetScopeStatsRow{
			{Name: "default", PacketCount: 100, ObserverCount: 5, NodeCount: 20},
			{Name: "quiet"},
		}, nil)
	hour := time.Date(2026, 1, 1, 3, 0, 0, 0, time.UTC)
	mock.EXPECT().
		GetScopeStatsHourly(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetScopeStatsHourlyRow{
			{Name: "default", Hour: pgtype.Timestamptz{Time: hour, Valid: true}, Packets: 60},
			{Name: "default", Hour: pgtype.Timestamptz{Time: hour.Add(time.Hour), Valid: true}, Packets: 40},
		}, nil)

	store := &Store{q: mock}
	items, err := store.GetScopeStats(context.Background(), []string{"YVR"}, time.Time{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("expected 2 items, got %d", len(items))
	}
	if h := items[0].Hourly; len(h) != 2 || h[0].Hour != hour.UnixMilli() || h[0].Packets+h[1].Packets != items[0].PacketCount {
		t.Errorf("hourly %+v doesn't split packetCount %d", h, items[0].PacketCount)
	}
	if items[1].Hourly == nil || len(items[1].Hourly) != 0 {
		t.Errorf("a scope without packets must have hourly [], got %#v", items[1].Hourly)
	}
	if items[0].Name != "default" {
		t.Errorf("expected Name default, got %s", items[0].Name)
	}
	if items[0].PacketCount != 100 {
		t.Errorf("expected PacketCount 100, got %d", items[0].PacketCount)
	}
}
