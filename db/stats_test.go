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

	mock.EXPECT().
		GetStatsOverview(gomock.Any(), []string{"YVR"}).
		Return(sqlc.GetStatsOverviewRow{
			TotalPackets:      100,
			TotalObservations: 500,
			ActiveObservers:   10,
			ActiveIatas:       3,
		}, nil)

	store := &Store{q: mock}
	result, err := store.GetStatsOverview(context.Background(), []string{"YVR"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalPackets != 100 {
		t.Errorf("expected TotalPackets 100, got %d", result.TotalPackets)
	}
	if result.WindowHours != 24 {
		t.Errorf("expected WindowHours 24, got %d", result.WindowHours)
	}
}

func TestGetStatsTopNodes_NilObservationCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	lastHeard := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	name := "test-node"

	mock.EXPECT().
		GetTopNodes(gomock.Any(), sqlc.GetTopNodesParams{
			Column1: []string{"YVR"},
			Limit:   5,
		}).
		Return([]sqlc.MvTopNodesByIatum{
			{
				NodeID:           nodeID,
				Name:             &name,
				NodeType:         1,
				Iata:             "YVR",
				ObservationCount: nil,
				LastHeard:        lastHeard,
			},
		}, nil)

	store := &Store{q: mock}
	items, err := store.GetStatsTopNodes(context.Background(), []string{"YVR"}, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].ObservationCount != 0 {
		t.Errorf("expected ObservationCount 0 for nil, got %d", items[0].ObservationCount)
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
				DisplayName:      &displayName,
				ObserverType:     &obsType,
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
		GetScopeStats(gomock.Any()).
		Return([]sqlc.GetScopeStatsRow{
			{Name: "default", PacketCount: 100, ObserverCount: 5, NodeCount: 20},
		}, nil)

	store := &Store{q: mock}
	items, err := store.GetScopeStats(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].Name != "default" {
		t.Errorf("expected Name default, got %s", items[0].Name)
	}
	if items[0].PacketCount != 100 {
		t.Errorf("expected PacketCount 100, got %d", items[0].PacketCount)
	}
}
