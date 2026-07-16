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
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/mock/gomock"
)

func TestListTraceTags_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		ListTraceTags(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListTraceTagsRow{}, nil)

	store := &Store{q: mock}
	items, err := store.ListTraceTags(context.Background(), []string{"YVR"}, "", "", time.Time{}, time.Time{}, time.Time{}, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 0 {
		t.Errorf("expected 0 items, got %d", len(items))
	}
}

func TestListTraceTags_WithPayload(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	firstHeard := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	lastHeard := pgtype.Timestamptz{Time: time.UnixMilli(1700000001000), Valid: true}
	payload := []byte(`{"pathHashes":["aabb","ccdd"],"snrValues":[10,20]}`)

	mock.EXPECT().
		ListTraceTags(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListTraceTagsRow{
			{
				TraceTag:     "trace-001",
				FirstHeardAt: firstHeard,
				LastHeardAt:  lastHeard,
				PacketCount:  3,
				IataCount:    1,
				TraceType:    "trace",
				BestPayload:  payload,
			},
		}, nil)

	store := &Store{q: mock}
	items, err := store.ListTraceTags(context.Background(), []string{"YVR"}, "", "", time.Time{}, time.Time{}, time.Time{}, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(items))
	}
	if items[0].TraceTag != "trace-001" {
		t.Errorf("expected TraceTag trace-001, got %s", items[0].TraceTag)
	}
	if len(items[0].PathHashes) != 2 {
		t.Errorf("expected 2 path hashes, got %d", len(items[0].PathHashes))
	}
	if items[0].SNRValues[0] != 10 {
		t.Errorf("expected SNR 10, got %f", items[0].SNRValues[0])
	}
}

func TestGetTraceByTag_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		GetPacketsByTraceTag(gomock.Any(), "trace-001").
		Return([]sqlc.GetPacketsByTraceTagRow{}, nil)

	store := &Store{q: mock}
	detail, err := store.GetTraceByTag(context.Background(), "trace-001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if detail != nil {
		t.Errorf("expected nil for empty result, got %v", detail)
	}
}

func TestGetTraceByTag_WithPacket(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	firstHeard := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	lastHeard := pgtype.Timestamptz{Time: time.UnixMilli(1700000001000), Valid: true}
	// aabbccdd is valid hex for the packet hash
	parsedPayload := []byte(`{"pathHashes":["aabb"],"snrValues":[15.0],"flags":0}`)

	scopeName := "default"
	mock.EXPECT().
		GetPacketsByTraceTag(gomock.Any(), "trace-001").
		Return([]sqlc.GetPacketsByTraceTagRow{
			{
				PacketHashHex: "aabbccdd",
				RouteType:     1,
				ScopeName:     &scopeName,
				FirstHeardAt:  firstHeard,
				LastHeardAt:   lastHeard,
				ParsedPayload: parsedPayload,
			},
		}, nil)

	mock.EXPECT().
		ListObservationsForPacket(gomock.Any(), []byte{0xaa, 0xbb, 0xcc, 0xdd}).
		Return([]sqlc.ListObservationsForPacketRow{
			{Iata: "YVR"},
		}, nil)

	mock.EXPECT().
		ResolvePathHashesP2(gomock.Any(), sqlc.ResolvePathHashesP2Params{
			Iata:    "YVR",
			Column2: [][]byte{{0xaa, 0xbb}},
		}).
		Return([]sqlc.ResolvePathHashesP2Row{}, nil)

	store := &Store{q: mock}
	detail, err := store.GetTraceByTag(context.Background(), "trace-001")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if detail == nil {
		t.Fatal("expected detail, got nil")
	}
	if detail.TraceTag != "trace-001" {
		t.Errorf("expected TraceTag trace-001, got %s", detail.TraceTag)
	}
	if len(detail.Packets) != 1 {
		t.Fatalf("expected 1 packet, got %d", len(detail.Packets))
	}
	if len(detail.Packets[0].RawPath) != 1 {
		t.Errorf("expected 1 raw hop, got %d", len(detail.Packets[0].RawPath))
	}
}

func TestGetTraceByTag_DBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		GetPacketsByTraceTag(gomock.Any(), "trace-001").
		Return(nil, errors.New("db error"))

	store := &Store{q: mock}
	_, err := store.GetTraceByTag(context.Background(), "trace-001")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
