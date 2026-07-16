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
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/mock/gomock"
)

func TestNullableUUID_Zero(t *testing.T) {
	if nullableUUID(uuid.UUID{}) != nil {
		t.Error("expected nil for zero UUID")
	}
}

func TestNullableUUID_NonZero(t *testing.T) {
	id := uuid.New()
	result := nullableUUID(id)
	if result == nil {
		t.Fatal("expected non-nil for non-zero UUID")
	}
	if *result != id {
		t.Errorf("expected %s, got %s", id, *result)
	}
}

func TestTristate_Nil(t *testing.T) {
	if tristate(nil) != "any" {
		t.Error("expected \"any\" for nil")
	}
}

func TestTristate_True(t *testing.T) {
	b := true
	if tristate(&b) != "true" {
		t.Error("expected \"true\" for true")
	}
}

func TestTristate_False(t *testing.T) {
	b := false
	if tristate(&b) != "false" {
		t.Error("expected \"false\" for false")
	}
}

func TestToChannelMessage(t *testing.T) {
	senderName := "Alice"
	content := "hello"
	channelHash := []byte{0xab}
	sentAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}

	msg := toChannelMessage(42, "deadbeef", channelHash, &senderName, &content, sentAt, 7)

	if msg.ID != 42 {
		t.Errorf("expected ID 42, got %d", msg.ID)
	}
	if msg.PacketHash != "deadbeef" {
		t.Errorf("expected PacketHash deadbeef, got %s", msg.PacketHash)
	}
	if msg.ChannelHash != "ab" {
		t.Errorf("expected ChannelHash ab, got %s", msg.ChannelHash)
	}
	if msg.SenderName != "Alice" {
		t.Errorf("expected SenderName Alice, got %s", msg.SenderName)
	}
	if msg.Content != "hello" {
		t.Errorf("expected Content hello, got %s", msg.Content)
	}
	if msg.SentAt != 1700000000000 {
		t.Errorf("expected SentAt 1700000000000, got %d", msg.SentAt)
	}
	if msg.ObservationCount != 7 {
		t.Errorf("expected ObservationCount 7, got %d", msg.ObservationCount)
	}
}

func TestToChannelMessage_NilFields(t *testing.T) {
	sentAt := pgtype.Timestamptz{Time: time.UnixMilli(0), Valid: true}
	msg := toChannelMessage(1, "abc", []byte{0x01}, nil, nil, sentAt, 0)
	if msg.SenderName != "" {
		t.Errorf("expected empty SenderName, got %s", msg.SenderName)
	}
	if msg.Content != "" {
		t.Errorf("expected empty Content, got %s", msg.Content)
	}
}

func TestResolvePathHashes_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	// no EXPECT — sqlc should never be called
	store := &Store{q: mock}

	result, err := store.ResolvePathHashes(context.Background(), "YVR", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %v", result)
	}
}

func TestResolvePathHashes_DBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	hashes := [][]byte{{0x01, 0x02}}
	mock.EXPECT().
		ResolvePathHashesP2(gomock.Any(), sqlc.ResolvePathHashesP2Params{
			Iata:    "YVR",
			Column2: hashes,
		}).
		Return(nil, errors.New("db error"))

	store := &Store{q: mock}
	result, err := store.ResolvePathHashes(context.Background(), "YVR", hashes)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if result != nil {
		t.Errorf("expected nil result on error, got %v", result)
	}
}

func TestResolvePathHashes_Mapping(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	name := "test-node"
	lat := 49.1967
	lon := -123.1815
	pubkey := []byte{0xde, 0xad}
	hashes := [][]byte{{0xab, 0xcd}}

	mock.EXPECT().
		ResolvePathHashesP2(gomock.Any(), sqlc.ResolvePathHashesP2Params{
			Iata:    "YVR",
			Column2: hashes,
		}).
		Return([]sqlc.ResolvePathHashesP2Row{
			{
				Hash:      []byte{0xab, 0xcd},
				NodeID:    nodeID,
				Name:      &name,
				Latitude:  &lat,
				Longitude: &lon,
				PublicKey: pubkey,
			},
		}, nil)

	store := &Store{q: mock}
	result, err := store.ResolvePathHashes(context.Background(), "YVR", hashes)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	entries, ok := result["abcd"]
	if !ok {
		t.Fatal("expected key abcd in result")
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].NodeID != nodeID {
		t.Errorf("expected NodeID %s, got %s", nodeID, entries[0].NodeID)
	}
	if entries[0].Name != &name {
		t.Errorf("expected Name %s, got %v", name, entries[0].Name)
	}
}
