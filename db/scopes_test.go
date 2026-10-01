// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"go.uber.org/mock/gomock"
)

func TestGetTransportScopes(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		GetTransportScopes(gomock.Any()).
		Return([]sqlc.GetTransportScopesRow{
			{
				Name:           "default",
				TransportKey:   []byte{0x01, 0x02},
				KeyFingerprint: []byte{0xde, 0xad},
			},
		}, nil)

	store := &Store{q: mock}
	entries, err := store.GetTransportScopes(context.Background())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected 1 entry, got %d", len(entries))
	}
	if entries[0].Name != "default" {
		t.Errorf("expected Name default, got %s", entries[0].Name)
	}
	if !bytes.Equal(entries[0].TransportKey, []byte{0x01, 0x02}) {
		t.Errorf("expected TransportKey [0x01 0x02], got %v", entries[0].TransportKey)
	}
}

func TestGetScopeByName(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		GetScopeByName(gomock.Any(), "default").
		Return(sqlc.GetScopeByNameRow{
			Name:          "default",
			PacketCount:   100,
			ObserverCount: 5,
			NodeCount:     20,
			IataCount:     3,
			Iatas:         []string{"YVR", "YYJ", "YYC"},
		}, nil)

	store := &Store{q: mock}
	detail, err := store.GetScopeByName(context.Background(), "default")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if detail.Name != "default" {
		t.Errorf("expected Name default, got %s", detail.Name)
	}
	if len(detail.IATAs) != 3 {
		t.Errorf("expected 3 IATAs, got %d", len(detail.IATAs))
	}
	if detail.PacketCount != 100 {
		t.Errorf("expected PacketCount 100, got %d", detail.PacketCount)
	}
}
