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

func TestListChannels_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		ListChannels(gomock.Any(), sqlc.ListChannelsParams{
			Column1: nil,
			Column2: "",
			Column3: pgtype.Timestamptz{},
			Limit:   11,
		}).
		Return([]sqlc.Channel{}, nil)

	store := &Store{q: mock}
	page, err := store.ListChannels(context.Background(), 10, nil, "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items) != 0 {
		t.Errorf("expected 0 items, got %d", len(page.Items))
	}
	if page.HasMore {
		t.Error("expected HasMore false")
	}
}

func TestListChannels_Pagination(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	isHashtag := false
	keyKnown := true
	lastSeen := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}

	// return limit+1 rows to trigger HasMore
	rows := make([]sqlc.Channel, 3)
	for i := range rows {
		rows[i] = sqlc.Channel{
			ID:          int32(i + 1),
			ChannelHash: []byte{0xab, 0xcd},
			LastSeen:    lastSeen,
			IsHashtag:   &isHashtag,
			KeyKnown:    &keyKnown,
		}
	}

	mock.EXPECT().
		ListChannels(gomock.Any(), sqlc.ListChannelsParams{
			Column1: nil,
			Column2: "",
			Column3: pgtype.Timestamptz{},
			Limit:   3, // limit+1
		}).
		Return(rows, nil)

	store := &Store{q: mock}
	page, err := store.ListChannels(context.Background(), 2, nil, "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items) != 2 {
		t.Errorf("expected 2 items, got %d", len(page.Items))
	}
	if !page.HasMore {
		t.Error("expected HasMore true")
	}
	if page.NextCursor == nil {
		t.Error("expected NextCursor to be set")
	}
}

func TestListChannels_DBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		ListChannels(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("db error"))

	store := &Store{q: mock}
	_, err := store.ListChannels(context.Background(), 10, nil, "", 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetChannel_Basic(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	isHashtag := false
	keyKnown := true
	msgCount := int64(5)
	lastSeen := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}

	mock.EXPECT().
		GetChannelByID(gomock.Any(), int32(1)).
		Return(sqlc.Channel{
			ID:           1,
			ChannelHash:  []byte{0xab, 0xcd},
			LastSeen:     lastSeen,
			IsHashtag:    &isHashtag,
			KeyKnown:     &keyKnown,
			MessageCount: &msgCount,
		}, nil)

	store := &Store{q: mock}
	ch, err := store.GetChannel(context.Background(), 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ch.ID != 1 {
		t.Errorf("expected ID 1, got %d", ch.ID)
	}
	if ch.ChannelHash != "abcd" {
		t.Errorf("expected ChannelHash abcd, got %s", ch.ChannelHash)
	}
	if ch.MessageCount != 5 {
		t.Errorf("expected MessageCount 5, got %d", ch.MessageCount)
	}
	if ch.KeyFingerprint != nil {
		t.Errorf("expected nil KeyFingerprint for non-hashtag channel")
	}
}

func TestGetChannel_HashtagWithFingerprint(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	isHashtag := true
	keyKnown := true
	fp := []byte{0xde, 0xad, 0xbe, 0xef}
	lastSeen := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}

	mock.EXPECT().
		GetChannelByID(gomock.Any(), int32(2)).
		Return(sqlc.Channel{
			ID:             2,
			ChannelHash:    []byte{0x01, 0x02},
			LastSeen:       lastSeen,
			IsHashtag:      &isHashtag,
			KeyKnown:       &keyKnown,
			KeyFingerprint: fp,
		}, nil)

	store := &Store{q: mock}
	ch, err := store.GetChannel(context.Background(), 2)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if ch.KeyFingerprint == nil {
		t.Fatal("expected KeyFingerprint to be set for hashtag channel")
	}
	if *ch.KeyFingerprint != "deadbeef" {
		t.Errorf("expected KeyFingerprint deadbeef, got %s", *ch.KeyFingerprint)
	}
}

func TestGetChannel_DBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		GetChannelByID(gomock.Any(), int32(1)).
		Return(sqlc.Channel{}, errors.New("db error"))

	store := &Store{q: mock}
	_, err := store.GetChannel(context.Background(), 1)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestListChannelMessages_AllChannels(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	sentAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	senderName := "Alice"
	content := "hello"

	mock.EXPECT().
		ListAllChannelMessages(gomock.Any(), sqlc.ListAllChannelMessagesParams{
			Column1: pgtype.Timestamptz{},
			Column2: []string{"YVR"},
			Column3: "",
			Column4: int64(0),
			Limit:   3,
		}).
		Return([]sqlc.ListAllChannelMessagesRow{
			{
				ID:               1,
				PacketHashHex:    "deadbeef",
				ChannelHash:      []byte{0xab},
				SenderName:       &senderName,
				Content:          &content,
				SentAt:           sentAt,
				ObservationCount: 2,
			},
			{
				ID:               2,
				PacketHashHex:    "cafebabe",
				ChannelHash:      []byte{0xcd},
				SenderName:       &senderName,
				Content:          &content,
				SentAt:           sentAt,
				ObservationCount: 1,
			},
			{
				ID:               3,
				PacketHashHex:    "deadcafe",
				ChannelHash:      []byte{0xef},
				SenderName:       &senderName,
				Content:          &content,
				SentAt:           sentAt,
				ObservationCount: 1,
			},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListChannelMessages(context.Background(), nil, time.Time{}, 2, []string{"YVR"}, "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items) != 2 {
		t.Errorf("expected 2 items, got %d", len(page.Items))
	}
	if !page.HasMore {
		t.Error("expected HasMore true")
	}
	if page.NextCursor == nil {
		t.Error("expected NextCursor to be set")
	}
}

func TestListChannelMessages_ByChannelID(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	channelID := int32(1)
	sentAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	senderName := "Bob"
	content := "world"

	mock.EXPECT().
		ListChannelMessages(gomock.Any(), sqlc.ListChannelMessagesParams{
			ChannelID: channelID,
			Column2:   pgtype.Timestamptz{},
			Column3:   []string{"YVR"},
			Column4:   "",
			Column5:   int64(0),
			Limit:     3,
		}).
		Return([]sqlc.ListChannelMessagesRow{
			{
				ID:               1,
				PacketHashHex:    "deadbeef",
				ChannelHash:      []byte{0xab},
				SenderName:       &senderName,
				Content:          &content,
				SentAt:           sentAt,
				ObservationCount: 1,
			},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListChannelMessages(context.Background(), &channelID, time.Time{}, 2, []string{"YVR"}, "", 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items) != 1 {
		t.Errorf("expected 1 item, got %d", len(page.Items))
	}
	if page.HasMore {
		t.Error("expected HasMore false")
	}
}

func TestListChannelMessages_DBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		ListAllChannelMessages(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("db error"))

	store := &Store{q: mock}
	_, err := store.ListChannelMessages(context.Background(), nil, time.Time{}, 10, nil, "", 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
