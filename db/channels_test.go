// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"math"
	"strings"
	"sync"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/mock/gomock"
)

func TestListChannels_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		ListChannels(gomock.Any(), sqlc.ListChannelsParams{
			ChannelHash: nil,
			Iatas:       nil,
			CursorTs:    pgtype.Timestamptz{},
			PageLimit:   11,
		}).
		Return([]sqlc.Channel{}, nil)

	store := &Store{q: mock}
	page, err := store.ListChannels(context.Background(), 10, nil, nil, nil, 0, nil)
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
			ChannelHash: nil,
			Iatas:       nil,
			CursorTs:    pgtype.Timestamptz{},
			PageLimit:   3, // limit+1
		}).
		Return(rows, nil)

	store := &Store{q: mock}
	page, err := store.ListChannels(context.Background(), 2, nil, nil, nil, 0, nil)
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
	_, err := store.ListChannels(context.Background(), 10, nil, nil, nil, 0, nil)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestListChannels_IATAFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		ListChannels(gomock.Any(), sqlc.ListChannelsParams{
			ChannelHash: nil,
			Iatas:       []string{"YOW", "YYZ"},
			CursorTs:    pgtype.Timestamptz{},
			PageLimit:   11,
		}).
		Return([]sqlc.Channel{}, nil)

	store := &Store{q: mock}
	_, err := store.ListChannels(context.Background(), 10, nil, []string{"YOW", "YYZ"}, nil, 0, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestListChannels_KeyKnownFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	keyKnown := false
	at := time.UnixMilli(1700000000000)

	mock.EXPECT().
		ListChannels(gomock.Any(), sqlc.ListChannelsParams{KeyKnown: &keyKnown, PageLimit: 11}).
		Return([]sqlc.Channel{}, nil)
	mock.EXPECT().
		ListChannelsAfter(gomock.Any(), sqlc.ListChannelsAfterParams{
			KeyKnown: &keyKnown, PageLimit: 11,
			CursorTs: pgtype.Timestamptz{Time: at, Valid: true}, CursorID: 7,
		}).
		Return([]sqlc.Channel{}, nil)

	store := &Store{q: mock}
	if _, err := store.ListChannels(context.Background(), 10, nil, nil, &keyKnown, 0, nil); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, err := store.ListChannels(context.Background(), 10, nil, nil, &keyKnown, 0, &api.ChannelCursor{LastSeen: at, ID: 7}); err != nil {
		t.Fatalf("unexpected error: %v", err)
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
			MessageCount: msgCount,
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
			Column3: "#bc",
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
	// Sites with a scope across every channel: the original statement, trusted or not.
	page, err := store.ListChannelMessages(context.Background(), nil, time.Time{}, 2, []string{"YVR"}, "#bc", 0)
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

// A channel's region page while the read model is not trusted answers ErrPagesNotReady and runs no statement: the
// per-channel original statement is the verification's alone.
func TestListChannelMessages_ByChannelIDNotReady(t *testing.T) {
	ctrl := gomock.NewController(t)
	store := &Store{q: mockdb.NewMockQuerier(ctrl)}
	channelID := int32(1)
	page, err := store.ListChannelMessages(context.Background(), &channelID, time.Time{}, 2, []string{"YVR"}, "", 0)
	if !errors.Is(err, api.ErrPagesNotReady) || page.Items != nil {
		t.Fatalf("page %+v, err %v; want ErrPagesNotReady", page, err)
	}
}

func TestListChannelMessages_DBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		ListAllChannelMessagesPlain(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("db error"))

	store := &Store{q: mock}
	_, err := store.ListChannelMessages(context.Background(), nil, time.Time{}, 10, nil, "", 0)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

var testFP = []byte{1, 2, 3, 4, 5, 6, 7, 8}

func strPtr(s string) *string { return &s }

func TestUpsertChannelWriteThroughCached(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	cache := NewChannelIDCache()
	cache.put(channelKey{hash: "\x11", fp: string(testFP)}, 7)
	store := &Store{q: mock, channelIDs: cache}

	mock.EXPECT().UpsertChannel(gomock.Any(), gomock.Any()).Times(0)
	gomock.InOrder(
		mock.EXPECT().TouchChannel(gomock.Any(), sqlc.TouchChannelParams{
			Name: strPtr("pub"), ID: 7, ChannelHash: []byte{0x11}, KeyFingerprint: testFP,
		}).Return(int32(7), nil),
		mock.EXPECT().TouchChannel(gomock.Any(), sqlc.TouchChannelParams{
			Name: nil, ID: 7, ChannelHash: []byte{0x11}, KeyFingerprint: testFP,
		}).Return(int32(7), nil),
	)

	for _, name := range []string{"pub", ""} {
		id, err := store.UpsertChannel(context.Background(), []byte{0x11}, testFP, name, "")
		if err != nil {
			t.Fatal(err)
		}
		if id != 7 {
			t.Fatalf("name %q: id = %d, want 7", name, id)
		}
	}
}

func upsertParams(hash, fp []byte, name, hashtag string) sqlc.UpsertChannelParams {
	p := sqlc.UpsertChannelParams{ChannelHash: hash, Column2: fp}
	if name != "" {
		p.Name = &name
	}
	if hashtag != "" {
		p.Hashtag = &hashtag
	}
	isHashtag := hashtag != ""
	p.IsHashtag = &isHashtag
	return p
}

func TestUpsertChannelWriteThroughFallback(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	cache := NewChannelIDCache()
	key := channelKey{hash: "\x11", fp: string(testFP)}
	cache.put(key, 7)
	store := &Store{q: mock, channelIDs: cache}

	gomock.InOrder(
		mock.EXPECT().TouchChannel(gomock.Any(), sqlc.TouchChannelParams{
			ID: 7, ChannelHash: []byte{0x11}, KeyFingerprint: testFP,
		}).Return(int32(0), pgx.ErrNoRows),
		mock.EXPECT().UpsertChannel(gomock.Any(), upsertParams([]byte{0x11}, testFP, "", "#tag")).
			Return(sqlc.Channel{ID: 9}, nil),
		mock.EXPECT().TouchChannel(gomock.Any(), sqlc.TouchChannelParams{
			ID: 9, ChannelHash: []byte{0x11}, KeyFingerprint: testFP,
		}).Return(int32(9), nil),
	)

	for i := 0; i < 2; i++ {
		id, err := store.UpsertChannel(context.Background(), []byte{0x11}, testFP, "", "#tag")
		if err != nil {
			t.Fatal(err)
		}
		if id != 9 {
			t.Fatalf("call %d: id = %d, want 9", i, id)
		}
	}
	if got, ok := cache.get(key); !ok || got != 9 {
		t.Fatalf("cache = %d, %v; want 9", got, ok)
	}
}

func TestUpsertChannelWriteThroughUnknownKeyCached(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	cache := NewChannelIDCache()
	store := &Store{q: mock, channelIDs: cache}

	gomock.InOrder(
		mock.EXPECT().UpsertChannel(gomock.Any(), upsertParams([]byte{0x11}, testFP, "pub", "")).
			Return(sqlc.Channel{ID: 4}, nil),
		mock.EXPECT().TouchChannel(gomock.Any(), sqlc.TouchChannelParams{
			Name: strPtr("pub"), ID: 4, ChannelHash: []byte{0x11}, KeyFingerprint: testFP,
		}).Return(int32(4), nil),
	)
	for i := 0; i < 2; i++ {
		id, err := store.UpsertChannel(context.Background(), []byte{0x11}, testFP, "pub", "")
		if err != nil || id != 4 {
			t.Fatalf("call %d: %d, %v; want 4", i, id, err)
		}
	}
	if cache.Len() != 1 {
		t.Fatalf("cache.Len() = %d, want 1", cache.Len())
	}
}

func TestUpsertChannelTouchErrorReturned(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	cache := NewChannelIDCache()
	key := channelKey{hash: "\x11", fp: string(testFP)}
	cache.put(key, 7)
	store := &Store{q: mock, channelIDs: cache}
	boom := errors.New("boom")

	mock.EXPECT().TouchChannel(gomock.Any(), gomock.Any()).Return(int32(0), boom)
	mock.EXPECT().UpsertChannel(gomock.Any(), gomock.Any()).Times(0)

	if _, err := store.UpsertChannel(context.Background(), []byte{0x11}, testFP, "", ""); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if got, ok := cache.get(key); !ok || got != 7 {
		t.Fatalf("cache = %d, %v; want 7 kept", got, ok)
	}
}

func TestUpsertChannelNilFingerprintBypassesCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	cache := NewChannelIDCache()
	store := &Store{q: mock, channelIDs: cache}

	mock.EXPECT().UpsertChannel(gomock.Any(), upsertParams([]byte{0x11}, nil, "pub", "")).
		Return(sqlc.Channel{ID: 5}, nil).Times(2)
	mock.EXPECT().TouchChannel(gomock.Any(), gomock.Any()).Times(0)
	mock.EXPECT().TouchChannelHashOnly(gomock.Any(), gomock.Any()).Times(0)

	for i := 0; i < 2; i++ {
		id, err := store.UpsertChannel(context.Background(), []byte{0x11}, nil, "pub", "")
		if err != nil || id != 5 {
			t.Fatalf("call %d: %d, %v; want 5", i, id, err)
		}
	}
	if cache.Len() != 0 {
		t.Fatalf("cache.Len() = %d, want 0", cache.Len())
	}
}

func TestUpsertChannelWithoutCacheUnchanged(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	store := &Store{q: mock}

	mock.EXPECT().UpsertChannel(gomock.Any(), upsertParams([]byte{0x11}, testFP, "pub", "")).
		Return(sqlc.Channel{ID: 3}, nil).Times(2)
	mock.EXPECT().UpsertChannelHashOnly(gomock.Any(), []byte{0x22}).Return(int32(6), nil).Times(2)
	mock.EXPECT().TouchChannel(gomock.Any(), gomock.Any()).Times(0)
	mock.EXPECT().TouchChannelHashOnly(gomock.Any(), gomock.Any()).Times(0)

	for i := 0; i < 2; i++ {
		if id, err := store.UpsertChannel(context.Background(), []byte{0x11}, testFP, "pub", ""); err != nil || id != 3 {
			t.Fatalf("UpsertChannel: %d, %v", id, err)
		}
		if id, err := store.UpsertChannelHashOnly(context.Background(), []byte{0x22}); err != nil || id != 6 {
			t.Fatalf("UpsertChannelHashOnly: %d, %v", id, err)
		}
	}
	if err := store.PreloadChannelIDs(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestUpsertChannelHashOnlyWriteThrough(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	cache := NewChannelIDCache()
	key := channelKey{hash: "\x22", hashOnly: true}
	cache.put(key, 5)
	store := &Store{q: mock, channelIDs: cache}

	mock.EXPECT().UpsertChannel(gomock.Any(), gomock.Any()).Times(0)
	gomock.InOrder(
		mock.EXPECT().TouchChannelHashOnly(gomock.Any(), sqlc.TouchChannelHashOnlyParams{ID: 5, ChannelHash: []byte{0x22}}).
			Return(int32(5), nil),
		mock.EXPECT().TouchChannelHashOnly(gomock.Any(), sqlc.TouchChannelHashOnlyParams{ID: 5, ChannelHash: []byte{0x22}}).
			Return(int32(0), pgx.ErrNoRows),
		mock.EXPECT().UpsertChannelHashOnly(gomock.Any(), []byte{0x22}).Return(int32(8), nil),
		mock.EXPECT().TouchChannelHashOnly(gomock.Any(), sqlc.TouchChannelHashOnlyParams{ID: 8, ChannelHash: []byte{0x22}}).
			Return(int32(8), nil),
	)
	for i, want := range []int{5, 8, 8} {
		id, err := store.UpsertChannelHashOnly(context.Background(), []byte{0x22})
		if err != nil || id != want {
			t.Fatalf("call %d: %d, %v; want %d", i, id, err, want)
		}
	}
	if got, ok := cache.get(key); !ok || got != 8 {
		t.Fatalf("cache = %d, %v; want 8", got, ok)
	}
}

func TestChannelIDCacheKeys(t *testing.T) {
	c := NewChannelIDCache()
	hashOnly := channelKey{hash: "\x11", hashOnly: true}
	empty := channelKey{hash: "\x11", fp: ""}
	keyed := channelKey{hash: "\x11", fp: string(testFP)}
	c.put(hashOnly, 1)
	c.put(empty, 2)
	c.put(keyed, 3)
	if c.Len() != 3 {
		t.Fatalf("Len = %d, want 3 distinct keys", c.Len())
	}
	for k, want := range map[channelKey]int32{hashOnly: 1, empty: 2, keyed: 3} {
		if got, ok := c.get(k); !ok || got != want {
			t.Fatalf("get(%+v) = %d, %v; want %d", k, got, ok, want)
		}
	}
	c.forget(empty)
	if _, ok := c.get(empty); ok {
		t.Fatal("forgotten key still cached")
	}
	if _, ok := c.get(hashOnly); !ok {
		t.Fatal("forget removed a different key")
	}
	c.put(keyed, 4)
	if got, _ := c.get(keyed); got != 4 {
		t.Fatalf("put did not overwrite: %d", got)
	}
	c.replace(map[channelKey]int32{{hash: "\x99", hashOnly: true}: 9})
	if c.Len() != 1 {
		t.Fatalf("Len after replace = %d, want 1", c.Len())
	}
	if _, ok := c.get(keyed); ok {
		t.Fatal("replace kept an old entry")
	}
	if got, ok := c.get(channelKey{hash: "\x99", hashOnly: true}); !ok || got != 9 {
		t.Fatalf("replaced entry = %d, %v", got, ok)
	}
}

func TestChannelIDCacheConcurrent(t *testing.T) {
	c := NewChannelIDCache()
	keys := make([]channelKey, 64)
	for i := range keys {
		keys[i] = channelKey{hash: string([]byte{byte(i)}), fp: string([]byte{byte(i), 1}), hashOnly: i%2 == 0}
	}
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 1000; i++ {
				k := keys[(g*31+i)%len(keys)]
				switch i % 4 {
				case 0:
					c.get(k)
				case 1:
					c.put(k, int32(i))
				case 2:
					c.forget(k)
				case 3:
					if i%100 == 3 {
						c.replace(map[channelKey]int32{k: int32(g)})
					} else {
						c.Len()
					}
				}
			}
		}(g)
	}
	wg.Wait()
	if c.Len() > len(keys) {
		t.Fatalf("Len = %d > %d keys", c.Len(), len(keys))
	}
}

func TestPreloadChannelIDs(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	cache := NewChannelIDCache()
	store := &Store{q: mock, channelIDs: cache}

	mock.EXPECT().ListChannelIDs(gomock.Any()).Return([]sqlc.ListChannelIDsRow{
		{ID: 1, ChannelHash: []byte{0x11}, KeyFingerprint: testFP},
		{ID: 2, ChannelHash: []byte{0x12}, KeyFingerprint: []byte{}},
		{ID: 3, ChannelHash: []byte{0x11}, KeyFingerprint: nil},
	}, nil)
	if err := store.PreloadChannelIDs(context.Background()); err != nil {
		t.Fatal(err)
	}
	if cache.Len() != 3 {
		t.Fatalf("Len = %d, want 3", cache.Len())
	}
	for k, want := range map[channelKey]int32{
		{hash: "\x11", fp: string(testFP)}: 1,
		{hash: "\x12", fp: ""}:             2,
		{hash: "\x11", hashOnly: true}:     3,
	} {
		if got, ok := cache.get(k); !ok || got != want {
			t.Fatalf("get(%+v) = %d, %v; want %d", k, got, ok, want)
		}
	}

	boom := errors.New("boom")
	mock.EXPECT().ListChannelIDs(gomock.Any()).Return(nil, boom)
	if err := store.PreloadChannelIDs(context.Background()); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if cache.Len() != 3 {
		t.Fatalf("cache changed on error: Len = %d", cache.Len())
	}

	nilStore := &Store{q: mock}
	mock.EXPECT().ListChannelIDs(gomock.Any()).Times(0)
	if err := nilStore.PreloadChannelIDs(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestUpsertChannelFallbackErrorForgetsKey(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	cache := NewChannelIDCache()
	key := channelKey{hash: "\x11", fp: string(testFP)}
	cache.put(key, 7)
	store := &Store{q: mock, channelIDs: cache}
	boom := errors.New("boom")

	mock.EXPECT().TouchChannel(gomock.Any(), gomock.Any()).Return(int32(0), pgx.ErrNoRows)
	mock.EXPECT().UpsertChannel(gomock.Any(), gomock.Any()).Return(sqlc.Channel{}, boom)

	if _, err := store.UpsertChannel(context.Background(), []byte{0x11}, testFP, "", ""); !errors.Is(err, boom) {
		t.Fatalf("err = %v, want boom", err)
	}
	if _, ok := cache.get(key); ok {
		t.Fatal("a key whose row is gone stayed cached")
	}
}

// earnedCmi returns read models whose cmi trust is earned.
func earnedCmi() *ReadModels {
	tr := NewTrust("cmi_dispatch_test", true, CmiTrustPolicy)
	tr.Earn()
	return &ReadModels{Cmi: tr}
}

func TestListChannelMessagesDispatch(t *testing.T) {
	ch := int32(7)
	since := time.Date(2026, 10, 18, 6, 48, 0, 0, time.UTC)
	ts := pgtype.Timestamptz{Time: since, Valid: true}
	scope := int32(3)
	const maxCursor = int64(math.MaxInt64)
	type call func(m *mockdb.MockQuerier)
	scopeLookup := func(m *mockdb.MockQuerier) {
		m.EXPECT().GetTransportScopeByName(gomock.Any(), "#fr").Return(scope, nil)
	}
	cases := []struct {
		name    string
		channel *int32
		since   time.Time
		iatas   []string
		scope   string
		trusted bool
		expect  []call
	}{
		{"by sites", &ch, time.Time{}, []string{"YUL", "YUL", "YOW"}, "", true, []call{func(m *mockdb.MockQuerier) {
			m.EXPECT().ListChannelMessagesByIATAs(gomock.Any(), sqlc.ListChannelMessagesByIATAsParams{Iatas: []string{"YUL", "YOW"}, ChannelID: 7, CursorID: maxCursor, PageLimit: 51}).Return([]sqlc.ListChannelMessagesByIATAsRow{{ID: 1}}, nil)
		}}},
		{"by sites and scope", &ch, time.Time{}, []string{"YUL"}, "#fr", true, []call{scopeLookup, func(m *mockdb.MockQuerier) {
			m.EXPECT().ListChannelMessagesByIATAsScoped(gomock.Any(), sqlc.ListChannelMessagesByIATAsScopedParams{Iatas: []string{"YUL"}, ChannelID: 7, ScopeID: 3, CursorID: maxCursor, PageLimit: 51}).Return(nil, nil)
		}}},
		{"plain nil sites", &ch, time.Time{}, nil, "", true, []call{func(m *mockdb.MockQuerier) {
			m.EXPECT().ListChannelMessagesPlain(gomock.Any(), sqlc.ListChannelMessagesPlainParams{ChannelID: 7, CursorID: maxCursor, PageLimit: 51}).Return(nil, nil)
		}}},
		{"plain empty sites untrusted", &ch, time.Time{}, []string{}, "", false, []call{func(m *mockdb.MockQuerier) {
			m.EXPECT().ListChannelMessagesPlain(gomock.Any(), sqlc.ListChannelMessagesPlainParams{ChannelID: 7, CursorID: maxCursor, PageLimit: 51}).Return(nil, nil)
		}}},
		{"plain scoped", &ch, time.Time{}, nil, "#fr", true, []call{scopeLookup, func(m *mockdb.MockQuerier) {
			m.EXPECT().ListChannelMessagesPlainScoped(gomock.Any(), sqlc.ListChannelMessagesPlainScopedParams{ChannelID: 7, ScopeID: 3, CursorID: maxCursor, PageLimit: 51}).Return(nil, nil)
		}}},
		{"since walk with sites and scope", &ch, since, []string{"YUL"}, "#fr", true, []call{scopeLookup, func(m *mockdb.MockQuerier) {
			m.EXPECT().ListChannelMessagesSinceWalk(gomock.Any(), sqlc.ListChannelMessagesSinceWalkParams{ChannelID: 7, Since: ts, Iatas: []string{"YUL"}, ScopeID: &scope, CursorID: maxCursor, PageLimit: 51}).Return(nil, nil)
		}}},
		{"since walk untrusted reads no cmi", &ch, since, nil, "", false, []call{func(m *mockdb.MockQuerier) {
			m.EXPECT().ListChannelMessagesSinceWalk(gomock.Any(), sqlc.ListChannelMessagesSinceWalkParams{ChannelID: 7, Since: ts, Iatas: []string{}, CursorID: maxCursor, PageLimit: 51}).Return(nil, nil)
		}}},
		{"all by sites", nil, time.Time{}, []string{"YUL"}, "", true, []call{func(m *mockdb.MockQuerier) {
			m.EXPECT().ListAllChannelMessagesByIATAs(gomock.Any(), sqlc.ListAllChannelMessagesByIATAsParams{Iatas: []string{"YUL"}, CursorID: maxCursor, PageLimit: 51}).Return(nil, nil)
		}}},
		// Sites with a scope or a since across every channel: each site's range has no bound on the scope or since,
		// so these run today's statement, trusted or not.
		{"all by sites and scope", nil, time.Time{}, []string{"YUL"}, "#fr", true, []call{func(m *mockdb.MockQuerier) {
			m.EXPECT().ListAllChannelMessages(gomock.Any(), sqlc.ListAllChannelMessagesParams{Column1: pgtype.Timestamptz{}, Column2: []string{"YUL"}, Column3: "#fr", Column4: 0, Limit: 51}).Return(nil, nil)
		}}},
		{"all by sites since", nil, since, []string{"YUL"}, "", true, []call{func(m *mockdb.MockQuerier) {
			m.EXPECT().ListAllChannelMessages(gomock.Any(), sqlc.ListAllChannelMessagesParams{Column1: ts, Column2: []string{"YUL"}, Column3: "", Column4: 0, Limit: 51}).Return(nil, nil)
		}}},
		{"all since scoped untrusted reads no cmi", nil, since, nil, "#fr", false, []call{scopeLookup, func(m *mockdb.MockQuerier) {
			m.EXPECT().ListAllChannelMessagesSince(gomock.Any(), sqlc.ListAllChannelMessagesSinceParams{Since: ts, ScopeID: &scope, CursorID: maxCursor, PageLimit: 51}).Return(nil, nil)
		}}},
		{"all plain untrusted", nil, time.Time{}, nil, "", false, []call{func(m *mockdb.MockQuerier) {
			m.EXPECT().ListAllChannelMessagesPlain(gomock.Any(), sqlc.ListAllChannelMessagesPlainParams{CursorID: maxCursor, PageLimit: 51}).Return(nil, nil)
		}}},
		{"all plain scoped", nil, time.Time{}, nil, "#fr", true, []call{scopeLookup, func(m *mockdb.MockQuerier) {
			m.EXPECT().ListAllChannelMessagesPlainScoped(gomock.Any(), sqlc.ListAllChannelMessagesPlainScopedParams{ScopeID: 3, CursorID: maxCursor, PageLimit: 51}).Return(nil, nil)
		}}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			m := mockdb.NewMockQuerier(ctrl)
			for _, e := range c.expect {
				e(m)
			}
			s := &Store{q: m}
			if c.trusted {
				s.readModels = earnedCmi()
			}
			page, err := s.ListChannelMessages(context.Background(), c.channel, c.since, 50, c.iatas, c.scope, 0)
			if err != nil || page.Items == nil {
				t.Fatalf("page = %+v, %v", page, err)
			}
		})
	}

	// Every shape that reads cmi answers ErrPagesNotReady with no statement when cmi is not trusted; sites with a
	// scope or a since across every channel run the original all-channel statement, trusted or not.
	disabled := NewTrust("cmi_dispatch_disabled", false, CmiTrustPolicy)
	disabled.Earn()
	dropped := NewTrust("cmi_dispatch_dropped", true, CmiTrustPolicy)
	dropped.Earn()
	for range 3 {
		dropped.ObserveRun(1)
	}
	untrusted := map[string]*ReadModels{
		"nil": nil, "nil trust": {}, "not earned": {Cmi: NewTrust("cmi_dispatch_new", true, CmiTrustPolicy)},
		"disabled": {Cmi: disabled}, "dropped": {Cmi: dropped},
	}
	shapes := []struct {
		channel *int32
		since   time.Time
		iatas   []string
		scope   string
	}{
		{&ch, time.Time{}, []string{"YUL"}, ""}, {&ch, time.Time{}, []string{"YUL"}, "#fr"}, {&ch, time.Time{}, nil, "#fr"},
		{&ch, since, []string{"YUL"}, ""}, {nil, time.Time{}, []string{"YUL"}, ""}, {nil, time.Time{}, []string{"YUL"}, "#fr"},
		{nil, since, []string{"YUL"}, "#fr"}, {nil, time.Time{}, nil, "#fr"},
	}
	for name, rm := range untrusted {
		for i, sh := range shapes {
			ctrl := gomock.NewController(t)
			m := mockdb.NewMockQuerier(ctrl)
			legacyTS := pgtype.Timestamptz{Time: sh.since, Valid: !sh.since.IsZero()}
			allFiltered := sh.channel == nil && len(sh.iatas) > 0 && (sh.scope != "" || !sh.since.IsZero())
			if allFiltered {
				m.EXPECT().ListAllChannelMessages(gomock.Any(), sqlc.ListAllChannelMessagesParams{Column1: legacyTS, Column2: sh.iatas, Column3: sh.scope, Column4: 0, Limit: 51}).Return(nil, nil)
			}
			s := &Store{q: m, readModels: rm}
			_, err := s.ListChannelMessages(context.Background(), sh.channel, sh.since, 50, sh.iatas, sh.scope, 0)
			if allFiltered && err != nil || !allFiltered && !errors.Is(err, api.ErrPagesNotReady) {
				t.Fatalf("%s shape %d: %v", name, i, err)
			}
			ctrl.Finish()
		}
	}

	// An unknown scope answers an empty page with no list query; another lookup error is returned.
	for _, trusted := range []bool{true, false} {
		ctrl := gomock.NewController(t)
		m := mockdb.NewMockQuerier(ctrl)
		m.EXPECT().GetTransportScopeByName(gomock.Any(), "#nope").Return(int32(0), pgx.ErrNoRows)
		s := &Store{q: m}
		var shapeChannel *int32
		shapeSince := time.Time{}
		if trusted {
			s.readModels = earnedCmi()
		} else {
			shapeSince = since // untrusted, only the non-cmi shapes look the scope up
		}
		page, err := s.ListChannelMessages(context.Background(), shapeChannel, shapeSince, 50, nil, "#nope", 0)
		if err != nil || page.Items == nil || len(page.Items) != 0 || page.HasMore || page.NextCursor != nil {
			t.Fatalf("unknown scope: %+v, %v", page, err)
		}
		ctrl.Finish()
	}
	ctrl := gomock.NewController(t)
	m := mockdb.NewMockQuerier(ctrl)
	m.EXPECT().GetTransportScopeByName(gomock.Any(), "#fr").Return(int32(0), errors.New("lookup failed"))
	if _, err := (&Store{q: m, readModels: earnedCmi()}).ListChannelMessages(context.Background(), &ch, time.Time{}, 50, nil, "#fr", 0); err == nil {
		t.Fatal("lookup error not returned")
	}

	// Cursors: 0 is the newest page, any other value is passed as given; limit+1 rows fetched.
	for _, cur := range []int64{-1, 3_000_000_000} {
		ctrl := gomock.NewController(t)
		m := mockdb.NewMockQuerier(ctrl)
		m.EXPECT().ListChannelMessagesPlain(gomock.Any(), sqlc.ListChannelMessagesPlainParams{ChannelID: 7, CursorID: cur, PageLimit: 51}).Return(nil, nil)
		if _, err := (&Store{q: m}).ListChannelMessages(context.Background(), &ch, time.Time{}, 50, nil, "", cur); err != nil {
			t.Fatal(err)
		}
		ctrl.Finish()
	}
	ctrl = gomock.NewController(t)
	m = mockdb.NewMockQuerier(ctrl)
	rows := make([]sqlc.ListChannelMessagesPlainRow, 51)
	for i := range rows {
		rows[i] = sqlc.ListChannelMessagesPlainRow{ID: int64(100 - i)}
	}
	m.EXPECT().ListChannelMessagesPlain(gomock.Any(), gomock.Any()).Return(rows, nil)
	page, err := (&Store{q: m}).ListChannelMessages(context.Background(), &ch, time.Time{}, 50, nil, "", 0)
	if err != nil || len(page.Items) != 50 || !page.HasMore || page.NextCursor == nil || *page.NextCursor != 51 {
		t.Fatalf("page of 51 = %d items, hasMore %v, next %v, %v", len(page.Items), page.HasMore, page.NextCursor, err)
	}
}

// Site lists come from the request with no cap: the dedupe is one pass, keeps first-occurrence order and is never nil.
func TestDedupeSitesLargeList(t *testing.T) {
	in := make([]string, 200_000)
	for i := range in {
		in[i] = []string{"YUL", "YOW", "YYZ"}[i%3]
	}
	in[199_999] = "ZZZ"
	start := time.Now()
	got := dedupeSites(in)
	if d := time.Since(start); d > time.Second {
		t.Fatalf("200K codes took %s", d)
	}
	if strings.Join(got, ",") != "YUL,YOW,YYZ,ZZZ" {
		t.Fatalf("dedupe = %v", got)
	}
	if empty := dedupeSites(nil); empty == nil || len(empty) != 0 {
		t.Fatalf("nil input gave %#v", empty)
	}
}
