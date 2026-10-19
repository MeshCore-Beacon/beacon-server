// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
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
