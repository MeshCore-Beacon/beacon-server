// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"slices"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/mock/gomock"
)

func TestUpsertPacket_WithTransportCodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	// little-endian: region=1, subregion=2
	transportCodes := []byte{0x01, 0x00, 0x02, 0x00}

	mock.EXPECT().
		UpsertPacket(gomock.Any(), gomock.Any()).
		Return(sqlc.UpsertPacketRow{Inserted: true}, nil)

	store := &Store{q: mock}
	inserted, err := store.UpsertPacket(context.Background(), ingest.UpsertPacketParams{
		PacketHash:     []byte{0xde, 0xad},
		TransportCodes: transportCodes,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !inserted {
		t.Error("expected inserted true")
	}
}

func TestUpsertPacket_WithoutTransportCodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		UpsertPacket(gomock.Any(), gomock.Any()).
		Return(sqlc.UpsertPacketRow{Inserted: false}, nil)

	store := &Store{q: mock}
	inserted, err := store.UpsertPacket(context.Background(), ingest.UpsertPacketParams{
		PacketHash: []byte{0xde, 0xad},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if inserted {
		t.Error("expected inserted false")
	}
}

func TestListPackets_Pagination(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	heardAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	rows := make([]sqlc.ListPacketsPageRow, 3)
	for i := range rows {
		rows[i] = sqlc.ListPacketsPageRow{
			PacketHash:   []byte{0xde, 0xad},
			FirstHeardAt: heardAt,
			LastHeardAt:  heardAt,
		}
	}

	mock.EXPECT().
		ListPacketsPage(gomock.Any(), gomock.Any()).
		Return(rows, nil)

	store := &Store{q: mock}
	page, err := store.ListPackets(context.Background(), nil, nil, nil, nil, time.Time{}, time.Time{}, 0, nil, 2)
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

func TestListPackets_LatestObserverNil(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	heardAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}

	mock.EXPECT().
		ListPacketsPage(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListPacketsPageRow{
			{
				PacketHash:       []byte{0xde, 0xad},
				FirstHeardAt:     heardAt,
				LastHeardAt:      heardAt,
				LatestObserverID: uuid.UUID{}, // zero UUID
			},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListPackets(context.Background(), nil, nil, nil, nil, time.Time{}, time.Time{}, 0, nil, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if page.Items[0].LatestObserver != nil {
		t.Error("expected nil LatestObserver for zero UUID")
	}
}

func TestListPackets_LatestObserverSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	heardAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	observerName := "test-observer"
	observerIATA := "YVR"

	mock.EXPECT().
		ListPacketsPage(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListPacketsPageRow{
			{
				PacketHash:         []byte{0xde, 0xad},
				FirstHeardAt:       heardAt,
				LastHeardAt:        heardAt,
				LatestObserverID:   observerID,
				LatestObserverName: &observerName,
				LatestObserverIata: observerIATA,
			},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListPackets(context.Background(), nil, nil, nil, nil, time.Time{}, time.Time{}, 0, nil, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if page.Items[0].LatestObserver == nil {
		t.Fatal("expected LatestObserver to be set")
	}
	if page.Items[0].LatestObserver.IATA != "" && page.Items[0].LatestObserver.IATA != "YVR" {
		t.Errorf("expected IATA YVR, got %v", page.Items[0].LatestObserver.IATA)
	}
}

func TestListPackets_LatestObserverPathFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	heardAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	pathLengthByte := int16(0x42)
	hashSize := int16(1)
	hopCount := int16(2)
	pathBytes := []byte{0xa1, 0xb2}

	mock.EXPECT().
		ListPacketsPage(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListPacketsPageRow{
			{
				PacketHash:                   []byte{0xde, 0xad},
				FirstHeardAt:                 heardAt,
				LastHeardAt:                  heardAt,
				LatestObserverID:             observerID,
				LatestObserverPathLengthByte: pathLengthByte,
				LatestObserverHashSize:       hashSize,
				LatestObserverHopCount:       hopCount,
				LatestObserverPathBytes:      pathBytes,
			},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListPackets(context.Background(), nil, nil, nil, nil, time.Time{}, time.Time{}, 0, nil, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	obs := page.Items[0].LatestObserver
	if obs == nil {
		t.Fatal("expected LatestObserver to be set")
	}
	if obs.PathLength == nil {
		t.Fatal("expected PathLength to be set")
	}
	if obs.PathLength.HashSize != 1 || obs.PathLength.HopCount != 2 {
		t.Errorf("expected hashSize=1 hopCount=2, got hashSize=%d hopCount=%d", obs.PathLength.HashSize, obs.PathLength.HopCount)
	}
	if obs.PathLength.Raw != "42" {
		t.Errorf("expected raw 42, got %s", obs.PathLength.Raw)
	}
	if obs.PathBytes == nil || *obs.PathBytes != "a1b2" {
		t.Errorf("expected pathBytes a1b2, got %v", obs.PathBytes)
	}
	// Legacy rows without a captured snapshot still omit resolved endpoints on lists.
	if obs.ResolvedPath != nil || obs.ResolvedSource != nil || obs.ResolvedDestination != nil {
		t.Error("expected no resolved path/source/destination on the list endpoint")
	}
}

func TestInsertObservation_Success(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		InsertObservation(gomock.Any(), gomock.Any()).
		Return(sqlc.InsertObservationRow{Inserted: true, ObservationCount: 4}, nil)

	store := &Store{q: mock}
	inserted, count, err := store.InsertObservation(context.Background(), ingest.InsertObservationParams{
		PacketHash: []byte{0xde, 0xad},
		ObserverID: observerID,
		IATA:       "YVR",
		HeardAt:    time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !inserted || count != 4 {
		t.Errorf("got inserted=%v count=%d, want true, 4", inserted, count)
	}
}

func TestInsertObservation_Conflict(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		InsertObservation(gomock.Any(), gomock.Any()).
		Return(sqlc.InsertObservationRow{Inserted: false, ObservationCount: 2}, nil)

	store := &Store{q: mock}
	inserted, count, err := store.InsertObservation(context.Background(), ingest.InsertObservationParams{
		PacketHash: []byte{0xde, 0xad},
		HeardAt:    time.Now(),
	})
	if err != nil {
		t.Fatalf("expected nil error on conflict, got %v", err)
	}
	if inserted || count != 2 {
		t.Errorf("got inserted=%v count=%d, want false, 2", inserted, count)
	}
}

func TestGetPacket_Basic(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	packetHash := []byte{0xde, 0xad, 0xbe, 0xef}
	heardAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	sourceBroker := "mqtt://test"

	mock.EXPECT().
		GetPacketByHash(gomock.Any(), packetHash).
		Return(sqlc.GetPacketByHashRow{
			PacketHash:    packetHash,
			RawHeader:     []byte{0x01},
			RawPayload:    []byte{0x02},
			ParsedPayload: []byte(`{}`),
			FirstHeardAt:  heardAt,
			LastHeardAt:   heardAt,
		}, nil)

	mock.EXPECT().
		ListObservationsForPacket(gomock.Any(), packetHash).
		Return([]sqlc.ListObservationsForPacketRow{
			{
				ID:           1,
				HeardAt:      heardAt,
				Iata:         "YVR",
				SourceBroker: &sourceBroker,
			},
		}, nil)

	store := &Store{q: mock}
	packet, err := store.GetPacket(context.Background(), packetHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if packet.PacketHash != "deadbeef" {
		t.Errorf("expected PacketHash deadbeef, got %s", packet.PacketHash)
	}
	if packet.ObservationCount != 1 {
		t.Errorf("expected ObservationCount 1, got %d", packet.ObservationCount)
	}
}

func TestGetPacket_TransportCodes(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	packetHash := []byte{0xde, 0xad, 0xbe, 0xef}
	heardAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	sourceBroker := "mqtt://test"
	hasTransport := true
	regionCode := int32(1)
	subRegionCode := int32(2)

	mock.EXPECT().
		GetPacketByHash(gomock.Any(), packetHash).
		Return(sqlc.GetPacketByHashRow{
			PacketHash:            packetHash,
			RawHeader:             []byte{0x01},
			RawPayload:            []byte{0x02},
			ParsedPayload:         []byte(`{}`),
			FirstHeardAt:          heardAt,
			LastHeardAt:           heardAt,
			TransportCodesPresent: &hasTransport,
			RegionCode:            &regionCode,
			SubRegionCode:         &subRegionCode,
		}, nil)

	mock.EXPECT().
		ListObservationsForPacket(gomock.Any(), packetHash).
		Return([]sqlc.ListObservationsForPacketRow{
			{ID: 1, HeardAt: heardAt, Iata: "YVR", SourceBroker: &sourceBroker},
		}, nil)

	store := &Store{q: mock}
	packet, err := store.GetPacket(context.Background(), packetHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if packet.TransportCodes == nil {
		t.Fatal("expected TransportCodes to be set")
	}
	if packet.TransportCodes.RegionCode != 1 {
		t.Errorf("expected RegionCode 1, got %d", packet.TransportCodes.RegionCode)
	}
	if packet.TransportCodes.SubRegionCode != 2 {
		t.Errorf("expected SubRegionCode 2, got %d", packet.TransportCodes.SubRegionCode)
	}
}

func TestGetPacket_FirstToLastMs(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	packetHash := []byte{0xde, 0xad, 0xbe, 0xef}
	sourceBroker := "mqtt://test"
	t1 := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	t2 := pgtype.Timestamptz{Time: time.UnixMilli(1700000001000), Valid: true}

	mock.EXPECT().
		GetPacketByHash(gomock.Any(), packetHash).
		Return(sqlc.GetPacketByHashRow{
			PacketHash:    packetHash,
			RawHeader:     []byte{0x01},
			RawPayload:    []byte{0x02},
			ParsedPayload: []byte(`{}`),
			FirstHeardAt:  t1,
			LastHeardAt:   t2,
		}, nil)

	mock.EXPECT().
		ListObservationsForPacket(gomock.Any(), packetHash).
		Return([]sqlc.ListObservationsForPacketRow{
			{ID: 1, HeardAt: t1, Iata: "YVR", SourceBroker: &sourceBroker},
			{ID: 2, HeardAt: t2, Iata: "YVR", SourceBroker: &sourceBroker},
		}, nil)

	store := &Store{q: mock}
	packet, err := store.GetPacket(context.Background(), packetHash)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if packet.FirstToLastMs != 1000 {
		t.Errorf("expected FirstToLastMs 1000, got %d", packet.FirstToLastMs)
	}
}

func TestListPacketsAfterID_PassesIATAsAsArray(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		ListPacketsAfterID(gomock.Any(), sqlc.ListPacketsAfterIDParams{
			ID:      0,
			Column2: int16(-1),
			Column3: int16(-1),
			Column4: []string{"ALF", "YYZ"},
			Column5: "",
			Limit:   50,
		}).
		Return([]sqlc.ListPacketsAfterIDRow{}, nil)

	store := &Store{q: mock}
	_, err := store.ListPacketsAfterID(context.Background(), 0, -1, -1, []string{"ALF", "YYZ"}, "", 50)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestListPacketsAfterID_LatestObserverPathFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	heardAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		ListPacketsAfterID(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListPacketsAfterIDRow{
			{
				PacketHash:                   []byte{0xde, 0xad},
				FirstHeardAt:                 heardAt,
				LastHeardAt:                  heardAt,
				LatestObserverID:             observerID,
				LatestObserverPathLengthByte: 0x42,
				LatestObserverHashSize:       1,
				LatestObserverHopCount:       2,
				LatestObserverPathBytes:      []byte{0xa1, 0xb2},
			},
		}, nil)

	store := &Store{q: mock}
	items, err := store.ListPacketsAfterID(context.Background(), 0, -1, -1, nil, "", 50)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	obs := items[0].LatestObserver
	if obs == nil || obs.PathLength == nil {
		t.Fatal("expected LatestObserver and PathLength to be set")
	}
	if obs.PathLength.HashSize != 1 || obs.PathLength.HopCount != 2 {
		t.Errorf("expected hashSize=1 hopCount=2, got hashSize=%d hopCount=%d", obs.PathLength.HashSize, obs.PathLength.HopCount)
	}
	if obs.PathBytes == nil || *obs.PathBytes != "a1b2" {
		t.Errorf("expected pathBytes a1b2, got %v", obs.PathBytes)
	}
}

func TestListNodeObservations_Pagination(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	heardAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}

	rows := make([]sqlc.ListNodeObservationsRow, 3)
	for i := range rows {
		rows[i] = sqlc.ListNodeObservationsRow{
			ID:      int64(i + 1),
			HeardAt: heardAt,
		}
	}

	mock.EXPECT().
		ListNodeObservations(gomock.Any(), gomock.Any()).
		Return(rows, nil)

	store := &Store{q: mock}
	page, err := store.ListNodeObservations(context.Background(), nodeID, 0, 2)
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

func TestListPackets_IATAFilterRoutesToObservationIndex(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	siteHeard := time.Date(2026, 7, 1, 12, 0, 0, 0, time.UTC)
	globalHeard := time.Date(2026, 7, 2, 8, 0, 0, 0, time.UTC)

	// limit=1 with 2 rows returned exercises the +1 trick and the trim.
	mock.EXPECT().
		ListPacketsByIATAs(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, p sqlc.ListPacketsByIATAsParams) ([]sqlc.ListPacketsByIATAsRow, error) {
			if len(p.Iatas) != 1 || p.Iatas[0] != "ALF" {
				t.Errorf("iatas param = %v, want [ALF]", p.Iatas)
			}
			if p.PageLimit != 2 { // limit+1
				t.Errorf("page limit = %d, want 2", p.PageLimit)
			}
			if p.ScanDepth != 16 { // (limit+1)*8
				t.Errorf("scan depth = %d, want 16", p.ScanDepth)
			}
			return []sqlc.ListPacketsByIATAsRow{
				{
					PacketHash:  []byte{0x01},
					LastHeardAt: pgtype.Timestamptz{Time: globalHeard, Valid: true},
					SiteHeardAt: pgtype.Timestamptz{Time: siteHeard, Valid: true},
				},
				{
					PacketHash:  []byte{0x02},
					LastHeardAt: pgtype.Timestamptz{Time: globalHeard, Valid: true},
					SiteHeardAt: pgtype.Timestamptz{Time: siteHeard.Add(-time.Hour), Valid: true},
				},
			}, nil
		})

	store := &Store{q: mock}
	page, err := store.ListPackets(context.Background(), nil, nil, []string{"ALF"}, nil, time.Time{}, time.Time{}, 0, nil, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items) != 1 || !page.HasMore {
		t.Fatalf("got %d items hasMore=%v, want 1 item hasMore=true", len(page.Items), page.HasMore)
	}
	// Cursor must follow site-local recency, not the packet's global last_heard_at.
	if page.NextCursor == nil || *page.NextCursor != siteHeard.UnixMilli() {
		t.Errorf("next cursor = %v, want %d (site heard_at)", page.NextCursor, siteHeard.UnixMilli())
	}
}

// A site whose packets are heard by more observers than scan_depth allows
// for collapses to a short page while history remains below the floor.
// The page must still report more, or the client stops paging for good.
func TestListPackets_SaturatedShortPageKeepsPaging(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	oldest := time.Date(2026, 8, 7, 12, 57, 25, 0, time.UTC)
	floor := time.Date(2026, 8, 7, 12, 50, 0, 0, time.UTC)

	// Two rows for a limit of 5: the scan filled up but collapsed to a short page.
	mock.EXPECT().
		ListPacketsByIATAs(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListPacketsByIATAsRow{
			{
				PacketHash:    []byte{0x01},
				SiteHeardAt:   pgtype.Timestamptz{Time: oldest.Add(time.Minute), Valid: true},
				ScanSaturated: true,
				ScanFloor:     pgtype.Timestamptz{Time: floor, Valid: true},
			},
			{
				PacketHash:    []byte{0x02},
				SiteHeardAt:   pgtype.Timestamptz{Time: oldest, Valid: true},
				ScanSaturated: true,
				ScanFloor:     pgtype.Timestamptz{Time: floor, Valid: true},
			},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListPackets(context.Background(), nil, nil, []string{"YOW"}, nil, time.Time{}, time.Time{}, 0, nil, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items) != 2 {
		t.Fatalf("got %d items, want 2", len(page.Items))
	}
	if !page.HasMore {
		t.Error("hasMore = false on a saturated short page, want true")
	}
	// Oldest returned item sits above the floor, so it is the safe cursor.
	if page.NextCursor == nil || *page.NextCursor != oldest.UnixMilli() {
		t.Errorf("next cursor = %v, want %d (oldest item)", page.NextCursor, oldest.UnixMilli())
	}
}

// The floor is newer than the oldest item, so paging past it would skip the
// band the saturated site never read.
func TestListPackets_CursorClampsToScanFloor(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	floor := time.Date(2026, 8, 7, 12, 50, 0, 0, time.UTC)
	oldest := floor.Add(-30 * time.Minute)

	mock.EXPECT().
		ListPacketsByIATAs(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListPacketsByIATAsRow{{
			PacketHash:    []byte{0x01},
			SiteHeardAt:   pgtype.Timestamptz{Time: oldest, Valid: true},
			ScanSaturated: true,
			ScanFloor:     pgtype.Timestamptz{Time: floor, Valid: true},
		}}, nil)

	store := &Store{q: mock}
	page, err := store.ListPackets(context.Background(), nil, nil, []string{"YOW", "YYZ"}, nil, time.Time{}, time.Time{}, 0, nil, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if page.NextCursor == nil || *page.NextCursor != floor.UnixMilli() {
		t.Errorf("next cursor = %v, want %d (clamped to floor)", page.NextCursor, floor.UnixMilli())
	}
}

// An unsaturated scan read the site dry, so paging has to stop.
func TestListPackets_UnsaturatedShortPageEndsPaging(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	oldest := time.Date(2026, 8, 7, 12, 57, 25, 0, time.UTC)

	mock.EXPECT().
		ListPacketsByIATAs(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListPacketsByIATAsRow{{
			PacketHash:    []byte{0x01},
			SiteHeardAt:   pgtype.Timestamptz{Time: oldest, Valid: true},
			ScanSaturated: false,
		}}, nil)

	store := &Store{q: mock}
	page, err := store.ListPackets(context.Background(), nil, nil, []string{"YOW"}, nil, time.Time{}, time.Time{}, 0, nil, 5)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if page.HasMore {
		t.Error("hasMore = true on an exhausted site, want false")
	}
	if page.NextCursor != nil {
		t.Errorf("next cursor = %v, want nil", page.NextCursor)
	}
}

func TestListPackets_UnfilteredKeepsGlobalQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	// gomock is strict: an unexpected ListPacketsByIATAs call fails the test.
	mock.EXPECT().
		ListPacketsPage(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListPacketsPageRow{}, nil)

	store := &Store{q: mock}
	if _, err := store.ListPackets(context.Background(), nil, nil, nil, nil, time.Time{}, time.Time{}, 0, nil, 50); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestDeleteOldPackets_LoopsUntilShortBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	store := &Store{q: mock}
	cutoff := time.Date(2026, 9, 18, 0, 0, 0, 0, time.UTC)

	byCutoff := gomock.Cond(func(p sqlc.DeleteOldPacketsParams) bool {
		return p.Cutoff.Time.Equal(cutoff) && p.BatchSize == packetDeleteBatch
	})
	gomock.InOrder(
		mock.EXPECT().DeleteOldPackets(gomock.Any(), byCutoff).Return(int64(packetDeleteBatch), nil),
		mock.EXPECT().DeleteOldPackets(gomock.Any(), byCutoff).Return(int64(packetDeleteBatch), nil),
		mock.EXPECT().DeleteOldPackets(gomock.Any(), byCutoff).Return(int64(7), nil),
	)

	if err := store.DeleteOldPackets(context.Background(), cutoff); err != nil {
		t.Fatal(err)
	}
}

// Each request shape runs exactly one statement (gomock fails on any other call), and a filter no row can match
// runs none and answers an empty page.
func TestListPacketsDispatch(t *testing.T) {
	since := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	allPayload := []int16{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}
	cases := []struct {
		name         string
		pt, rt       []int16
		iatas        []string
		scopes       []string
		since, until time.Time
		expect       func(m *mockdb.MockQuerier)
	}{
		{name: "unfiltered", expect: func(m *mockdb.MockQuerier) {
			m.EXPECT().ListPacketsPage(gomock.Any(), gomock.Any()).Return(nil, nil)
		}},
		{name: "payload types", pt: []int16{4}, expect: func(m *mockdb.MockQuerier) {
			m.EXPECT().ListPacketsByTypes(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsByTypesParams) bool {
				return slices.Equal(p.PayloadTypes, []int16{4}) && slices.Equal(p.RouteTypes, []int16{0, 1, 2, 3})
			})).Return(nil, nil)
		}},
		{name: "route types only", rt: []int16{3}, expect: func(m *mockdb.MockQuerier) {
			m.EXPECT().ListPacketsByTypes(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsByTypesParams) bool {
				return slices.Equal(p.PayloadTypes, allPayload) && slices.Equal(p.RouteTypes, []int16{3})
			})).Return(nil, nil)
		}},
		{name: "scopes", scopes: []string{"#a"}, expect: func(m *mockdb.MockQuerier) {
			m.EXPECT().ListPacketsByScopes(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsByScopesParams) bool {
				return slices.Equal(p.ScopeNames, []string{"#a"})
			})).Return(nil, nil)
		}},
		{name: "scopes and types", pt: []int16{4}, scopes: []string{"#a"}, expect: func(m *mockdb.MockQuerier) {
			m.EXPECT().ListPacketsByScopesTypes(gomock.Any(), gomock.Any()).Return(nil, nil)
		}},
		{name: "since", since: since, expect: func(m *mockdb.MockQuerier) {
			m.EXPECT().ListPacketsWindow(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsWindowParams) bool {
				return p.SinceTs.Time.Equal(since) && p.UntilTs.InfinityModifier == pgtype.Infinity
			})).Return(nil, nil)
		}},
		{name: "since and scopes", since: since, scopes: []string{"#a"}, expect: func(m *mockdb.MockQuerier) {
			m.EXPECT().ListPacketsWindowScoped(gomock.Any(), gomock.Any()).Return(nil, nil)
		}},
		{name: "iatas", pt: []int16{4}, iatas: []string{"YVR"}, scopes: []string{"#a"}, since: since, expect: func(m *mockdb.MockQuerier) {
			m.EXPECT().ListPacketsByIATAs(gomock.Any(), sqlc.ListPacketsByIATAsParams{
				Iatas:        []string{"YVR"},
				ScanDepth:    (7 + 1) * 8,
				PayloadTypes: []int16{4},
				SinceTs:      pgtype.Timestamptz{Time: since, Valid: true},
				ScopeNames:   []string{"#a"},
				PageLimit:    7 + 1,
			}).Return(nil, nil)
		}},
		{name: "unknown payload type", pt: []int16{99}, expect: func(*mockdb.MockQuerier) {}},
		{name: "since after until", since: since, until: since.Add(-time.Millisecond), expect: func(*mockdb.MockQuerier) {}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mock := mockdb.NewMockQuerier(ctrl)
			tc.expect(mock)
			store := &Store{q: mock}
			page, err := store.ListPackets(context.Background(), tc.pt, tc.rt, tc.iatas, tc.scopes, tc.since, tc.until, 0, nil, 7)
			if err != nil {
				t.Fatal(err)
			}
			if page.Items == nil {
				t.Error("Items is nil, want an empty slice")
			}
			if len(page.Items) != 0 || page.HasMore || page.NextCursor != nil || page.NextPageCursor != nil {
				t.Errorf("page = %+v, want empty", page)
			}
		})
	}
}

// pgx sends a nil []byte as NULL, and (k, h) < (ts, NULL) matches no row: every statement must get a non-nil hash.
func TestListPackets_CursorHashNeverNil(t *testing.T) {
	since := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	shapes := []struct {
		name   string
		pt     []int16
		scopes []string
		since  time.Time
	}{
		{name: "page"},
		{name: "types", pt: []int16{4}},
		{name: "scopes", scopes: []string{"#a"}},
		{name: "scopes types", pt: []int16{4}, scopes: []string{"#a"}},
		{name: "window", since: since},
		{name: "window scoped", since: since, scopes: []string{"#a"}},
	}
	cursors := []struct {
		name       string
		cursor     int64
		pageCursor *api.PacketCursor
	}{
		{name: "first page"},
		{name: "legacy cursor", cursor: 1792442625019},
		{name: "pageCursor", pageCursor: &api.PacketCursor{At: since, Hash: []byte{0xab}}},
	}
	for _, sh := range shapes {
		for _, cur := range cursors {
			t.Run(sh.name+"/"+cur.name, func(t *testing.T) {
				ctrl := gomock.NewController(t)
				mock := mockdb.NewMockQuerier(ctrl)
				var hash []byte
				mock.EXPECT().ListPacketsPage(gomock.Any(), gomock.Any()).AnyTimes().
					DoAndReturn(func(_ context.Context, p sqlc.ListPacketsPageParams) ([]sqlc.ListPacketsPageRow, error) {
						hash = p.CursorHash
						return nil, nil
					})
				mock.EXPECT().ListPacketsByTypes(gomock.Any(), gomock.Any()).AnyTimes().
					DoAndReturn(func(_ context.Context, p sqlc.ListPacketsByTypesParams) ([]sqlc.ListPacketsByTypesRow, error) {
						hash = p.CursorHash
						return nil, nil
					})
				mock.EXPECT().ListPacketsByScopes(gomock.Any(), gomock.Any()).AnyTimes().
					DoAndReturn(func(_ context.Context, p sqlc.ListPacketsByScopesParams) ([]sqlc.ListPacketsByScopesRow, error) {
						hash = p.CursorHash
						return nil, nil
					})
				mock.EXPECT().ListPacketsByScopesTypes(gomock.Any(), gomock.Any()).AnyTimes().
					DoAndReturn(func(_ context.Context, p sqlc.ListPacketsByScopesTypesParams) ([]sqlc.ListPacketsByScopesTypesRow, error) {
						hash = p.CursorHash
						return nil, nil
					})
				mock.EXPECT().ListPacketsWindow(gomock.Any(), gomock.Any()).AnyTimes().
					DoAndReturn(func(_ context.Context, p sqlc.ListPacketsWindowParams) ([]sqlc.ListPacketsWindowRow, error) {
						hash = p.CursorHash
						return nil, nil
					})
				mock.EXPECT().ListPacketsWindowScoped(gomock.Any(), gomock.Any()).AnyTimes().
					DoAndReturn(func(_ context.Context, p sqlc.ListPacketsWindowScopedParams) ([]sqlc.ListPacketsWindowScopedRow, error) {
						hash = p.CursorHash
						return nil, nil
					})
				store := &Store{q: mock}
				if _, err := store.ListPackets(context.Background(), sh.pt, nil, nil, sh.scopes, sh.since, time.Time{}, cur.cursor, cur.pageCursor, 50); err != nil {
					t.Fatal(err)
				}
				if hash == nil {
					t.Fatal("CursorHash is nil")
				}
				if cur.pageCursor != nil && !bytes.Equal(hash, cur.pageCursor.Hash) {
					t.Errorf("CursorHash = %x, want %x", hash, cur.pageCursor.Hash)
				}
			})
		}
	}
}

// A full page carries both cursors from its last row; the final page carries neither.
func TestListPackets_NextPageCursor(t *testing.T) {
	base := time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC)
	rows := []sqlc.ListPacketsPageRow{
		{PacketHash: []byte{0x03}, LastHeardAt: pgtype.Timestamptz{Time: base.Add(3*time.Millisecond + 7*time.Microsecond), Valid: true}},
		{PacketHash: []byte{0x02, 0xff}, LastHeardAt: pgtype.Timestamptz{Time: base.Add(2*time.Millisecond + 5*time.Microsecond), Valid: true}},
		{PacketHash: []byte{0x01}, LastHeardAt: pgtype.Timestamptz{Time: base.Add(time.Millisecond), Valid: true}},
	}
	run := func(t *testing.T, rows []sqlc.ListPacketsPageRow) api.PacketPage {
		t.Helper()
		ctrl := gomock.NewController(t)
		mock := mockdb.NewMockQuerier(ctrl)
		mock.EXPECT().ListPacketsPage(gomock.Any(), gomock.Any()).Return(rows, nil)
		page, err := (&Store{q: mock}).ListPackets(context.Background(), nil, nil, nil, nil, time.Time{}, time.Time{}, 0, nil, 2)
		if err != nil {
			t.Fatal(err)
		}
		return page
	}

	t.Run("full page", func(t *testing.T) {
		page := run(t, rows)
		last := rows[1]
		wantPC := api.PacketCursor{At: last.LastHeardAt.Time, Hash: last.PacketHash}.String()
		if len(page.Items) != 2 || !page.HasMore {
			t.Fatalf("items %d, hasMore %v; want 2, true", len(page.Items), page.HasMore)
		}
		if page.NextCursor == nil || *page.NextCursor != last.LastHeardAt.Time.UnixMilli() {
			t.Errorf("NextCursor = %v, want %d", page.NextCursor, last.LastHeardAt.Time.UnixMilli())
		}
		if page.NextPageCursor == nil || *page.NextPageCursor != wantPC {
			t.Errorf("NextPageCursor = %v, want %s", page.NextPageCursor, wantPC)
		}
	})
	t.Run("last page", func(t *testing.T) {
		page := run(t, rows[:2])
		if len(page.Items) != 2 || page.HasMore || page.NextCursor != nil || page.NextPageCursor != nil {
			t.Errorf("page = %+v, want 2 items and no cursors", page)
		}
	})
}

// The region path keeps today's numeric cursor, saturation padding and scan-floor clamp, and emits no pageCursor.
func TestListPackets_RegionPathIgnoresPageCursor(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	const cursor = int64(1792442625019)
	oldest := time.Date(2026, 8, 7, 12, 57, 25, 0, time.UTC)
	floor := oldest.Add(30 * time.Second)

	mock.EXPECT().
		ListPacketsByIATAs(gomock.Any(), gomock.Cond(func(p sqlc.ListPacketsByIATAsParams) bool {
			return p.CursorTs.Valid && p.CursorTs.Time.Equal(time.UnixMilli(cursor)) && slices.Equal(p.Iatas, []string{"YVR"})
		})).
		Return([]sqlc.ListPacketsByIATAsRow{{
			PacketHash:    []byte{0x01},
			SiteHeardAt:   pgtype.Timestamptz{Time: oldest, Valid: true},
			ScanSaturated: true,
			ScanFloor:     pgtype.Timestamptz{Time: floor, Valid: true},
		}}, nil)

	pc := &api.PacketCursor{At: time.Date(2026, 1, 10, 0, 0, 0, 0, time.UTC), Hash: []byte{0xab}}
	page, err := (&Store{q: mock}).ListPackets(context.Background(), nil, nil, []string{"YVR"}, nil, time.Time{}, time.Time{}, cursor, pc, 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || !page.HasMore {
		t.Fatalf("items %d, hasMore %v; want 1, true", len(page.Items), page.HasMore)
	}
	if page.NextCursor == nil || *page.NextCursor != floor.UnixMilli() {
		t.Errorf("NextCursor = %v, want the scan floor %d", page.NextCursor, floor.UnixMilli())
	}
	if page.NextPageCursor != nil {
		t.Errorf("NextPageCursor = %q, want nil", *page.NextPageCursor)
	}
}
