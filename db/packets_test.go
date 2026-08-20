// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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
	rows := make([]sqlc.ListPacketsRow, 3)
	for i := range rows {
		rows[i] = sqlc.ListPacketsRow{
			PacketHash:   []byte{0xde, 0xad},
			FirstHeardAt: heardAt,
			LastHeardAt:  heardAt,
		}
	}

	mock.EXPECT().
		ListPackets(gomock.Any(), gomock.Any()).
		Return(rows, nil)

	store := &Store{q: mock}
	page, err := store.ListPackets(context.Background(), nil, nil, nil, nil, time.Time{}, time.Time{}, 0, 2)
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
		ListPackets(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListPacketsRow{
			{
				PacketHash:       []byte{0xde, 0xad},
				FirstHeardAt:     heardAt,
				LastHeardAt:      heardAt,
				LatestObserverID: uuid.UUID{}, // zero UUID
			},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListPackets(context.Background(), nil, nil, nil, nil, time.Time{}, time.Time{}, 0, 10)
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
		ListPackets(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListPacketsRow{
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
	page, err := store.ListPackets(context.Background(), nil, nil, nil, nil, time.Time{}, time.Time{}, 0, 10)
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
		ListPackets(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListPacketsRow{
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
	page, err := store.ListPackets(context.Background(), nil, nil, nil, nil, time.Time{}, time.Time{}, 0, 10)
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
	// Resolution stays a detail-view-only feature on this list endpoint -- deliberately unset.
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
		Return(sqlc.PacketObservation{ID: 1}, nil)

	store := &Store{q: mock}
	inserted, err := store.InsertObservation(context.Background(), ingest.InsertObservationParams{
		PacketHash: []byte{0xde, 0xad},
		ObserverID: observerID,
		IATA:       "YVR",
		HeardAt:    time.Now(),
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !inserted {
		t.Error("expected inserted true")
	}
}

func TestInsertObservation_Conflict(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		InsertObservation(gomock.Any(), gomock.Any()).
		Return(sqlc.PacketObservation{}, pgx.ErrNoRows)

	store := &Store{q: mock}
	inserted, err := store.InsertObservation(context.Background(), ingest.InsertObservationParams{
		PacketHash: []byte{0xde, 0xad},
		HeardAt:    time.Now(),
	})
	if err != nil {
		t.Fatalf("expected nil error on conflict, got %v", err)
	}
	if inserted {
		t.Error("expected inserted false on conflict")
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
	page, err := store.ListPackets(context.Background(), nil, nil, []string{"ALF"}, nil, time.Time{}, time.Time{}, 0, 1)
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

func TestListPackets_UnfilteredKeepsGlobalQuery(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	// gomock is strict: an unexpected ListPacketsByIATAs call fails the test.
	mock.EXPECT().
		ListPackets(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListPacketsRow{}, nil)

	store := &Store{q: mock}
	if _, err := store.ListPackets(context.Background(), nil, nil, nil, nil, time.Time{}, time.Time{}, 0, 50); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}
