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
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"go.uber.org/mock/gomock"
)

func TestUpsertObserver_NilDisplayName(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	pubkey := []byte{0x01, 0x02}

	mock.EXPECT().
		UpsertObserver(gomock.Any(), pubkey).
		Return(sqlc.Observer{ID: observerID, DisplayName: nil}, nil)

	store := &Store{q: mock}
	id, displayName, err := store.UpsertObserver(context.Background(), pubkey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if id != observerID {
		t.Errorf("expected ID %s, got %s", observerID, id)
	}
	if displayName != "" {
		t.Errorf("expected empty displayName, got %s", displayName)
	}
}

func TestUpsertObserver_WithDisplayName(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	pubkey := []byte{0x01, 0x02}
	name := "test-observer"

	mock.EXPECT().
		UpsertObserver(gomock.Any(), pubkey).
		Return(sqlc.Observer{ID: observerID, DisplayName: &name}, nil)

	store := &Store{q: mock}
	_, displayName, err := store.UpsertObserver(context.Background(), pubkey)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if displayName != "test-observer" {
		t.Errorf("expected displayName test-observer, got %s", displayName)
	}
}

func TestListObservers_Pagination(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	lastStatusAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	rows := make([]sqlc.ListObserversRow, 3)
	for i := range rows {
		rows[i] = sqlc.ListObserversRow{
			ID:           observerID,
			LastStatusAt: lastStatusAt,
		}
	}

	mock.EXPECT().
		ListObservers(gomock.Any(), gomock.Any()).
		Return(rows, nil)

	store := &Store{q: mock}
	page, err := store.ListObservers(context.Background(), []string{"YVR"}, "", "", "", "", "", 0, 2)
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

func TestListObservers_RadioStringFormatting(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	freq := float32(915.0)
	sf := int16(7)
	bw := float32(125.0)

	mock.EXPECT().
		ListObservers(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListObserversRow{
			{
				ID:           observerID,
				RadioFreqMhz: &freq,
				RadioSf:      &sf,
				RadioBwKhz:   &bw,
			},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListObservers(context.Background(), nil, "", "", "", "", "", 0, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(page.Items) != 1 {
		t.Fatalf("expected 1 item, got %d", len(page.Items))
	}
	if page.Items[0].Radio == nil {
		t.Fatal("expected Radio to be set")
	}
	if *page.Items[0].Radio != "915,125,7" {
		t.Errorf("expected Radio 915,125,7, got %s", *page.Items[0].Radio)
	}
}

func TestListObservers_NilRadioFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		ListObservers(gomock.Any(), gomock.Any()).
		Return([]sqlc.ListObserversRow{
			{ID: observerID, RadioFreqMhz: nil, RadioSf: nil, RadioBwKhz: nil},
		}, nil)

	store := &Store{q: mock}
	page, err := store.ListObservers(context.Background(), nil, "", "", "", "", "", 0, 10)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if page.Items[0].Radio != nil {
		t.Errorf("expected nil Radio, got %s", *page.Items[0].Radio)
	}
}

func TestListObservers_DBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	mock.EXPECT().
		ListObservers(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("db error"))

	store := &Store{q: mock}
	_, err := store.ListObservers(context.Background(), nil, "", "", "", "", "", 0, 10)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetObserver_OnlineStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	obsCount := int64(10)

	mock.EXPECT().
		GetObserverByID(gomock.Any(), observerID).
		Return(sqlc.Observer{
			ID:               observerID,
			PublicKey:        []byte{0x01},
			ObservationCount: &obsCount,
			FirstSeen:        pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
			LastSeen:         pgtype.Timestamptz{Time: time.Now().Add(-time.Minute), Valid: true},
			LastStatusAt:     pgtype.Timestamptz{Time: time.Now().Add(-time.Minute), Valid: true},
		}, nil)

	mock.EXPECT().
		GetObserverBrokers(gomock.Any(), observerID).
		Return([]sqlc.GetObserverBrokersRow{}, nil)

	mock.EXPECT().
		GetObserverScopes(gomock.Any(), observerID).
		Return([]string{"default"}, nil)

	mock.EXPECT().
		GetObserverLastIATA(gomock.Any(), observerID).
		Return("YVR", nil)

	store := &Store{q: mock}
	observer, err := store.GetObserver(context.Background(), observerID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if observer.Status != "online" {
		t.Errorf("expected status online, got %s", observer.Status)
	}
	if observer.IATA != "YVR" {
		t.Errorf("expected IATA YVR, got %s", observer.IATA)
	}
}

func TestGetObserver_OfflineStatus(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	obsCount := int64(10)

	mock.EXPECT().
		GetObserverByID(gomock.Any(), observerID).
		Return(sqlc.Observer{
			ID:               observerID,
			PublicKey:        []byte{0x01},
			ObservationCount: &obsCount,
			FirstSeen:        pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
			LastSeen:         pgtype.Timestamptz{Time: time.Now().Add(-10 * time.Minute), Valid: true},
			LastStatusAt:     pgtype.Timestamptz{Time: time.Now().Add(-10 * time.Minute), Valid: true},
		}, nil)

	mock.EXPECT().
		GetObserverBrokers(gomock.Any(), observerID).
		Return([]sqlc.GetObserverBrokersRow{}, nil)

	mock.EXPECT().
		GetObserverScopes(gomock.Any(), observerID).
		Return([]string{}, nil)

	mock.EXPECT().
		GetObserverLastIATA(gomock.Any(), observerID).
		Return("YVR", nil)

	store := &Store{q: mock}
	observer, err := store.GetObserver(context.Background(), observerID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if observer.Status != "offline" {
		t.Errorf("expected status offline, got %s", observer.Status)
	}
}

func TestGetObserver_BrokerLastPacketAtNil(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	obsCount := int64(10)

	mock.EXPECT().
		GetObserverByID(gomock.Any(), observerID).
		Return(sqlc.Observer{
			ID:               observerID,
			PublicKey:        []byte{0x01},
			ObservationCount: &obsCount,
			FirstSeen:        pgtype.Timestamptz{Time: time.Now().Add(-time.Hour), Valid: true},
			LastSeen:         pgtype.Timestamptz{Time: time.Now(), Valid: true},
		}, nil)

	mock.EXPECT().
		GetObserverBrokers(gomock.Any(), observerID).
		Return([]sqlc.GetObserverBrokersRow{
			{
				BrokerName:   "mqtt://test",
				LastPacketAt: pgtype.Timestamptz{Valid: false},
				LastSeen:     pgtype.Timestamptz{Time: time.Now(), Valid: true},
			},
		}, nil)

	mock.EXPECT().
		GetObserverScopes(gomock.Any(), observerID).
		Return([]string{}, nil)

	mock.EXPECT().
		GetObserverLastIATA(gomock.Any(), observerID).
		Return("YVR", nil)

	store := &Store{q: mock}
	observer, err := store.GetObserver(context.Background(), observerID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(observer.Brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(observer.Brokers))
	}
	if observer.Brokers[0].LastPacketAt != 0 {
		t.Errorf("expected LastPacketAt 0 for nil, got %d", observer.Brokers[0].LastPacketAt)
	}
}

func TestGetObserver_DBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		GetObserverByID(gomock.Any(), observerID).
		Return(sqlc.Observer{}, errors.New("db error"))

	store := &Store{q: mock}
	_, err := store.GetObserver(context.Background(), observerID)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetObserverTelemetry_Mapping(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	reportedAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}
	batteryMV := int32(3700)
	noiseFloor := float32(-90.0)
	uptime := int64(3600)

	mock.EXPECT().
		GetObserverTelemetry(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverTelemetryRow{
			{
				ReportedAt:       reportedAt,
				BatteryVoltageMv: &batteryMV,
				NoiseFloorDb:     &noiseFloor,
				UptimeSeconds:    &uptime,
			},
		}, nil)

	store := &Store{q: mock}
	result, err := store.GetObserverTelemetry(context.Background(), observerID, time.Time{}, time.Time{}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(result.Points))
	}
	if result.Points[0].T != 1700000000000 {
		t.Errorf("expected T 1700000000000, got %d", result.Points[0].T)
	}
	if *result.Points[0].BatteryMV != 3700 {
		t.Errorf("expected BatteryMV 3700, got %d", *result.Points[0].BatteryMV)
	}
}

func TestGetObserverTelemetry_Empty(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		GetObserverTelemetry(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverTelemetryRow{}, nil)

	store := &Store{q: mock}
	result, err := store.GetObserverTelemetry(context.Background(), observerID, time.Time{}, time.Time{}, 0)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result.Points) != 0 {
		t.Errorf("expected 0 points, got %d", len(result.Points))
	}
}

func TestGetObserverTelemetryBucketed_Mapping(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	bucket := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}

	mock.EXPECT().
		GetObserverTelemetryBucketed(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverTelemetryBucketedRow{
			{
				Bucket:           bucket,
				BatteryVoltageMv: 3700,
				NoiseFloorDb:     -90.0,
				UptimeSeconds:    3600,
			},
		}, nil)

	store := &Store{q: mock}
	points, err := store.GetObserverTelemetryBucketed(context.Background(), observerID, time.Time{}, time.Time{}, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(points))
	}
	if points[0].T != 1700000000000 {
		t.Errorf("expected T 1700000000000, got %d", points[0].T)
	}
	if *points[0].BatteryMV != 3700 {
		t.Errorf("expected BatteryMV 3700, got %d", *points[0].BatteryMV)
	}
}

func TestListObserverAdverts_Pagination(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	heardAt := pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true}

	rows := make([]sqlc.ListObserverAdvertsRow, 3)
	for i := range rows {
		rows[i] = sqlc.ListObserverAdvertsRow{
			ID:      int64(i + 1),
			HeardAt: heardAt,
		}
	}

	mock.EXPECT().
		ListObserverAdverts(gomock.Any(), gomock.Any()).
		Return(rows, nil)

	store := &Store{q: mock}
	page, err := store.ListObserverAdverts(context.Background(), observerID, 0, 2)
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

func TestListObserverAdverts_DBError(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		ListObserverAdverts(gomock.Any(), gomock.Any()).
		Return(nil, errors.New("db error"))

	store := &Store{q: mock}
	_, err := store.ListObserverAdverts(context.Background(), observerID, 0, 10)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestGetObserverRadio_NilFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().
		GetObserverRadio(gomock.Any(), observerID).
		Return(sqlc.GetObserverRadioRow{}, nil)

	store := &Store{q: mock}
	settings, err := store.GetObserverRadio(context.Background(), observerID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if settings.FreqMHz != 0 {
		t.Errorf("expected FreqMHz 0, got %f", settings.FreqMHz)
	}
	if settings.SF != 0 {
		t.Errorf("expected SF 0, got %d", settings.SF)
	}
}

func TestGetObserverRadio_WithFields(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	freq := float32(915.0)
	sf := int16(7)
	bw := float32(125.0)
	cr := int16(5)

	mock.EXPECT().
		GetObserverRadio(gomock.Any(), observerID).
		Return(sqlc.GetObserverRadioRow{
			RadioFreqMhz: &freq,
			RadioSf:      &sf,
			RadioBwKhz:   &bw,
			RadioCr:      &cr,
		}, nil)

	store := &Store{q: mock}
	settings, err := store.GetObserverRadio(context.Background(), observerID)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if settings.FreqMHz != 915.0 {
		t.Errorf("expected FreqMHz 915.0, got %f", settings.FreqMHz)
	}
	if settings.SF != 7 {
		t.Errorf("expected SF 7, got %d", settings.SF)
	}
	if settings.BWKHz != 125.0 {
		t.Errorf("expected BWKHz 125.0, got %f", settings.BWKHz)
	}
}

func TestIsObserverByPubkey_Found(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	pubkey := []byte{0x01, 0x02}

	mock.EXPECT().
		GetObserverByPubkey(gomock.Any(), pubkey).
		Return(sqlc.Observer{}, nil)

	store := &Store{q: mock}
	if !store.IsObserverByPubkey(context.Background(), pubkey) {
		t.Error("expected true for found observer")
	}
}

func TestIsObserverByPubkey_NotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)

	pubkey := []byte{0x01, 0x02}

	mock.EXPECT().
		GetObserverByPubkey(gomock.Any(), pubkey).
		Return(sqlc.Observer{}, errors.New("not found"))

	store := &Store{q: mock}
	if store.IsObserverByPubkey(context.Background(), pubkey) {
		t.Error("expected false for missing observer")
	}
}

func activityObserver(sf, cr *int16, bw, freq *float32) sqlc.Observer {
	return sqlc.Observer{
		ID:           uuid.MustParse("00000000-0000-0000-0000-000000000001"),
		RadioFreqMhz: freq,
		RadioSf:      sf,
		RadioBwKhz:   bw,
		RadioCr:      cr,
	}
}

func i16(v int16) *int16     { return &v }
func f32(v float32) *float32 { return &v }

func TestGetObserverActivity_HourlyFoldNoSignal(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().GetObserverByID(gomock.Any(), observerID).
		Return(activityObserver(i16(10), i16(5), f32(62.5), f32(910.525)), nil)
	mock.EXPECT().GetObserverActivityHourly(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityHourlyRow{{
			Bucket:       pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true},
			Observations: 12,
			AirtimeMs:    0,
			AirtimeN:     0,
			SnrSum:       0,
			SnrN:         0,
			SnrMin:       0,
			RssiSum:      0,
			RssiN:        0,
		}}, nil)
	mock.EXPECT().GetObserverActivityHourlyPayloadTypes(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityHourlyPayloadTypesRow{
			{PayloadType: i16(4), Count: 9},
			{PayloadType: nil, Count: 3},
		}, nil)

	store := &Store{q: mock}
	got, err := store.GetObserverActivity(context.Background(), observerID, 24*time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got.Points) != 1 {
		t.Fatalf("expected 1 point, got %d", len(got.Points))
	}
	p := got.Points[0]
	if p.T != 1700000000000 {
		t.Errorf("expected T 1700000000000, got %d", p.T)
	}
	if p.Observations != 12 {
		t.Errorf("expected 12 observations, got %d", p.Observations)
	}
	if p.AirtimeMs != nil {
		t.Errorf("expected nil AirtimeMs, got %v", *p.AirtimeMs)
	}
	if p.SNRAvg != nil || p.SNRMin != nil {
		t.Errorf("expected nil SNR fields, got %v %v", p.SNRAvg, p.SNRMin)
	}
	if p.RSSIAvg != nil {
		t.Errorf("expected nil RSSIAvg, got %v", *p.RSSIAvg)
	}
	if len(got.PayloadTypes) != 1 || got.PayloadTypes[0].PayloadType != 4 || got.PayloadTypes[0].Count != 9 {
		t.Fatalf("expected one payload type 4 with count 9, got %+v", got.PayloadTypes)
	}
	if got.PayloadTypes[0].PayloadTypeName != api.PayloadTypeName(4) {
		t.Errorf("expected payload type name %q, got %q", api.PayloadTypeName(4), got.PayloadTypes[0].PayloadTypeName)
	}
	if got.Radio == nil {
		t.Fatal("expected radio")
	}
	if got.Radio.SF != 10 || got.Radio.BWKHz != 62.5 || got.Radio.CR != 5 || got.Radio.PreambleSymbols != 16 {
		t.Errorf("unexpected radio %+v", *got.Radio)
	}
	if got.Radio.FreqMHz == nil || *got.Radio.FreqMHz != 910.525 {
		t.Errorf("unexpected freq %v", got.Radio.FreqMHz)
	}
	if got.Range != "" || got.Interval != "" {
		t.Errorf("expected range/interval left for the handler, got %q %q", got.Range, got.Interval)
	}
}

func TestGetObserverActivity_HourlyFoldWeightedAverages(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().GetObserverByID(gomock.Any(), observerID).
		Return(activityObserver(i16(7), i16(5), f32(250), nil), nil)
	mock.EXPECT().GetObserverActivityHourly(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityHourlyRow{{
			Bucket:       pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true},
			Observations: 8,
			AirtimeMs:    123.5,
			AirtimeN:     3,
			SnrSum:       30,
			SnrN:         4,
			SnrMin:       -3.5,
			RssiSum:      -400,
			RssiN:        4,
		}}, nil)
	mock.EXPECT().GetObserverActivityHourlyPayloadTypes(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityHourlyPayloadTypesRow{}, nil)

	store := &Store{q: mock}
	got, err := store.GetObserverActivity(context.Background(), observerID, 7*24*time.Hour, 6*time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	p := got.Points[0]
	if p.AirtimeMs == nil || *p.AirtimeMs != 123.5 {
		t.Errorf("expected AirtimeMs 123.5, got %v", p.AirtimeMs)
	}
	if p.SNRAvg == nil || *p.SNRAvg != 7.5 {
		t.Errorf("expected SNRAvg 7.5, got %v", p.SNRAvg)
	}
	if p.SNRMin == nil || *p.SNRMin != -3.5 {
		t.Errorf("expected SNRMin -3.5, got %v", p.SNRMin)
	}
	if p.RSSIAvg == nil || *p.RSSIAvg != -100 {
		t.Errorf("expected RSSIAvg -100, got %v", p.RSSIAvg)
	}
	if got.PayloadTypes == nil {
		t.Error("expected non-nil PayloadTypes slice")
	}
	if got.Radio.FreqMHz != nil {
		t.Errorf("expected nil freq, got %v", *got.Radio.FreqMHz)
	}
	if got.Radio.PreambleSymbols != 32 {
		t.Errorf("expected 32 preamble symbols at SF7, got %d", got.Radio.PreambleSymbols)
	}
}

func TestGetObserverActivity_RawPath(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().GetObserverByID(gomock.Any(), observerID).
		Return(activityObserver(i16(10), i16(5), f32(62.5), nil), nil)
	mock.EXPECT().GetObserverActivityRaw(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityRawRow{{
			Bucket:       pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true},
			Observations: 5,
			AirtimeMs:    50,
			AirtimeN:     5,
			SnrAvg:       4.25,
			SnrMin:       1.5,
			SnrN:         2,
			RssiAvg:      -95.5,
			RssiN:        0,
		}}, nil)
	mock.EXPECT().GetObserverActivityRawPayloadTypes(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityRawPayloadTypesRow{{PayloadType: i16(1), Count: 5}}, nil)

	store := &Store{q: mock}
	got, err := store.GetObserverActivity(context.Background(), observerID, 6*time.Hour, 15*time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	p := got.Points[0]
	if p.Observations != 5 {
		t.Errorf("expected 5 observations, got %d", p.Observations)
	}
	if p.AirtimeMs == nil || *p.AirtimeMs != 50 {
		t.Errorf("expected AirtimeMs 50, got %v", p.AirtimeMs)
	}
	if p.SNRAvg == nil || *p.SNRAvg != 4.25 {
		t.Errorf("expected SNRAvg 4.25, got %v", p.SNRAvg)
	}
	if p.SNRMin == nil || *p.SNRMin != 1.5 {
		t.Errorf("expected SNRMin 1.5, got %v", p.SNRMin)
	}
	if p.RSSIAvg != nil {
		t.Errorf("expected nil RSSIAvg with rssi_n 0, got %v", *p.RSSIAvg)
	}
	if len(got.PayloadTypes) != 1 || got.PayloadTypes[0].Count != 5 {
		t.Errorf("unexpected payload types %+v", got.PayloadTypes)
	}
}

func TestGetObserverActivity_RawFoldNoSignal(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().GetObserverByID(gomock.Any(), observerID).
		Return(activityObserver(i16(10), i16(5), f32(62.5), nil), nil)
	mock.EXPECT().GetObserverActivityRaw(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityRawRow{{
			Bucket:       pgtype.Timestamptz{Time: time.UnixMilli(1700000000000), Valid: true},
			Observations: 7,
			AirtimeMs:    0,
			AirtimeN:     0,
			SnrAvg:       0,
			SnrMin:       0,
			SnrN:         0,
			RssiAvg:      0,
			RssiN:        0,
		}}, nil)
	mock.EXPECT().GetObserverActivityRawPayloadTypes(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityRawPayloadTypesRow{}, nil)

	store := &Store{q: mock}
	got, err := store.GetObserverActivity(context.Background(), observerID, 6*time.Hour, 15*time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	p := got.Points[0]
	if p.Observations != 7 {
		t.Errorf("expected 7 observations, got %d", p.Observations)
	}
	if p.AirtimeMs != nil {
		t.Errorf("expected nil AirtimeMs with airtime_n 0, got %v", *p.AirtimeMs)
	}
	if p.SNRAvg != nil || p.SNRMin != nil {
		t.Errorf("expected nil SNR fields with snr_n 0, got %v %v", p.SNRAvg, p.SNRMin)
	}
	if p.RSSIAvg != nil {
		t.Errorf("expected nil RSSIAvg with rssi_n 0, got %v", *p.RSSIAvg)
	}
}

func TestGetObserverActivity_RawPathEmpty(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().GetObserverByID(gomock.Any(), observerID).
		Return(activityObserver(i16(10), i16(5), f32(62.5), nil), nil)
	mock.EXPECT().GetObserverActivityRaw(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityRawRow{}, nil)
	mock.EXPECT().GetObserverActivityRawPayloadTypes(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityRawPayloadTypesRow{}, nil)

	store := &Store{q: mock}
	got, err := store.GetObserverActivity(context.Background(), observerID, 6*time.Hour, 15*time.Minute)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Points == nil || len(got.Points) != 0 {
		t.Errorf("expected empty non-nil points, got %#v", got.Points)
	}
	if got.PayloadTypes == nil || len(got.PayloadTypes) != 0 {
		t.Errorf("expected empty non-nil payload types, got %#v", got.PayloadTypes)
	}
}

func TestGetObserverActivity_HourlyPathAtOneHour(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mock.EXPECT().GetObserverByID(gomock.Any(), observerID).
		Return(activityObserver(i16(10), i16(5), f32(62.5), nil), nil)
	mock.EXPECT().GetObserverActivityHourly(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityHourlyRow{}, nil)
	mock.EXPECT().GetObserverActivityHourlyPayloadTypes(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityHourlyPayloadTypesRow{}, nil)

	store := &Store{q: mock}
	got, err := store.GetObserverActivity(context.Background(), observerID, 24*time.Hour, time.Hour)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.Points == nil || len(got.Points) != 0 {
		t.Errorf("expected empty non-nil points, got %#v", got.Points)
	}
}

func TestGetObserverActivity_UnknownObserver(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	mock.EXPECT().GetObserverByID(gomock.Any(), observerID).
		Return(sqlc.Observer{}, pgx.ErrNoRows)

	store := &Store{q: mock}
	got, err := store.GetObserverActivity(context.Background(), observerID, time.Hour, time.Hour)
	if !errors.Is(err, pgx.ErrNoRows) {
		t.Fatalf("expected pgx.ErrNoRows, got %v", err)
	}
	if got != nil {
		t.Errorf("expected nil result, got %+v", got)
	}
}

func TestGetObserverActivity_RadioNilWhenIncomplete(t *testing.T) {
	cases := []struct {
		name   string
		sf, cr *int16
		bw     *float32
	}{
		{name: "sf nil", sf: nil, cr: i16(5), bw: f32(62.5)},
		{name: "sf zero", sf: i16(0), cr: i16(5), bw: f32(62.5)},
		{name: "sf below lora range", sf: i16(6), cr: i16(5), bw: f32(62.5)},
		{name: "sf above lora range", sf: i16(13), cr: i16(5), bw: f32(62.5)},
		{name: "bw zero", sf: i16(10), cr: i16(5), bw: f32(0)},
		{name: "cr nil", sf: i16(10), cr: nil, bw: f32(62.5)},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mock := mockdb.NewMockQuerier(ctrl)
			observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

			mock.EXPECT().GetObserverByID(gomock.Any(), observerID).
				Return(activityObserver(tc.sf, tc.cr, tc.bw, nil), nil)
			mock.EXPECT().GetObserverActivityHourly(gomock.Any(), gomock.Any()).
				Return([]sqlc.GetObserverActivityHourlyRow{}, nil)
			mock.EXPECT().GetObserverActivityHourlyPayloadTypes(gomock.Any(), gomock.Any()).
				Return([]sqlc.GetObserverActivityHourlyPayloadTypesRow{}, nil)

			store := &Store{q: mock}
			got, err := store.GetObserverActivity(context.Background(), observerID, 24*time.Hour, time.Hour)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.Radio != nil {
				t.Errorf("expected nil radio, got %+v", *got.Radio)
			}
		})
	}
}

func TestGetObserverActivity_SinceAlignedToInterval(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	window, interval := 24*time.Hour, 6*time.Hour

	var gotSince time.Time
	var gotInterval pgtype.Interval
	mock.EXPECT().GetObserverByID(gomock.Any(), observerID).
		Return(activityObserver(i16(10), i16(5), f32(62.5), nil), nil)
	mock.EXPECT().GetObserverActivityHourly(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, arg sqlc.GetObserverActivityHourlyParams) ([]sqlc.GetObserverActivityHourlyRow, error) {
			gotSince, gotInterval = arg.Column2.Time, arg.Column3
			return nil, nil
		})
	mock.EXPECT().GetObserverActivityHourlyPayloadTypes(gomock.Any(), gomock.Any()).
		Return([]sqlc.GetObserverActivityHourlyPayloadTypesRow{}, nil)

	before := time.Now().Add(-window)
	store := &Store{q: mock}
	if _, err := store.GetObserverActivity(context.Background(), observerID, window, interval); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	after := time.Now().Add(-window)

	if gotSince.UnixNano()%int64(interval) != 0 {
		t.Errorf("since %s is not aligned to %s", gotSince, interval)
	}
	if gotSince.Before(before) {
		t.Errorf("since %s is before the window start %s", gotSince, before)
	}
	if !gotSince.Before(after.Add(interval)) {
		t.Errorf("since %s is more than one interval past the window start %s", gotSince, after)
	}
	if !gotInterval.Valid || gotInterval.Microseconds != interval.Microseconds() {
		t.Errorf("expected interval %d us, got %+v", interval.Microseconds(), gotInterval)
	}
}
