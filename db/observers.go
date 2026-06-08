// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: agpl

package db

import (
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"strings"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) UpsertObserver(ctx context.Context, pubkey []byte) (uuid.UUID, string, error) {
	row, err := s.q.UpsertObserver(ctx, pubkey)
	if err != nil {
		return uuid.Nil, "", err
	}
	displayName := ""
	if row.DisplayName != nil {
		displayName = *row.DisplayName
	}
	return row.ID, displayName, err
}

func (s *Store) ListObservers(ctx context.Context, iatas []string, observerType, broker, status, name, scope string, cursor int64, limit int32) (api.Page[api.ObserverSummary], error) {
	var cursorTS pgtype.Timestamptz
	if cursor > 0 {
		cursorTS = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	iataFilter := strings.Join(iatas, ",")
	params := sqlc.ListObserversParams{
		Column1: iataFilter,
		Column2: observerType,
		Column3: broker,
		Column4: status,
		Column5: name,
		Column6: cursorTS,
		Limit:   limit + 1,
		Column8: scope,
	}
	rows, err := s.q.ListObservers(ctx, params)
	if err != nil {
		return api.Page[api.ObserverSummary]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.ObserverSummary, 0, len(rows))
	for _, v := range rows {
		observer := api.ObserverSummary{
			ID:     v.ID,
			IATA:   v.Iata,
			Status: v.Status,
			Scopes: v.Scopes,
		}
		if v.RadioFreqMhz != nil && v.RadioSf != nil && v.RadioBwKhz != nil {
			s := fmt.Sprintf("%.1f,%g,%d", *v.RadioFreqMhz, *v.RadioBwKhz, *v.RadioSf)
			observer.Radio = &s
		}
		if v.DisplayName != nil {
			observer.DisplayName = v.DisplayName
		}
		if v.ObserverType != nil {
			observer.ObserverType = v.ObserverType
		}
		items = append(items, observer)
	}
	var nextCursor *int64
	if hasMore {
		// observers use UUID so encode last_seen as cursor
		if rows[len(rows)-1].LastStatusAt.Valid {
			ms := rows[len(rows)-1].LastStatusAt.Time.UnixMilli()
			nextCursor = &ms
		}
	}
	return api.Page[api.ObserverSummary]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (s *Store) GetObserver(ctx context.Context, observerID uuid.UUID) (*api.Observer, error) {
	obs, err := s.q.GetObserverByID(ctx, observerID)
	if err != nil {
		return nil, err
	}
	brokerRows, err := s.q.GetObserverBrokers(ctx, observerID)
	if err != nil {
		return nil, err
	}
	observer := api.Observer{
		ObserverSummary: api.ObserverSummary{
			ID:           obs.ID,
			DisplayName:  obs.DisplayName,
			ObserverType: obs.ObserverType,
			Status:       "offline",
		},
		PublicKey:        hex.EncodeToString(obs.PublicKey),
		SoftwareVersion:  obs.SoftwareVersion,
		HardwareModel:    obs.HardwareModel,
		FirmwareVersion:  obs.FirmwareVersion,
		FirmwareBuild:    obs.FirmwareBuild,
		RadioFreqMHz:     obs.RadioFreqMhz,
		RadioSF:          obs.RadioSf,
		RadioBWKHz:       obs.RadioBwKhz,
		RadioCR:          obs.RadioCr,
		BatteryLevel:     obs.BatteryLevel,
		UptimeSeconds:    obs.UptimeSeconds,
		StatusMetadata:   obs.StatusMetadata,
		FirstSeen:        obs.FirstSeen.Time.UnixMilli(),
		LastSeen:         obs.LastSeen.Time.UnixMilli(),
		ObservationCount: *obs.ObservationCount,
	}
	scopes, err := s.GetObserverScopes(ctx, observerID)
	if err != nil {
		log.Printf("store: GetObserverScopes failed for %s: %v", observerID, err)
		scopes = []string{}
	}
	observer.Scopes = scopes
	brokers := make([]api.ObserverBroker, 0, len(brokerRows))
	for _, v := range brokerRows {
		var lastPacketAt int64
		if v.LastPacketAt.Valid {
			lastPacketAt = v.LastPacketAt.Time.UnixMilli()
		}
		brokers = append(brokers, api.ObserverBroker{
			Name:         v.BrokerName,
			LastPacketAt: lastPacketAt,
			LastSeenAt:   v.LastSeen.Time.UnixMilli(),
		})
	}
	observer.Brokers = brokers
	if obs.LastStatusAt.Valid && time.Since(obs.LastStatusAt.Time) < 5*time.Minute {
		observer.Status = "online"
	}
	var lastStatusAt *int64
	if obs.LastStatusAt.Valid {
		ms := obs.LastStatusAt.Time.UnixMilli()
		lastStatusAt = &ms
	}
	observer.LastStatusAt = lastStatusAt
	observer.IATA, _ = s.GetObserverLastIATA(ctx, observerID)
	return &observer, nil
}

func (s *Store) InsertObserverTelemetry(ctx context.Context, observerID uuid.UUID, reportedAt time.Time, batteryMV *int32, txAirSecs, rxAirSecs *float32, noiseFloor float32, uptimeSeconds int64, queueLen, debugFlags, recvErrors *int32) error {
	return s.q.InsertObserverTelemetry(ctx, sqlc.InsertObserverTelemetryParams{
		ObserverID:       observerID,
		ReportedAt:       pgtype.Timestamptz{Time: reportedAt, Valid: true},
		BatteryVoltageMv: batteryMV,
		AirtimeTxPct:     txAirSecs,
		AirtimeRxPct:     rxAirSecs,
		NoiseFloorDb:     &noiseFloor,
		UptimeSeconds:    &uptimeSeconds,
		QueueLength:      queueLen,
		DebugFlags:       debugFlags,
		ReceiveErrors:    recvErrors,
	})
}

func (s *Store) GetObserverTelemetry(ctx context.Context, observerID uuid.UUID, since, until time.Time, afterID int64) (*api.ObserverTelemetry, error) {
	rows, err := s.q.GetObserverTelemetry(ctx, sqlc.GetObserverTelemetryParams{
		ObserverID: observerID,
		Column2:    pgtype.Timestamptz{Time: since, Valid: !since.IsZero()},
		Column3:    pgtype.Timestamptz{Time: until, Valid: !until.IsZero()},
		Column4:    afterID,
	})
	if err != nil {
		return nil, err
	}
	points := make([]api.ObserverTelemetryPoint, 0, len(rows))
	for _, v := range rows {
		points = append(points, api.ObserverTelemetryPoint{
			T:             v.ReportedAt.Time.Unix(),
			BatteryMV:     v.BatteryVoltageMv,
			AirtimeTxPct:  v.AirtimeTxPct,
			AirtimeRxPct:  v.AirtimeRxPct,
			NoiseFloorDB:  v.NoiseFloorDb,
			UptimeSeconds: v.UptimeSeconds,
			QueueLength:   v.QueueLength,
			ReceiveErrors: v.ReceiveErrors,
		})
	}
	return &api.ObserverTelemetry{Points: points}, nil
}

func (s *Store) GetObserverTelemetryBucketed(ctx context.Context, observerID uuid.UUID, since, until time.Time, bucketHours int32) ([]api.ObserverTelemetryPoint, error) {
	var sinceTS, untilTS pgtype.Timestamptz
	if !since.IsZero() {
		sinceTS = pgtype.Timestamptz{Time: since, Valid: true}
	}
	if !until.IsZero() {
		untilTS = pgtype.Timestamptz{Time: until, Valid: true}
	}
	rows, err := s.q.GetObserverTelemetryBucketed(ctx, sqlc.GetObserverTelemetryBucketedParams{
		ObserverID: observerID,
		Column2:    sinceTS,
		Column3:    untilTS,
		Column4:    bucketHours,
	})
	if err != nil {
		return nil, err
	}
	points := make([]api.ObserverTelemetryPoint, 0, len(rows))
	for _, r := range rows {
		points = append(points, api.ObserverTelemetryPoint{
			T:             r.Bucket.Time.UnixMilli(),
			BatteryMV:     &r.BatteryVoltageMv,
			AirtimeTxPct:  &r.AirtimeTxPct,
			AirtimeRxPct:  &r.AirtimeRxPct,
			NoiseFloorDB:  &r.NoiseFloorDb,
			UptimeSeconds: &r.UptimeSeconds,
			QueueLength:   &r.QueueLength,
			ReceiveErrors: &r.ReceiveErrors,
		})
	}
	return points, nil
}

func (s *Store) ListObserverAdverts(ctx context.Context, observerID uuid.UUID, cursor int64, limit int32) (api.Page[api.AdvertObservation], error) {
	rows, err := s.q.ListObserverAdverts(ctx, sqlc.ListObserverAdvertsParams{
		ObserverID: observerID,
		Column2:    cursor,
		Limit:      limit + 1, // fetch one extra to detect hasMore
	})
	if err != nil {
		log.Printf("api: ListObserverAdverts failed: %v", err)
		return api.Page[api.AdvertObservation]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.AdvertObservation, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.AdvertObservation{
			PacketObservationSummary: api.PacketObservationSummary{
				ID:              v.ID,
				PacketHash:      v.PacketHashHex,
				PayloadType:     v.PayloadType,
				PayloadTypeName: api.PayloadTypeName(v.PayloadType),
				IATA:            v.Iata,
				HeardAt:         v.HeardAt.Time.UnixMilli(),
				RSSI:            v.Rssi,
				SNR:             v.Snr,
				HopCount:        &v.HopCount,
			},
			NodeName:      v.NodeName,
			NodePublicKey: &v.NodePublicKey,
		})
	}
	var nextCursor *int64
	if hasMore {
		last := items[len(items)-1].ID
		nextCursor = &last
	}
	return api.Page[api.AdvertObservation]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (s *Store) UpdateObserverStatus(ctx context.Context, p ingest.UpdateObserverStatusParams) (uuid.UUID, error) {
	params := sqlc.UpdateObserverStatusParams{PublicKey: p.PublicKey, Column2: p.DisplayName, Column3: p.ObserverType, SoftwareVersion: &p.SoftwareVersion, HardwareModel: &p.HardwareModel, FirmwareVersion: &p.FirmwareVersion, FirmwareBuild: &p.FirmwareBuild, RadioFreqMhz: &p.RadioFreqMHz, RadioSf: &p.RadioSF, RadioBwKhz: &p.RadioBWKHz, RadioCr: &p.RadioCR, BatteryLevel: p.BatteryLevel, UptimeSeconds: p.UptimeSeconds, StatusMetadata: p.StatusMetadata}
	return s.q.UpdateObserverStatus(ctx, params)
}

func (s *Store) GetObserverLastIATA(ctx context.Context, observerID uuid.UUID) (string, error) {
	return s.q.GetObserverLastIATA(ctx, observerID)
}

func (s *Store) GetObserverRadio(ctx context.Context, observerID uuid.UUID) (ingest.RadioSettings, error) {
	row, err := s.q.GetObserverRadio(ctx, observerID)
	if err != nil {
		return ingest.RadioSettings{}, err
	}
	var settings ingest.RadioSettings
	if row.RadioFreqMhz != nil {
		settings.FreqMHz = *row.RadioFreqMhz
	}
	if row.RadioSf != nil {
		settings.SF = *row.RadioSf
	}
	if row.RadioBwKhz != nil {
		settings.BWKHz = *row.RadioBwKhz
	}
	if row.RadioCr != nil {
		settings.CR = *row.RadioCr
	}
	return settings, nil
}

func (s *Store) UpsertObserverBroker(ctx context.Context, observerID uuid.UUID, brokerName string) error {
	params := sqlc.UpsertObserverBrokerParams{
		ObserverID: observerID,
		BrokerName: brokerName,
	}
	return s.q.UpsertObserverBroker(ctx, params)
}

func (s *Store) UpsertObserverScope(ctx context.Context, observerID uuid.UUID, scopeID int32) error {
	return s.q.UpsertObserverScope(ctx, sqlc.UpsertObserverScopeParams{
		ObserverID: observerID,
		ScopeID:    scopeID,
	})
}

func (s *Store) GetObserverScopes(ctx context.Context, observerID uuid.UUID) ([]string, error) {
	return s.q.GetObserverScopes(ctx, observerID)
}

func (s *Store) IsObserverByPubkey(ctx context.Context, pubkey []byte) bool {
	_, err := s.q.GetObserverByPubkey(ctx, pubkey)
	return err == nil
}

func (s *Store) DeleteOldTelemetry(ctx context.Context, cutoff time.Time) error {
	return s.q.DeleteOldTelemetry(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true})
}
