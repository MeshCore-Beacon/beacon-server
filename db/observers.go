// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"fmt"
	"log/slog"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/MeshCore-Beacon/beacon-server/internal/lora"
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
	params := sqlc.ListObserversParams{
		Column1: iatas,
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
			s := fmt.Sprintf("%g,%g,%d", *v.RadioFreqMhz, *v.RadioBwKhz, *v.RadioSf)
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
		slog.Error(fmt.Sprintf("store: GetObserverScopes failed for %s", observerID), "component", "db", "error", err)
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
	if (obs.LastStatusAt.Valid && time.Since(obs.LastStatusAt.Time) < 5*time.Minute) ||
		(obs.LastSeen.Valid && time.Since(obs.LastSeen.Time) < 5*time.Minute) {
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
		AirtimeTxSecs:    txAirSecs,
		AirtimeRxSecs:    rxAirSecs,
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
			T:             v.ReportedAt.Time.UnixMilli(),
			BatteryMV:     v.BatteryVoltageMv,
			AirtimeTxSecs: v.AirtimeTxSecs,
			AirtimeRxSecs: v.AirtimeRxSecs,
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
			AirtimeTxSecs: &r.AirtimeTxSecs,
			AirtimeRxSecs: &r.AirtimeRxSecs,
			NoiseFloorDB:  &r.NoiseFloorDb,
			UptimeSeconds: &r.UptimeSeconds,
			QueueLength:   &r.QueueLength,
			ReceiveErrors: &r.ReceiveErrors,
		})
	}
	return points, nil
}

// GetObserverActivity returns bucketed heard-activity for an observer over the trailing window.
// Buckets of an hour or coarser come from the hourly rollup; anything finer reads observations directly.
// Range and Interval are left empty for the handler to fill.
func (s *Store) GetObserverActivity(ctx context.Context, observerID uuid.UUID, window, interval time.Duration) (*api.ObserverActivity, error) {
	obs, err := s.q.GetObserverByID(ctx, observerID)
	if err != nil {
		return nil, err
	}
	activity := &api.ObserverActivity{}
	// radio is non-nil only when airtime is actually costable, so radio != null implies costed buckets
	if obs.RadioSf != nil && obs.RadioBwKhz != nil && obs.RadioCr != nil &&
		*obs.RadioSf >= 7 && *obs.RadioSf <= 12 && *obs.RadioBwKhz > 0 && *obs.RadioCr > 0 {
		activity.Radio = &api.ObserverActivityRadio{
			FreqMHz:         obs.RadioFreqMhz,
			SF:              *obs.RadioSf,
			BWKHz:           *obs.RadioBwKhz,
			CR:              *obs.RadioCr,
			PreambleSymbols: lora.PreambleSymbols(int(*obs.RadioSf)),
		}
	}
	// round the window start up to a bucket boundary so the first bucket is never a partial one
	start := time.Now().Add(-window).UTC()
	since := start.Truncate(interval)
	if since.Before(start) {
		since = since.Add(interval)
	}
	sinceTS := pgtype.Timestamptz{Time: since, Valid: true}
	binWidth := pgtype.Interval{Microseconds: interval.Microseconds(), Valid: true}

	if interval >= time.Hour {
		rows, err := s.q.GetObserverActivityHourly(ctx, sqlc.GetObserverActivityHourlyParams{
			ObserverID: observerID,
			Column2:    sinceTS,
			Column3:    binWidth,
		})
		if err != nil {
			return nil, err
		}
		activity.Points = make([]api.ObserverActivityPoint, 0, len(rows))
		for _, r := range rows {
			p := api.ObserverActivityPoint{T: r.Bucket.Time.UnixMilli(), Observations: r.Observations}
			if r.AirtimeN > 0 {
				airtime := r.AirtimeMs
				p.AirtimeMs = &airtime
			}
			if r.SnrN > 0 {
				avg := r.SnrSum / float32(r.SnrN)
				min := r.SnrMin
				p.SNRAvg, p.SNRMin = &avg, &min
			}
			if r.RssiN > 0 {
				avg := float32(r.RssiSum) / float32(r.RssiN)
				p.RSSIAvg = &avg
			}
			activity.Points = append(activity.Points, p)
		}
		typeRows, err := s.q.GetObserverActivityHourlyPayloadTypes(ctx, sqlc.GetObserverActivityHourlyPayloadTypesParams{
			ObserverID: observerID,
			Column2:    sinceTS,
		})
		if err != nil {
			return nil, err
		}
		activity.PayloadTypes = make([]api.PayloadBreakdownItem, 0, len(typeRows))
		for _, v := range typeRows {
			if v.PayloadType == nil {
				continue
			}
			activity.PayloadTypes = append(activity.PayloadTypes, api.PayloadBreakdownItem{
				PayloadType:     *v.PayloadType,
				PayloadTypeName: api.PayloadTypeName(*v.PayloadType),
				Count:           v.Count,
			})
		}
		return activity, nil
	}

	rows, err := s.q.GetObserverActivityRaw(ctx, sqlc.GetObserverActivityRawParams{
		ObserverID: observerID,
		Column2:    sinceTS,
		Column3:    binWidth,
	})
	if err != nil {
		return nil, err
	}
	activity.Points = make([]api.ObserverActivityPoint, 0, len(rows))
	for _, r := range rows {
		p := api.ObserverActivityPoint{T: r.Bucket.Time.UnixMilli(), Observations: r.Observations}
		if r.AirtimeN > 0 {
			airtime := r.AirtimeMs
			p.AirtimeMs = &airtime
		}
		if r.SnrN > 0 {
			avg, min := r.SnrAvg, r.SnrMin
			p.SNRAvg, p.SNRMin = &avg, &min
		}
		if r.RssiN > 0 {
			avg := r.RssiAvg
			p.RSSIAvg = &avg
		}
		activity.Points = append(activity.Points, p)
	}
	typeRows, err := s.q.GetObserverActivityRawPayloadTypes(ctx, sqlc.GetObserverActivityRawPayloadTypesParams{
		ObserverID: observerID,
		Column2:    sinceTS,
	})
	if err != nil {
		return nil, err
	}
	activity.PayloadTypes = make([]api.PayloadBreakdownItem, 0, len(typeRows))
	for _, v := range typeRows {
		if v.PayloadType == nil {
			continue
		}
		activity.PayloadTypes = append(activity.PayloadTypes, api.PayloadBreakdownItem{
			PayloadType:     *v.PayloadType,
			PayloadTypeName: api.PayloadTypeName(*v.PayloadType),
			Count:           v.Count,
		})
	}
	return activity, nil
}

func (s *Store) ListObserverAdverts(ctx context.Context, observerID uuid.UUID, cursor int64, limit int32) (api.Page[api.AdvertObservation], error) {
	rows, err := s.q.ListObserverAdverts(ctx, sqlc.ListObserverAdvertsParams{
		ObserverID: observerID,
		Column2:    cursor,
		Limit:      limit + 1, // fetch one extra to detect hasMore
	})
	if err != nil {
		slog.Error("api: ListObserverAdverts failed", "component", "db", "error", err)
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

// UpdateObserverRegionScope records the observer's own OTA-reported region scope
// (the "self" field of a /neighbors report). Unrelated to UpsertObserverScope
// above, which links an observer to a named transport_scopes row matched by
// key fingerprint; this stores the raw OTA-configured scope string instead.
func (s *Store) UpdateObserverRegionScope(ctx context.Context, observerID uuid.UUID, regionScope string) error {
	return s.q.UpdateObserverRegionScope(ctx, sqlc.UpdateObserverRegionScopeParams{
		ID:          observerID,
		RegionScope: &regionScope,
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

func (s *Store) DeleteOldObservers(ctx context.Context, cutoff time.Time) ([]uuid.UUID, error) {
	return s.q.DeleteOldObservers(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true})
}
