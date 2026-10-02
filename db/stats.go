// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// rollupSince snaps a window start to its UTC hour; zero means def before now.
func rollupSince(since time.Time, def time.Duration) pgtype.Timestamptz {
	if since.IsZero() {
		since = time.Now().Add(-def)
	}
	return ts(since.UTC().Truncate(time.Hour))
}

// optional maps the ” the rollup queries use for a missing name back to nil.
func optional(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}

func pgUUID(id pgtype.UUID) *uuid.UUID {
	if !id.Valid {
		return nil
	}
	u := uuid.UUID(id.Bytes)
	return &u
}

// GetStatsOverview summarises the 24 most recent hours that can have been rolled;
// WindowHours counts the complete ones the totals actually cover.
func (s *Store) GetStatsOverview(ctx context.Context, iatas []string) (*api.StatsOverview, error) {
	until := time.Now().UTC().Add(-rollupDelay).Truncate(time.Hour).Add(time.Hour)
	since := until.Add(-24 * time.Hour)
	series, err := s.GetStatsSeries(ctx, since, until, iatas)
	if err != nil {
		return nil, err
	}
	return &api.StatsOverview{
		TotalPackets:      series.Summary.UniquePackets,
		TotalObservations: series.Summary.Observations,
		ActiveObservers:   series.Summary.ActiveObservers,
		ActiveIATAs:       series.Summary.ActiveIATAs,
		WindowHours:       series.CompleteHours,
		Since:             since.UnixMilli(),
		Until:             until.UnixMilli(),
	}, nil
}

func (s *Store) GetStatsObservations(ctx context.Context, iatas []string, since time.Time) ([]api.ObservationPoint, error) {
	rows, err := s.q.GetHourlyStats(ctx, sqlc.GetHourlyStatsParams{Iatas: iatas, Since: rollupSince(since, 7*24*time.Hour)})
	if err != nil {
		return nil, err
	}
	points := make([]api.ObservationPoint, 0, len(rows))
	for _, v := range rows {
		points = append(points, api.ObservationPoint{
			Hour:             v.Hour.Time.UnixMilli(),
			IATA:             v.Iata,
			ObservationCount: v.ObservationCount,
		})
	}
	return points, nil
}

func (s *Store) GetStatsPayloadBreakdown(ctx context.Context, iatas []string, since time.Time) ([]api.PayloadBreakdownItem, error) {
	rows, err := s.q.GetStatsPayloadBreakdown(ctx, sqlc.GetStatsPayloadBreakdownParams{Iatas: iatas, Since: rollupSince(since, 24*time.Hour)})
	if err != nil {
		return nil, err
	}
	items := make([]api.PayloadBreakdownItem, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.PayloadBreakdownItem{
			PayloadType:     v.PayloadType,
			PayloadTypeName: api.PayloadTypeName(v.PayloadType),
			Count:           v.Count,
		})
	}
	return items, nil
}

func (s *Store) GetStatsTopNodes(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopNode, error) {
	rows, err := s.q.GetTopNodes(ctx, sqlc.GetTopNodesParams{Since: rollupSince(since, 7*24*time.Hour), Iatas: iatas, RowLimit: limit})
	if err != nil {
		return nil, err
	}
	items := make([]api.TopNode, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.TopNode{
			NodeID:           pgUUID(v.NodeID),
			PublicKey:        v.PublicKey,
			NodeName:         optional(v.Name),
			NodeType:         v.NodeType,
			NodeTypeName:     api.NodeTypeName(v.NodeType),
			IATA:             v.Iata,
			ObservationCount: v.ObservationCount,
			LastHeard:        v.LastHeard.Time.UnixMilli(),
		})
	}
	return items, nil
}

func (s *Store) GetStatsTopObservers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopObserver, error) {
	rows, err := s.q.GetStatsTopObservers(ctx, sqlc.GetStatsTopObserversParams{Since: rollupSince(since, 24*time.Hour), Iatas: iatas, RowLimit: limit})
	if err != nil {
		return nil, err
	}
	items := make([]api.TopObserver, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.TopObserver{
			ObserverID:       v.ID,
			DisplayName:      optional(v.DisplayName),
			ObserverType:     optional(v.ObserverType),
			IATA:             v.Iata,
			ObservationCount: v.ObservationCount,
		})
	}
	return items, nil
}

func (s *Store) GetStatsTopAdvertisers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopAdvertiser, error) {
	rows, err := s.q.GetStatsTopAdvertisers(ctx, sqlc.GetStatsTopAdvertisersParams{Since: rollupSince(since, 24*time.Hour), Iatas: iatas, RowLimit: limit})
	if err != nil {
		return nil, err
	}
	items := make([]api.TopAdvertiser, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.TopAdvertiser{
			NodeID:            pgUUID(v.NodeID),
			PublicKey:         v.PublicKey,
			NodeName:          optional(v.Name),
			NodeType:          v.NodeType,
			NodeTypeName:      api.NodeTypeName(v.NodeType),
			IATA:              v.Iata,
			AdvertCount:       v.AdvertCount,
			FloodAdvertCount:  v.FloodAdvertCount,
			DirectAdvertCount: v.DirectAdvertCount,
			LastHeard:         v.LastHeard.Time.UnixMilli(),
		})
	}
	return items, nil
}

// GetStatsClockDrift returns repeaters/room servers whose current advert-derived clock
// drift exceeds the Store's configured threshold, worst first.
func (s *Store) GetStatsClockDrift(ctx context.Context, iatas []string, limit int32) ([]api.ClockDriftEntry, error) {
	thresholdSeconds := int32(s.clockDriftThreshold / time.Second)
	rows, err := s.q.GetStatsClockDrift(ctx, sqlc.GetStatsClockDriftParams{
		Column1: thresholdSeconds,
		Column2: iatas,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.ClockDriftEntry, 0, len(rows))
	for _, v := range rows {
		entry := api.ClockDriftEntry{
			NodeID:            v.ID,
			NodeName:          v.Name,
			NodeType:          v.NodeType,
			NodeTypeName:      api.NodeTypeName(v.NodeType),
			ClockDriftSeconds: int(*v.DeviceClockDriftSeconds),
			ClockCheckedAt:    v.LastAdvertAt.Time.UnixMilli(),
		}
		if len(v.Iatas) > 0 {
			if err := json.Unmarshal(v.Iatas, &entry.IATAs); err != nil {
				slog.Error("store: failed to unmarshal clock drift node iatas", "component", "db", "error", err)
				entry.IATAs = []api.NodeIATA{}
			}
		}
		items = append(items, entry)
	}
	return items, nil
}

func (s *Store) GetStatsTopTalkers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopTalker, error) {
	rows, err := s.q.GetStatsTopTalkers(ctx, sqlc.GetStatsTopTalkersParams{Since: rollupSince(since, 24*time.Hour), Iatas: iatas, RowLimit: limit})
	if err != nil {
		return nil, err
	}
	items := make([]api.TopTalker, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.TopTalker{
			SenderName:   v.SenderName,
			MessageCount: v.MessageCount,
			LastSent:     v.LastSent.Time.UnixMilli(),
		})
	}
	return items, nil
}

func (s *Store) GetRadioPresets(ctx context.Context, preset string, iatas []string) ([]api.RadioPreset, error) {
	rows, err := s.q.GetRadioPresets(ctx, sqlc.GetRadioPresetsParams{
		Column1: preset,
		Column2: iatas,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.RadioPreset, 0, len(rows))
	for _, v := range rows {
		if v.Iata == nil {
			continue
		}
		items = append(items, api.RadioPreset{
			Preset:     v.Preset,
			IATA:       *v.Iata,
			SourceType: v.SourceType,
			Count:      v.Count,
		})
	}
	return items, nil
}

func (s *Store) GetScopeStats(ctx context.Context, iatas []string, since time.Time) ([]api.ScopeStats, error) {
	start := rollupSince(since, 7*24*time.Hour)
	rows, err := s.q.GetScopeStats(ctx, sqlc.GetScopeStatsParams{Since: start, Iatas: iatas})
	if err != nil {
		return nil, err
	}
	hours, err := s.q.GetScopeStatsHourly(ctx, sqlc.GetScopeStatsHourlyParams{Since: start, Iatas: iatas})
	if err != nil {
		return nil, err
	}
	byScope := make(map[string][]api.ScopeHour)
	for _, h := range hours {
		byScope[h.Name] = append(byScope[h.Name], api.ScopeHour{Hour: h.Hour.Time.UnixMilli(), Packets: h.Packets, Observers: h.Observers, Nodes: h.Nodes})
	}
	items := make([]api.ScopeStats, 0, len(rows))
	for _, r := range rows {
		hourly := byScope[r.Name]
		if hourly == nil {
			hourly = []api.ScopeHour{}
		}
		items = append(items, api.ScopeStats{
			Name:          r.Name,
			PacketCount:   r.PacketCount,
			ObserverCount: r.ObserverCount,
			NodeCount:     r.NodeCount,
			Hourly:        hourly,
		})
	}
	return items, nil
}

func (s *Store) GetStatsNodeTypes(ctx context.Context, iatas []string) ([]api.NodeTypeCount, error) {
	rows, err := s.q.GetStatsNodeTypes(ctx, iatas)
	if err != nil {
		return nil, err
	}
	result := make([]api.NodeTypeCount, 0, len(rows))
	for _, r := range rows {
		result = append(result, api.NodeTypeCount{
			NodeType:     r.NodeType,
			NodeTypeName: api.NodeTypeName(r.NodeType),
			Count:        r.Count,
		})
	}
	return result, nil
}

func (s *Store) RefreshRadioPresets(ctx context.Context) error {
	return s.q.RefreshRadioPresets(ctx)
}
