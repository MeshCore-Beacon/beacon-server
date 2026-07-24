// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) GetStatsOverview(ctx context.Context, iatas []string) (*api.StatsOverview, error) {
	row, err := s.q.GetStatsOverview(ctx, iatas)
	if err != nil {
		return nil, err
	}
	return &api.StatsOverview{
		TotalPackets:      row.TotalPackets,
		TotalObservations: row.TotalObservations,
		ActiveObservers:   row.ActiveObservers,
		ActiveIATAs:       row.ActiveIatas,
		WindowHours:       24,
	}, nil
}

func (s *Store) GetStatsObservations(ctx context.Context, iatas []string, since time.Time) ([]api.ObservationPoint, error) {
	if since.IsZero() {
		since = time.Now().Add(-7 * 24 * time.Hour)
	}
	interval := time.Since(since)
	rows, err := s.q.GetHourlyStats(ctx, sqlc.GetHourlyStatsParams{
		Column1: iatas,
		Column2: pgtype.Interval{Microseconds: int64(interval.Hours()) * 3600 * 1e6, Valid: true},
	})
	if err != nil {
		return nil, err
	}
	points := make([]api.ObservationPoint, 0, len(rows))
	for _, v := range rows {
		points = append(points, api.ObservationPoint{
			Hour:             v.Hour.Time.UnixMilli(),
			IATA:             v.Iata,
			ObservationCount: v.ObservationCount,
			UniquePackets:    v.UniquePackets,
			ActiveObservers:  v.ActiveObservers,
		})
	}
	return points, nil
}

func (s *Store) GetStatsPayloadBreakdown(ctx context.Context, iatas []string, since time.Time) ([]api.PayloadBreakdownItem, error) {
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	interval := time.Since(since)
	rows, err := s.q.GetStatsPayloadBreakdown(ctx, sqlc.GetStatsPayloadBreakdownParams{
		Column1: iatas,
		Column2: pgtype.Interval{Microseconds: int64(interval.Hours()) * 3600 * 1e6, Valid: true},
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.PayloadBreakdownItem, 0, len(rows))
	for _, v := range rows {
		if v.PayloadType == nil {
			continue
		}
		items = append(items, api.PayloadBreakdownItem{
			PayloadType:     *v.PayloadType,
			PayloadTypeName: api.PayloadTypeName(*v.PayloadType),
			Count:           v.Count,
		})
	}
	return items, nil
}

func (s *Store) GetStatsTopNodes(ctx context.Context, iatas []string, limit int32) ([]api.TopNode, error) {
	rows, err := s.q.GetTopNodes(ctx, sqlc.GetTopNodesParams{
		Column1: iatas,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.TopNode, 0, len(rows))
	for _, v := range rows {
		var count int64
		if v.ObservationCount != nil {
			count = *v.ObservationCount
		}
		items = append(items, api.TopNode{
			NodeID:           v.NodeID,
			NodeName:         v.Name,
			NodeType:         v.NodeType,
			NodeTypeName:     api.NodeTypeName(v.NodeType),
			IATA:             v.Iata,
			ObservationCount: count,
			LastHeard:        v.LastHeard.Time.UnixMilli(),
		})
	}
	return items, nil
}

func (s *Store) GetStatsTopObservers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopObserver, error) {
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	interval := time.Since(since)
	rows, err := s.q.GetStatsTopObservers(ctx, sqlc.GetStatsTopObserversParams{
		Column1: pgtype.Interval{Microseconds: int64(interval.Hours()) * 3600 * 1e6, Valid: true},
		Column2: iatas,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.TopObserver, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.TopObserver{
			ObserverID:       v.ID,
			DisplayName:      v.DisplayName,
			ObserverType:     v.ObserverType,
			IATA:             v.Iata,
			ObservationCount: v.ObservationCount,
		})
	}
	return items, nil
}

func (s *Store) GetStatsTopAdvertisers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopAdvertiser, error) {
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	interval := time.Since(since)
	rows, err := s.q.GetStatsTopAdvertisers(ctx, sqlc.GetStatsTopAdvertisersParams{
		Column1: pgtype.Interval{Microseconds: int64(interval.Hours()) * 3600 * 1e6, Valid: true},
		Column2: iatas,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.TopAdvertiser, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.TopAdvertiser{
			NodeID:       v.ID,
			NodeName:     v.Name,
			NodeType:     v.NodeType,
			NodeTypeName: api.NodeTypeName(v.NodeType),
			IATA:         v.Iata,
			AdvertCount:  v.AdvertCount,
			LastHeard:    v.LastHeard.Time.UnixMilli(),
		})
	}
	return items, nil
}

func (s *Store) GetStatsTopTalkers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopTalker, error) {
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	interval := time.Since(since)
	rows, err := s.q.GetStatsTopTalkers(ctx, sqlc.GetStatsTopTalkersParams{
		Column1: pgtype.Interval{Microseconds: int64(interval.Hours()) * 3600 * 1e6, Valid: true},
		Column2: iatas,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.TopTalker, 0, len(rows))
	for _, v := range rows {
		var senderName string
		if v.SenderName != nil {
			senderName = *v.SenderName
		}
		items = append(items, api.TopTalker{
			SenderName:   senderName,
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
		items = append(items, api.RadioPreset{
			Preset:     v.Preset,
			IATA:       v.Iata,
			SourceType: v.SourceType,
			Count:      v.Count,
		})
	}
	return items, nil
}

func (s *Store) GetScopeStats(ctx context.Context) ([]api.ScopeStats, error) {
	rows, err := s.q.GetScopeStats(ctx)
	if err != nil {
		return nil, err
	}
	items := make([]api.ScopeStats, 0, len(rows))
	for _, r := range rows {
		items = append(items, api.ScopeStats{
			Name:          r.Name,
			PacketCount:   r.PacketCount,
			ObserverCount: r.ObserverCount,
			NodeCount:     r.NodeCount,
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

func (s *Store) RefreshHourlyStats(ctx context.Context) error {
	return s.q.RefreshHourlyStats(ctx)
}

func (s *Store) RefreshTopNodes(ctx context.Context) error {
	return s.q.RefreshTopNodes(ctx)
}

func (s *Store) RefreshPayloadBreakdown(ctx context.Context) error {
	return s.q.RefreshPayloadBreakdown(ctx)
}

func (s *Store) RefreshTopTalkers(ctx context.Context) error {
	return s.q.RefreshTopTalkers(ctx)
}

func (s *Store) RefreshTopAdvertisers(ctx context.Context) error {
	return s.q.RefreshTopAdvertisers(ctx)
}

func (s *Store) RefreshTopObservers(ctx context.Context) error {
	return s.q.RefreshTopObservers(ctx)
}

func (s *Store) RefreshRadioPresets(ctx context.Context) error {
	return s.q.RefreshRadioPresets(ctx)
}
