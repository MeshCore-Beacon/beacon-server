// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

func (s *Store) GetStatsSeries(ctx context.Context, since, until time.Time, iatas []string) (*api.StatsSeries, error) {
	// Read the revision first: a roll landing mid-request then shows up as a newer revision next time.
	rev, err := s.q.GetAnalyticsRevision(ctx)
	if err != nil {
		return nil, err
	}
	earliest, err := s.q.GetEarliestCompleteRollupHour(ctx)
	if err != nil {
		return nil, err
	}
	rows, err := s.q.GetStatsSeries(ctx, sqlc.GetStatsSeriesParams{Since: ts(since), Until: ts(until), Iatas: iatas})
	if err != nil {
		return nil, err
	}
	out := &api.StatsSeries{
		Since:    since.UnixMilli(),
		Until:    until.UnixMilli(),
		Revision: rev,
		Hours:    make([]api.StatsSeriesHour, 0, len(rows)),
	}
	if earliest.Valid {
		ms := earliest.Time.UnixMilli()
		out.EarliestComplete = &ms
	}
	for _, r := range rows {
		v := api.StatsSeriesValues{
			Observations:    r.Observations,
			UniquePackets:   r.UniquePackets,
			ActiveObservers: r.ActiveObservers,
			ActiveIATAs:     r.ActiveIatas,
			ScopedPackets:   r.ScopedPackets,
			ActiveScopes:    r.ActiveScopes,
			MaxPathEntries:  r.MaxPathEntries,
			SNRSum:          r.SnrSum,
			SNRSamples:      r.SnrSamples,
			RSSISum:         r.RssiSum,
			RSSISamples:     r.RssiSamples,
		}
		if !r.Hour.Valid {
			out.Summary = v
			continue
		}
		h := api.StatsSeriesHour{Hour: r.Hour.Time.UnixMilli(), Status: r.Status}
		if r.Status == "complete" {
			h.Values = &v
			out.CompleteHours++
		}
		out.Hours = append(out.Hours, h)
	}
	return out, nil
}
