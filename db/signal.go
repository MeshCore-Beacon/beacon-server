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

func (s *Store) GetSignalStats(ctx context.Context, since, until time.Time, iatas []string) (*api.SignalStats, error) {
	rows, err := s.q.GetSignalStats(ctx, sqlc.GetSignalStatsParams{
		Since: pgtype.Timestamptz{Time: since, Valid: true}, Until: pgtype.Timestamptz{Time: until, Valid: true}, Iatas: iatas,
	})
	if err != nil {
		return nil, err
	}
	stats := &api.SignalStats{Since: since.UnixMilli(), Until: until.UnixMilli(),
		SNR: api.SignalMetric{Histogram: signalBins(-30, 5, 12)}, RSSI: api.SignalMetric{Histogram: signalBins(-140, 10, 14)}, Hourly: []api.SignalHour{}}
	for _, row := range rows {
		// GROUPING bits distinguish real null buckets from dimensions not in the set.
		switch row.Kind {
		case 7:
			stats.Receptions = row.Receptions
			stats.SNR.Samples, stats.SNR.Average = row.SnrSamples, signalAverage(row.SnrAverage, row.SnrSamples)
			stats.RSSI.Samples, stats.RSSI.Average = row.RssiSamples, signalAverage(row.RssiAverage, row.RssiSamples)
		case 3:
			stats.Hourly = append(stats.Hourly, api.SignalHour{Hour: row.Hour.Time.UnixMilli(), Receptions: row.Receptions,
				SNRSamples: row.SnrSamples, SNRAverage: signalAverage(row.SnrAverage, row.SnrSamples),
				RSSISamples: row.RssiSamples, RSSIAverage: signalAverage(row.RssiAverage, row.RssiSamples)})
		case 5:
			if row.SnrBin >= 0 {
				stats.SNR.Histogram[row.SnrBin].Count = row.Receptions
			}
		case 6:
			if row.RssiBin >= 0 {
				stats.RSSI.Histogram[row.RssiBin].Count = row.Receptions
			}
		}
	}
	return stats, nil
}

func signalAverage(value float64, count int64) *float64 {
	if count == 0 {
		return nil
	}
	return &value
}

func signalBins(lower, width float64, count int) []api.SignalBin {
	bins := make([]api.SignalBin, count+2)
	for i := range bins {
		if i > 0 {
			v := lower + float64(i-1)*width
			bins[i].Lower = &v
		}
		if i <= count {
			v := lower + float64(i)*width
			bins[i].Upper = &v
		}
	}
	return bins
}
