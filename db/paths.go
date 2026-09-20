// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"fmt"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5/pgtype"
)

func (s *Store) GetPathStats(ctx context.Context, since, until time.Time, iatas []string) (*api.PathStats, error) {
	rows, err := s.q.GetPathStats(ctx, sqlc.GetPathStatsParams{Since: pgtype.Timestamptz{Time: since, Valid: true}, Until: pgtype.Timestamptz{Time: until, Valid: true}, Iatas: iatas})
	if err != nil {
		return nil, err
	}
	stats := &api.PathStats{Since: since.UnixMilli(), Until: until.UnixMilli(), HashWidths: []api.PathHashWidth{{Bytes: 1}, {Bytes: 2}, {Bytes: 3}}, PathLengths: []api.PathLengthBin{}, Hourly: []api.PathHour{}}
	var lengths [64]int64
	for _, row := range rows {
		if row.Category == 0 && (row.HashBytes < 1 || int(row.HashBytes) > len(stats.HashWidths) || row.Entries < 0 || int(row.Entries) >= len(lengths)) {
			return nil, fmt.Errorf("invalid path statistics bucket")
		}
		if row.IsHourly {
			hour := row.Hour.Time.UnixMilli()
			if len(stats.Hourly) == 0 || stats.Hourly[len(stats.Hourly)-1].Hour != hour {
				stats.Hourly = append(stats.Hourly, api.PathHour{Hour: hour})
			}
			point := &stats.Hourly[len(stats.Hourly)-1]
			point.Receptions += row.Receptions
			switch row.Category {
			case 0:
				switch row.HashBytes {
				case 1:
					point.OneByte += row.Receptions
				case 2:
					point.TwoByte += row.Receptions
				case 3:
					point.ThreeByte += row.Receptions
				}
			case 1:
				point.Empty += row.Receptions
			case 2:
				point.Trace += row.Receptions
			case 3:
				point.Unclassified += row.Receptions
			}
			continue
		}
		stats.Receptions += row.Receptions
		switch row.Category {
		case 0:
			stats.Hashed += row.Receptions
			stats.HashWidths[row.HashBytes-1].Receptions += row.Receptions
			lengths[row.Entries] += row.Receptions
		case 1:
			stats.Empty += row.Receptions
			lengths[0] += row.Receptions
		case 2:
			stats.Trace += row.Receptions
		case 3:
			stats.Unclassified += row.Receptions
		}
	}
	for entries, count := range lengths {
		if count > 0 {
			stats.PathLengths = append(stats.PathLengths, api.PathLengthBin{Entries: int32(entries), Receptions: count})
		}
	}
	return stats, nil
}

func (s *Store) RefreshPathStats(ctx context.Context) error { return s.q.RefreshPathStats(ctx) }
