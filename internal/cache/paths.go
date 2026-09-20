// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

func (cr *CachedReader) GetPathStats(ctx context.Context, since, until time.Time, iatas []string) (*api.PathStats, error) {
	sorted := append([]string{}, iatas...)
	slices.Sort(sorted)
	sorted = slices.Compact(sorted)
	segment, _ := json.Marshal(sorted) // [] and [""] must remain distinct (global vs empty region).
	key := fmt.Sprintf("%s%s:%d:%d", keyPathStatsPrefix, segment, since.UnixMilli(), until.UnixMilli())
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, func() (*api.PathStats, error) {
		return cr.inner.GetPathStats(ctx, since, until, iatas)
	})
}
