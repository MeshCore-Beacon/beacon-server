// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

// revisionTTL bounds how stale a rollup revision read can be before a new roll shows up.
const revisionTTL = 10 * time.Second

type revisionSource interface {
	AnalyticsRevision(ctx context.Context) (int64, error)
}

// revisionMemo spares Postgres a revision read on every cached stats request.
type revisionMemo struct {
	mu  sync.Mutex
	rev int64
	at  time.Time
	now func() time.Time
}

// get returns the analytics revision; ok is false when the inner reader can't supply one.
func (m *revisionMemo) get(ctx context.Context, inner api.Reader) (rev int64, ok bool) {
	src, isSource := inner.(revisionSource)
	if !isSource {
		return 0, false
	}
	now := time.Now
	if m.now != nil {
		now = m.now
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if !m.at.IsZero() && now().Sub(m.at) < revisionTTL {
		return m.rev, true
	}
	rev, err := src.AnalyticsRevision(ctx)
	if err != nil {
		return 0, false
	}
	m.rev, m.at = rev, now()
	return rev, true
}

// rollupKey builds a stats key that changes with the rollup revision. [] and [""] stay
// distinct (all IATAs vs an empty region).
func (cr *CachedReader) rollupKey(ctx context.Context, prefix string, iatas []string, parts ...any) (string, bool) {
	rev, ok := cr.rev.get(ctx, cr.inner)
	if !ok {
		return "", false
	}
	sorted := append([]string{}, iatas...)
	slices.Sort(sorted)
	segment, _ := json.Marshal(slices.Compact(sorted))
	key := fmt.Sprintf("%sr%d:%s", prefix, rev, segment)
	for _, p := range parts {
		key += fmt.Sprintf(":%v", p)
	}
	return key, true
}

// cachedRollup caches a rollup-backed read under a revision key, or reads through without one.
func cachedRollup[T any](ctx context.Context, cr *CachedReader, prefix string, iatas []string, fetch func(context.Context) (T, error), parts ...any) (T, error) {
	key, ok := cr.rollupKey(ctx, prefix, iatas, parts...)
	if !ok {
		return fetch(ctx)
	}
	return getOrSet(ctx, cr.c, key, cr.ttl.Stats, fetch)
}

// hourKey matches the store's hour snapping so polls within an hour share a key.
func hourKey(t time.Time) int64 {
	return t.UTC().Truncate(time.Hour).UnixMilli()
}

// sinceKey keys a zero since on the current hour, since the store's default window slides with it.
func (cr *CachedReader) sinceKey(since time.Time) string {
	if since.IsZero() {
		return fmt.Sprintf("d%d", hourKey(cr.clock()))
	}
	return strconv.FormatInt(hourKey(since), 10)
}

func (cr *CachedReader) clock() time.Time {
	if cr.now == nil {
		return time.Now()
	}
	return cr.now()
}

// GetStatsSeries implements [api.Reader].
func (cr *CachedReader) GetStatsSeries(ctx context.Context, since, until time.Time, iatas []string) (*api.StatsSeries, error) {
	return cachedRollup(ctx, cr, keyStatsSeriesPrefix, iatas, func(ctx context.Context) (*api.StatsSeries, error) {
		return cr.inner.GetStatsSeries(ctx, since, until, iatas)
	}, since.UnixMilli(), until.UnixMilli())
}

// GetSignalStats implements [api.Reader].
func (cr *CachedReader) GetSignalStats(ctx context.Context, since, until time.Time, iatas []string) (*api.SignalStats, error) {
	return cachedRollup(ctx, cr, keySignalStatsPrefix, iatas, func(ctx context.Context) (*api.SignalStats, error) {
		return cr.inner.GetSignalStats(ctx, since, until, iatas)
	}, since.UnixMilli(), until.UnixMilli())
}

// GetPathStats implements [api.Reader].
func (cr *CachedReader) GetPathStats(ctx context.Context, since, until time.Time, iatas []string) (*api.PathStats, error) {
	return cachedRollup(ctx, cr, keyPathStatsPrefix, iatas, func(ctx context.Context) (*api.PathStats, error) {
		return cr.inner.GetPathStats(ctx, since, until, iatas)
	}, since.UnixMilli(), until.UnixMilli())
}

// GetStatsOverview implements [api.Reader]. Its window follows the clock, so the current hour is part of the key.
func (cr *CachedReader) GetStatsOverview(ctx context.Context, iatas []string) (*api.StatsOverview, error) {
	return cachedRollup(ctx, cr, keyStatsOverviewPrefix, iatas, func(ctx context.Context) (*api.StatsOverview, error) {
		return cr.inner.GetStatsOverview(ctx, iatas)
	}, hourKey(cr.clock()))
}

// GetStatsObservations implements [api.Reader].
func (cr *CachedReader) GetStatsObservations(ctx context.Context, iatas []string, since time.Time) ([]api.ObservationPoint, error) {
	return cachedRollup(ctx, cr, keyStatsObservationsPrefix, iatas, func(ctx context.Context) ([]api.ObservationPoint, error) {
		return cr.inner.GetStatsObservations(ctx, iatas, since)
	}, cr.sinceKey(since))
}

// GetStatsPayloadBreakdown implements [api.Reader].
func (cr *CachedReader) GetStatsPayloadBreakdown(ctx context.Context, iatas []string, since time.Time) ([]api.PayloadBreakdownItem, error) {
	return cachedRollup(ctx, cr, keyStatsBreakdownPrefix, iatas, func(ctx context.Context) ([]api.PayloadBreakdownItem, error) {
		return cr.inner.GetStatsPayloadBreakdown(ctx, iatas, since)
	}, cr.sinceKey(since))
}

// GetStatsTopNodes implements [api.Reader].
func (cr *CachedReader) GetStatsTopNodes(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopNode, error) {
	return cachedRollup(ctx, cr, keyStatsTopNodesPrefix, iatas, func(ctx context.Context) ([]api.TopNode, error) {
		return cr.inner.GetStatsTopNodes(ctx, iatas, since, limit)
	}, cr.sinceKey(since), limit)
}

// GetStatsTopObservers implements [api.Reader].
func (cr *CachedReader) GetStatsTopObservers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopObserver, error) {
	return cachedRollup(ctx, cr, keyStatsTopObsPrefix, iatas, func(ctx context.Context) ([]api.TopObserver, error) {
		return cr.inner.GetStatsTopObservers(ctx, iatas, since, limit)
	}, cr.sinceKey(since), limit)
}

// GetStatsTopAdvertisers implements [api.Reader].
func (cr *CachedReader) GetStatsTopAdvertisers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopAdvertiser, error) {
	return cachedRollup(ctx, cr, keyStatsTopAdvPrefix, iatas, func(ctx context.Context) ([]api.TopAdvertiser, error) {
		return cr.inner.GetStatsTopAdvertisers(ctx, iatas, since, limit)
	}, cr.sinceKey(since), limit)
}

// GetStatsTopTalkers implements [api.Reader].
func (cr *CachedReader) GetStatsTopTalkers(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopTalker, error) {
	return cachedRollup(ctx, cr, keyStatsTopTalkersPrefix, iatas, func(ctx context.Context) ([]api.TopTalker, error) {
		return cr.inner.GetStatsTopTalkers(ctx, iatas, since, limit)
	}, cr.sinceKey(since), limit)
}

// GetScopeStats implements [api.Reader].
func (cr *CachedReader) GetScopeStats(ctx context.Context, iatas []string, since time.Time) ([]api.ScopeStats, error) {
	// The suffix changes with the hourly shape so older cached entries aren't served.
	return cachedRollup(ctx, cr, keyScopeStats+":h2:", iatas, func(ctx context.Context) ([]api.ScopeStats, error) {
		return cr.inner.GetScopeStats(ctx, iatas, since)
	}, cr.sinceKey(since))
}

// GetScopeByName implements [api.Reader].
func (cr *CachedReader) GetScopeByName(ctx context.Context, name string) (*api.ScopeDetail, error) {
	return cachedRollup(ctx, cr, keyScopeByNamePrefix, nil, func(ctx context.Context) (*api.ScopeDetail, error) {
		return cr.inner.GetScopeByName(ctx, name)
	}, name)
}
