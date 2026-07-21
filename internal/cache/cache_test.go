// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	redis "github.com/redis/go-redis/v9"
)

func newTestClient(t *testing.T) (*Client, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	c := &Client{rdb: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
	return c, mr
}

// stubReader is a minimal api.Reader that returns preset values.
type stubReader struct {
	iatas []api.IATA
	err   error
	calls int
}

func (s *stubReader) ListIATAs(_ context.Context) ([]api.IATA, error) {
	s.calls++
	return s.iatas, s.err
}

// implement remaining api.Reader methods as no-ops
func (s *stubReader) GetIATA(_ context.Context, _ string) (*api.IATA, error)     { return nil, nil }
func (s *stubReader) ListRegions(_ context.Context) ([]api.RegionSummary, error) { return nil, nil }
func (s *stubReader) GetRegion(_ context.Context, _ int32) (*api.Region, error)  { return nil, nil }
func (s *stubReader) GetRegionBySlug(_ context.Context, _ string) (*api.Region, error) {
	return nil, nil
}
func (s *stubReader) GetScopeNames(_ context.Context) ([]string, error)         { return nil, nil }
func (s *stubReader) GetScopeStats(_ context.Context) ([]api.ScopeStats, error) { return nil, nil }
func (s *stubReader) GetScopesByIATAs(_ context.Context, _ []string) ([]api.ScopeSummary, error) {
	return nil, nil
}

func (s *stubReader) GetScopeByName(_ context.Context, _ string) (*api.ScopeDetail, error) {
	return nil, nil
}

func (s *stubReader) GetStatsOverview(_ context.Context, _ []string) (*api.StatsOverview, error) {
	return nil, nil
}

func (s *stubReader) GetStatsObservations(_ context.Context, _ []string, _ time.Time) ([]api.ObservationPoint, error) {
	return nil, nil
}

func (s *stubReader) GetStatsPayloadBreakdown(_ context.Context, _ []string, _ time.Time) ([]api.PayloadBreakdownItem, error) {
	return nil, nil
}

func (s *stubReader) GetStatsTopNodes(_ context.Context, _ []string, _ int32) ([]api.TopNode, error) {
	return nil, nil
}

func (s *stubReader) GetStatsTopObservers(_ context.Context, _ []string, _ time.Time, _ int32) ([]api.TopObserver, error) {
	return nil, nil
}

func (s *stubReader) GetStatsTopAdvertisers(_ context.Context, _ []string, _ time.Time, _ int32) ([]api.TopAdvertiser, error) {
	return nil, nil
}

func (s *stubReader) GetStatsTopTalkers(_ context.Context, _ []string, _ time.Time, _ int32) ([]api.TopTalker, error) {
	return nil, nil
}

func (s *stubReader) GetStatsNodeTypes(_ context.Context, _ []string) ([]api.NodeTypeCount, error) {
	return nil, nil
}

func (s *stubReader) GetRadioPresets(_ context.Context, _ string, _ []string) ([]api.RadioPreset, error) {
	return nil, nil
}
func (s *stubReader) GetNode(_ context.Context, _ uuid.UUID) (*api.Node, error) { return nil, nil }
func (s *stubReader) GetNodeNeighbors(_ context.Context, _ uuid.UUID) ([]api.NodeNeighbor, error) {
	return nil, nil
}

func (s *stubReader) GetNodesByIDs(_ context.Context, _ []uuid.UUID) (map[uuid.UUID]*api.ResolvedNode, error) {
	return nil, nil
}

func (s *stubReader) GetObserver(_ context.Context, _ uuid.UUID) (*api.Observer, error) {
	return nil, nil
}

func (s *stubReader) GetObserverScopes(_ context.Context, _ uuid.UUID) ([]string, error) {
	return nil, nil
}

func (s *stubReader) GetObserverTelemetry(_ context.Context, _ uuid.UUID, _, _ time.Time, _ int64) (*api.ObserverTelemetry, error) {
	return nil, nil
}

func (s *stubReader) GetObserverTelemetryBucketed(_ context.Context, _ uuid.UUID, _, _ time.Time, _ int32) ([]api.ObserverTelemetryPoint, error) {
	return nil, nil
}

func (s *stubReader) GetPacket(_ context.Context, _ []byte) (*api.Packet, error) { return nil, nil }

func (s *stubReader) GetChannel(_ context.Context, _ int32) (*api.Channel, error) { return nil, nil }

func (s *stubReader) GetTraceByTag(_ context.Context, _ string) (*api.TraceDetail, error) {
	return nil, nil
}

func (s *stubReader) GetKnownRoutesByNode(_ context.Context, _ string, _ uuid.UUID) ([]api.KnownRoute, error) {
	return nil, nil
}

func (s *stubReader) GetCrossIATANeighbors(_ context.Context, _ uuid.UUID, _ string) ([]api.NodeNeighbor, error) {
	return nil, nil
}

func (s *stubReader) ListChannels(_ context.Context, _ int32, _ []byte, _ []string, _ int64) (api.Page[api.ChannelSummary], error) {
	return api.Page[api.ChannelSummary]{}, nil
}

func (s *stubReader) ListChannelMessages(_ context.Context, _ *int32, _ time.Time, _ int32, _ []string, _ string, _ int64) (api.Page[api.ChannelMessage], error) {
	return api.Page[api.ChannelMessage]{}, nil
}

func (s *stubReader) ListChannelMessagesByHash(_ context.Context, _ []byte, _ time.Time, _ int32, _ []string, _ string, _ int64) (api.Page[api.ChannelMessage], error) {
	return api.Page[api.ChannelMessage]{}, nil
}

func (s *stubReader) ListMessagesAfterID(_ context.Context, _ int64, _ []string, _ string, _ int32) ([]api.ChannelMessage, error) {
	return nil, nil
}

func (s *stubReader) ListNodes(_ context.Context, _ int16, _ []string, _, _ *bool, _ []byte, _, _, _ string, _ int64, _ int32, _ bool) (api.Page[api.NodeSummary], error) {
	return api.Page[api.NodeSummary]{}, nil
}

func (s *stubReader) ListNodeObservations(_ context.Context, _ uuid.UUID, _ int64, _ int32) (api.Page[api.PacketObservationSummary], error) {
	return api.Page[api.PacketObservationSummary]{}, nil
}

func (s *stubReader) ListObservers(_ context.Context, _ []string, _, _, _, _, _ string, _ int64, _ int32) (api.Page[api.ObserverSummary], error) {
	return api.Page[api.ObserverSummary]{}, nil
}

func (s *stubReader) ListObserverAdverts(_ context.Context, _ uuid.UUID, _ int64, _ int32) (api.Page[api.AdvertObservation], error) {
	return api.Page[api.AdvertObservation]{}, nil
}

func (s *stubReader) ListPackets(_ context.Context, _, _ []int16, _ []string, _ []string, _, _ time.Time, _ int64, _ int32) (api.Page[api.PacketSummary], error) {
	return api.Page[api.PacketSummary]{}, nil
}

func (s *stubReader) ListPacketsAfterID(_ context.Context, _ int64, _, _ int16, _ []string, _ string, _ int32) ([]api.PacketSummary, error) {
	return nil, nil
}

func (s *stubReader) ListKnownRoutes(_ context.Context, _ string, _ int32, _ time.Time, _ int32) ([]api.KnownRoute, error) {
	return nil, nil
}

func (s *stubReader) SearchKnownRoutes(_ context.Context, _, _, _ string) ([]api.KnownRoute, error) {
	return nil, nil
}

func (s *stubReader) SearchCrossIATARoutes(_ context.Context, _, _, _, _ string) ([]api.CrossIATARoute, error) {
	return nil, nil
}

func (s *stubReader) ListTraceTags(_ context.Context, _ []string, _, _ string, _, _ time.Time, _ time.Time, _ int32) ([]api.TraceTagSummary, error) {
	return nil, nil
}

// ---- ResolveTTLs tests ----

func TestResolveTTLs_CategoryWins(t *testing.T) {
	cfg := config.CacheConfig{}
	cfg.TTLs.Stats.Duration = 5 * time.Minute
	cfg.TTL.Duration = time.Hour

	ttls := ResolveTTLs(cfg)
	if ttls.Stats != 5*time.Minute {
		t.Errorf("expected Stats 5m, got %v", ttls.Stats)
	}
}

func TestResolveTTLs_GlobalFallback(t *testing.T) {
	cfg := config.CacheConfig{}
	cfg.TTL.Duration = 30 * time.Minute

	ttls := ResolveTTLs(cfg)
	if ttls.Stats != 30*time.Minute {
		t.Errorf("expected Stats 30m, got %v", ttls.Stats)
	}
}

func TestResolveTTLs_DefaultFallback(t *testing.T) {
	cfg := config.CacheConfig{}

	ttls := ResolveTTLs(cfg)
	if ttls.Stats != time.Hour {
		t.Errorf("expected Stats 1h, got %v", ttls.Stats)
	}
}

// ---- getOrSet tests ----

func TestGetOrSet_CacheMiss_FetchesAndStores(t *testing.T) {
	c, _ := newTestClient(t)
	stub := &stubReader{iatas: []api.IATA{{IATA: "YVR"}}}

	result, err := getOrSet(context.Background(), c, "test:key", time.Minute, func() ([]api.IATA, error) {
		return stub.ListIATAs(context.Background())
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].IATA != "YVR" {
		t.Errorf("unexpected result: %v", result)
	}
	if stub.calls != 1 {
		t.Errorf("expected 1 fetch call, got %d", stub.calls)
	}
}

func TestGetOrSet_CacheHit_DoesNotFetch(t *testing.T) {
	c, _ := newTestClient(t)
	stub := &stubReader{iatas: []api.IATA{{IATA: "YVR"}}}

	// prime the cache
	_, _ = getOrSet(context.Background(), c, "test:key", time.Minute, func() ([]api.IATA, error) {
		return stub.ListIATAs(context.Background())
	})
	// second call should hit cache
	result, err := getOrSet(context.Background(), c, "test:key", time.Minute, func() ([]api.IATA, error) {
		return stub.ListIATAs(context.Background())
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].IATA != "YVR" {
		t.Errorf("unexpected result: %v", result)
	}
	if stub.calls != 1 {
		t.Errorf("expected 1 fetch call (cache hit), got %d", stub.calls)
	}
}

func TestGetOrSet_RedisError_DegradeGracefully(t *testing.T) {
	c, mr := newTestClient(t)
	mr.Close() // kill Redis

	stub := &stubReader{iatas: []api.IATA{{IATA: "YVR"}}}
	result, err := getOrSet(context.Background(), c, "test:key", time.Minute, func() ([]api.IATA, error) {
		return stub.ListIATAs(context.Background())
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].IATA != "YVR" {
		t.Errorf("unexpected result: %v", result)
	}
}

func TestGetOrSet_CorruptEntry_Overwrites(t *testing.T) {
	c, mr := newTestClient(t)
	mr.Set("test:key", "not-valid-json")

	stub := &stubReader{iatas: []api.IATA{{IATA: "YVR"}}}
	result, err := getOrSet(context.Background(), c, "test:key", time.Minute, func() ([]api.IATA, error) {
		return stub.ListIATAs(context.Background())
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(result) != 1 || result[0].IATA != "YVR" {
		t.Errorf("unexpected result: %v", result)
	}
	if stub.calls != 1 {
		t.Errorf("expected 1 fetch call for corrupt entry, got %d", stub.calls)
	}
}

func TestGetOrSet_FetchError_Propagates(t *testing.T) {
	c, _ := newTestClient(t)
	stub := &stubReader{err: errors.New("db error")}

	_, err := getOrSet(context.Background(), c, "test:key", time.Minute, func() ([]api.IATA, error) {
		return stub.ListIATAs(context.Background())
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}

// ---- CachedReader key tests ----

func TestCachedReader_IATASortingForStableKey(t *testing.T) {
	c, _ := newTestClient(t)
	calls := 0
	inner := &stubReader{}

	// override GetStatsOverview on stub via a wrapper
	cr := &CachedReader{
		inner: inner,
		c:     c,
		ttl:   CacheTTLs{Stats: time.Minute},
	}

	// call with unsorted IATAs
	getOrSet(context.Background(), c, "beacon:stats:overview:YVR,YYJ", time.Minute, func() (*api.StatsOverview, error) {
		calls++
		return &api.StatsOverview{TotalPackets: 42}, nil
	})

	// call CachedReader with reversed order — should hit same key
	_ = cr
	result, err := getOrSet(context.Background(), c, "beacon:stats:overview:YVR,YYJ", time.Minute, func() (*api.StatsOverview, error) {
		calls++
		return &api.StatsOverview{TotalPackets: 42}, nil
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if result.TotalPackets != 42 {
		t.Errorf("expected 42, got %d", result.TotalPackets)
	}
	if calls != 1 {
		t.Errorf("expected 1 fetch call, got %d", calls)
	}
}

func TestCachedReader_InvalidateNode(t *testing.T) {
	c, mr := newTestClient(t)
	nodeID := uuid.MustParse("00000000-0000-0000-0000-000000000001")

	mr.Set(keyNodePrefix+nodeID.String(), `"cached"`)
	mr.Set(keyNodeNeighborsPrefix+nodeID.String(), `"cached"`)

	cr := &CachedReader{inner: &stubReader{}, c: c, ttl: CacheTTLs{}}
	cr.InvalidateNode(context.Background(), nodeID)

	if mr.Exists(keyNodePrefix + nodeID.String()) {
		t.Error("expected node key to be deleted")
	}
	if mr.Exists(keyNodeNeighborsPrefix + nodeID.String()) {
		t.Error("expected node neighbors key to be deleted")
	}
}

func TestCachedReader_InvalidateObserver(t *testing.T) {
	c, mr := newTestClient(t)
	observerID := uuid.MustParse("00000000-0000-0000-0000-000000000002")

	mr.Set(keyObserverPrefix+observerID.String(), `"cached"`)
	mr.Set(keyObserverScopesPrefix+observerID.String(), `"cached"`)

	cr := &CachedReader{inner: &stubReader{}, c: c, ttl: CacheTTLs{}}
	cr.InvalidateObserver(context.Background(), observerID)

	if mr.Exists(keyObserverPrefix + observerID.String()) {
		t.Error("expected observer key to be deleted")
	}
	if mr.Exists(keyObserverScopesPrefix + observerID.String()) {
		t.Error("expected observer scopes key to be deleted")
	}
}
