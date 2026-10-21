// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package cache provides a Redis-backed caching layer for the Beacon API.
// It wraps api.Reader with a CachedReader that transparently caches responses
// for read-heavy, slow-changing endpoints. If Redis is unavailable, all
// operations degrade gracefully to the underlying reader.
package cache

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	redis "github.com/redis/go-redis/v9"
	"golang.org/x/sync/singleflight"
)

// Client wraps a Redis client with helper methods used by CachedReader.
type Client struct {
	rdb *redis.Client
	sf  singleflight.Group // one fetch per key across concurrent misses
}

// NewClient creates a new Redis client from the given address, password, and
// database index. addr should be in "host:port" form e.g. "localhost:6379".
// password may be empty if the Redis instance requires no authentication.
// db is the Redis database index, typically 0.
func NewClient(addr, password string, db int) *Client {
	opts := redis.Options{
		Addr:                  addr,
		Password:              password,
		DB:                    db,
		DialTimeout:           500 * time.Millisecond,
		ReadTimeout:           500 * time.Millisecond,
		WriteTimeout:          500 * time.Millisecond,
		PoolTimeout:           500 * time.Millisecond,
		MaxRetries:            -1,
		ContextTimeoutEnabled: true,
	}
	return &Client{
		rdb: redis.NewClient(&opts),
	}
}

// ResolveTTLs builds a CacheTTLs from the given config, applying the fallback
// chain: category TTL → global TTL → default (1h).
func ResolveTTLs(cfg config.CacheConfig) CacheTTLs {
	ttls := CacheTTLs{
		Stats:     resolve(cfg.TTLs.Stats.Duration, cfg.TTL.Duration),
		Reference: resolve(cfg.TTLs.Reference.Duration, cfg.TTL.Duration),
		Nodes:     resolve(cfg.TTLs.Nodes.Duration, cfg.TTL.Duration),
		Observers: resolve(cfg.TTLs.Observers.Duration, cfg.TTL.Duration),
	}
	return ttls
}

// resolve returns the first non-zero duration from category, global, or 1h.
func resolve(category, global time.Duration) time.Duration {
	if category != 0 {
		return category
	}
	if global != 0 {
		return global
	}
	return time.Hour
}

// Ping checks connectivity to Redis. Call this on startup to verify the
// connection before serving requests.
func (c *Client) Ping(ctx context.Context) error {
	return c.rdb.Ping(ctx).Err()
}

// Close closes the underlying Redis connection pool.
func (c *Client) Close() error {
	return c.rdb.Close()
}

func (c *Client) del(ctx context.Context, keys ...string) {
	c.rdb.Del(ctx, keys...)
}

// delPrefix SCANs the whole keyspace; call it rarely.
func (c *Client) delPrefix(ctx context.Context, prefix string) {
	var cursor uint64
	for {
		keys, next, err := c.rdb.Scan(ctx, cursor, prefix+"*", 100).Result()
		if err != nil {
			return
		}
		if len(keys) > 0 {
			c.rdb.Del(ctx, keys...)
		}
		if next == 0 {
			return
		}
		cursor = next
	}
}

// getOrSet retrieves a cached value by key, deserialising it into T.
// On a cache miss it calls fetch, stores the result under key with the given
// TTL, and returns it. Redis failures share the same bounded/coalesced fetch as
// misses, so a cache outage cannot multiply concurrent database work. If a cached entry
// fails to unmarshal (corrupt or schema-changed), the entry is overwritten
// with a fresh fetch. Errors from Set are ignored so a Redis hiccup never
// fails a request.
func getOrSet[T any](ctx context.Context, c *Client, key string, ttl time.Duration, fetch func(context.Context) (T, error)) (T, error) {
	var zero, out T
	if err := ctx.Err(); err != nil {
		return zero, err
	}
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		metrics.CacheRequests.WithLabelValues("error").Inc()
		slog.DebugContext(ctx, "cache bypass", "component", "cache", "reason", "read_error")
	}
	if err == nil {
		if err = json.Unmarshal(raw, &out); err == nil {
			metrics.CacheRequests.WithLabelValues("hit").Inc()
			slog.DebugContext(ctx, "cache hit", "component", "cache")
			return out, nil
		}
		slog.DebugContext(ctx, "cache invalid entry", "component", "cache")
		metrics.CacheRequests.WithLabelValues("invalid").Inc()
	} else {
		if errors.Is(err, redis.Nil) {
			metrics.CacheRequests.WithLabelValues("miss").Inc()
		}
		slog.DebugContext(ctx, "cache miss", "component", "cache")
	}
	raw, err = fetchShared(ctx, c, key, ttl, fetch)
	if err != nil {
		return zero, err
	}
	// Each caller decodes its own copy: handlers may decorate the value they get.
	if err := json.Unmarshal(raw, &out); err != nil {
		return zero, err
	}
	return out, nil
}

// sharedFetchTimeout bounds a fetch that no single request owns.
const sharedFetchTimeout = 30 * time.Second

// fetchShared runs one fetch per key across concurrent misses and returns its JSON. The fetch
// gets its own bounded context so one caller disconnecting can't fail the others, and each
// caller still stops waiting when its own context ends.
func fetchShared[T any](ctx context.Context, c *Client, key string, ttl time.Duration, fetch func(context.Context) (T, error)) ([]byte, error) {
	ch := c.sf.DoChan(key, func() (any, error) {
		fctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), sharedFetchTimeout)
		defer cancel()
		// A previous flight may have filled the key after this caller's miss.
		// Recheck inside the group so late callers do not start another DB read.
		if data, err := c.rdb.Get(fctx, key).Bytes(); err == nil {
			var cached T
			if json.Unmarshal(data, &cached) == nil {
				return data, nil
			}
		}
		val, err := fetch(fctx)
		if err != nil {
			return nil, err
		}
		data, err := json.Marshal(val)
		if err != nil {
			return nil, err
		}
		_ = c.rdb.Set(fctx, key, data, ttl)
		return data, nil
	})
	select {
	case r := <-ch:
		if r.Err != nil {
			return nil, r.Err
		}
		return r.Val.([]byte), nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
