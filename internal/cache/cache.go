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
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	redis "github.com/redis/go-redis/v9"
)

// Client wraps a Redis client with helper methods used by CachedReader.
type Client struct {
	rdb *redis.Client
}

// NewClient creates a new Redis client from the given address, password, and
// database index. addr should be in "host:port" form e.g. "localhost:6379".
// password may be empty if the Redis instance requires no authentication.
// db is the Redis database index, typically 0.
func NewClient(addr, password string, db int) *Client {
	opts := redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
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

// getOrSet retrieves a cached value by key, deserialising it into T.
// On a cache miss it calls fetch, stores the result under key with the given
// TTL, and returns it. If Redis is unavailable or returns an unexpected error,
// fetch is called directly and the result is not cached. If a cached entry
// fails to unmarshal (corrupt or schema-changed), the entry is overwritten
// with a fresh fetch. Errors from Set are ignored so a Redis hiccup never
// fails a request.
func getOrSet[T any](ctx context.Context, c *Client, key string, ttl time.Duration, fetch func() (T, error)) (T, error) {
	raw, err := c.rdb.Get(ctx, key).Bytes()
	if err != nil && !errors.Is(err, redis.Nil) {
		// real Redis error, degrade gracefully
		return fetch()
	}
	var zero, out T
	if errors.Is(err, redis.Nil) {
		// cache miss — fetch, store, return
		val, err := fetch()
		if err != nil {
			return zero, err
		}
		data, jsonErr := json.Marshal(val)
		if jsonErr != nil {
			return val, nil
		}
		_ = c.rdb.Set(ctx, key, data, ttl)
		return val, nil
	}
	if err = json.Unmarshal(raw, &out); err != nil {
		// corrupt cache entry — overwrite it
		val, err := fetch()
		if err != nil {
			return zero, err
		}
		data, jsonErr := json.Marshal(val)
		if jsonErr != nil {
			return val, nil
		}
		_ = c.rdb.Set(ctx, key, data, ttl)
		return val, nil
	}
	return out, nil
}
