// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"sync"
)

// channelKey identifies a channels row by its conflict key. Hash-only rows have a
// NULL key_fingerprint, which is distinct from an empty one.
type channelKey struct {
	hash, fp string
	hashOnly bool
}

// ChannelIDCache maps channel keys to row ids so a hearing of a known channel can
// update its row by id instead of running an upsert, which burns a sequence value
// every time. It has no eviction: hash-only rows are bounded by the 256 one-byte
// hashes and keyed rows by the configured and imported keys. Safe for concurrent use.
type ChannelIDCache struct {
	mu  sync.RWMutex
	ids map[channelKey]int32
}

func NewChannelIDCache() *ChannelIDCache {
	return &ChannelIDCache{ids: make(map[channelKey]int32)}
}

// Len returns the number of cached keys.
func (c *ChannelIDCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.ids)
}

func (c *ChannelIDCache) get(k channelKey) (int32, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	id, ok := c.ids[k]
	return id, ok
}

func (c *ChannelIDCache) put(k channelKey, id int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids[k] = id
}

func (c *ChannelIDCache) forget(k channelKey) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.ids, k)
}

// replace swaps in m as the whole cache; the caller must not use m afterwards.
func (c *ChannelIDCache) replace(m map[channelKey]int32) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ids = m
}

// Option configures a Store built by New.
type Option func(*Store)

// WithChannelIDs makes the store write channel hearings through c. Stores that write
// channels share one cache.
func WithChannelIDs(c *ChannelIDCache) Option {
	return func(s *Store) { s.channelIDs = c }
}

// PreloadChannelIDs loads every channel key and id into the store's cache. A store
// without a cache does nothing. On error the cache is left as it was.
func (s *Store) PreloadChannelIDs(ctx context.Context) error {
	if s.channelIDs == nil {
		return nil
	}
	rows, err := s.q.ListChannelIDs(ctx)
	if err != nil {
		return err
	}
	m := make(map[channelKey]int32, len(rows))
	for _, r := range rows {
		if r.KeyFingerprint == nil {
			m[channelKey{hash: string(r.ChannelHash), hashOnly: true}] = r.ID
		} else {
			m[channelKey{hash: string(r.ChannelHash), fp: string(r.KeyFingerprint)}] = r.ID
		}
	}
	s.channelIDs.replace(m)
	return nil
}
