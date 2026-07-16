// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package presence batches the per-packet presence bookkeeping (observer and
// packet last-seen bumps) that would otherwise hit Postgres as one tiny
// transaction per observation. Repeat writes are absorbed in memory and
// flushed on an interval as one batched statement per table; first-time
// writes (new observer, new packet, new broker pair) still write through so
// rows exist before anything references them.
package presence

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
)

// Store is the database surface the coalescer decorates: the full ingest
// interface plus the batched flush queries.
type Store interface {
	ingest.DB

	// TouchObservers applies coalesced last_seen bumps and observation_count
	// deltas for the given observer IDs in one statement.
	TouchObservers(ctx context.Context, ids []uuid.UUID, seen []time.Time, counts []int32) error

	// TouchObserverBrokers applies coalesced last_seen/last_packet_at bumps
	// for the given (observer, broker) pairs in one statement.
	TouchObserverBrokers(ctx context.Context, ids []uuid.UUID, brokers []string, seen []time.Time) error

	// TouchPackets applies coalesced last_heard_at bumps for the given packet
	// hashes in one statement.
	TouchPackets(ctx context.Context, hashes [][]byte, heard []time.Time) error
}

type identity struct {
	id   uuid.UUID
	name string
}

type observerBump struct {
	seen  time.Time
	count int32
}

type brokerKey struct {
	id     uuid.UUID
	broker string
}

// Coalescer wraps a Store and absorbs repeat presence writes in memory,
// flushing them on an interval. All other ingest.DB calls pass through.
type Coalescer struct {
	Store
	flushInterval time.Duration
	packetTTL     time.Duration
	now           func() time.Time

	mu             sync.Mutex
	identities     map[string]identity // pubkey -> observer row
	dirtyObservers map[uuid.UUID]observerBump
	knownBrokers   map[brokerKey]struct{}
	dirtyBrokers   map[brokerKey]time.Time
	knownIATAs     map[string]struct{}
	packetsSeen    map[string]time.Time // hash -> last observation
	dirtyPackets   map[string]time.Time
}

// New creates a Coalescer flushing every flushInterval. packetTTL controls
// how long a quiet packet hash stays suppressed before a re-observation
// writes through again.
func New(store Store, flushInterval, packetTTL time.Duration) *Coalescer {
	return &Coalescer{
		Store:          store,
		flushInterval:  flushInterval,
		packetTTL:      packetTTL,
		now:            time.Now,
		identities:     make(map[string]identity),
		dirtyObservers: make(map[uuid.UUID]observerBump),
		knownBrokers:   make(map[brokerKey]struct{}),
		dirtyBrokers:   make(map[brokerKey]time.Time),
		knownIATAs:     make(map[string]struct{}),
		packetsSeen:    make(map[string]time.Time),
		dirtyPackets:   make(map[string]time.Time),
	}
}

// UpsertObserver serves repeat lookups from the identity cache and records a
// last_seen/observation_count bump instead of writing through.
func (c *Coalescer) UpsertObserver(ctx context.Context, pubkey []byte) (uuid.UUID, string, error) {
	key := string(pubkey)
	c.mu.Lock()
	if ident, ok := c.identities[key]; ok {
		bump := c.dirtyObservers[ident.id]
		bump.seen = c.now()
		bump.count++
		c.dirtyObservers[ident.id] = bump
		c.mu.Unlock()
		return ident.id, ident.name, nil
	}
	c.mu.Unlock()

	id, name, err := c.Store.UpsertObserver(ctx, pubkey)
	if err != nil {
		return id, name, err
	}
	c.mu.Lock()
	c.identities[key] = identity{id: id, name: name}
	c.mu.Unlock()
	return id, name, nil
}

// UpsertObserverBroker writes through the first time a pair is seen and
// records a bump afterwards.
func (c *Coalescer) UpsertObserverBroker(ctx context.Context, observerID uuid.UUID, brokerName string) error {
	key := brokerKey{id: observerID, broker: brokerName}
	c.mu.Lock()
	if _, ok := c.knownBrokers[key]; ok {
		c.dirtyBrokers[key] = c.now()
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if err := c.Store.UpsertObserverBroker(ctx, observerID, brokerName); err != nil {
		return err
	}
	c.mu.Lock()
	c.knownBrokers[key] = struct{}{}
	c.mu.Unlock()
	return nil
}

// UpsertIATA only hits the store the first time each code shows up; the row
// is insert-once so there is nothing to bump.
func (c *Coalescer) UpsertIATA(ctx context.Context, iata string) error {
	c.mu.Lock()
	if _, ok := c.knownIATAs[iata]; ok {
		c.mu.Unlock()
		return nil
	}
	c.mu.Unlock()

	if err := c.Store.UpsertIATA(ctx, iata); err != nil {
		return err
	}
	c.mu.Lock()
	c.knownIATAs[iata] = struct{}{}
	c.mu.Unlock()
	return nil
}

// UpsertPacket suppresses the last_heard_at bump for hashes seen recently and
// records it for the next flush instead. Hashes with no re-observation within
// packetTTL are forgotten, so the next occurrence writes through again.
func (c *Coalescer) UpsertPacket(ctx context.Context, p ingest.UpsertPacketParams) (bool, error) {
	key := string(p.PacketHash)
	c.mu.Lock()
	if _, ok := c.packetsSeen[key]; ok {
		now := c.now()
		c.packetsSeen[key] = now
		c.dirtyPackets[key] = now
		c.mu.Unlock()
		return false, nil
	}
	c.mu.Unlock()

	isNew, err := c.Store.UpsertPacket(ctx, p)
	if err != nil {
		return isNew, err
	}
	c.mu.Lock()
	c.packetsSeen[key] = c.now()
	c.mu.Unlock()
	return isNew, nil
}

// UpdateObserverStatus passes through and drops the cached identity, since a
// status message can set the display name returned by UpsertObserver.
func (c *Coalescer) UpdateObserverStatus(ctx context.Context, p ingest.UpdateObserverStatusParams) (uuid.UUID, error) {
	id, err := c.Store.UpdateObserverStatus(ctx, p)
	if err == nil {
		c.mu.Lock()
		delete(c.identities, string(p.PublicKey))
		c.mu.Unlock()
	}
	return id, err
}

// Run flushes on the configured interval until ctx is cancelled, then does a
// final flush so a clean shutdown loses nothing.
func (c *Coalescer) Run(ctx context.Context) {
	ticker := time.NewTicker(c.flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			c.Flush(ctx)
		case <-ctx.Done():
			flushCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			c.Flush(flushCtx)
			cancel()
			return
		}
	}
}

// Flush writes all dirty presence state to the store in batched statements,
// one per table. Failed batches are dropped rather than retried: presence
// data self-heals on the next observation.
func (c *Coalescer) Flush(ctx context.Context) {
	c.mu.Lock()
	observers := c.dirtyObservers
	brokers := c.dirtyBrokers
	packets := c.dirtyPackets
	c.dirtyObservers = make(map[uuid.UUID]observerBump)
	c.dirtyBrokers = make(map[brokerKey]time.Time)
	c.dirtyPackets = make(map[string]time.Time)
	cutoff := c.now().Add(-c.packetTTL)
	for key, seen := range c.packetsSeen {
		if seen.Before(cutoff) {
			delete(c.packetsSeen, key)
		}
	}
	c.mu.Unlock()

	if len(observers) > 0 {
		ids := make([]uuid.UUID, 0, len(observers))
		seen := make([]time.Time, 0, len(observers))
		counts := make([]int32, 0, len(observers))
		for id, bump := range observers {
			ids = append(ids, id)
			seen = append(seen, bump.seen)
			counts = append(counts, bump.count)
		}
		if err := c.Store.TouchObservers(ctx, ids, seen, counts); err != nil {
			log.Printf("presence: flush observers failed (%d rows dropped): %v", len(ids), err)
		}
	}

	if len(brokers) > 0 {
		ids := make([]uuid.UUID, 0, len(brokers))
		names := make([]string, 0, len(brokers))
		seen := make([]time.Time, 0, len(brokers))
		for key, ts := range brokers {
			ids = append(ids, key.id)
			names = append(names, key.broker)
			seen = append(seen, ts)
		}
		if err := c.Store.TouchObserverBrokers(ctx, ids, names, seen); err != nil {
			log.Printf("presence: flush observer brokers failed (%d rows dropped): %v", len(ids), err)
		}
	}

	if len(packets) > 0 {
		hashes := make([][]byte, 0, len(packets))
		heard := make([]time.Time, 0, len(packets))
		for key, ts := range packets {
			hashes = append(hashes, []byte(key))
			heard = append(heard, ts)
		}
		if err := c.Store.TouchPackets(ctx, hashes, heard); err != nil {
			log.Printf("presence: flush packets failed (%d rows dropped): %v", len(hashes), err)
		}
	}
}
