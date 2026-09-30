// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package hub

import (
	"hash/maphash"
	"sync"
	"time"
)

const (
	sentPathsTTL = 2 * time.Minute
	sentPathsMax = 200_000
)

// sentPaths remembers recently heard (packet, observer, path) hearings. It is shared by all
// broker workers; keys are hashed so memory stays small at the size bound.
type sentPaths struct {
	mu    sync.Mutex
	seed  maphash.Seed
	ttl   time.Duration
	max   int
	at    map[uint64]time.Time
	order []sentPath // oldest first
}

type sentPath struct {
	key uint64
	at  time.Time
}

func newSentPaths(ttl time.Duration, max int) *sentPaths {
	return &sentPaths{seed: maphash.MakeSeed(), ttl: ttl, max: max, at: make(map[uint64]time.Time)}
}

// mark records the hearing and reports whether it was not already recorded within the TTL.
func (s *sentPaths) mark(packetHash, observerID, path []byte, now time.Time) bool {
	var h maphash.Hash
	h.SetSeed(s.seed)
	for _, part := range [][]byte{packetHash, observerID, path} {
		h.WriteByte(byte(len(part)))
		h.Write(part)
	}
	key := h.Sum64()

	s.mu.Lock()
	defer s.mu.Unlock()
	for len(s.order) > 0 && (now.Sub(s.order[0].at) >= s.ttl || len(s.order) >= s.max) {
		delete(s.at, s.order[0].key)
		s.order = s.order[1:]
	}
	if _, ok := s.at[key]; ok {
		return false
	}
	s.at[key] = now
	s.order = append(s.order, sentPath{key, now})
	return true
}
