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

// mark records the hearing and reports whether it is a new path for a (packet, observer) already
// heard over another one. Keying the first hearing by pair, not by which worker won the insert,
// means a broker copy of it is never a repeat however the two workers interleave.
func (s *sentPaths) mark(packetHash, observerID, path []byte, now time.Time) bool {
	pair, hearing := s.key(packetHash, observerID), s.key(packetHash, observerID, path)
	s.mu.Lock()
	defer s.mu.Unlock()
	firstHearing := s.add(pair, now)
	return s.add(hearing, now) && !firstHearing
}

func (s *sentPaths) key(parts ...[]byte) uint64 {
	var h maphash.Hash
	h.SetSeed(s.seed)
	for _, part := range parts {
		h.WriteByte(byte(len(part)))
		h.Write(part)
	}
	return h.Sum64()
}

// add records key unless already present within the TTL and reports whether it was new.
func (s *sentPaths) add(key uint64, now time.Time) bool {
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
