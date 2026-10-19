// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"log/slog"
)

// ObservationPayloadTypeKey is the background_state key of the observation payload-type backfill.
const ObservationPayloadTypeKey = "observation_payload_type"

// ObservationPayloadTypeState is that key's value: the next id to examine, and whether every
// observation below the walk's end carries its payload type.
type ObservationPayloadTypeState struct {
	NextID int64 `json:"next_id"`
	Done   bool  `json:"done"`
}

// WithPayloadTypeGate makes ListObserverAdverts read the backfill state and switch to the
// observation-typed statement once it reports done.
func WithPayloadTypeGate() Option {
	return func(s *Store) { s.payloadTypeGate = true }
}

// payloadTypesReady reports whether every observation carries its payload type. Without the
// gate it is false. Once the backfill state reads done, the answer is cached for the life of
// the store; until then each call reads the state again. A read error counts as not done.
func (s *Store) payloadTypesReady(ctx context.Context) bool {
	if !s.payloadTypeGate {
		return false
	}
	if s.payloadTypesDone.Load() {
		return true
	}
	var st ObservationPayloadTypeState
	found, err := s.LoadState(ctx, ObservationPayloadTypeKey, &st)
	if err != nil {
		slog.Warn("db: reading the payload-type backfill state failed", "component", "db", "error", err)
		return false
	}
	if found && st.Done {
		s.payloadTypesDone.Store(true)
		return true
	}
	return false
}
