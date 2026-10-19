// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"log/slog"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

// ObservationPayloadTypeKey is the background_state key of the observation payload-type backfill.
const ObservationPayloadTypeKey = "observation_payload_type"

// ObservationPayloadTypeState is that key's value: the next id to examine, and whether every
// observation below the walk's end carries its payload type.
type ObservationPayloadTypeState struct {
	NextID int64 `json:"next_id"`
	Done   bool  `json:"done"`
}

// payloadTypeWarnEvery spaces the Warn logged when the gate cannot read the backfill state.
const payloadTypeWarnEvery = time.Minute

// WithPayloadTypeGate makes ListObserverAdverts read the backfill state and switch to the
// observation-typed statement once it reports done.
func WithPayloadTypeGate() Option {
	return func(s *Store) { s.payloadTypeGate = true }
}

// payloadTypesReady reports whether every observation carries its payload type. Without the
// gate it is false. Once the backfill state reads done, the answer is cached for the life of
// the store; until then each call reads the state again. A read error counts as not done and
// is logged at most once per payloadTypeWarnEvery.
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
		now := time.Now().UnixNano()
		if last := s.payloadTypeWarnAt.Load(); (last == 0 || now-last >= int64(payloadTypeWarnEvery)) && s.payloadTypeWarnAt.CompareAndSwap(last, now) {
			slog.Warn("db: reading the payload-type backfill state failed", "component", "db", "error", err)
		}
		return false
	}
	if found && st.Done {
		s.payloadTypesDone.Store(true)
		return true
	}
	return false
}

// ObservationIDRange returns min(id) and max(id) of packet_observations, both 0 when it is empty.
func (s *Store) ObservationIDRange(ctx context.Context) (minID, maxID int64, err error) {
	r, err := s.q.GetObservationIDRange(ctx)
	if err != nil {
		return 0, 0, err
	}
	return r.MinID, r.MaxID, nil
}

// BackfillObservationPayloadTypes types, from their own packet, the first maxRows observations in
// [fromID, toID) that carry no payload type, in id order. It returns the rows typed and where the
// walk continues: after the last row it picked when it picked maxRows, otherwise toID.
func (s *Store) BackfillObservationPayloadTypes(ctx context.Context, fromID, toID int64, maxRows int32) (typed, next int64, err error) {
	r, err := s.q.BackfillObservationPayloadTypes(ctx, sqlc.BackfillObservationPayloadTypesParams{FromID: fromID, ToID: toID, MaxRows: maxRows})
	if err != nil {
		return 0, 0, err
	}
	if r.Picked >= int64(maxRows) {
		return r.Typed, r.LastID + 1, nil
	}
	return r.Typed, toID, nil
}
