// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"

	"github.com/google/uuid"
)

// runCapabilityDetection checks hash sizes and flips firmware capability flags.
// Called only when the observation INSERT succeeded (no dedup conflict).
//
// Rules (from design doc):
//   - hash_size == 1: do nothing (proves nothing about firmware)
//   - duplicate hash prefixes within the path: skip entirely
//   - non-trace + hash_size 2 or 3 → supports_multibyte_paths = TRUE
//   - trace (0x09)  + hash_size 2 or 4 → supports_multibyte_traces = TRUE
func (w *Worker) runCapabilityDetection(ctx context.Context, payloadType uint8, hashSize uint8, resolvedNodeIDs []uuid.UUID) {
	if hashSize < 2 {
		return
	}
	for _, nodeID := range resolvedNodeIDs {
		switch {
		case payloadType != 0x09 && (hashSize == 2 || hashSize == 3):
			_ = w.db.SetNodeCapability(ctx, nodeID, true, false)
		case payloadType == 0x09 && (hashSize == 2 || hashSize == 4):
			_ = w.db.SetNodeCapability(ctx, nodeID, false, true)
		}
	}
}
