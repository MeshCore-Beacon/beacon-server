// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"sync"

	"github.com/google/uuid"
)

const capabilityCacheLimit = 4096

type capabilityCache struct {
	mu    sync.Mutex
	nodes map[uuid.UUID]uint8
}

func (w *Worker) runCapabilityDetection(ctx context.Context, payloadType uint8, hashSize uint8, resolvedNodeIDs []uuid.UUID) {
	var flag uint8
	switch {
	case payloadType != 0x09 && (hashSize == 2 || hashSize == 3):
		flag = 1
	case payloadType == 0x09 && (hashSize == 2 || hashSize == 4):
		flag = 2
	default:
		return
	}
	for _, nodeID := range resolvedNodeIDs {
		w.capabilities.mu.Lock()
		recorded := w.capabilities.nodes[nodeID]&flag != 0
		w.capabilities.mu.Unlock()
		if recorded {
			continue
		}
		if err := w.db.SetNodeCapability(ctx, nodeID, flag == 1, flag == 2); err != nil {
			continue
		}
		w.capabilities.mu.Lock()
		if w.capabilities.nodes == nil {
			w.capabilities.nodes = make(map[uuid.UUID]uint8)
		}
		// Eviction only costs another write; capabilities never downgrade.
		if len(w.capabilities.nodes) >= capabilityCacheLimit {
			clear(w.capabilities.nodes)
		}
		w.capabilities.nodes[nodeID] |= flag
		w.capabilities.mu.Unlock()
	}
}
