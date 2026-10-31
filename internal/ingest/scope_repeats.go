// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/hex"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
)

// Only accumulated flood paths demonstrate repeating. Direct paths are requested
// routes; TRACE's header bytes are SNR, not identities. Unknown/ambiguous prefixes
// never vote. SQL checks the resolved full IDs are repeaters, not companions.
func scopeRepeatNodes(packet *meshcore.Packet, matched *string, resolved map[string][]api.ResolvedPathEntry) (string, []uuid.UUID) {
	if packet.PayloadType() == meshcore.PayloadTypeTrace {
		return "", nil
	}
	scope := "*"
	switch packet.RouteType() {
	case meshcore.RouteTypeFlood:
	case meshcore.RouteTypeTransportFlood:
		scope = "?" // scoped on the wire, but its name cannot be resolved
		if matched != nil {
			scope = *matched
		}
	default:
		return "", nil
	}
	size := packet.PathHashSize()
	if size < 1 || size > 3 || len(packet.Path) != int(size)*int(packet.PathHashCount()) {
		return "", nil
	}
	seen := map[uuid.UUID]bool{}
	ids := []uuid.UUID{}
	for _, hash := range packet.PathHashes() {
		entries := resolved[hex.EncodeToString(hash)]
		if len(entries) != 1 || seen[entries[0].NodeID] {
			continue
		}
		seen[entries[0].NodeID] = true
		ids = append(ids, entries[0].NodeID)
	}
	return scope, ids
}

func (w *Worker) recordScopeRepeats(ctx context.Context, packet *meshcore.Packet, matched *string, resolved map[string][]api.ResolvedPathEntry, heardAt time.Time) {
	scope, ids := scopeRepeatNodes(packet, matched, resolved)
	if len(ids) == 0 {
		return
	}
	if err := w.db.RecordScopeRepeats(ctx, ids, scope, heardAt, packet.PathHashSize()); err != nil {
		w.log.Error("scope repeat evidence write failed", "error", err)
	}
}
