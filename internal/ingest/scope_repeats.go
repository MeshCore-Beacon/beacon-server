// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"crypto/sha256"
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

type ScopeObservation struct {
	NodeIDs                       []uuid.UUID
	ObserverID                    uuid.UUID
	IATA, Scope                   string
	HeardAt                       time.Time
	HashBytes                     uint8
	PathKey                       []byte
	UnresolvedHops, AmbiguousHops int64
}

func (w *Worker) recordScopeRepeats(ctx context.Context, packet *meshcore.Packet, matched *string, resolved map[string][]api.ResolvedPathEntry, heardAt time.Time, observerID uuid.UUID, iata string) {
	scope, ids := scopeRepeatNodes(packet, matched, resolved)
	if scope == "" {
		return
	}
	p := ScopeObservation{NodeIDs: ids, ObserverID: observerID, IATA: iata, Scope: scope, HeardAt: heardAt, HashBytes: packet.PathHashSize()}
	digest := sha256.New()
	hash := packet.PacketHash()
	digest.Write(hash[:])
	digest.Write([]byte{packet.PathHashSize()})
	digest.Write(packet.Path)
	// Full resolved identities are part of the sample key: equal short prefixes in
	// different resolution contexts must never be collapsed into one claimed route.
	for _, id := range ids {
		digest.Write(id[:])
	}
	p.PathKey = digest.Sum(nil)
	for _, hash := range packet.PathHashes() {
		entries := resolved[hex.EncodeToString(hash)]
		if len(entries) == 0 {
			p.UnresolvedHops++
		} else if len(entries) > 1 {
			p.AmbiguousHops++
		}
	}
	if err := w.db.RecordScopeObservation(ctx, p); err != nil {
		w.log.Error("scope repeat evidence write failed", "error", err)
	}
}
