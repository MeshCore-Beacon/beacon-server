// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"encoding/hex"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/meshcore-go/meshcore-go"
	"testing"
)

func TestScopeRepeatEvidence(t *testing.T) {
	id, other := uuid.New(), uuid.New()
	scope := "#YKF"
	for width := 1; width <= 3; width++ {
		hash := []byte{0x12, 0x34, 0x56}[:width]
		wire := []byte{meshcore.MakeHeader(meshcore.RouteTypeTransportFlood, meshcore.PayloadTypeGrpTxt, 0), 0, 0, 0, 0, byte((width-1)<<6 | 2)}
		wire = append(append(wire, hash...), hash...)
		p, err := meshcore.PacketFromBytes(append(wire, 0, 0))
		if err != nil {
			t.Fatal(err)
		}
		resolved := map[string][]api.ResolvedPathEntry{hex.EncodeToString(hash): {{NodeID: id}}}
		name, ids := scopeRepeatNodes(p, &scope, resolved)
		if name != scope || len(ids) != 1 || ids[0] != id {
			t.Fatalf("width %d: %q %v", width, name, ids)
		}
		resolved[hex.EncodeToString(hash)] = append(resolved[hex.EncodeToString(hash)], api.ResolvedPathEntry{NodeID: other})
		if _, ids = scopeRepeatNodes(p, &scope, resolved); len(ids) != 0 {
			t.Fatal("ambiguous prefix voted")
		}
		resolved[hex.EncodeToString(hash)] = resolved[hex.EncodeToString(hash)][:1]
		if _, ids = scopeRepeatNodes(p, nil, resolved); len(ids) != 0 {
			t.Fatal("unmatched transport became wildcard")
		}
		for _, route := range []uint8{meshcore.RouteTypeDirect, meshcore.RouteTypeTransportDirect} {
			p.Header = byte(meshcore.PayloadTypeGrpTxt<<2 | route)
			if _, ids = scopeRepeatNodes(p, &scope, resolved); len(ids) != 0 {
				t.Fatal("requested route voted")
			}
		}
		p.Header = byte(meshcore.PayloadTypeTrace<<2 | meshcore.RouteTypeFlood)
		if _, ids = scopeRepeatNodes(p, nil, resolved); len(ids) != 0 {
			t.Fatal("trace SNR voted")
		}
		p.Header = byte(meshcore.PayloadTypeGrpTxt<<2 | meshcore.RouteTypeFlood)
		if name, ids = scopeRepeatNodes(p, nil, resolved); name != "*" || len(ids) != 1 {
			t.Fatal("explicit unscoped flood not recorded")
		}
	}
}
