// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/meshcore-go/meshcore-go"
)

type endpointLookupDB struct {
	*stubDB
	calls [][][]byte
}

func (d *endpointLookupDB) ResolveEndpointHashes(_ context.Context, _ string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	d.calls = append(d.calls, hashes)
	return nil, nil
}
func endpointTestPacket() *meshcore.Packet {
	return &meshcore.Packet{Header: meshcore.MakeHeader(meshcore.RouteTypeFlood, meshcore.PayloadTypeTxtMsg, 0), Payload: append([]byte{0xbb, 0xaa, 0, 0}, make([]byte, 16)...)}
}
func TestEndpointLookupsOnlyForLiveHearings(t *testing.T) {
	r := newRepeatHarness(t, true)
	d := &endpointLookupDB{stubDB: r.db}
	r.w.db = d
	packet := endpointTestPacket()
	hear := func(inserted bool, path byte) {
		d.observationInserted = inserted
		packet.Path = []byte{path}
		packet.PathLength = 1
		r.w.handlePacket(r.ctx, "YOW", "0102", packetEnvelope(t, packet))
	}
	hear(true, 0x11)
	first := len(d.calls)
	if first == 0 {
		t.Fatal("first hearing missing endpoint lookup")
	}
	hear(false, 0x11)
	if len(d.calls) != first {
		t.Fatal("suppressed copy resolved endpoints")
	}
	hear(false, 0x22)
	if len(d.calls) != 2*first {
		t.Fatal("new path missing endpoint lookup")
	}
	hear(false, 0x22)
	if len(d.calls) != 2*first {
		t.Fatal("suppressed repeat resolved endpoints")
	}
}
