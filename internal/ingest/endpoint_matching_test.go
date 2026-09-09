// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/hex"
	"reflect"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/meshcore-go/meshcore-go"
)

type endpointRoutingDB struct {
	*stubDB
	endpoints []string
	paths     []string
}

func (s *endpointRoutingDB) ResolveEndpointHashes(_ context.Context, iata string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	for _, hash := range hashes {
		s.endpoints = append(s.endpoints, iata+":"+hex.EncodeToString(hash))
	}
	return nil, nil
}
func (s *endpointRoutingDB) ResolvePathHashes(_ context.Context, iata string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	for _, hash := range hashes {
		s.paths = append(s.paths, iata+":"+hex.EncodeToString(hash))
	}
	return nil, nil
}

func TestHandlePacketSeparatesEndpointAndRelayMatching(t *testing.T) {
	for _, kind := range []uint8{meshcore.PayloadTypeReq, meshcore.PayloadTypeResponse, meshcore.PayloadTypeTxtMsg, meshcore.PayloadTypePath} {
		w, base := newTestWorker()
		db := &endpointRoutingDB{stubDB: base}
		w.db = db
		packet := &meshcore.Packet{Header: meshcore.MakeHeader(meshcore.RouteTypeFlood, kind, 0),
			PathLength: 1, Path: []byte{0xcc}, Payload: append([]byte{0xbb, 0xaa, 0, 0}, make([]byte, 16)...)}
		w.handlePacket(context.Background(), "YVR", "0102", packetEnvelope(t, packet))
		if !reflect.DeepEqual(db.endpoints, []string{"YVR:aa", "YVR:bb"}) {
			t.Errorf("payload %d endpoint dispatch: %v", kind, db.endpoints)
		}
		if !reflect.DeepEqual(db.paths, []string{"YVR:cc"}) {
			t.Errorf("payload %d relay dispatch: %v", kind, db.paths)
		}
	}
}
