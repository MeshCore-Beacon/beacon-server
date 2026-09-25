// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"log/slog"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/meshcore-go/meshcore-go"
)

// packetEndpoints is what a packet's payload says about its sender and recipient.
type packetEndpoints struct {
	advert              bool   // source is the advert's own key, not a hash
	advertKey           []byte // nil for legacy adverts stored without one
	source, destination []byte // one-byte hashes
}

func parsePacketEndpoints(payloadType int16, raw, originPubkey []byte) packetEndpoints {
	var ep packetEndpoints
	switch payloadType {
	case int16(meshcore.PayloadTypeAdvert):
		ep.advert, ep.advertKey = true, originPubkey
	case int16(meshcore.PayloadTypeAnonReq):
		if p, err := meshcore.AnonReqFromBytes(raw); err == nil {
			ep.destination = []byte{p.Destination}
		}
	case int16(meshcore.PayloadTypeReq):
		if p, err := meshcore.RequestFromBytes(raw); err == nil {
			ep.source, ep.destination = []byte{p.Source}, []byte{p.Destination}
		}
	case int16(meshcore.PayloadTypeResponse):
		if p, err := meshcore.ResponseFromBytes(raw); err == nil {
			ep.source, ep.destination = []byte{p.Source}, []byte{p.Destination}
		}
	case int16(meshcore.PayloadTypeTxtMsg):
		if p, err := meshcore.TextMessageFromBytes(raw); err == nil {
			ep.source, ep.destination = []byte{p.Source}, []byte{p.Destination}
		}
	case int16(meshcore.PayloadTypePath):
		if p, err := meshcore.PathFromBytes(raw); err == nil {
			ep.source, ep.destination = []byte{p.Source}, []byte{p.Destination}
		}
	}
	return ep
}

type endpointLookup struct {
	iata string
	ep   packetEndpoints
}

// resolveEndpoints resolves a whole page of endpoints in at most two queries.
// Results line up with lookups; nil means no such endpoint or a failed lookup.
func (s *Store) resolveEndpoints(ctx context.Context, lookups []endpointLookup) (sources, destinations []*api.ResolvedHop) {
	iatas, hashes, keys := map[string]bool{}, map[byte]bool{}, map[string][]byte{}
	for _, l := range lookups {
		for _, h := range [][]byte{l.ep.source, l.ep.destination} {
			if len(h) == 1 {
				iatas[l.iata], hashes[h[0]] = true, true
			}
		}
		if len(l.ep.advertKey) > 0 {
			keys[string(l.ep.advertKey)] = l.ep.advertKey
		}
	}

	candidates := map[string]map[string][]api.ResolvedPathEntry{} // iata -> hash hex
	hashesOK := len(hashes) > 0
	if hashesOK {
		params := sqlc.ResolveEndpointHashPairsParams{}
		for iata := range iatas {
			params.Iatas = append(params.Iatas, iata)
		}
		for h := range hashes {
			params.Hashes = append(params.Hashes, []byte{h})
		}
		rows, err := s.q.ResolveEndpointHashPairs(ctx, params)
		if err != nil {
			slog.Error("store: endpoint resolution failed", "component", "db", "error", err)
			hashesOK = false
		}
		for _, r := range rows {
			if candidates[r.Iata] == nil {
				candidates[r.Iata] = map[string][]api.ResolvedPathEntry{}
			}
			key := hex.EncodeToString(r.Hash)
			candidates[r.Iata][key] = append(candidates[r.Iata][key], api.ResolvedPathEntry{
				NodeID: r.NodeID, Name: r.Name, Latitude: r.Latitude,
				Longitude: r.Longitude, PublicKey: r.PublicKey,
			})
		}
	}

	adverts := map[string]*api.ResolvedNode{}
	advertsOK := true
	if len(keys) > 0 {
		pubkeys := make([][]byte, 0, len(keys))
		for _, k := range keys {
			pubkeys = append(pubkeys, k)
		}
		rows, err := s.q.GetNodesByPubkeys(ctx, pubkeys)
		if err != nil {
			slog.Error("store: advert source lookup failed", "component", "db", "error", err)
			advertsOK = false
		}
		for _, r := range rows {
			adverts[string(r.PublicKey)] = &api.ResolvedNode{
				ID: r.ID, Name: r.Name, PublicKey: hex.EncodeToString(r.PublicKey),
				Latitude: r.Latitude, Longitude: r.Longitude,
			}
		}
	}

	hop := func(iata string, h []byte) *api.ResolvedHop {
		if len(h) != 1 || !hashesOK {
			return nil
		}
		r := api.BuildResolvedPath([][]byte{h}, candidates[iata])[0]
		return &r
	}
	sources = make([]*api.ResolvedHop, len(lookups))
	destinations = make([]*api.ResolvedHop, len(lookups))
	for i, l := range lookups {
		if l.ep.advert {
			if advertsOK {
				r := api.ResolveExactNode(adverts[string(l.ep.advertKey)])
				sources[i] = &r
			}
		} else {
			sources[i] = hop(l.iata, l.ep.source)
		}
		destinations[i] = hop(l.iata, l.ep.destination)
	}
	return sources, destinations
}

// fillLatestEndpoints resolves the latest observer's endpoints for a page of packets.
// lookups line up with items; rows without a latest observer are skipped.
func (s *Store) fillLatestEndpoints(ctx context.Context, items []api.PacketSummary, lookups []endpointLookup) {
	sources, destinations := s.resolveEndpoints(ctx, lookups)
	for i := range items {
		if lo := items[i].LatestObserver; lo != nil {
			lo.ResolvedSource, lo.ResolvedDestination = sources[i], destinations[i]
		}
	}
}
