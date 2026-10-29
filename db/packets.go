// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/meshcore-go/meshcore-go"
)

func (s *Store) UpsertPacket(ctx context.Context, p ingest.UpsertPacketParams) (bool, error) {
	var regionCode, subRegionCode *int32
	hasTransportCodes := len(p.TransportCodes) == 4
	if hasTransportCodes {
		r := int32(binary.LittleEndian.Uint16(p.TransportCodes[0:2]))
		s := int32(binary.LittleEndian.Uint16(p.TransportCodes[2:4]))
		regionCode = &r
		subRegionCode = &s
	}
	params := sqlc.UpsertPacketParams{
		PacketHash:            p.PacketHash,
		PayloadType:           int16(p.PayloadType),
		PayloadVersion:        int16(p.PayloadVersion),
		RouteType:             int16(p.RouteType),
		TransportCodesPresent: &hasTransportCodes,
		RegionCode:            regionCode,
		SubRegionCode:         subRegionCode,
		OriginPubkey:          p.OriginPubkey,
		RawPayload:            p.RawPayload,
		RawHeader:             p.RawHeader,
		ParsedPayload:         p.ParsedPayload,
		ChannelHash:           p.ChannelHash,
		ScopeID:               p.ScopeID,
		TraceTag:              p.TraceTag,
	}
	row, err := s.q.UpsertPacket(ctx, params)
	if err != nil {
		return false, err
	}
	return row.Inserted, nil
}

func (s *Store) SetPacketDecrypted(ctx context.Context, hash []byte) error {
	return s.q.SetPacketDecrypted(ctx, hash)
}

// buildLatestObserverPath fills in PacketLatestObserver's optional path fields from the
// nullable path_length_byte/hash_size/hop_count/path_bytes columns joined in alongside the
// latest (or matching) observation. hashSize/hopCount nil means no observation joined at all
// (only possible via ListPackets/ListPacketsByIATAs' LEFT JOIN LATERAL -- ListPacketsAfterID's
// inner join always has them, callers there can pass &v.Field directly).
func buildLatestObserverPath(pathLengthByte, hashSize, hopCount *int16, pathBytes []byte) (*api.PacketPathLength, *string) {
	if hashSize == nil || hopCount == nil {
		return nil, nil
	}
	raw := ""
	if pathLengthByte != nil {
		raw = fmt.Sprintf("%02x", *pathLengthByte)
	}
	pathLength := &api.PacketPathLength{
		Raw:      raw,
		HashSize: *hashSize,
		HopCount: *hopCount,
	}
	var pathBytesHex *string
	if pathBytes != nil {
		s := hex.EncodeToString(pathBytes)
		pathBytesHex = &s
	}
	return pathLength, pathBytesHex
}

// packetRow is the shared row of the six global packets statements; the other five Row types convert to it.
type packetRow = sqlc.ListPacketsPageRow

// ListPackets serves the packet list. Region (iatas) requests page by site-local recency (listPacketsByRegion).
// Every other request is normalised and sent to the one statement for its filter shape. pageCursor wins over
// cursor wherever a page can return nextPageCursor.
func (s *Store) ListPackets(ctx context.Context, payloadTypes, routeTypes []int16, iatas []string, scopes []string, since, until time.Time, cursor int64, pageCursor *api.PacketCursor, limit int32) (api.PacketPage, error) {
	if len(iatas) > 0 {
		return s.listPacketsByRegion(ctx, payloadTypes, routeTypes, iatas, scopes, since, until, cursor, pageCursor, limit)
	}
	f, ok := newPacketFilter(payloadTypes, routeTypes, scopes, since, until, cursor, pageCursor, limit)
	if !ok {
		return api.PacketPage{Page: api.Page[api.PacketSummary]{Items: []api.PacketSummary{}}}, nil
	}
	rows, err := s.listPacketRows(ctx, f)
	if err != nil {
		return api.PacketPage{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	page := api.PacketPage{Page: api.Page[api.PacketSummary]{
		Items:   s.packetSummaries(ctx, rows),
		HasMore: hasMore,
	}}
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		ms := last.LastHeardAt.Time.UnixMilli()
		next := api.PacketCursor{At: last.LastHeardAt.Time, Hash: last.PacketHash}.String()
		page.NextCursor = &ms
		page.NextPageCursor = &next
	}
	return page, nil
}

// listPacketRows runs the statement for f's filter shape and returns up to f.pageLimit rows in
// (last_heard_at DESC, packet_hash DESC) order.
func (s *Store) listPacketRows(ctx context.Context, f packetFilter) ([]packetRow, error) {
	switch {
	case f.windowed && len(f.scopes) > 0:
		rows, err := s.q.ListPacketsWindowScoped(ctx, sqlc.ListPacketsWindowScopedParams{
			SinceTs: f.since, CursorTs: f.cursorTS, CursorHash: f.cursorHash, UntilTs: f.until,
			PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, PageLimit: f.pageLimit, ScopeNames: f.scopes,
		})
		return convertPacketRows(rows, err, func(r sqlc.ListPacketsWindowScopedRow) packetRow { return packetRow(r) })
	case f.windowed:
		rows, err := s.q.ListPacketsWindow(ctx, sqlc.ListPacketsWindowParams{
			SinceTs: f.since, CursorTs: f.cursorTS, CursorHash: f.cursorHash, UntilTs: f.until,
			PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, PageLimit: f.pageLimit,
		})
		return convertPacketRows(rows, err, func(r sqlc.ListPacketsWindowRow) packetRow { return packetRow(r) })
	case len(f.scopes) > 0 && f.typed:
		rows, err := s.q.ListPacketsByScopesTypes(ctx, sqlc.ListPacketsByScopesTypesParams{
			CursorTs: f.cursorTS, CursorHash: f.cursorHash, PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes,
			PageLimit: f.pageLimit, ScopeNames: f.scopes,
		})
		return convertPacketRows(rows, err, func(r sqlc.ListPacketsByScopesTypesRow) packetRow { return packetRow(r) })
	case len(f.scopes) > 0:
		rows, err := s.q.ListPacketsByScopes(ctx, sqlc.ListPacketsByScopesParams{
			CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit, ScopeNames: f.scopes,
		})
		return convertPacketRows(rows, err, func(r sqlc.ListPacketsByScopesRow) packetRow { return packetRow(r) })
	case f.typed:
		rows, err := s.q.ListPacketsByTypes(ctx, sqlc.ListPacketsByTypesParams{
			PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, CursorTs: f.cursorTS, CursorHash: f.cursorHash,
			PageLimit: f.pageLimit,
		})
		return convertPacketRows(rows, err, func(r sqlc.ListPacketsByTypesRow) packetRow { return packetRow(r) })
	}
	return s.q.ListPacketsPage(ctx, sqlc.ListPacketsPageParams{
		CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit,
	})
}

// convertPacketRows converts one statement's rows to packetRow with conv, a plain Go conversion (the Row types
// share every field name and type).
func convertPacketRows[R any](rows []R, err error, conv func(R) packetRow) ([]packetRow, error) {
	if err != nil {
		return nil, err
	}
	out := make([]packetRow, len(rows))
	for i, r := range rows {
		out[i] = conv(r)
	}
	return out, nil
}

// packetSummaries builds the API items for global packet rows and resolves their endpoints.
func (s *Store) packetSummaries(ctx context.Context, rows []packetRow) []api.PacketSummary {
	items := make([]api.PacketSummary, 0, len(rows))
	lookups := make([]endpointLookup, 0, len(rows))
	for _, v := range rows {
		var lookup endpointLookup
		item := api.PacketSummary{
			PacketHash:       hex.EncodeToString(v.PacketHash),
			PayloadType:      v.PayloadType,
			PayloadTypeName:  api.PayloadTypeName(v.PayloadType),
			RouteType:        v.RouteType,
			RouteTypeName:    api.RouteTypeName(v.RouteType),
			Scope:            v.ScopeName,
			FirstHeardAt:     v.FirstHeardAt.Time.UnixMilli(),
			LastHeardAt:      v.LastHeardAt.Time.UnixMilli(),
			ObservationCount: int32(v.ObservationCount),
		}
		if v.Summary != "" {
			item.Summary = &v.Summary
		}
		if v.LatestObserverID != (uuid.UUID{}) {
			item.LatestObserver = &api.PacketLatestObserver{
				ID:          v.LatestObserverID,
				DisplayName: v.LatestObserverName,
				IATA:        v.LatestObserverIata,
			}
			lookup = endpointLookup{iata: v.LatestObserverIata, ep: parsePacketEndpoints(v.PayloadType, v.RawPayload, v.OriginPubkey)}
			item.LatestObserver.PathLength, item.LatestObserver.PathBytes = buildLatestObserverPath(
				&v.LatestObserverPathLengthByte, &v.LatestObserverHashSize, &v.LatestObserverHopCount, v.LatestObserverPathBytes,
			)
		}
		items = append(items, item)
		lookups = append(lookups, lookup)
	}
	s.fillLatestEndpoints(ctx, items, lookups)
	return items
}

// SearchPacketsByHashPrefix lists packets whose hash is in [lo, hi); nil hi is unbounded.
func (s *Store) SearchPacketsByHashPrefix(ctx context.Context, lo, hi []byte, payloadTypes, routeTypes []int16, iatas []string, scopes []string, since, until time.Time, cursor int64, limit int32) (api.Page[api.PacketSummary], error) {
	var cursorTS pgtype.Timestamptz
	if cursor > 0 {
		cursorTS = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	var sinceTS pgtype.Timestamptz
	if !since.IsZero() {
		sinceTS = pgtype.Timestamptz{Time: since, Valid: true}
	}
	var untilTS pgtype.Timestamptz
	if !until.IsZero() {
		untilTS = pgtype.Timestamptz{Time: until, Valid: true}
	}
	if hi == nil {
		// Sorts above any stored hash, keeping the range closed for the planner.
		hi = bytes.Repeat([]byte{0xff}, 64)
	}
	rows, err := s.q.SearchPacketsByHashPrefix(ctx, sqlc.SearchPacketsByHashPrefixParams{
		HashLo:       lo,
		HashHi:       hi,
		PayloadTypes: payloadTypes,
		RouteTypes:   routeTypes,
		SinceTs:      sinceTS,
		UntilTs:      untilTS,
		CursorTs:     cursorTS,
		ScopeNames:   scopes,
		Iatas:        iatas,
		PageLimit:    limit + 1,
	})
	if err != nil {
		return api.Page[api.PacketSummary]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.PacketSummary, 0, len(rows))
	lookups := make([]endpointLookup, 0, len(rows))
	for _, v := range rows {
		var lookup endpointLookup
		item := api.PacketSummary{
			PacketHash:       hex.EncodeToString(v.PacketHash),
			PayloadType:      v.PayloadType,
			PayloadTypeName:  api.PayloadTypeName(v.PayloadType),
			RouteType:        v.RouteType,
			RouteTypeName:    api.RouteTypeName(v.RouteType),
			Scope:            v.ScopeName,
			FirstHeardAt:     v.FirstHeardAt.Time.UnixMilli(),
			LastHeardAt:      v.LastHeardAt.Time.UnixMilli(),
			ObservationCount: int32(v.ObservationCount),
		}
		if v.Summary != "" {
			item.Summary = &v.Summary
		}
		if v.LatestObserverID != (uuid.UUID{}) {
			item.LatestObserver = &api.PacketLatestObserver{
				ID:          v.LatestObserverID,
				DisplayName: v.LatestObserverName,
				IATA:        v.LatestObserverIata,
			}
			lookup = endpointLookup{iata: v.LatestObserverIata, ep: parsePacketEndpoints(v.PayloadType, v.RawPayload, v.OriginPubkey)}
			item.LatestObserver.PathLength, item.LatestObserver.PathBytes = buildLatestObserverPath(
				&v.LatestObserverPathLengthByte, &v.LatestObserverHashSize, &v.LatestObserverHopCount, v.LatestObserverPathBytes,
			)
		}
		items = append(items, item)
		lookups = append(lookups, lookup)
	}
	s.fillLatestEndpoints(ctx, items, lookups)
	var nextCursor *int64
	if hasMore && len(items) > 0 {
		last := items[len(items)-1].LastHeardAt
		nextCursor = &last
	}
	return api.Page[api.PacketSummary]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

func (s *Store) ListPacketsAfterID(ctx context.Context, afterObservationID int64, payloadType, routeType int16, iatas []string, scope string, limit int32) ([]api.PacketSummary, error) {
	rows, err := s.q.ListPacketsAfterID(ctx, sqlc.ListPacketsAfterIDParams{
		ID:      afterObservationID,
		Column2: payloadType,
		Column3: routeType,
		Column4: iatas,
		Column5: scope,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.PacketSummary, 0, len(rows))
	lookups := make([]endpointLookup, 0, len(rows))
	for _, v := range rows {
		var lookup endpointLookup
		item := api.PacketSummary{
			PacketHash:       hex.EncodeToString(v.PacketHash),
			PayloadType:      v.PayloadType,
			PayloadTypeName:  api.PayloadTypeName(v.PayloadType),
			RouteType:        v.RouteType,
			RouteTypeName:    api.RouteTypeName(v.RouteType),
			Scope:            v.ScopeName,
			FirstHeardAt:     v.FirstHeardAt.Time.UnixMilli(),
			LastHeardAt:      v.LastHeardAt.Time.UnixMilli(),
			ObservationCount: int32(v.ObservationCount),
		}
		if v.Summary != "" {
			item.Summary = &v.Summary
		}
		if v.LatestObserverID != (uuid.UUID{}) {
			item.LatestObserver = &api.PacketLatestObserver{
				ID:          v.LatestObserverID,
				DisplayName: v.LatestObserverName,
				IATA:        v.LatestObserverIata,
			}
			lookup = endpointLookup{iata: v.LatestObserverIata, ep: parsePacketEndpoints(v.PayloadType, v.RawPayload, v.OriginPubkey)}
			// Inner join here (unlike ListPackets/listPacketsByIATAs' LEFT JOIN LATERAL), so
			// these are never nil when an observer was joined at all.
			item.LatestObserver.PathLength, item.LatestObserver.PathBytes = buildLatestObserverPath(
				&v.LatestObserverPathLengthByte, &v.LatestObserverHashSize, &v.LatestObserverHopCount, v.LatestObserverPathBytes,
			)
		}
		items = append(items, item)
		lookups = append(lookups, lookup)
	}
	s.fillLatestEndpoints(ctx, items, lookups)
	return items, nil
}

func (s *Store) GetPacket(ctx context.Context, packetHash []byte) (*api.Packet, error) {
	row, err := s.q.GetPacketByHash(ctx, packetHash)
	if err != nil {
		return nil, err
	}
	if row.PayloadType == int16(meshcore.PayloadTypeGrpTxt) && row.CmSenderName != nil {
		var base struct {
			Type             string `json:"type"`
			Raw              string `json:"raw"`
			ChannelHash      string `json:"channelHash"`
			CipherMac        string `json:"cipherMac"`
			Ciphertext       string `json:"ciphertext"`
			CiphertextLength int    `json:"ciphertextLength"`
			Decrypted        *struct {
				Sender  string `json:"sender"`
				Content string `json:"content"`
				SentAt  int64  `json:"sentAt"`
			} `json:"decrypted"`
		}
		if err := json.Unmarshal(row.ParsedPayload, &base); err == nil {
			base.Decrypted = &struct {
				Sender  string `json:"sender"`
				Content string `json:"content"`
				SentAt  int64  `json:"sentAt"`
			}{
				Sender: *row.CmSenderName,
				Content: func() string {
					if row.CmContent != nil {
						return *row.CmContent
					}
					return ""
				}(),
				SentAt: row.CmSentAt.Time.UnixMilli(),
			}
			if updated, err := json.Marshal(base); err == nil {
				row.ParsedPayload = updated
			}
		}
	}
	obsRows, err := s.q.ListObservationsForPacket(ctx, packetHash)
	if err != nil {
		return nil, err
	}
	p := &api.Packet{
		PacketHash: hex.EncodeToString(row.PacketHash),
		Header: api.PacketHeader{
			Raw:             hex.EncodeToString(row.RawHeader),
			RouteType:       row.RouteType,
			RouteTypeName:   api.RouteTypeName(row.RouteType),
			PayloadType:     row.PayloadType,
			PayloadTypeName: api.PayloadTypeName(row.PayloadType),
			PayloadVersion:  row.PayloadVersion,
		},
		ParsedPayload:    row.ParsedPayload,
		RawPayload:       hex.EncodeToString(row.RawPayload),
		Decrypted:        row.Decrypted != nil && *row.Decrypted,
		Scope:            row.ScopeName,
		FirstHeardAt:     row.FirstHeardAt.Time.UnixMilli(),
		LastHeardAt:      row.LastHeardAt.Time.UnixMilli(),
		ObservationCount: int32(len(obsRows)),
		Observations:     make([]api.PacketObservationDetail, 0, len(obsRows)),
	}
	var minHeardAt time.Time
	if len(obsRows) > 0 {
		minHeardAt = obsRows[0].HeardAt.Time
	}
	if len(obsRows) > 1 {
		maxHeardAt := obsRows[0].HeardAt.Time
		for _, v := range obsRows[1:] {
			if v.HeardAt.Time.Before(minHeardAt) {
				minHeardAt = v.HeardAt.Time
			}
			if v.HeardAt.Time.After(maxHeardAt) {
				maxHeardAt = v.HeardAt.Time
			}
		}
		p.FirstToLastMs = maxHeardAt.Sub(minHeardAt).Milliseconds()
	}
	if row.OriginPubkey != nil {
		s := hex.EncodeToString(row.OriginPubkey)
		p.OriginPubkey = &s
	}
	if row.ChannelHash != nil {
		ch := hex.EncodeToString(row.ChannelHash)
		p.ChannelHash = &ch
	}
	if row.TransportCodesPresent != nil && *row.TransportCodesPresent {
		tc := &api.PacketTransportCodes{}
		if row.RegionCode != nil {
			tc.RegionCode = *row.RegionCode
		}
		if row.SubRegionCode != nil {
			tc.SubRegionCode = *row.SubRegionCode
		}
		p.TransportCodes = tc
	}
	// TRACE payloads repurpose the per-observation Path field to carry per-hop SNR bytes
	// rather than hashes (see internal/ingest/packet.go), so per-observation PathBytes
	// can't be resolved the normal way for TRACE. The actual resolvable path is the
	// trace payload's own embedded PathHashes -- constant for this packet hash, so
	// compute it once rather than per observation.
	var traceRawHashes [][]byte
	if row.PayloadType == int16(meshcore.PayloadTypeTrace) {
		if trace, err := meshcore.TraceFromBytes(row.RawPayload); err == nil {
			hashSize := int(trace.PathHashSize())
			for i := 0; i+hashSize <= len(trace.PathHashes); i += hashSize {
				traceRawHashes = append(traceRawHashes, trace.PathHashes[i:i+hashSize])
			}
		}
	}
	// Endpoints resolve against each observation's IATA, all in one batch.
	ep := parsePacketEndpoints(row.PayloadType, row.RawPayload, row.OriginPubkey)
	lookups := make([]endpointLookup, len(obsRows))
	for i, v := range obsRows {
		lookups[i] = endpointLookup{iata: v.Iata, ep: ep}
	}
	sources, destinations := s.resolveEndpoints(ctx, lookups)
	for i, v := range obsRows {
		obs := api.PacketObservationDetail{
			ID:           v.ID,
			ObserverID:   v.ObserverID,
			ObserverName: v.ObserverName,
			IATA:         v.Iata,
			HeardAt:      v.HeardAt.Time.UnixMilli(),
			PathLength: api.PacketPathLength{
				Raw:      fmt.Sprintf("%02x", v.PathLengthByte),
				HashSize: v.HashSize,
				HopCount: v.HopCount,
			},
			RSSI: v.Rssi,
			SNR:  v.Snr,
		}
		if v.SourceBroker != nil {
			obs.SourceBroker = *v.SourceBroker
		}
		prop := int32(v.HeardAt.Time.Sub(minHeardAt).Milliseconds())
		obs.PropagationTimeMs = &prop
		resolvedPath := []api.ResolvedHop{}
		if row.PayloadType == int16(meshcore.PayloadTypeTrace) {
			if len(traceRawHashes) > 0 {
				resolved, err := s.ResolvePathHashes(ctx, v.Iata, traceRawHashes)
				if err != nil {
					slog.Error(fmt.Sprintf("store: path resolution failed for observation %d", v.ID), "component", "db", "error", err)
				} else {
					resolvedPath = api.BuildResolvedPath(traceRawHashes, resolved)
				}
			}
		} else if v.PathBytes != nil && v.HashSize > 0 {
			hashSize := int(v.HashSize)
			hashes := make([][]byte, 0, len(v.PathBytes)/hashSize)
			for i := 0; i+hashSize <= len(v.PathBytes); i += hashSize {
				hashes = append(hashes, v.PathBytes[i:i+hashSize])
			}
			resolved, err := s.ResolvePathHashes(ctx, v.Iata, hashes)
			if err != nil {
				slog.Error(fmt.Sprintf("store: path resolution failed for observation %d", v.ID), "component", "db", "error", err)
			} else {
				resolvedPath = api.BuildResolvedPath(hashes, resolved)
			}
		}
		obs.ResolvedPath = resolvedPath
		obs.ResolvedSource, obs.ResolvedDestination = sources[i], destinations[i]
		if row.PayloadType == int16(meshcore.PayloadTypeTrace) && len(traceRawHashes) > 0 {
			// Swap in the trace's own path hashes so PathData's hop-block split (driven by
			// pathBytes + hashSize) lines up 1:1 with resolvedPath -- the raw SNR bytes
			// from the physical Path field don't chunk into the same hop count.
			var traceHashBytes []byte
			for _, h := range traceRawHashes {
				traceHashBytes = append(traceHashBytes, h...)
			}
			pb := hex.EncodeToString(traceHashBytes)
			obs.PathBytes = &pb
			obs.PathLength.HashSize = int16(len(traceRawHashes[0]))
			obs.PathLength.HopCount = int16(len(traceRawHashes))
		} else if v.PathBytes != nil {
			pb := hex.EncodeToString(v.PathBytes)
			obs.PathBytes = &pb
		}
		if v.RadioFreqMhz != nil || v.SpreadFactor != nil || v.BandwidthKhz != nil || v.CodingRate != nil {
			obs.Radio = &api.PacketRadio{
				FreqMHz:      v.RadioFreqMhz,
				SpreadFactor: v.SpreadFactor,
				BandwidthKHz: v.BandwidthKhz,
				CodingRate:   v.CodingRate,
			}
		}
		p.Observations = append(p.Observations, obs)
	}
	if row.PayloadType == 9 && len(obsRows) > 0 {
		iatas := make([]string, 0, len(obsRows))
		seen := make(map[string]struct{})
		for _, v := range obsRows {
			if _, ok := seen[v.Iata]; !ok {
				seen[v.Iata] = struct{}{}
				iatas = append(iatas, v.Iata)
			}
		}
		var parsed tracePayload
		if err := json.Unmarshal(row.ParsedPayload, &parsed); err == nil {
			p.ResolvedRoute = s.resolveTraceRoute(ctx, &parsed, iatas)
		}
	}
	return p, nil
}

func (s *Store) UpsertIATA(ctx context.Context, iata string) error {
	return s.q.UpsertIATA(ctx, iata)
}

// observationParams maps an observation to the statement's parameters.
func observationParams(o ingest.InsertObservationParams) sqlc.InsertObservationParams {
	return sqlc.InsertObservationParams{
		PacketHash:        o.PacketHash,
		ObserverID:        o.ObserverID,
		Iata:              o.IATA,
		HeardAt:           pgtype.Timestamptz{Time: o.HeardAt, Valid: true},
		PathLengthByte:    int16(o.PathLengthByte),
		HashSize:          int16(o.HashSize),
		HopCount:          int16(o.HopCount),
		PathBytes:         o.PathBytes,
		Rssi:              &o.RSSI,
		Snr:               &o.SNR,
		PropagationTimeMs: &o.PropagationTimeMs,
		RadioFreqMhz:      &o.RadioFreqMHz,
		SpreadFactor:      &o.SpreadFactor,
		BandwidthKhz:      &o.BandwidthKHz,
		CodingRate:        &o.CodingRate,
		SourceBroker:      &o.SourceBroker,
		PayloadType:       &o.PayloadType,
		AirtimeMs:         o.AirtimeMs,
	}
}

func (s *Store) InsertObservation(ctx context.Context, o ingest.InsertObservationParams) (bool, int64, error) {
	params := observationParams(o)
	row, err := s.q.InsertObservation(ctx, params)
	if err == nil {
		if row.Inserted {
			s.obsCounts.Add(o.HeardAt, o.ObserverID, o.IATA)
			s.obsActivity.Add(o.HeardAt, o.ObserverID, o.PayloadType, o.RSSI, o.SNR, o.AirtimeMs)
		}
		return row.Inserted, row.ObservationCount, nil
	}
	// Whatever failed, region pages stop reading packet_sites before anything else is written, until a reconcile
	// that begins after this has repaired it.
	s.readModels.invalidatePacketSites("an observation insert with its site row failed")
	// A cancelled request and a packet that is gone (23503, which today's statement hits too) are returned as
	// they are. Any other failure stores the observation without its site row and leaves the row to the
	// reconcile.
	var pgErr *pgconn.PgError
	if ctx.Err() != nil || (errors.As(err, &pgErr) && pgErr.Code == "23503") {
		return false, 0, err
	}
	slog.Warn("observation site row failed; storing the observation without it", "component", "db",
		"packet_hash", hex.EncodeToString(o.PacketHash), "iata", o.IATA, "error", err)
	fallback, err := s.q.InsertObservationUnsited(ctx, sqlc.InsertObservationUnsitedParams(params))
	// Again once the fallback has returned: a reconcile that took its snapshot before this commit must not be the
	// one that restores trust.
	s.readModels.invalidatePacketSites("an observation was stored without its site row")
	if err != nil {
		return false, 0, err
	}
	if fallback.Inserted {
		s.obsCounts.Add(o.HeardAt, o.ObserverID, o.IATA)
		s.obsActivity.Add(o.HeardAt, o.ObserverID, o.PayloadType, o.RSSI, o.SNR, o.AirtimeMs)
	}
	return fallback.Inserted, fallback.ObservationCount, nil
}

func (s *Store) ListNodeObservations(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (api.Page[api.PacketObservationSummary], error) {
	rows, err := s.q.ListNodeObservations(ctx, sqlc.ListNodeObservationsParams{
		ID:      nodeID,
		Column2: cursor,
		Limit:   limit + 1,
	})
	if err != nil {
		return api.Page[api.PacketObservationSummary]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.PacketObservationSummary, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.PacketObservationSummary{
			ID:              v.ID,
			PacketHash:      v.PacketHashHex,
			PayloadType:     v.PayloadType,
			PayloadTypeName: api.PayloadTypeName(v.PayloadType),
			IATA:            v.Iata,
			HeardAt:         v.HeardAt.Time.UnixMilli(),
			RSSI:            v.Rssi,
			SNR:             v.Snr,
			HopCount:        &v.HopCount,
		})
	}
	var nextCursor *int64
	if hasMore && len(items) > 0 {
		last := items[len(items)-1].ID
		nextCursor = &last
	}
	return api.Page[api.PacketObservationSummary]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// Small because each packet cascades to its observations.
const packetDeleteBatch = 1000

func (s *Store) DeleteOldPackets(ctx context.Context, cutoff time.Time) error {
	return deleteInBatches(ctx, packetDeleteBatch, func(ctx context.Context, n int32) (int64, error) {
		return s.q.DeleteOldPackets(ctx, sqlc.DeleteOldPacketsParams{
			Cutoff:    pgtype.Timestamptz{Time: cutoff, Valid: true},
			BatchSize: n,
		})
	})
}
