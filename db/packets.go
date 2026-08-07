// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
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

func (s *Store) ListPackets(ctx context.Context, payloadTypes, routeTypes []int16, iatas []string, scopes []string, since, until time.Time, cursor int64, limit int32) (api.Page[api.PacketSummary], error) {
	if len(iatas) > 0 {
		return s.listPacketsByIATAs(ctx, payloadTypes, routeTypes, iatas, scopes, since, until, cursor, limit)
	}
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
	rows, err := s.q.ListPackets(ctx, sqlc.ListPacketsParams{
		Column1: payloadTypes,
		Column2: routeTypes,
		Column3: sinceTS,
		Column4: untilTS,
		Column5: cursorTS,
		Limit:   limit + 1,
		Column7: scopes,
	})
	if err != nil {
		return api.Page[api.PacketSummary]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.PacketSummary, 0, len(rows))
	for _, v := range rows {
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
		if v.LatestObserverID != (uuid.UUID{}) {
			item.LatestObserver = &api.PacketLatestObserver{
				ID:          v.LatestObserverID,
				DisplayName: v.LatestObserverName,
				IATA:        v.LatestObserverIata,
			}
			item.LatestObserver.PathLength, item.LatestObserver.PathBytes = buildLatestObserverPath(
				&v.LatestObserverPathLengthByte, &v.LatestObserverHashSize, &v.LatestObserverHopCount, v.LatestObserverPathBytes,
			)
		}
		items = append(items, item)
	}
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

func (s *Store) listPacketsByIATAs(ctx context.Context, payloadTypes, routeTypes []int16, iatas []string, scopes []string, since, until time.Time, cursor int64, limit int32) (api.Page[api.PacketSummary], error) {
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
	// A packet appears once per observer that heard it at a site
	// ((packet_hash, observer_id) is unique), so scan deep enough per site
	// to still fill the page after collapsing those duplicates.
	rows, err := s.q.ListPacketsByIATAs(ctx, sqlc.ListPacketsByIATAsParams{
		Iatas:        iatas,
		ScanDepth:    (limit + 1) * 8,
		CursorTs:     cursorTS,
		PayloadTypes: payloadTypes,
		RouteTypes:   routeTypes,
		SinceTs:      sinceTS,
		UntilTs:      untilTS,
		ScopeNames:   scopes,
		PageLimit:    limit + 1,
	})
	if err != nil {
		return api.Page[api.PacketSummary]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	// Observers duplicate a packet across the scan, so the page can collapse
	// short while the site still has history below scan_floor. Stopping here
	// would strand every older packet behind a hasMore=false.
	var scanFloor pgtype.Timestamptz
	if len(rows) > 0 && rows[0].ScanSaturated {
		hasMore = true
		scanFloor = rows[0].ScanFloor
	}
	items := make([]api.PacketSummary, 0, len(rows))
	for _, v := range rows {
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
		if v.LatestObserverID != (uuid.UUID{}) {
			item.LatestObserver = &api.PacketLatestObserver{
				ID:          v.LatestObserverID,
				DisplayName: v.LatestObserverName,
				IATA:        v.LatestObserverIata,
			}
			item.LatestObserver.PathLength, item.LatestObserver.PathBytes = buildLatestObserverPath(
				&v.LatestObserverPathLengthByte, &v.LatestObserverHashSize, &v.LatestObserverHopCount, v.LatestObserverPathBytes,
			)
		}
		items = append(items, item)
	}
	// Cursor follows site-local recency, not the packet's global last_heard_at.
	var nextCursor *int64
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1].SiteHeardAt.Time
		// A saturated scan never read below its floor. Paging past it would
		// skip that band; resuming at it only repeats packets already shown.
		if scanFloor.Valid && scanFloor.Time.After(last) {
			last = scanFloor.Time
		}
		ms := last.UnixMilli()
		nextCursor = &ms
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
	for _, v := range rows {
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
		if v.LatestObserverID != (uuid.UUID{}) {
			item.LatestObserver = &api.PacketLatestObserver{
				ID:          v.LatestObserverID,
				DisplayName: v.LatestObserverName,
				IATA:        v.LatestObserverIata,
			}
			// Inner join here (unlike ListPackets/listPacketsByIATAs' LEFT JOIN LATERAL), so
			// these are never nil when an observer was joined at all.
			item.LatestObserver.PathLength, item.LatestObserver.PathBytes = buildLatestObserverPath(
				&v.LatestObserverPathLengthByte, &v.LatestObserverHashSize, &v.LatestObserverHopCount, v.LatestObserverPathBytes,
			)
		}
		items = append(items, item)
	}
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
	minHeardAt := obsRows[0].HeardAt.Time
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
	// 1-byte source/destination hashes for the payload types that carry a resolvable
	// endpoint (REQUEST, RESPONSE, TEXT_MESSAGE, PATH, ANON_REQ's destination). Constant
	// for this packet hash, so parsed once; resolution itself still happens per-observation
	// below since candidates depend on the observation's IATA, same as the intermediate
	// hop resolution already does.
	var sourceHashByte, destHashByte []byte
	switch row.PayloadType {
	case int16(meshcore.PayloadTypeAnonReq):
		if anonReq, err := meshcore.AnonReqFromBytes(row.RawPayload); err == nil {
			destHashByte = []byte{anonReq.Destination}
		}
	case int16(meshcore.PayloadTypeReq):
		if req, err := meshcore.RequestFromBytes(row.RawPayload); err == nil {
			sourceHashByte = []byte{req.Source}
			destHashByte = []byte{req.Destination}
		}
	case int16(meshcore.PayloadTypeResponse):
		if resp, err := meshcore.ResponseFromBytes(row.RawPayload); err == nil {
			sourceHashByte = []byte{resp.Source}
			destHashByte = []byte{resp.Destination}
		}
	case int16(meshcore.PayloadTypeTxtMsg):
		if txt, err := meshcore.TextMessageFromBytes(row.RawPayload); err == nil {
			sourceHashByte = []byte{txt.Source}
			destHashByte = []byte{txt.Destination}
		}
	case int16(meshcore.PayloadTypePath):
		if path, err := meshcore.PathFromBytes(row.RawPayload); err == nil {
			sourceHashByte = []byte{path.Source}
			destHashByte = []byte{path.Destination}
		}
	}
	// ADVERT's source is an exact pubkey match, not ambiguous like the above -- and unlike
	// them it doesn't depend on IATA, so resolve it once here rather than per observation.
	var resolvedAdvertSource *api.ResolvedNode
	if row.PayloadType == int16(meshcore.PayloadTypeAdvert) && row.OriginPubkey != nil {
		if nodeID, err := s.GetNodeByPubkey(ctx, row.OriginPubkey); err == nil {
			if nodes, err := s.GetNodesByIDs(ctx, []uuid.UUID{nodeID}); err == nil {
				resolvedAdvertSource = nodes[nodeID]
			}
		}
	}
	for _, v := range obsRows {
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
			RSSI:         v.Rssi,
			SNR:          v.Snr,
			SourceBroker: *v.SourceBroker,
		}
		prop := int32(v.HeardAt.Time.Sub(minHeardAt).Milliseconds())
		obs.PropagationTimeMs = &prop
		resolvedPath := []api.ResolvedHop{}
		if row.PayloadType == int16(meshcore.PayloadTypeTrace) {
			if len(traceRawHashes) > 0 {
				resolved, err := s.ResolvePathHashes(ctx, v.Iata, traceRawHashes)
				if err != nil {
					log.Printf("store: path resolution failed for observation %d: %v", v.ID, err)
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
				log.Printf("store: path resolution failed for observation %d: %v", v.ID, err)
			} else {
				resolvedPath = api.BuildResolvedPath(hashes, resolved)
			}
		}
		obs.ResolvedPath = resolvedPath
		if row.PayloadType == int16(meshcore.PayloadTypeAdvert) {
			hop := api.ResolveExactNode(resolvedAdvertSource)
			obs.ResolvedSource = &hop
		} else if len(sourceHashByte) == 1 {
			if r, err := s.ResolvePathHashes(ctx, v.Iata, [][]byte{sourceHashByte}); err != nil {
				log.Printf("store: source resolution failed for observation %d: %v", v.ID, err)
			} else {
				hop := api.BuildResolvedPath([][]byte{sourceHashByte}, r)[0]
				obs.ResolvedSource = &hop
			}
		}
		if len(destHashByte) == 1 {
			if r, err := s.ResolvePathHashes(ctx, v.Iata, [][]byte{destHashByte}); err != nil {
				log.Printf("store: destination resolution failed for observation %d: %v", v.ID, err)
			} else {
				hop := api.BuildResolvedPath([][]byte{destHashByte}, r)[0]
				obs.ResolvedDestination = &hop
			}
		}
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

func (s *Store) InsertObservation(ctx context.Context, o ingest.InsertObservationParams) (bool, error) {
	params := sqlc.InsertObservationParams{
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
	}
	row, err := s.q.InsertObservation(ctx, params)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // conflict, not an error
	}
	if err != nil {
		return false, err
	}
	return row.ID != 0, nil
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

func (s *Store) GetPacketObservationCount(ctx context.Context, packetHash []byte) (int64, error) {
	return s.q.GetPacketObservationCount(ctx, packetHash)
}

func (s *Store) DeleteOldPackets(ctx context.Context, cutoff time.Time) error {
	return s.q.DeleteOldPackets(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true})
}
