package db

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strings"
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

func (s *Store) ListPackets(ctx context.Context, payloadType, routeType int16, iatas []string, scope string, since, until time.Time, cursor int64, limit int32) (api.Page[api.PacketSummary], error) {
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
	iataFilter := strings.Join(iatas, ",")
	rows, err := s.q.ListPackets(ctx, sqlc.ListPacketsParams{
		Column1: payloadType,
		Column2: routeType,
		Column3: iataFilter,
		Column4: sinceTS,
		Column5: untilTS,
		Column6: cursorTS,
		Limit:   limit + 1,
		Column8: scope,
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

func (s *Store) ListPacketsAfterID(ctx context.Context, afterObservationID int64, payloadType, routeType int16, iatas []string, scope string, limit int32) ([]api.PacketSummary, error) {
	iataFilter := strings.Join(iatas, ",")
	rows, err := s.q.ListPacketsAfterID(ctx, sqlc.ListPacketsAfterIDParams{
		ID:      afterObservationID,
		Column2: payloadType,
		Column3: routeType,
		Column4: iataFilter,
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
		if v.PathBytes != nil && v.HashSize > 0 {
			hashSize := int(v.HashSize)
			hashes := make([][]byte, 0, len(v.PathBytes)/hashSize)
			for i := 0; i+hashSize <= len(v.PathBytes); i += hashSize {
				hashes = append(hashes, v.PathBytes[i:i+hashSize])
			}
			resolved, err := s.ResolvePathHashes(ctx, v.Iata, hashes)
			if err != nil {
				log.Printf("store: path resolution failed for observation %d: %v", v.ID, err)
			} else {
				for _, hash := range hashes {
					key := hex.EncodeToString(hash)
					entries := resolved[key]
					hop := api.ResolvedHop{
						Nodes: make([]api.ResolvedNode, 0, len(entries)),
					}
					switch len(entries) {
					case 0:
						hop.Confidence = "none"
					case 1:
						hop.Confidence = "high"
					default:
						hop.Confidence = "ambiguous"
					}
					for _, e := range entries {
						hop.Nodes = append(hop.Nodes, api.ResolvedNode{
							ID:        e.NodeID,
							Name:      e.Name,
							Latitude:  e.Latitude,
							Longitude: e.Longitude,
							PublicKey: hex.EncodeToString(e.PublicKey),
						})
					}
					resolvedPath = append(resolvedPath, hop)
				}
			}
		}
		obs.ResolvedPath = resolvedPath
		if v.PathBytes != nil {
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
		p.ResolvedRoute = s.resolveTraceRoute(ctx, row.ParsedPayload, iatas)
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
