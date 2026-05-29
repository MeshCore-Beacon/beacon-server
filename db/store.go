// Package db implements the ingest.DB interface using sqlc-generated queries
// over a pgx/v5 connection pool. Each method is a thin mapping layer between
// the ingest param structs and the sqlc-generated param structs.
package db

import (
	"context"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"log"
	"time"

	sqlc "github.com/MeshCore-Tower/tower-server/db/sqlc"
	"github.com/MeshCore-Tower/tower-server/internal/api"
	"github.com/MeshCore-Tower/tower-server/internal/ingest"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	q *sqlc.Queries
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{q: sqlc.New(pool)}
}

// UpsertObserver upserts the observers row keyed on pubkey.
func (s *Store) UpsertObserver(ctx context.Context, pubkey []byte) (uuid.UUID, string, error) {
	row, err := s.q.UpsertObserver(ctx, pubkey)
	if err != nil {
		return uuid.Nil, "", err
	}
	displayName := ""
	if row.DisplayName != nil {
		displayName = *row.DisplayName
	}
	return row.ID, displayName, err
}

// UpsertObserverBroker records that this observer was seen on brokerName.
func (s *Store) UpsertObserverBroker(ctx context.Context, observerID uuid.UUID, brokerName string) error {
	params := sqlc.UpsertObserverBrokerParams{
		ObserverID: observerID,
		BrokerName: brokerName,
	}
	return s.q.UpsertObserverBroker(ctx, params)
}

// UpsertIATA auto-creates an iata_codes row if it doesn't exist yet.
func (s *Store) UpsertIATA(ctx context.Context, iata string) error {
	return s.q.UpsertIATA(ctx, iata)
}

// UpsertPacket inserts or bumps the packets row. Returns (isNew, error).
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
		ParsedPayload:         p.ParsedPayload,
		ChannelHash:           p.ChannelHash,
	}
	row, err := s.q.UpsertPacket(ctx, params)
	if err != nil {
		return false, err
	}
	return row.Inserted, nil
}

// InsertObservation inserts a packet_observations row.
// Returns (inserted, error); inserted=false means ON CONFLICT DO NOTHING fired.
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

// SetNodeCapability flips supports_multibyte_paths or supports_multibyte_traces
// for a node, never downgrading an existing TRUE.
func (s *Store) SetNodeCapability(ctx context.Context, nodeID uuid.UUID, paths, traces bool) error {
	var errs []error
	if paths {
		errs = append(errs, s.q.SetNodeMultibytePaths(ctx, nodeID))
	}
	if traces {
		errs = append(errs, s.q.SetNodeMultibyteTraces(ctx, nodeID))
	}
	return errors.Join(errs...)
}

// UpsertNode upserts a nodes row from an advert payload.
func (s *Store) UpsertNode(ctx context.Context, n ingest.UpsertNodeParams) (uuid.UUID, error) {
	params := sqlc.UpsertNodeParams{
		PublicKey: n.PublicKey,
		NodeType:  int16(n.NodeType),
		Name:      &n.Name,
		Latitude:  n.Latitude,
		Longitude: n.Longitude,
	}
	row, err := s.q.UpsertNode(ctx, params)
	if err != nil {
		return uuid.Nil, err
	}
	return row.ID, nil
}

// UpsertNodeIATA upserts a node_iatas row.
func (s *Store) UpsertNodeIATA(ctx context.Context, nodeID uuid.UUID, iata string) error {
	params := sqlc.UpsertNodeIATAParams{NodeID: nodeID, Iata: iata}
	return s.q.UpsertNodeIATA(ctx, params)
}

// InsertChannelMessage stores a decrypted group text message.
func (s *Store) InsertChannelMessage(ctx context.Context, m ingest.InsertChannelMessageParams) (bool, error) {
	params := sqlc.InsertChannelMessageParams{ChannelID: int32(m.ChannelID), PacketHash: m.PacketHash, SenderName: &m.SenderName, Content: &m.Content, SentAt: pgtype.Timestamptz{Time: m.SentAt, Valid: true}}
	_, err := s.q.InsertChannelMessage(ctx, params)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, nil // duplicate
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// UpdateObserverStatus updates the observer row from a /status message.
// Column2 = display_name, Column3 = observer_type (sqlc loses names inside CASE expressions).
func (s *Store) UpdateObserverStatus(ctx context.Context, p ingest.UpdateObserverStatusParams) (uuid.UUID, error) {
	params := sqlc.UpdateObserverStatusParams{PublicKey: p.PublicKey, Column2: p.DisplayName, Column3: p.ObserverType, SoftwareVersion: &p.SoftwareVersion, HardwareModel: &p.HardwareModel, FirmwareVersion: &p.FirmwareVersion, FirmwareBuild: &p.FirmwareBuild, RadioFreqMhz: &p.RadioFreqMHz, RadioSf: &p.RadioSF, RadioBwKhz: &p.RadioBWKHz, RadioCr: &p.RadioCR, BatteryLevel: p.BatteryLevel, UptimeSeconds: p.UptimeSeconds, StatusMetadata: p.StatusMetadata}
	return s.q.UpdateObserverStatus(ctx, params)
}

// GetObserverLastIATA returns the IATA from the most recent observation for the given observer.
func (s *Store) GetObserverLastIATA(ctx context.Context, observerID uuid.UUID) (string, error) {
	return s.q.GetObserverLastIATA(ctx, observerID)
}

// GetObserverRadio returns the current radio settings for the given observer.
func (s *Store) GetObserverRadio(ctx context.Context, observerID uuid.UUID) (ingest.RadioSettings, error) {
	row, err := s.q.GetObserverRadio(ctx, observerID)
	if err != nil {
		return ingest.RadioSettings{}, err
	}
	var settings ingest.RadioSettings
	if row.RadioFreqMhz != nil {
		settings.FreqMHz = *row.RadioFreqMhz
	}
	if row.RadioSf != nil {
		settings.SF = *row.RadioSf
	}
	if row.RadioBwKhz != nil {
		settings.BWKHz = *row.RadioBwKhz
	}
	if row.RadioCr != nil {
		settings.CR = *row.RadioCr
	}
	return settings, nil
}

// ResolvePathHashes returns a list of node UUIDs for the given path hash prefixes and IATA.
// Hash size is inferred from the length of the first element in hashes.
func (s *Store) ResolvePathHashes(ctx context.Context, iata string, hashes [][]byte) ([]uuid.UUID, error) {
	rows, err := s.q.ResolvePathHashes(ctx, sqlc.ResolvePathHashesParams{
		Iata:    iata,
		Column2: hashes,
	})
	if err != nil {
		return nil, err
	}

	ids := make([]uuid.UUID, len(rows))
	copy(ids, rows)
	return ids, nil
}

// UpsertChannel upserts a channel row by (hash, keyFingerprint) and returns its integer ID.
// Pass nil keyFingerprint to record a hash-only row when the key is unknown.
// name and hashtag are optional metadata stored on the channel row.
func (s *Store) UpsertChannel(ctx context.Context, channelHash []byte, keyFingerprint []byte, name string, hashtag string) (int, error) {
	var namePtr, hashtagPtr *string
	if name != "" {
		namePtr = &name
	}
	if hashtag != "" {
		hashtagPtr = &hashtag
	}
	isHashtag := hashtag != ""
	row, err := s.q.UpsertChannel(ctx, sqlc.UpsertChannelParams{
		ChannelHash:  channelHash,
		Column2:      keyFingerprint, // key_fingerprint
		Name:         namePtr,
		Hashtag:      hashtagPtr,
		IsHashtag:    &isHashtag,
		MessageCount: nil, // message count bumped separately by InsertChannelMessage
	})
	if err != nil {
		return 0, err
	}
	return int(row.ID), nil
}

// UpsertChannelHashOnly upserts a hash-only channel row for cases where the
// channel key is unknown. Uses the partial unique index to ensure only one
// hash-only row exists per channel hash. The return value is the channel ID
// but can be safely ignored since unknown-key channels have no messages.
func (s *Store) UpsertChannelHashOnly(ctx context.Context, channelHash []byte) (int, error) {
	rowID, err := s.q.UpsertChannelHashOnly(ctx, channelHash)
	if err != nil {
		return 0, err
	}
	return int(rowID), nil
}

// ListIATAs returns all known IATA codes with display name and coordinates.
// IATAs are auto-created on first packet arrival from that location.
func (s *Store) ListIATAs(ctx context.Context) ([]api.IATA, error) {
	rows, err := s.q.ListIATAs(ctx)
	if err != nil {
		return nil, err
	}
	iatas := make([]api.IATA, 0, len(rows))
	for _, v := range rows {
		iatas = append(iatas, api.IATA{
			IATA:        v.Iata,
			DisplayName: v.DisplayName,
			Lat:         v.ApproxLat,
			Lng:         v.ApproxLng,
		})
	}
	return iatas, nil
}

// GetIATA returns a single IATA code by its 3-letter identifier.
// Returns nil, error if the IATA code is not found.
func (s *Store) GetIATA(ctx context.Context, iata string) (*api.IATA, error) {
	i, err := s.q.GetIATA(ctx, iata)
	if err != nil {
		return nil, err
	}
	return &api.IATA{
		IATA:        i.Iata,
		DisplayName: i.DisplayName,
		Lat:         i.ApproxLat,
		Lng:         i.ApproxLng,
	}, nil
}

// ListRegions returns a summary list of all regions ordered by display_order then name.
// Use GetRegion for full detail including associated IATAs.
func (s *Store) ListRegions(ctx context.Context) ([]api.RegionSummary, error) {
	rows, err := s.q.ListRegions(ctx)
	if err != nil {
		return nil, err
	}
	regions := make([]api.RegionSummary, 0, len(rows))
	for _, v := range rows {
		regions = append(regions, api.RegionSummary{
			ID:   int(v.ID),
			Slug: v.Slug,
			Name: v.Name,
		})
	}
	return regions, nil
}

// GetRegion returns full detail for a single region including its associated IATA codes.
// Returns nil, pgx.ErrNoRows if the region is not found.
func (s *Store) GetRegion(ctx context.Context, regionID int32) (*api.Region, error) {
	region, err := s.q.GetRegion(ctx, regionID)
	if err != nil {
		return nil, err
	}
	result := api.Region{
		RegionSummary: api.RegionSummary{
			ID:   int(region.ID),
			Slug: region.Slug,
			Name: region.Name,
		},
		Description: region.Description,
		CenterLat:   region.CenterLat,
		CenterLng:   region.CenterLng,
	}
	var zoomLevel *int
	if region.ZoomLevel != nil {
		z := int(*region.ZoomLevel)
		zoomLevel = &z
	}
	result.ZoomLevel = zoomLevel
	iatas, err := s.q.GetRegionIATAs(ctx, regionID)
	if err != nil {
		return nil, err
	}
	result.IATAs = iatas
	return &result, nil
}

// UpsertIATADetails updates an existing iata_codes row with display name and coordinates.
// The row must already exist (auto-created on first packet arrival).
// Safe to call on startup — does nothing if the IATA has not been seen yet.
func (s *Store) UpsertIATADetails(ctx context.Context, iata string, name string, lat, lng *float64) error {
	return s.q.UpsertIATADetails(ctx, sqlc.UpsertIATADetailsParams{
		Iata:        iata,
		DisplayName: &name,
		ApproxLat:   lat,
		ApproxLng:   lng,
	})
}

// UpsertRegion inserts or updates a region row by slug. Returns the region ID.
func (s *Store) UpsertRegion(ctx context.Context, slug, name, description string, displayOrder int, centerLat, centerLng *float64, zoomLevel *int) (int32, error) {
	var zl *int32
	if zoomLevel != nil {
		z := int32(*zoomLevel)
		zl = &z
	}
	do := int32(displayOrder)
	return s.q.UpsertRegion(ctx, sqlc.UpsertRegionParams{
		Slug:         slug,
		Name:         name,
		Description:  &description,
		DisplayOrder: &do,
		CenterLat:    centerLat,
		CenterLng:    centerLng,
		ZoomLevel:    zl,
	})
}

// UpsertRegionIATA adds an IATA code to a region. Safe to call repeatedly.
func (s *Store) UpsertRegionIATA(ctx context.Context, regionID int32, iata string) error {
	return s.q.UpsertRegionIATA(ctx, sqlc.UpsertRegionIATAParams{
		RegionID: regionID,
		Iata:     iata,
	})
}

// ListChannels returns a paginated list of channels ordered by last seen.
// Pass nil hash to skip hash filtering. Pass empty string iata to return channels from all IATAs.
// IATA filtering returns channels that have messages heard in the given IATA.
// Channels with unknown keys are included with KeyKnown=false.
// cursor is last_seen epoch ms of the last item; pass 0 to start from the beginning.
func (s *Store) ListChannels(ctx context.Context, limit int32, hash []byte, iata string, cursor int64) (api.Page[api.ChannelSummary], error) {
	var cursorTS pgtype.Timestamptz
	if cursor > 0 {
		cursorTS = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	rows, err := s.q.ListChannels(ctx, sqlc.ListChannelsParams{
		Column1: hash,
		Column2: iata,
		Column3: cursorTS,
		Limit:   limit + 1,
	})
	if err != nil {
		return api.Page[api.ChannelSummary]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.ChannelSummary, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.ChannelSummary{
			ID:          int(v.ID),
			Name:        v.Name,
			ChannelHash: hex.EncodeToString(v.ChannelHash),
			LastSeen:    v.LastSeen.Time.UnixMilli(),
			IsHashtag:   v.IsHashtag != nil && *v.IsHashtag,
			KeyKnown:    v.KeyKnown != nil && *v.KeyKnown,
		})
	}
	var nextCursor *int64
	if hasMore {
		last := items[len(items)-1].LastSeen
		nextCursor = &last
	}
	return api.Page[api.ChannelSummary]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// GetChannel returns full detail for a single channel by its integer ID.
// Returns nil, pgx.ErrNoRows if the channel is not found.
func (s *Store) GetChannel(ctx context.Context, channelID int32) (*api.Channel, error) {
	row, err := s.q.GetChannelByID(ctx, channelID)
	if err != nil {
		return nil, err
	}
	channel := api.Channel{
		ChannelSummary: api.ChannelSummary{
			ID:          int(row.ID),
			Name:        row.Name,
			ChannelHash: hex.EncodeToString(row.ChannelHash),
			LastSeen:    row.LastSeen.Time.UnixMilli(),
			IsHashtag:   row.IsHashtag != nil && *row.IsHashtag,
			KeyKnown:    row.KeyKnown != nil && *row.KeyKnown,
		},
		Hashtag:      row.Hashtag,
		MessageCount: 0,
	}
	if row.MessageCount != nil {
		channel.MessageCount = *row.MessageCount
	}
	if row.IsHashtag != nil && *row.IsHashtag && row.KeyFingerprint != nil {
		fp := hex.EncodeToString(row.KeyFingerprint)
		channel.KeyFingerprint = &fp
	}
	return &channel, nil
}

// ListChannelMessages returns paginated messages with optional channel ID, time, IATA and cursor filters.
// Pass nil channelID to return messages across all channels.
// Pass a zero time.Time for since to return all messages up to limit.
// Pass empty string iata to return messages from all IATAs.
// Pass cursor=0 to start from the beginning.
func (s *Store) ListChannelMessages(ctx context.Context, channelID *int32, since time.Time, limit int32, iata string, cursor int64) (api.Page[api.ChannelMessage], error) {
	ts := pgtype.Timestamptz{Time: since, Valid: !since.IsZero()}
	var messages []api.ChannelMessage
	var hasMore bool

	if channelID == nil {
		rows, err := s.q.ListAllChannelMessages(ctx, sqlc.ListAllChannelMessagesParams{
			Column1: ts,
			Column2: iata,
			Column3: cursor,
			Limit:   limit + 1,
		})
		if err != nil {
			return api.Page[api.ChannelMessage]{}, err
		}
		hasMore = len(rows) > int(limit)
		if hasMore {
			rows = rows[:limit]
		}
		messages = make([]api.ChannelMessage, 0, len(rows))
		for _, v := range rows {
			messages = append(messages, toChannelMessage(v.ID, v.PacketHashHex, v.ChannelHash, v.SenderName, v.Content, v.SentAt))
		}
	} else {
		rows, err := s.q.ListChannelMessages(ctx, sqlc.ListChannelMessagesParams{
			ChannelID: *channelID,
			Column2:   ts,
			Column3:   iata,
			Column4:   cursor,
			Limit:     limit + 1,
		})
		if err != nil {
			return api.Page[api.ChannelMessage]{}, err
		}
		hasMore = len(rows) > int(limit)
		if hasMore {
			rows = rows[:limit]
		}
		messages = make([]api.ChannelMessage, 0, len(rows))
		for _, v := range rows {
			messages = append(messages, toChannelMessage(v.ID, v.PacketHashHex, v.ChannelHash, v.SenderName, v.Content, v.SentAt))
		}
	}

	var nextCursor *int64
	if hasMore && len(messages) > 0 {
		last := messages[len(messages)-1].ID
		nextCursor = &last
	}
	return api.Page[api.ChannelMessage]{
		Items:      messages,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// ListChannelMessagesByHash returns paginated messages for all channels matching the given hash.
// May return messages from multiple channels if the hash collides across different keys.
// Pass a zero time.Time for since to return all messages up to limit.
// Pass empty string iata to return messages from all IATAs.
// Pass cursor=0 to start from the beginning.
func (s *Store) ListChannelMessagesByHash(ctx context.Context, hash []byte, since time.Time, limit int32, iata string, cursor int64) (api.Page[api.ChannelMessage], error) {
	rows, err := s.q.ListChannelMessagesByHash(ctx, sqlc.ListChannelMessagesByHashParams{
		ChannelHash: hash,
		Column2:     pgtype.Timestamptz{Time: since, Valid: !since.IsZero()},
		Column3:     iata,
		Column4:     cursor,
		Limit:       limit + 1,
	})
	if err != nil {
		return api.Page[api.ChannelMessage]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	messages := make([]api.ChannelMessage, 0, len(rows))
	for _, v := range rows {
		messages = append(messages, toChannelMessage(v.ID, hex.EncodeToString(v.PacketHash), v.ChannelHash, v.SenderName, v.Content, v.SentAt))
	}
	var nextCursor *int64
	if hasMore && len(messages) > 0 {
		last := messages[len(messages)-1].ID
		nextCursor = &last
	}
	return api.Page[api.ChannelMessage]{
		Items:      messages,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// ListObservers returns a summary list of observers with optional filters.
// All filter params are optional — pass empty string to skip a filter.
// status is "online" or "offline" derived from last_status_at recency.
// ListObservers returns a paginated list of observers with optional filters.
// cursor is last_seen epoch ms of the last observer; pass 0 to start from the beginning.
func (s *Store) ListObservers(ctx context.Context, iata, observerType, broker, status, name string, cursor int64, limit int32) (api.Page[api.ObserverSummary], error) {
	var cursorTS pgtype.Timestamptz
	if cursor > 0 {
		cursorTS = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	params := sqlc.ListObserversParams{
		Column1: iata,
		Column2: observerType,
		Column3: broker,
		Column4: status,
		Column5: name,
		Column6: cursorTS,
		Limit:   limit + 1,
	}
	rows, err := s.q.ListObservers(ctx, params)
	if err != nil {
		return api.Page[api.ObserverSummary]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.ObserverSummary, 0, len(rows))
	for _, v := range rows {
		observer := api.ObserverSummary{
			ID:     v.ID,
			IATA:   v.Iata,
			Status: v.Status,
		}
		if v.DisplayName != nil {
			observer.DisplayName = v.DisplayName
		}
		if v.ObserverType != nil {
			observer.ObserverType = v.ObserverType
		}
		items = append(items, observer)
	}
	var nextCursor *int64
	if hasMore {
		// observers use UUID so encode last_seen as cursor
		if rows[len(rows)-1].LastStatusAt.Valid {
			ms := rows[len(rows)-1].LastStatusAt.Time.UnixMilli()
			nextCursor = &ms
		}
	}
	return api.Page[api.ObserverSummary]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// GetObserver returns full detail for a single observer by UUID.
// Returns nil, pgx.ErrNoRows if the observer is not found.
func (s *Store) GetObserver(ctx context.Context, observerID uuid.UUID) (*api.Observer, error) {
	obs, err := s.q.GetObserverByID(ctx, observerID)
	if err != nil {
		return nil, err
	}
	brokerRows, err := s.q.GetObserverBrokers(ctx, observerID)
	if err != nil {
		return nil, err
	}
	observer := api.Observer{
		ObserverSummary: api.ObserverSummary{
			ID:           obs.ID,
			DisplayName:  obs.DisplayName,
			ObserverType: obs.ObserverType,
			Status:       "offline",
		},
		PublicKey:        hex.EncodeToString(obs.PublicKey),
		SoftwareVersion:  obs.SoftwareVersion,
		HardwareModel:    obs.HardwareModel,
		FirmwareVersion:  obs.FirmwareVersion,
		FirmwareBuild:    obs.FirmwareBuild,
		RadioFreqMHz:     obs.RadioFreqMhz,
		RadioSF:          obs.RadioSf,
		RadioBWKHz:       obs.RadioBwKhz,
		RadioCR:          obs.RadioCr,
		BatteryLevel:     obs.BatteryLevel,
		UptimeSeconds:    obs.UptimeSeconds,
		StatusMetadata:   obs.StatusMetadata,
		FirstSeen:        obs.FirstSeen.Time.UnixMilli(),
		LastSeen:         obs.LastSeen.Time.UnixMilli(),
		ObservationCount: *obs.ObservationCount,
	}
	brokers := make([]api.ObserverBroker, 0, len(brokerRows))
	for _, v := range brokerRows {
		var lastPacketAt int64
		if v.LastPacketAt.Valid {
			lastPacketAt = v.LastPacketAt.Time.UnixMilli()
		}
		brokers = append(brokers, api.ObserverBroker{
			Name:         v.BrokerName,
			LastPacketAt: lastPacketAt,
			LastSeenAt:   v.LastSeen.Time.UnixMilli(),
		})
	}
	observer.Brokers = brokers
	if obs.LastStatusAt.Valid && time.Since(obs.LastStatusAt.Time) < 5*time.Minute {
		observer.Status = "online"
	}
	var lastStatusAt *int64
	if obs.LastStatusAt.Valid {
		ms := obs.LastStatusAt.Time.UnixMilli()
		lastStatusAt = &ms
	}
	observer.LastStatusAt = lastStatusAt
	observer.IATA, _ = s.GetObserverLastIATA(ctx, observerID)
	return &observer, nil
}

func toChannelMessage(id int64, packetHashHex string, channelHash []byte, senderName *string, content *string, sentAt pgtype.Timestamptz) api.ChannelMessage {
	sn := ""
	if senderName != nil {
		sn = *senderName
	}
	ct := ""
	if content != nil {
		ct = *content
	}
	return api.ChannelMessage{
		ID:          id,
		PacketHash:  packetHashHex,
		ChannelHash: hex.EncodeToString(channelHash),
		SenderName:  sn,
		Content:     ct,
		SentAt:      sentAt.Time.UnixMilli(),
	}
}

// InsertObserverTelemetry stores a telemetry snapshot for an observer.
// reportedAt should be truncated to the configured resolution before calling
// so that ON CONFLICT deduplicates within the resolution window.
func (s *Store) InsertObserverTelemetry(ctx context.Context, observerID uuid.UUID, reportedAt time.Time, batteryMV *int32, txAirSecs, rxAirSecs *float32, noiseFloor float32, uptimeSeconds int64, queueLen, debugFlags, recvErrors *int32) error {
	return s.q.InsertObserverTelemetry(ctx, sqlc.InsertObserverTelemetryParams{
		ObserverID:       observerID,
		ReportedAt:       pgtype.Timestamptz{Time: reportedAt, Valid: true},
		BatteryVoltageMv: batteryMV,
		AirtimeTxPct:     txAirSecs,
		AirtimeRxPct:     rxAirSecs,
		NoiseFloorDb:     &noiseFloor,
		UptimeSeconds:    &uptimeSeconds,
		QueueLength:      queueLen,
		DebugFlags:       debugFlags,
		ReceiveErrors:    recvErrors,
	})
}

// DeleteOldTelemetry removes telemetry rows older than cutoff.
// Called by the cleanup goroutine in main.
func (s *Store) DeleteOldTelemetry(ctx context.Context, cutoff time.Time) error {
	return s.q.DeleteOldTelemetry(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true})
}

// DeleteOldPackets removes packets older than cutoff.
// packet_observations cascade-delete via FK constraint.
// Called by the cleanup goroutine in main.
func (s *Store) DeleteOldPackets(ctx context.Context, cutoff time.Time) error {
	return s.q.DeleteOldPackets(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true})
}

// RefreshHourlyStats refreshes the mv_hourly_iata_stats materialized view.
// Called by the cleanup goroutine to keep hourly observation stats current.
func (s *Store) RefreshHourlyStats(ctx context.Context) error {
	return s.q.RefreshHourlyStats(ctx)
}

// RefreshTopNodes refreshes the mv_top_nodes_by_iata materialized view.
// Called by the cleanup goroutine to keep top node rankings current.
func (s *Store) RefreshTopNodes(ctx context.Context) error {
	return s.q.RefreshTopNodes(ctx)
}

// GetObserverTelemetry returns telemetry points for an observer within the given time range.
// since and until define the window; pass zero times to use defaults (last 24h).
// TODO: implement server-side bucketing by interval when needed.
// Currently returns all points in the range at stored resolution.
func (s *Store) GetObserverTelemetry(ctx context.Context, observerID uuid.UUID, since, until time.Time, afterID int64) (*api.ObserverTelemetry, error) {
	rows, err := s.q.GetObserverTelemetry(ctx, sqlc.GetObserverTelemetryParams{
		ObserverID: observerID,
		Column2:    pgtype.Timestamptz{Time: since, Valid: !since.IsZero()},
		Column3:    pgtype.Timestamptz{Time: until, Valid: !until.IsZero()},
		Column4:    afterID,
	})
	if err != nil {
		return nil, err
	}
	points := make([]api.ObserverTelemetryPoint, 0, len(rows))
	for _, v := range rows {
		points = append(points, api.ObserverTelemetryPoint{
			T:             v.ReportedAt.Time.Unix(),
			BatteryMV:     v.BatteryVoltageMv,
			AirtimeTxPct:  v.AirtimeTxPct,
			AirtimeRxPct:  v.AirtimeRxPct,
			NoiseFloorDB:  v.NoiseFloorDb,
			UptimeSeconds: v.UptimeSeconds,
			QueueLength:   v.QueueLength,
			ReceiveErrors: v.ReceiveErrors,
		})
	}
	return &api.ObserverTelemetry{Points: points}, nil
}

// ListObserverAdverts returns a paginated list of advert packets heard by an observer.
// Pass cursor=0 to start from the beginning.
func (s *Store) ListObserverAdverts(ctx context.Context, observerID uuid.UUID, cursor int64, limit int32) (api.Page[api.AdvertObservation], error) {
	rows, err := s.q.ListObserverAdverts(ctx, sqlc.ListObserverAdvertsParams{
		ObserverID: observerID,
		Column2:    cursor,
		Limit:      limit + 1, // fetch one extra to detect hasMore
	})
	if err != nil {
		log.Printf("api: ListObserverAdverts failed: %v", err)
		return api.Page[api.AdvertObservation]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.AdvertObservation, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.AdvertObservation{
			PacketObservationSummary: api.PacketObservationSummary{
				ID:              v.ID,
				PacketHash:      v.PacketHashHex,
				PayloadType:     v.PayloadType,
				PayloadTypeName: api.PayloadTypeName(v.PayloadType),
				IATA:            v.Iata,
				HeardAt:         v.HeardAt.Time.UnixMilli(),
				RSSI:            v.Rssi,
				SNR:             v.Snr,
				HopCount:        &v.HopCount,
			},
			NodeName:      v.NodeName,
			NodePublicKey: &v.NodePublicKey,
		})
	}
	var nextCursor *int64
	if hasMore {
		last := items[len(items)-1].ID
		nextCursor = &last
	}
	return api.Page[api.AdvertObservation]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// ListNodes returns a paginated list of nodes with optional filters.
// Pass 0 for nodeType, empty string for iata/name, nil for pubkey to skip those filters.
// cursor is last_seen epoch ms; pass 0 to start from the beginning.
func (s *Store) ListNodes(ctx context.Context, nodeType int16, iata string, supportsMultibytePaths, supportsMultibyteTraces bool, pubkey []byte, name string, cursor int64, limit int32) (api.Page[api.NodeSummary], error) {
	var cursorTS pgtype.Timestamptz
	if cursor > 0 {
		cursorTS = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	}
	rows, err := s.q.ListNodes(ctx, sqlc.ListNodesParams{
		Column1: nodeType,
		Column2: iata,
		Column3: supportsMultibytePaths,
		Column4: supportsMultibyteTraces,
		Column5: pubkey,
		Column6: name,
		Column7: cursorTS,
		Limit:   limit + 1,
	})
	if err != nil {
		return api.Page[api.NodeSummary]{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	items := make([]api.NodeSummary, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.NodeSummary{
			ID:           v.ID,
			PublicKey:    hex.EncodeToString(v.PublicKey),
			NodeType:     v.NodeType,
			NodeTypeName: api.NodeTypeName(v.NodeType),
			Name:         v.Name,
			Latitude:     v.Latitude,
			Longitude:    v.Longitude,
		})
	}
	var nextCursor *int64
	if hasMore && len(items) > 0 {
		ms := rows[len(rows)-1].LastSeen.Time.UnixMilli()
		nextCursor = &ms
	}
	return api.Page[api.NodeSummary]{
		Items:      items,
		NextCursor: nextCursor,
		HasMore:    hasMore,
	}, nil
}

// GetNode returns full detail for a single node by UUID.
// Returns nil, pgx.ErrNoRows if the node is not found.
func (s *Store) GetNode(ctx context.Context, nodeID uuid.UUID) (*api.Node, error) {
	row, err := s.q.GetNodeByID(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	node := &api.Node{
		NodeSummary: api.NodeSummary{
			ID:           row.ID,
			PublicKey:    hex.EncodeToString(row.PublicKey),
			NodeType:     row.NodeType,
			NodeTypeName: api.NodeTypeName(row.NodeType),
			Name:         row.Name,
			Latitude:     row.Latitude,
			Longitude:    row.Longitude,
		},
		LocationSource:          row.LocationSource,
		SupportsMultibytePaths:  row.SupportsMultibytePaths,
		SupportsMultibyteTraces: row.SupportsMultibyteTraces,
		MinFirmwareVersion:      row.MinFirmwareVersion,
		FirstSeen:               row.FirstSeen.Time.UnixMilli(),
		LastSeen:                row.LastSeen.Time.UnixMilli(),
		Metadata:                row.Metadata,
	}
	if row.LastAdvertAt.Valid {
		ms := row.LastAdvertAt.Time.UnixMilli()
		node.LastAdvertAt = &ms
	}
	return node, nil
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

func (s *Store) ListPackets(ctx context.Context, payloadType, routeType int16, iata string, since, until time.Time, cursor int64, limit int32) (api.Page[api.PacketSummary], error) {
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
		Column1: payloadType,
		Column2: routeType,
		Column3: iata,
		Column4: sinceTS,
		Column5: untilTS,
		Column6: cursorTS,
		Limit:   limit + 1,
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
			FirstHeardAt:     v.FirstHeardAt.Time.UnixMilli(),
			LastHeardAt:      v.LastHeardAt.Time.UnixMilli(),
			ObservationCount: *v.ObservationCount,
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

func (s *Store) GetPacket(ctx context.Context, packetHash []byte) (*api.Packet, error) {
	row, err := s.q.GetPacketByHash(ctx, packetHash)
	if err != nil {
		return nil, err
	}
	obsRows, err := s.q.ListObservationsForPacket(ctx, packetHash)
	if err != nil {
		return nil, err
	}
	p := &api.Packet{
		PacketHash:      hex.EncodeToString(row.PacketHash),
		PayloadType:     row.PayloadType,
		PayloadTypeName: api.PayloadTypeName(row.PayloadType),
		PayloadVersion:  row.PayloadVersion,
		RouteType:       row.RouteType,
		RouteTypeName:   api.RouteTypeName(row.RouteType),
		RawPayload:      hex.EncodeToString(row.RawPayload),
		ParsedPayload:   row.ParsedPayload,
		Decrypted:       row.Decrypted != nil && *row.Decrypted,
		FirstHeardAt:    row.FirstHeardAt.Time.UnixMilli(),
		LastHeardAt:     row.LastHeardAt.Time.UnixMilli(),
		Observations:    make([]api.PacketObservationDetail, 0, len(obsRows)),
	}
	if row.ObservationCount != nil {
		p.ObservationCount = *row.ObservationCount
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
		// TODO: decode transport codes from region_code/sub_region_code
	}
	for _, v := range obsRows {
		obs := api.PacketObservationDetail{
			ID:             v.ID,
			ObserverID:     v.ObserverID,
			ObserverName:   v.ObserverName,
			IATA:           v.Iata,
			HeardAt:        v.HeardAt.Time.UnixMilli(),
			PathLengthByte: v.PathLengthByte,
			HashSize:       v.HashSize,
			HopCount:       v.HopCount,
			RSSI:           v.Rssi,
			SNR:            v.Snr,
			SourceBroker:   *v.SourceBroker,
			ResolvedPath:   []api.ResolvedHop{}, // TODO: implement path resolution
		}
		if v.PathBytes != nil {
			pb := hex.EncodeToString(v.PathBytes)
			obs.PathBytes = &pb
		}
		if v.PropagationTimeMs != nil {
			obs.PropagationTimeMs = v.PropagationTimeMs
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
	return p, nil
}

// GetStatsOverview returns top-line network figures for the last 24 hours.
func (s *Store) GetStatsOverview(ctx context.Context, iata string) (*api.StatsOverview, error) {
	row, err := s.q.GetStatsOverview(ctx, iata)
	if err != nil {
		return nil, err
	}
	return &api.StatsOverview{
		TotalPackets:      row.TotalPackets,
		TotalObservations: row.TotalObservations,
		ActiveObservers:   row.ActiveObservers,
		ActiveIATAs:       row.ActiveIatas,
		WindowHours:       24,
	}, nil
}

// GetStatsObservations returns hourly observation counts for charting.
func (s *Store) GetStatsObservations(ctx context.Context, iata string, since time.Time) ([]api.ObservationPoint, error) {
	if since.IsZero() {
		since = time.Now().Add(-7 * 24 * time.Hour)
	}
	interval := time.Since(since)
	rows, err := s.q.GetHourlyStats(ctx, sqlc.GetHourlyStatsParams{
		Column1: iata,
		Column2: pgtype.Interval{Microseconds: int64(interval.Hours()) * 3600 * 1e6, Valid: true},
	})
	if err != nil {
		return nil, err
	}
	points := make([]api.ObservationPoint, 0, len(rows))
	for _, v := range rows {
		points = append(points, api.ObservationPoint{
			Hour:             v.Hour.Time.UnixMilli(),
			IATA:             v.Iata,
			ObservationCount: v.ObservationCount,
			UniquePackets:    v.UniquePackets,
			ActiveObservers:  v.ActiveObservers,
		})
	}
	return points, nil
}

func (s *Store) GetStatsPayloadBreakdown(ctx context.Context, iata string, since time.Time) ([]api.PayloadBreakdownItem, error) {
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	rows, err := s.q.GetStatsPayloadBreakdown(ctx, sqlc.GetStatsPayloadBreakdownParams{
		HeardAt: pgtype.Timestamptz{Time: since, Valid: true},
		Column2: iata,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.PayloadBreakdownItem, 0, len(rows))
	for _, v := range rows {
		items = append(items, api.PayloadBreakdownItem{
			PayloadType:     v.PayloadType,
			PayloadTypeName: api.PayloadTypeName(v.PayloadType),
			Count:           v.Count,
		})
	}
	return items, nil
}

func (s *Store) GetStatsTopNodes(ctx context.Context, iata string, limit int32) ([]api.TopNode, error) {
	rows, err := s.q.GetTopNodes(ctx, sqlc.GetTopNodesParams{
		Column1: iata,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.TopNode, 0, len(rows))
	for _, v := range rows {
		var count int64
		if v.ObservationCount != nil {
			count = *v.ObservationCount
		}
		items = append(items, api.TopNode{
			NodeID:           v.NodeID,
			NodeName:         v.Name,
			NodeType:         v.NodeType,
			NodeTypeName:     api.NodeTypeName(v.NodeType),
			IATA:             v.Iata,
			ObservationCount: count,
			LastHeard:        v.LastHeard.Time.UnixMilli(),
		})
	}
	return items, nil
}

func (s *Store) GetStatsTopObservers(ctx context.Context, iata string, since time.Time, limit int32) ([]api.TopObserver, error) {
	if since.IsZero() {
		since = time.Now().Add(-24 * time.Hour)
	}
	rows, err := s.q.GetStatsTopObservers(ctx, sqlc.GetStatsTopObserversParams{
		HeardAt: pgtype.Timestamptz{Time: since, Valid: true},
		Column2: iata,
		Limit:   limit,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.TopObserver, 0, len(rows))
	for _, v := range rows {
		iata, _ := v.Iata.(string)
		items = append(items, api.TopObserver{
			ObserverID:       v.ID,
			DisplayName:      v.DisplayName,
			ObserverType:     v.ObserverType,
			IATA:             iata,
			ObservationCount: v.ObservationCount,
		})
	}
	return items, nil
}
