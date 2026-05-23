// Package db implements the ingest.DB interface using sqlc-generated queries
// over a pgx/v5 connection pool. Each method is a thin mapping layer between
// the ingest param structs and the sqlc-generated param structs.
package db

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"strings"
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
	q    *sqlc.Queries
	pool *pgxpool.Pool
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{q: sqlc.New(pool), pool: pool}
}

// UpsertObserver upserts the observers row keyed on pubkey.
func (s *Store) UpsertObserver(ctx context.Context, pubkey []byte) (uuid.UUID, error) {
	row, err := s.q.UpsertObserver(ctx, pubkey)
	if err != nil {
		return uuid.Nil, err
	}
	return row.ID, err
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
func (s *Store) InsertChannelMessage(ctx context.Context, m ingest.InsertChannelMessageParams) error {
	params := sqlc.InsertChannelMessageParams{ChannelID: int32(m.ChannelID), PacketHash: m.PacketHash, SenderName: &m.SenderName, Content: &m.Content, SentAt: pgtype.Timestamptz{Time: m.SentAt, Valid: true}}
	return s.q.InsertChannelMessage(ctx, params)
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

// GetMapState returns the sanitized, mappable state used by the Tower web map.
// Route topology remains empty until ordered per-hop path confidence is complete.
func (s *Store) GetMapState(ctx context.Context, filter api.MapStateFilter) (*api.MapState, error) {
	iatas := normalizeMapIATAs(filter.IATAs)

	nodes, err := s.listMapNodes(ctx, iatas)
	if err != nil {
		return nil, err
	}
	observers, err := s.listMapObservers(ctx, iatas)
	if err != nil {
		return nil, err
	}
	activity, err := s.mapActivitySummary(ctx, iatas)
	if err != nil {
		return nil, err
	}

	return &api.MapState{
		ServerTime: time.Now().UnixMilli(),
		Scope: api.MapScope{
			IATAs:    iatas,
			RegionID: filter.RegionID,
		},
		Metadata: api.MapMetadata{
			Basemap:            "openfreemap",
			RoutesComplete:     false,
			RoutesStatus:       "blocked_by_ordered_path_confidence",
			LiveDefaultEnabled: false,
		},
		Nodes:           nodes,
		Observers:       observers,
		Routes:          []api.MapRoute{},
		ActivitySummary: activity,
	}, nil
}

func (s *Store) listMapNodes(ctx context.Context, iatas []string) ([]api.MapNode, error) {
	rows, err := s.pool.Query(ctx, `
SELECT
  n.id,
  n.node_type,
  n.name,
  n.latitude,
  n.longitude,
  n.first_seen,
  n.last_seen,
  COALESCE(SUM(ni.observation_count), 0)::bigint AS activity_count,
  COALESCE(
    array_agg(DISTINCT trim(ni.iata)::text ORDER BY trim(ni.iata)::text)
      FILTER (WHERE ni.iata IS NOT NULL),
    ARRAY[]::text[]
  ) AS iatas_heard_in
FROM nodes n
LEFT JOIN node_iatas ni ON ni.node_id = n.id
WHERE n.latitude IS NOT NULL
  AND n.longitude IS NOT NULL
  AND (
    $1::text[] IS NULL
    OR EXISTS (
      SELECT 1
      FROM node_iatas scoped
      WHERE scoped.node_id = n.id
        AND trim(scoped.iata)::text = ANY($1::text[])
    )
  )
GROUP BY n.id
ORDER BY n.last_seen DESC
LIMIT 2000
`, mapIATAParam(iatas))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	nodes := []api.MapNode{}
	for rows.Next() {
		var id uuid.UUID
		var nodeType int16
		var name *string
		var lat, lng float64
		var firstSeen, lastSeen time.Time
		var activityCount int64
		var heardIn []string
		if err := rows.Scan(&id, &nodeType, &name, &lat, &lng, &firstSeen, &lastSeen, &activityCount, &heardIn); err != nil {
			return nil, err
		}
		role := mapNodeRole(nodeType)
		nodes = append(nodes, api.MapNode{
			ID:            id.String(),
			Label:         mapDisplayLabel(name, role),
			Role:          role,
			Lat:           lat,
			Lng:           lng,
			FirstSeen:     firstSeen.UnixMilli(),
			LastSeen:      lastSeen.UnixMilli(),
			IATAsHeardIn:  heardIn,
			ActivityCount: activityCount,
		})
	}
	return nodes, rows.Err()
}

func (s *Store) listMapObservers(ctx context.Context, iatas []string) ([]api.MapObserver, error) {
	rows, err := s.pool.Query(ctx, `
WITH latest_iata AS (
  SELECT DISTINCT ON (observer_id)
    observer_id,
    trim(iata)::text AS iata
  FROM packet_observations
  ORDER BY observer_id, heard_at DESC
),
latest_location AS (
  SELECT DISTINCT ON (observer_id)
    observer_id,
    NULLIF(trim(iata)::text, '') AS iata,
    latitude,
    longitude
  FROM observer_locations
  WHERE latitude IS NOT NULL
    AND longitude IS NOT NULL
  ORDER BY observer_id, reported_at DESC
)
SELECT
  o.id,
  o.display_name,
  o.observer_type,
  COALESCE(ll.iata, li.iata, '') AS iata,
  ll.latitude,
  ll.longitude,
  o.last_seen,
  COALESCE(o.observation_count, 0)::bigint AS observation_count,
  o.last_seen >= NOW() - INTERVAL '5 minutes' AS online
FROM observers o
JOIN latest_location ll ON ll.observer_id = o.id
LEFT JOIN latest_iata li ON li.observer_id = o.id
WHERE (
  $1::text[] IS NULL
  OR COALESCE(ll.iata, li.iata, '') = ANY($1::text[])
)
ORDER BY o.last_seen DESC
LIMIT 1000
`, mapIATAParam(iatas))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	observers := []api.MapObserver{}
	for rows.Next() {
		var id uuid.UUID
		var name, observerType *string
		var iata string
		var lat, lng float64
		var lastSeen time.Time
		var observationCount int64
		var online bool
		if err := rows.Scan(&id, &name, &observerType, &iata, &lat, &lng, &lastSeen, &observationCount, &online); err != nil {
			return nil, err
		}
		observers = append(observers, api.MapObserver{
			ID:               id.String(),
			Label:            mapObserverLabel(name, iata),
			Type:             ptrString(observerType, "unknown"),
			IATA:             iata,
			Lat:              lat,
			Lng:              lng,
			Online:           online,
			LastSeen:         lastSeen.UnixMilli(),
			ObservationCount: observationCount,
		})
	}
	return observers, rows.Err()
}

func (s *Store) mapActivitySummary(ctx context.Context, iatas []string) (api.MapActivitySummary, error) {
	var summary api.MapActivitySummary
	var lastHeard pgtype.Timestamptz
	err := s.pool.QueryRow(ctx, `
SELECT
  COUNT(DISTINCT packet_hash)::bigint AS packets_24h,
  COUNT(*)::bigint AS observations_24h,
  COUNT(DISTINCT observer_id)::bigint AS active_observers_24h,
  COUNT(DISTINCT trim(iata)::text)::bigint AS active_iatas_24h,
  MAX(heard_at) AS last_heard_at
FROM packet_observations
WHERE heard_at > NOW() - INTERVAL '24 hours'
  AND ($1::text[] IS NULL OR trim(iata)::text = ANY($1::text[]))
`, mapIATAParam(iatas)).Scan(
		&summary.Packets24h,
		&summary.Observations24h,
		&summary.ActiveObservers24h,
		&summary.ActiveIATAs24h,
		&lastHeard,
	)
	if err != nil {
		return api.MapActivitySummary{}, err
	}
	if lastHeard.Valid {
		ms := lastHeard.Time.UnixMilli()
		summary.LastHeardAt = &ms
	}
	return summary, nil
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

func normalizeMapIATAs(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	seen := map[string]struct{}{}
	out := []string{}
	for _, value := range values {
		iata := strings.ToUpper(strings.TrimSpace(value))
		if iata == "" || iata == "*" {
			continue
		}
		if _, ok := seen[iata]; ok {
			continue
		}
		seen[iata] = struct{}{}
		out = append(out, iata)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func mapIATAParam(iatas []string) any {
	if len(iatas) == 0 {
		return nil
	}
	return iatas
}

func mapNodeRole(nodeType int16) string {
	switch nodeType {
	case 1:
		return "companion"
	case 2:
		return "repeater"
	case 3:
		return "room_server"
	default:
		return "unknown"
	}
}

func mapDisplayLabel(name *string, role string) string {
	if name != nil {
		trimmed := strings.TrimSpace(*name)
		if trimmed != "" {
			return trimmed
		}
	}
	switch role {
	case "repeater":
		return "Repeater"
	case "room_server":
		return "Room server"
	case "companion":
		return "Companion"
	default:
		return "Node"
	}
}

func mapObserverLabel(name *string, iata string) string {
	if name != nil {
		trimmed := strings.TrimSpace(*name)
		if trimmed != "" {
			return trimmed
		}
	}
	if iata != "" {
		return fmt.Sprintf("%s observer", iata)
	}
	return "Observer"
}

func ptrString(value *string, fallback string) string {
	if value == nil {
		return fallback
	}
	trimmed := strings.TrimSpace(*value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}
