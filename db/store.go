// Package db implements the ingest.DB interface using sqlc-generated queries
// over a pgx/v5 connection pool. Each method is a thin mapping layer between
// the ingest param structs and the sqlc-generated param structs.
package db

import (
	"context"
	"encoding/binary"
	"errors"

	sqlc "tower/db/sqlc"
	"tower/internal/api"
	"tower/internal/ingest"

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

// UpsertChannel upserts a channel row and returns its integer ID.
func (s *Store) UpsertChannel(ctx context.Context, channelHash []byte) (int, error) {
	row, err := s.q.UpsertChannel(ctx, sqlc.UpsertChannelParams{
		ChannelHash:  channelHash,
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
