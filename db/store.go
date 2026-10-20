// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package db implements the ingest.DB interface using sqlc-generated queries
// over a pgx/v5 connection pool. Each method is a thin mapping layer between
// the ingest param structs and the sqlc-generated param structs.
package db

import (
	"context"
	"encoding/hex"
	"sync/atomic"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps the sqlc-generated Queries and implements both ingest.DB and api.Reader.
type Store struct {
	q                   sqlc.Querier
	pool                *pgxpool.Pool   // dedicated connections (rollups, reconfirm, catalog reads); nil for the API store and mocks
	clockDriftThreshold time.Duration   // see api.Node.ClockOutOfSync
	staleThreshold      time.Duration   // see api.NodeSummary.Stale
	pgVersion           atomic.Int32    // server_version_num, read by the first PgActivity call
	channelIDs          *ChannelIDCache // shared by the stores that write channels; nil for the API store and mocks
	payloadTypeGate     bool            // see WithPayloadTypeGate
	payloadTypesDone    atomic.Bool     // the payload-type backfill state has read done
	payloadTypeWarnAt   atomic.Int64    // unix ns of the last state-read Warn; see payloadTypesReady
	readModels          *ReadModels     // see WithReadModels; nil: the read-model pages answer api.ErrPagesNotReady
	packetSitesVacuums  atomic.Int32    // VACUUM (ANALYZE) runs of packet_sites by the first-time verification
}

// New creates a Store whose statements run on dbtx (a pool, or a PlanRouter for the API).
// pool serves the work that needs a dedicated connection; the API store passes nil.
// clockDriftThreshold is the |device clock - server clock| magnitude above which a repeater/room
// server's clockOutOfSync is reported true; see internal/config.ResolvedConfig.ClockDriftThreshold.
// staleThreshold is how long since last_seen before a node's Stale is reported true; see
// internal/config.ResolvedConfig.NodeStaleThreshold.
func New(dbtx sqlc.DBTX, pool *pgxpool.Pool, clockDriftThreshold, staleThreshold time.Duration, opts ...Option) *Store {
	s := &Store{q: sqlc.New(dbtx), pool: pool, clockDriftThreshold: clockDriftThreshold, staleThreshold: staleThreshold}
	for _, o := range opts {
		o(s)
	}
	return s
}

func (s *Store) ResolvePathHashes(ctx context.Context, iata string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	// One query per prefix width; the row shapes are identical.
	var rows []sqlc.ResolvePathHashesP4Row
	var err error
	switch len(hashes[0]) {
	case 1:
		var rs []sqlc.ResolvePathHashesP1Row
		rs, err = s.q.ResolvePathHashesP1(ctx, sqlc.ResolvePathHashesP1Params{Iata: iata, Column2: hashes})
		for _, r := range rs {
			rows = append(rows, sqlc.ResolvePathHashesP4Row(r))
		}
	case 2:
		var rs []sqlc.ResolvePathHashesP2Row
		rs, err = s.q.ResolvePathHashesP2(ctx, sqlc.ResolvePathHashesP2Params{Iata: iata, Column2: hashes})
		for _, r := range rs {
			rows = append(rows, sqlc.ResolvePathHashesP4Row(r))
		}
	case 3:
		var rs []sqlc.ResolvePathHashesP3Row
		rs, err = s.q.ResolvePathHashesP3(ctx, sqlc.ResolvePathHashesP3Params{Iata: iata, Column2: hashes})
		for _, r := range rs {
			rows = append(rows, sqlc.ResolvePathHashesP4Row(r))
		}
	case 4:
		rows, err = s.q.ResolvePathHashesP4(ctx, sqlc.ResolvePathHashesP4Params{Iata: iata, Column2: hashes})
	default:
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	result := make(map[string][]api.ResolvedPathEntry)
	for _, row := range rows {
		key := hex.EncodeToString(row.Hash[:len(hashes[0])])
		result[key] = append(result[key], api.ResolvedPathEntry{
			NodeID:    row.NodeID,
			Name:      row.Name,
			Latitude:  row.Latitude,
			Longitude: row.Longitude,
			PublicKey: row.PublicKey,
		})
	}
	return result, nil
}

// ResolveEndpointHashes matches one-byte packet endpoints across advertised roles.
// Keep this separate from the infrastructure-only intermediate path resolver.
func (s *Store) ResolveEndpointHashes(ctx context.Context, iata string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	for _, hash := range hashes {
		if len(hash) != 1 {
			return nil, nil
		}
	}
	rows, err := s.q.ResolveEndpointHashes(ctx, sqlc.ResolveEndpointHashesParams{Iata: iata, Column2: hashes})
	if err != nil {
		return nil, err
	}
	result := make(map[string][]api.ResolvedPathEntry)
	for _, row := range rows {
		key := hex.EncodeToString(row.Hash)
		result[key] = append(result[key], api.ResolvedPathEntry{
			NodeID: row.NodeID, Name: row.Name, Latitude: row.Latitude,
			Longitude: row.Longitude, PublicKey: row.PublicKey,
		})
	}
	return result, nil
}

// nullableUUID returns nil for a zero UUID, or a pointer to the UUID otherwise.
func nullableUUID(id uuid.UUID) *uuid.UUID {
	if id == (uuid.UUID{}) {
		return nil
	}
	return &id
}

// tristate converts a *bool to a SQL-friendly string for the ListNodes filter:
// nil → "any", true → "true", false → "false".
func tristate(b *bool) string {
	if b == nil {
		return "any"
	}
	if *b {
		return "true"
	}
	return "false"
}

// toChannelMessage maps raw sqlc row fields to an api.ChannelMessage.
func toChannelMessage(id int64, packetHashHex string, channelHash []byte, senderName *string, content *string, sentAt pgtype.Timestamptz, observationCount int64, scope *string, transport *bool) api.ChannelMessage {
	sn := ""
	if senderName != nil {
		sn = *senderName
	}
	ct := ""
	if content != nil {
		ct = *content
	}
	return api.ChannelMessage{
		ID:               id,
		PacketHash:       packetHashHex,
		ChannelHash:      hex.EncodeToString(channelHash),
		SenderName:       sn,
		Content:          ct,
		SentAt:           sentAt.Time.UnixMilli(),
		ObservationCount: observationCount,
		Scope:            scope,
		ScopeStatus:      api.RecordedChannelScopeStatus(scope, transport),
	}
}

// deleteInBatches repeats a delete until a batch comes back short.
func deleteInBatches(ctx context.Context, batchSize int32, del func(context.Context, int32) (int64, error)) error {
	for {
		n, err := del(ctx, batchSize)
		if err != nil {
			return err
		}
		if n < int64(batchSize) {
			return nil
		}
		if err := ctx.Err(); err != nil {
			return err
		}
	}
}
