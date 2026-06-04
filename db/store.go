// Package db implements the ingest.DB interface using sqlc-generated queries
// over a pgx/v5 connection pool. Each method is a thin mapping layer between
// the ingest param structs and the sqlc-generated param structs.
package db

import (
	"context"
	"encoding/hex"

	sqlc "github.com/MeshCore-Tower/tower-server/db/sqlc"
	"github.com/MeshCore-Tower/tower-server/internal/api"
	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	q *sqlc.Queries
}

func New(pool *pgxpool.Pool) *Store {
	return &Store{q: sqlc.New(pool)}
}

func (s *Store) ResolvePathHashes(ctx context.Context, iata string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	if len(hashes) == 0 {
		return nil, nil
	}
	rows, err := s.q.ResolvePathHashes(ctx, sqlc.ResolvePathHashesParams{
		Iata:    iata,
		Column2: hashes,
	})
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

func nullableUUID(id uuid.UUID) *uuid.UUID {
	if id == (uuid.UUID{}) {
		return nil
	}
	return &id
}

func tristate(b *bool) string {
	if b == nil {
		return "any"
	}
	if *b {
		return "true"
	}
	return "false"
}

func toChannelMessage(id int64, packetHashHex string, channelHash []byte, senderName *string, content *string, sentAt pgtype.Timestamptz, observationCount int64) api.ChannelMessage {
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
	}
}
