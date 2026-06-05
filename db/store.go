// Package db implements the ingest.DB interface using sqlc-generated queries
// over a pgx/v5 connection pool. Each method is a thin mapping layer between
// the ingest param structs and the sqlc-generated param structs.
package db

import (
	"context"
	"encoding/hex"
	"encoding/json"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Store wraps the sqlc-generated Queries and implements both ingest.DB and api.Reader.
type Store struct {
	q *sqlc.Queries
}

// New creates a Store backed by the given pgxpool connection pool.
func New(pool *pgxpool.Pool) *Store {
	return &Store{q: sqlc.New(pool)}
}

// ResolvePathHashes returns a map of hex-encoded path hash → matching node entries for
// the given IATA. Hash size is inferred from the length of the first element in hashes.
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

// resolveTraceRoute resolves the path hashes from a trace parsedPayload across
// all provided IATAs, merging results per hop with best-confidence-wins semantics.
// Returns nil if the payload cannot be parsed or contains no path hashes.
func (s *Store) resolveTraceRoute(ctx context.Context, parsedPayload []byte, iatas []string) []api.ResolvedHop {
	var tracePayload struct {
		PathHashes []string `json:"pathHashes"`
		Flags      byte     `json:"flags"`
	}
	if err := json.Unmarshal(parsedPayload, &tracePayload); err != nil || len(tracePayload.PathHashes) == 0 {
		return nil
	}
	hashSize := int(1 << (tracePayload.Flags & 0x03))
	hashes := make([][]byte, 0, len(tracePayload.PathHashes))
	for _, h := range tracePayload.PathHashes {
		b, err := hex.DecodeString(h)
		if err == nil {
			hashes = append(hashes, b)
		}
	}
	confidenceRank := map[string]int{"none": 0, "ambiguous": 1, "high": 2}
	type hopResult struct {
		confidence string
		entries    []api.ResolvedPathEntry
	}
	merged := make([]hopResult, len(hashes))
	for i := range merged {
		merged[i] = hopResult{confidence: "none"}
	}
	for _, iata := range iatas {
		resolved, err := s.ResolvePathHashes(ctx, iata, hashes)
		if err != nil {
			continue
		}
		for i, hash := range hashes {
			key := hex.EncodeToString(hash[:hashSize])
			entries := resolved[key]
			var confidence string
			switch len(entries) {
			case 0:
				confidence = "none"
			case 1:
				confidence = "high"
			default:
				confidence = "ambiguous"
			}
			if confidenceRank[confidence] > confidenceRank[merged[i].confidence] {
				merged[i] = hopResult{confidence: confidence, entries: entries}
			}
		}
	}
	route := make([]api.ResolvedHop, 0, len(hashes))
	for _, hr := range merged {
		hop := api.ResolvedHop{
			Confidence: hr.confidence,
			Nodes:      make([]api.ResolvedNode, 0, len(hr.entries)),
		}
		for _, e := range hr.entries {
			hop.Nodes = append(hop.Nodes, api.ResolvedNode{
				ID:        e.NodeID,
				Name:      e.Name,
				Latitude:  e.Latitude,
				Longitude: e.Longitude,
				PublicKey: hex.EncodeToString(e.PublicKey),
			})
		}
		route = append(route, hop)
	}
	return route
}
