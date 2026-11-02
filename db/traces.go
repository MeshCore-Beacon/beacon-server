// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/MeshCore-Beacon/beacon-server/internal/tracequality"
	"github.com/google/uuid"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/jackc/pgx/v5/pgtype"
)

type tracePayload struct {
	Raw        string               `json:"raw"`
	Quality    tracequality.Quality `json:"quality"`
	PathHashes []string             `json:"pathHashes"`
	Flags      byte                 `json:"flags"`
	SNRValues  []float32            `json:"snrValues"`
}

func (s *Store) RecordTrace(ctx context.Context, h ingest.TraceHearing) error {
	return s.q.RecordTrace(ctx, sqlc.RecordTraceParams{
		TraceTag: h.TraceTag,
		Iata:     h.IATA,
		HeardAt:  pgtype.Timestamptz{Time: h.HeardAt, Valid: true},
	})
}

func (s *Store) DeleteOldTraceTags(ctx context.Context, cutoff time.Time) error {
	return s.q.DeleteOldTraceTags(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true})
}

func (s *Store) DeleteOldTraceIATAs(ctx context.Context, cutoff time.Time) error {
	return s.q.DeleteOldTraceIATAs(ctx, pgtype.Timestamptz{Time: cutoff, Valid: true})
}

func (s *Store) ListTraceTags(ctx context.Context, iatas []string, scope, traceType string, since, until time.Time, cursor time.Time, cursorTag string, limit int32) ([]api.TraceTagSummary, error) {
	var sinceTS, untilTS, cursorTS pgtype.Timestamptz
	if !since.IsZero() {
		sinceTS = pgtype.Timestamptz{Time: since, Valid: true}
	}
	if !until.IsZero() {
		untilTS = pgtype.Timestamptz{Time: until, Valid: true}
	}
	if !cursor.IsZero() {
		cursorTS = pgtype.Timestamptz{Time: cursor, Valid: true}
	}
	var tag []byte
	if cursorTag != "" {
		b, err := hex.DecodeString(cursorTag)
		if err != nil {
			return nil, fmt.Errorf("cursor tag: %w", err)
		}
		tag = b
	}
	rows, err := s.q.ListTraceTags(ctx, sqlc.ListTraceTagsParams{
		Column1: iatas,
		Column2: scope,
		Column3: sinceTS,
		Column4: untilTS,
		Column5: cursorTS,
		Limit:   limit,
		Column7: traceType,
		Column8: tag,
	})
	if err != nil {
		return nil, err
	}
	items := make([]api.TraceTagSummary, 0, len(rows))
	payloads := make([]tracePayload, 0, len(rows))
	allIATAs := map[string]bool{}
	allHashes := map[string][]byte{}
	for _, r := range rows {
		var best tracePayload
		if len(r.BestPayload) > 0 {
			_ = json.Unmarshal(r.BestPayload, &best)
		}
		quality := tracequality.Stored(best.Raw, best.Flags, best.PathHashes, best.SNRValues)
		for _, reason := range best.Quality.Reasons {
			if reason != "ambiguous_prefix" {
				quality.Add(reason)
			}
		}
		if r.UnsupportedVersion {
			quality.Add("unsupported_version")
		}
		if r.InvalidSnrPath {
			quality.Add("invalid_snr_path")
		}
		if r.NonDirect {
			quality.Add("non_direct_trace")
		}
		payloads = append(payloads, best)
		if quality.Status == "supported" {
			for _, iata := range r.Iatas {
				allIATAs[iata] = true
			}
			for _, h := range best.PathHashes {
				b, _ := hex.DecodeString(h)
				allHashes[h] = b
			}
		}
		items = append(items, api.TraceTagSummary{
			Quality:      quality,
			TraceTag:     r.TraceTag,
			FirstHeardAt: r.FirstHeardAt.Time.UnixMilli(),
			LastHeardAt:  r.LastHeardAt.Time.UnixMilli(),
			PacketCount:  r.PacketCount,
			IATACount:    r.IataCount,
			TraceType:    r.TraceType,
			PathHashes:   best.PathHashes,
			SNRValues:    best.SNRValues,
		})
	}
	// One bounded candidate query for the entire page, including all hash widths.
	regions := make([]string, 0, len(allIATAs))
	for iata := range allIATAs {
		regions = append(regions, iata)
	}
	hashes := make([][]byte, 0, len(allHashes))
	for _, h := range allHashes {
		hashes = append(hashes, h)
	}
	candidates, err := s.traceCandidates(ctx, regions, hashes)
	if err != nil {
		return nil, err
	}
	for i := range items {
		if items[i].Quality.Status != "supported" {
			continue
		}
		for _, hop := range traceRoute(&payloads[i], rows[i].Iatas, candidates) {
			if hop.Confidence == "ambiguous" {
				items[i].Quality.Add("ambiguous_prefix")
				break
			}
		}
	}
	return items, nil
}

func (s *Store) GetTraceByTag(ctx context.Context, tag string) (*api.TraceDetail, error) {
	rows, err := s.q.GetPacketsByTraceTag(ctx, tag)
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return nil, nil
	}
	detail := &api.TraceDetail{
		TraceTag: tag,
		Packets:  make([]api.TracePacket, 0, len(rows)),
	}
	for _, r := range rows {
		packet := api.TracePacket{
			PacketHash:    r.PacketHashHex,
			RouteType:     r.RouteType,
			RouteTypeName: api.RouteTypeName(r.RouteType),
			Scope:         r.ScopeName,
			FirstHeardAt:  r.FirstHeardAt.Time.UnixMilli(),
			LastHeardAt:   r.LastHeardAt.Time.UnixMilli(),
		}
		var parsed tracePayload
		if err := json.Unmarshal(r.ParsedPayload, &parsed); err == nil {
			// build raw path
			rawPath := make([]api.RawHop, 0, len(parsed.PathHashes))
			for i, h := range parsed.PathHashes {
				hop := api.RawHop{Hash: h}
				if i < len(parsed.SNRValues) {
					snr := parsed.SNRValues[i]
					hop.SNR = &snr
				}
				rawPath = append(rawPath, hop)
			}
			packet.RawPath = rawPath
		}
		packet.Quality = tracequality.Assess(r.RawPayload, int(r.PayloadVersion), int(r.RouteType), len(parsed.SNRValues))
		if r.InvalidSnrPath {
			packet.Quality.Add("invalid_snr_path")
		}
		stored := tracequality.Stored(parsed.Raw, parsed.Flags, parsed.PathHashes, parsed.SNRValues)
		for _, reason := range stored.Reasons {
			packet.Quality.Add(reason)
		}
		if len(r.Iatas) > 0 && packet.Quality.Status == "supported" {
			packet.ResolvedRoute = s.resolveTraceRoute(ctx, &parsed, r.Iatas)
		}
		for _, hop := range packet.ResolvedRoute {
			if hop.Confidence == "ambiguous" {
				packet.Quality.Add("ambiguous_prefix")
			}
		}
		detail.Packets = append(detail.Packets, packet)
	}
	return detail, nil
}

// Candidates remain separated by hearing region until a trace is assembled.
// Never replace an ambiguous region with a convenient singleton from another.
type traceCandidateMap map[string]map[string][]api.ResolvedPathEntry

func (s *Store) traceCandidates(ctx context.Context, iatas []string, hashes [][]byte) (traceCandidateMap, error) {
	out := traceCandidateMap{}
	if len(iatas) == 0 || len(hashes) == 0 {
		return out, nil
	}
	rows, err := s.q.ResolveTraceHashes(ctx, sqlc.ResolveTraceHashesParams{Iatas: iatas, Hashes: hashes})
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		if out[r.Iata] == nil {
			out[r.Iata] = map[string][]api.ResolvedPathEntry{}
		}
		h := hex.EncodeToString(r.Hash)
		out[r.Iata][h] = append(out[r.Iata][h], api.ResolvedPathEntry{NodeID: r.NodeID, Name: r.Name, Latitude: r.Latitude, Longitude: r.Longitude, PublicKey: r.PublicKey})
	}
	return out, nil
}

func (s *Store) ResolveTracePathHashes(ctx context.Context, iata string, hashes [][]byte) (map[string][]api.ResolvedPathEntry, error) {
	candidates, err := s.traceCandidates(ctx, []string{iata}, hashes)
	return candidates[iata], err
}

func (s *Store) resolveTraceRoute(ctx context.Context, payload *tracePayload, iatas []string) []api.ResolvedHop {
	if payload == nil || tracequality.Stored(payload.Raw, payload.Flags, payload.PathHashes, payload.SNRValues).Status != "supported" {
		return nil
	}
	hashes := make([][]byte, 0, len(payload.PathHashes))
	for _, h := range payload.PathHashes {
		b, _ := hex.DecodeString(h)
		hashes = append(hashes, b)
	}
	candidates, err := s.traceCandidates(ctx, iatas, hashes)
	if err != nil {
		return nil
	}
	return traceRoute(payload, iatas, candidates)
}

func traceRoute(payload *tracePayload, iatas []string, candidates traceCandidateMap) []api.ResolvedHop {
	route := make([]api.ResolvedHop, 0, len(payload.PathHashes))
	for i, h := range payload.PathHashes {
		hop := api.ResolvedHop{Confidence: "none", Nodes: []api.ResolvedNode{}}
		seen := map[uuid.UUID]bool{}
		for _, iata := range iatas {
			for _, e := range candidates[iata][h] {
				if seen[e.NodeID] {
					continue
				}
				seen[e.NodeID] = true
				hop.Nodes = append(hop.Nodes, api.ResolvedNode{ID: e.NodeID, Name: e.Name, Latitude: e.Latitude, Longitude: e.Longitude, PublicKey: hex.EncodeToString(e.PublicKey)})
			}
		}
		if len(hop.Nodes) == 1 {
			hop.Confidence = "high"
		} else if len(hop.Nodes) > 1 {
			hop.Confidence = "ambiguous"
		}
		if i < len(payload.SNRValues) {
			snr := payload.SNRValues[i]
			hop.SNR = &snr
		}
		route = append(route, hop)
	}
	return route
}
