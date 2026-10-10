// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"cmp"
	"context"
	"crypto/md5"
	"encoding/hex"
	"slices"
	"strings"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
)

// routePathKey is the route's identity digest: md5 over the comma-joined
// node UUIDs, matching Postgres's decode(md5(array_to_string(node_ids, ',')), 'hex').
func routePathKey(nodeIDs []uuid.UUID) []byte {
	parts := make([]string, len(nodeIDs))
	for i, id := range nodeIDs {
		parts[i] = id.String()
	}
	sum := md5.Sum([]byte(strings.Join(parts, ",")))
	return sum[:]
}

func (s *Store) UpsertKnownRoute(ctx context.Context, nodeIDs []uuid.UUID, hashPrefix [][]byte, iata string, hopCount int32) error {
	return s.q.UpsertKnownRoute(ctx, sqlc.UpsertKnownRouteParams{
		PathKey:    routePathKey(nodeIDs),
		NodeIds:    nodeIDs,
		HashPrefix: hashPrefix,
		Iata:       iata,
		HopCount:   hopCount,
	})
}

func (s *Store) ListKnownRoutes(ctx context.Context, iata string, hopCount int32, cursor time.Time, cursorID int64, limit int32) ([]api.KnownRoute, error) {
	var cursorTS pgtype.Timestamptz
	if !cursor.IsZero() {
		cursorTS = pgtype.Timestamptz{Time: cursor, Valid: true}
	}
	sqlRows, err := s.q.ListKnownRoutes(ctx, sqlc.ListKnownRoutesParams{
		Column1: iata,
		Column2: hopCount,
		Column3: cursorTS,
		Limit:   limit,
		Column5: cursorID,
	})
	if err != nil {
		return nil, err
	}
	rows := make([]knownRouteRow, len(sqlRows))
	for i, r := range sqlRows {
		rows[i] = knownRouteRow{ID: r.ID, NodeIds: r.NodeIds, HashPrefix: r.HashPrefix, Iata: r.Iata, HopCount: r.HopCount, FirstSeen: r.FirstSeen, LastSeen: r.LastSeen, ObservationCount: r.ObservationCount}
	}
	ids := collectNodeIDs(rows)
	nodes, err := s.GetNodesByIDs(ctx, ids)
	if err != nil {
		return nil, err
	}
	return toKnownRoutes(rows, nodes), nil
}

// Work bounds for one route search; the query count is fixed, these cap row volume.
const (
	routeSearchLimit      = 500  // routes either search returns
	crossMaxEndpointIATAs = 50   // IATAs a cross endpoint may resolve uniquely in
	searchMaxCandidates   = 2000 // relays a search endpoint may resolve to
	crossRouteRowLimit    = 500  // routes loaded per cross endpoint
	crossMaxHops          = 2000 // hops per side fed to the neighbor query
	crossMaxStitched      = 5000 // cross routes collected before sort and truncate
)

func (s *Store) SearchKnownRoutes(ctx context.Context, iatas []string, fromHash, toHash string) ([]api.KnownRoute, error) {
	fromBytes, toBytes, err := decodeHashPair(fromHash, toHash)
	if err != nil {
		return nil, err
	}
	if iatas, err = s.scopeIATAs(ctx, iatas); err != nil {
		return nil, err
	}
	from, err := s.resolveRelays(ctx, iatas, fromBytes)
	if err != nil {
		return nil, err
	}
	to, err := s.resolveRelays(ctx, iatas, toBytes)
	if err != nil {
		return nil, err
	}
	if from.count() > searchMaxCandidates || to.count() > searchMaxCandidates {
		return nil, api.ErrRouteSearchTooBroad
	}
	var shared []string
	for iata := range from {
		if _, ok := to[iata]; ok {
			shared = append(shared, iata)
		}
	}
	items := []api.KnownRoute{}
	if len(shared) == 0 {
		return items, nil
	}
	sqlRows, err := s.q.SearchKnownRoutesByNodes(ctx, sqlc.SearchKnownRoutesByNodesParams{
		FromNodes: from.nodes(), ToNodes: to.nodes(), Iatas: shared, RowLimit: routeSearchLimit,
	})
	if err != nil {
		return nil, err
	}
	rows := make([]knownRouteRow, len(sqlRows))
	for i, r := range sqlRows {
		rows[i] = knownRouteRow(r)
	}
	nodes, err := s.GetNodesByIDs(ctx, collectNodeIDs(rows))
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		fromPos, toPos := -1, -1
		for i, id := range r.NodeIds {
			if fromPos == -1 && slices.Contains(from[r.Iata], id) {
				fromPos = i
			} else if fromPos != -1 && slices.Contains(to[r.Iata], id) {
				toPos = i
				break
			}
		}
		if toPos == -1 {
			continue
		}
		hops := make([]api.RouteHop, 0, toPos-fromPos+1)
		for i := fromPos; i <= toPos; i++ {
			hop := api.RouteHop{NodeID: r.NodeIds[i], Node: nodes[r.NodeIds[i]]}
			if i < len(r.HashPrefix) {
				hop.HashBytes = hex.EncodeToString(r.HashPrefix[i])
			}
			hops = append(hops, hop)
		}
		items = append(items, api.KnownRoute{
			PathKey:          hex.EncodeToString(routePathKey(r.NodeIds)),
			ID:               r.ID,
			IATA:             r.Iata,
			HopCount:         int32(len(hops)),
			Hops:             hops,
			FirstSeen:        r.FirstSeen.Time.UnixMilli(),
			LastSeen:         r.LastSeen.Time.UnixMilli(),
			ObservationCount: r.ObservationCount,
		})
	}
	return items, nil
}

// crossRoute is a route containing a searched endpoint at index end.
type crossRoute struct {
	api.KnownRoute
	end int
}

func (s *Store) SearchCrossIATARoutes(ctx context.Context, q api.CrossRouteSearch) ([]api.CrossIATARoute, error) {
	fromBytes, toBytes, err := decodeHashPair(q.FromHash, q.ToHash)
	if err != nil {
		return nil, err
	}

	// 1. resolve each hash per IATA, keeping only IATAs where it names exactly one relay
	fromIATAs, err := s.scopeIATAs(ctx, q.FromIATAs)
	if err != nil {
		return nil, err
	}
	toIATAs, err := s.scopeIATAs(ctx, q.ToIATAs)
	if err != nil {
		return nil, err
	}
	fromCands, err := s.resolveRelays(ctx, fromIATAs, fromBytes)
	if err != nil {
		return nil, err
	}
	toCands, err := s.resolveRelays(ctx, toIATAs, toBytes)
	if err != nil {
		return nil, err
	}
	fromNodes, toNodes := fromCands.unique(), toCands.unique()
	if len(fromNodes) > crossMaxEndpointIATAs || len(toNodes) > crossMaxEndpointIATAs {
		return nil, api.ErrRouteSearchTooBroad
	}
	if len(fromNodes) == 0 || len(toNodes) == 0 {
		return nil, nil
	}

	// 2. routes through each endpoint, in the IATA it resolved in
	sourceRows, err := s.routesThrough(ctx, fromNodes)
	if err != nil {
		return nil, err
	}
	targetRows, err := s.routesThrough(ctx, toNodes)
	if err != nil {
		return nil, err
	}
	if len(sourceRows) == 0 || len(targetRows) == 0 {
		return nil, nil
	}
	nodes, err := s.GetNodesByIDs(ctx, collectNodeIDs(slices.Concat(sourceRows, targetRows)))
	if err != nil {
		return nil, err
	}
	sources := toCrossRoutes(sourceRows, nodes, fromNodes)
	targets := toCrossRoutes(targetRows, nodes, toNodes)

	// 3. one neighbor query linking source hops from the source endpoint on
	//    to target hops up to the destination endpoint
	type targetRef struct{ route, hop int }
	targetsByNode := make(map[uuid.UUID][]targetRef)
	var tgtHops []uuid.UUID
	for ti, tr := range targets {
		for j := 0; j <= tr.end; j++ {
			id := tr.Hops[j].NodeID
			if _, ok := targetsByNode[id]; !ok {
				if len(tgtHops) >= crossMaxHops {
					continue
				}
				tgtHops = append(tgtHops, id)
			}
			targetsByNode[id] = append(targetsByNode[id], targetRef{ti, j})
		}
	}
	srcSet := make(map[uuid.UUID]struct{})
	var srcHops []uuid.UUID
	for _, sr := range sources {
		for _, hop := range sr.Hops[sr.end:] {
			if _, ok := srcSet[hop.NodeID]; !ok && len(srcHops) < crossMaxHops {
				srcSet[hop.NodeID] = struct{}{}
				srcHops = append(srcHops, hop.NodeID)
			}
		}
	}
	links, err := s.q.GetNeighborLinks(ctx, sqlc.GetNeighborLinksParams{ANodes: srcHops, BNodes: tgtHops})
	if err != nil {
		return nil, err
	}
	// adjacency source hop → target hop → newest last_seen, whichever way the row was written
	adj := make(map[uuid.UUID]map[uuid.UUID]int64)
	link := func(src, tgt uuid.UUID, seen int64) {
		_, isSrc := srcSet[src]
		_, isTgt := targetsByNode[tgt]
		if !isSrc || !isTgt {
			return
		}
		if adj[src] == nil {
			adj[src] = make(map[uuid.UUID]int64)
		}
		adj[src][tgt] = max(adj[src][tgt], seen)
	}
	for _, l := range links {
		seen := l.LastSeen.Time.UnixMilli()
		link(l.NodeID, l.NeighborID, seen)
		link(l.NeighborID, l.NodeID, seen)
	}

	// 4. stitch source segment → cross hop → target segment
	type stitched struct {
		route api.CrossIATARoute
		key   string
	}
	var found []stitched
	dedupe := make(map[string]struct{})
stitch:
	for _, sr := range sources {
		for i := sr.end; i < len(sr.Hops); i++ {
			hop := sr.Hops[i]
			for tgt, seen := range adj[hop.NodeID] {
				for _, ref := range targetsByNode[tgt] {
					tr := targets[ref.route]
					if tr.IATA == sr.IATA {
						continue
					}
					source := sr.Hops[sr.end : i+1]
					target := tr.Hops[ref.hop : tr.end+1]
					key := crossRouteKey(sr.IATA, tr.IATA, source, target)
					if _, dup := dedupe[key]; dup {
						continue
					}
					dedupe[key] = struct{}{}
					found = append(found, stitched{key: key, route: api.CrossIATARoute{
						SourceSegment: source,
						CrossHop: api.CrossIATAHop{
							FromNode: resolvedOrID(hop.Node, hop.NodeID),
							ToNode:   resolvedOrID(nodes[tgt], tgt),
							FromIATA: sr.IATA,
							ToIATA:   tr.IATA,
							LastSeen: seen,
						},
						TargetSegment: target,
						TotalHops:     len(source) + len(target),
					}})
					if len(found) >= crossMaxStitched {
						break stitch
					}
				}
			}
		}
	}
	slices.SortFunc(found, func(a, b stitched) int {
		return cmp.Or(
			cmp.Compare(a.route.TotalHops, b.route.TotalHops),
			cmp.Compare(b.route.CrossHop.LastSeen, a.route.CrossHop.LastSeen),
			strings.Compare(a.key, b.key),
		)
	})
	results := make([]api.CrossIATARoute, 0, min(len(found), routeSearchLimit))
	for _, f := range found[:min(len(found), routeSearchLimit)] {
		results = append(results, f.route)
	}
	return results, nil
}

// relayCandidates maps each IATA to the relays a hash prefix names there.
type relayCandidates map[string][]uuid.UUID

func (c relayCandidates) count() int {
	n := 0
	for _, ids := range c {
		n += len(ids)
	}
	return n
}

// unique keeps the IATAs where the hash names exactly one relay.
func (c relayCandidates) unique() map[string]uuid.UUID {
	out := make(map[string]uuid.UUID)
	for iata, ids := range c {
		if len(ids) == 1 {
			out[iata] = ids[0]
		}
	}
	return out
}

func (c relayCandidates) nodes() []uuid.UUID {
	var nodes []uuid.UUID
	for _, ids := range c {
		nodes = append(nodes, ids...)
	}
	return nodes
}

func decodeHashPair(from, to string) ([]byte, []byte, error) {
	f, err := hex.DecodeString(from)
	if err != nil {
		return nil, nil, err
	}
	t, err := hex.DecodeString(to)
	return f, t, err
}

// scopeIATAs returns iatas, or every known IATA when none are given.
func (s *Store) scopeIATAs(ctx context.Context, iatas []string) ([]string, error) {
	if len(iatas) > 0 {
		return iatas, nil
	}
	return s.ListKnownIATAs(ctx)
}

// resolveRelays maps each IATA to the relays whose hash starts with hash there.
func (s *Store) resolveRelays(ctx context.Context, iatas []string, hash []byte) (relayCandidates, error) {
	type pair struct {
		iata string
		node uuid.UUID
	}
	var rows []pair
	hashes := [][]byte{hash}
	switch len(hash) {
	case 1:
		rs, err := s.q.ResolveRelayHashPairsP1(ctx, sqlc.ResolveRelayHashPairsP1Params{Iatas: iatas, Hashes: hashes})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			rows = append(rows, pair{r.Iata, r.NodeID})
		}
	case 2:
		rs, err := s.q.ResolveRelayHashPairsP2(ctx, sqlc.ResolveRelayHashPairsP2Params{Iatas: iatas, Hashes: hashes})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			rows = append(rows, pair{r.Iata, r.NodeID})
		}
	case 3:
		rs, err := s.q.ResolveRelayHashPairsP3(ctx, sqlc.ResolveRelayHashPairsP3Params{Iatas: iatas, Hashes: hashes})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			rows = append(rows, pair{r.Iata, r.NodeID})
		}
	case 4:
		rs, err := s.q.ResolveRelayHashPairsP4(ctx, sqlc.ResolveRelayHashPairsP4Params{Iatas: iatas, Hashes: hashes})
		if err != nil {
			return nil, err
		}
		for _, r := range rs {
			rows = append(rows, pair{r.Iata, r.NodeID})
		}
	default:
		return nil, nil
	}
	c := make(relayCandidates)
	for _, r := range rows {
		if !slices.Contains(c[r.iata], r.node) {
			c[r.iata] = append(c[r.iata], r.node)
		}
	}
	return c, nil
}

// routesThrough returns routes containing endpoints[iata] in that same IATA.
func (s *Store) routesThrough(ctx context.Context, endpoints map[string]uuid.UUID) ([]knownRouteRow, error) {
	iatas := make([]string, 0, len(endpoints))
	nodes := make([]uuid.UUID, 0, len(endpoints))
	for iata, id := range endpoints {
		iatas = append(iatas, iata)
		nodes = append(nodes, id)
	}
	sqlRows, err := s.q.GetKnownRoutesByNodes(ctx, sqlc.GetKnownRoutesByNodesParams{
		Nodes: nodes, Iatas: iatas, RowLimit: crossRouteRowLimit,
	})
	if err != nil {
		return nil, err
	}
	rows := make([]knownRouteRow, len(sqlRows))
	for i, r := range sqlRows {
		rows[i] = knownRouteRow(r)
	}
	return rows, nil
}

func toCrossRoutes(rows []knownRouteRow, nodes map[uuid.UUID]*api.ResolvedNode, endpoints map[string]uuid.UUID) []crossRoute {
	routes := toKnownRoutes(rows, nodes)
	out := make([]crossRoute, len(routes))
	for i, r := range routes {
		out[i] = crossRoute{KnownRoute: r, end: slices.Index(rows[i].NodeIds, endpoints[r.IATA])}
	}
	return out
}

func crossRouteKey(fromIATA, toIATA string, source, target []api.RouteHop) string {
	var b strings.Builder
	b.WriteString(fromIATA + toIATA)
	for _, h := range source {
		b.WriteString(h.NodeID.String())
	}
	b.WriteByte('|')
	for _, h := range target {
		b.WriteString(h.NodeID.String())
	}
	return b.String()
}

func resolvedOrID(n *api.ResolvedNode, id uuid.UUID) api.ResolvedNode {
	if n == nil {
		return api.ResolvedNode{ID: id}
	}
	return *n
}

// AmbiguousPrefixes is the per-IATA set of hop prefixes that resolve to more than one node.
type AmbiguousPrefixes struct {
	IATAs    []string
	Lens     []int32
	Prefixes [][]byte
}

func (s *Store) AmbiguousPrefixes(ctx context.Context) (AmbiguousPrefixes, error) {
	rows, err := s.q.AmbiguousPrefixes(ctx)
	if err != nil {
		return AmbiguousPrefixes{}, err
	}
	amb := AmbiguousPrefixes{IATAs: make([]string, 0, len(rows)), Lens: make([]int32, 0, len(rows)), Prefixes: make([][]byte, 0, len(rows))}
	for _, r := range rows {
		amb.IATAs = append(amb.IATAs, r.Iata)
		amb.Lens = append(amb.Lens, r.Len)
		amb.Prefixes = append(amb.Prefixes, r.Prefix)
	}
	return amb, nil
}

// ReconfirmRoutes checks the batchSize least-recently-reconfirmed routes,
// deleting stale or ambiguous ones and stamping the survivors.
func (s *Store) ReconfirmRoutes(ctx context.Context, batchSize int32, before time.Time, amb AmbiguousPrefixes) (int64, error) {
	return s.q.ReconfirmRoutes(ctx, sqlc.ReconfirmRoutesParams{
		BatchSize: batchSize,
		Before:    pgtype.Timestamptz{Time: before, Valid: true},
		AmbIata:   amb.IATAs,
		AmbLen:    amb.Lens,
		AmbPrefix: amb.Prefixes,
	})
}

const routeDeleteBatch = 10000

// DeleteOldRoutes prunes routes per the retention rule: unconditionally past
// retentionCutoff, and past graceCutoff when observed fewer than minObservations times.
func (s *Store) DeleteOldRoutes(ctx context.Context, retentionCutoff time.Time, minObservations int64, graceCutoff time.Time) error {
	return deleteInBatches(ctx, routeDeleteBatch, func(ctx context.Context, n int32) (int64, error) {
		return s.q.DeleteOldRoutes(ctx, sqlc.DeleteOldRoutesParams{
			RetentionCutoff: pgtype.Timestamptz{Time: retentionCutoff, Valid: true},
			GraceCutoff:     pgtype.Timestamptz{Time: graceCutoff, Valid: true},
			MinObservations: minObservations,
			BatchSize:       n,
		})
	})
}

// knownRouteRow normalizes the per-query sqlc row structs (identical
// columns, distinct generated types) so the helpers below share one body.
type knownRouteRow struct {
	ID               int64
	NodeIds          []uuid.UUID
	HashPrefix       [][]byte
	Iata             string
	HopCount         int32
	FirstSeen        pgtype.Timestamptz
	LastSeen         pgtype.Timestamptz
	ObservationCount int64
}

func toKnownRoutes(rows []knownRouteRow, nodes map[uuid.UUID]*api.ResolvedNode) []api.KnownRoute {
	items := make([]api.KnownRoute, 0, len(rows))
	for _, r := range rows {
		hops := make([]api.RouteHop, 0, len(r.NodeIds))
		for i, nodeID := range r.NodeIds {
			hop := api.RouteHop{
				NodeID: nodeID,
				Node:   nodes[nodeID],
			}
			if i < len(r.HashPrefix) {
				hop.HashBytes = hex.EncodeToString(r.HashPrefix[i])
			}
			hops = append(hops, hop)
		}
		items = append(items, api.KnownRoute{
			PathKey:          hex.EncodeToString(routePathKey(r.NodeIds)),
			ID:               r.ID,
			IATA:             r.Iata,
			HopCount:         r.HopCount,
			Hops:             hops,
			FirstSeen:        r.FirstSeen.Time.UnixMilli(),
			LastSeen:         r.LastSeen.Time.UnixMilli(),
			ObservationCount: r.ObservationCount,
		})
	}
	return items
}

func collectNodeIDs(rows []knownRouteRow) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{})
	var ids []uuid.UUID
	for _, r := range rows {
		for _, id := range r.NodeIds {
			if _, ok := seen[id]; !ok {
				seen[id] = struct{}{}
				ids = append(ids, id)
			}
		}
	}
	return ids
}
