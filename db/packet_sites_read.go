// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5/pgtype"
)

// siteRow is the row of the site list statements; every one of them converts to it with siteRow(r).
type siteRow = sqlc.ListPacketsBySiteRow

// siteToPacketRow drops the site key: packetRow's fields are siteRow's without SiteHeardAt.
func siteToPacketRow(r siteRow) packetRow {
	return packetRow{
		PacketHash: r.PacketHash, PayloadType: r.PayloadType, Summary: r.Summary, RouteType: r.RouteType,
		FirstHeardAt: r.FirstHeardAt, LastHeardAt: r.LastHeardAt, ScopeID: r.ScopeID, ScopeName: r.ScopeName,
		ObservationCount: r.ObservationCount, LatestObserverID: r.LatestObserverID, LatestObserverName: r.LatestObserverName,
		LatestObserverIata: r.LatestObserverIata, LatestObserverPathLengthByte: r.LatestObserverPathLengthByte,
		LatestObserverHashSize: r.LatestObserverHashSize, LatestObserverHopCount: r.LatestObserverHopCount,
		LatestObserverPathBytes: r.LatestObserverPathBytes, RawPayload: r.RawPayload, OriginPubkey: r.OriginPubkey,
	}
}

// siteFamily is the shape of a region request after normalisation.
type siteFamily int

const (
	sitePlain  siteFamily = iota // no filter: one exact range per site
	siteTyped                    // a few (payload type, route type) pairs: one exact range per site and pair
	siteSince                    // a window or many type pairs: bounded reads of each site range, filtered
	siteScoped                   // named scopes: bounded reads of each (site, scope) range, filtered
)

// siteTypedPairs is the most (payload type, route type) pairs the typed statement walks as separate ranges; more
// pairs read each site range once, bounded, instead.
const siteTypedPairs = 4

// siteScanDepthPerRow is how many entries of each range the bounded statements read per page row, as deep as the
// observation statement reads each site's observations.
const siteScanDepthPerRow = 8

// siteFamilyOf picks the family: scopes, then a window, then types.
func siteFamilyOf(f packetFilter) siteFamily {
	switch {
	case len(f.scopes) > 0:
		return siteScoped
	case f.windowed:
		return siteSince
	case f.typed && len(f.payloadTypes)*len(f.routeTypes) > siteTypedPairs:
		return siteSince
	case f.typed:
		return siteTyped
	}
	return sitePlain
}

// siteStatement names the statement for a family after the pre-filter: the re-check form only with a cursor and
// two or more sites.
func siteStatement(fam siteFamily, sites int, hasCursor bool) string {
	recheck := hasCursor && sites > 1
	switch fam {
	case siteTyped:
		if recheck {
			return "ListPacketsBySitesTyped"
		}
		return "ListPacketsBySitesTypedFirst"
	case siteSince:
		return "ListPacketsBySitesSince"
	case siteScoped:
		return "ListPacketsBySitesScoped"
	}
	switch {
	case sites == 1:
		return "ListPacketsBySite"
	case recheck:
		return "ListPacketsBySites"
	}
	return "ListPacketsBySitesFirst"
}

// siteKey is a region page key: a packet's newest hearing at the requested sites, and its hash.
type siteKey struct {
	at   time.Time
	hash []byte
}

// listSiteRows pre-filters unknown sites (typed, since and scoped families), dispatches and returns up to
// f.pageLimit rows in (site_heard_at DESC, packet_hash DESC) order. floor is set when a bounded statement left
// rows below it for the next page. rows is nil when the pre-filter leaves no site. Shared by the store and the
// first-time verification.
func listSiteRows(ctx context.Context, q sqlc.Querier, f packetFilter, sites []string) (rows []siteRow, floor *siteKey, err error) {
	fam := siteFamilyOf(f)
	if fam != sitePlain {
		if sites, err = q.FilterKnownSites(ctx, sites); err != nil || len(sites) == 0 {
			return nil, nil, err
		}
	}
	hasCursor := f.cursorTS.InfinityModifier != pgtype.Infinity
	recheck := hasCursor && len(sites) > 1
	scanDepth := f.pageLimit * siteScanDepthPerRow
	switch siteStatement(fam, len(sites), hasCursor) {
	case "ListPacketsBySite":
		rows, err = q.ListPacketsBySite(ctx, sqlc.ListPacketsBySiteParams{Iata: sites[0], CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit})
		return rows, nil, err
	case "ListPacketsBySites":
		r, err := q.ListPacketsBySites(ctx, sqlc.ListPacketsBySitesParams{Iatas: sites, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit})
		return convertSiteRows(r, func(x sqlc.ListPacketsBySitesRow) siteRow { return siteRow(x) }), nil, err
	case "ListPacketsBySitesFirst":
		r, err := q.ListPacketsBySitesFirst(ctx, sqlc.ListPacketsBySitesFirstParams{Iatas: sites, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit})
		return convertSiteRows(r, func(x sqlc.ListPacketsBySitesFirstRow) siteRow { return siteRow(x) }), nil, err
	case "ListPacketsBySitesTyped":
		r, err := q.ListPacketsBySitesTyped(ctx, sqlc.ListPacketsBySitesTypedParams{Iatas: sites, PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit})
		return convertSiteRows(r, func(x sqlc.ListPacketsBySitesTypedRow) siteRow { return siteRow(x) }), nil, err
	case "ListPacketsBySitesTypedFirst":
		r, err := q.ListPacketsBySitesTypedFirst(ctx, sqlc.ListPacketsBySitesTypedFirstParams{Iatas: sites, PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit})
		return convertSiteRows(r, func(x sqlc.ListPacketsBySitesTypedFirstRow) siteRow { return siteRow(x) }), nil, err
	case "ListPacketsBySitesSince":
		r, err := q.ListPacketsBySitesSince(ctx, sqlc.ListPacketsBySitesSinceParams{
			Iatas: sites, CursorTs: f.cursorTS, CursorHash: f.cursorHash, ScanDepth: scanDepth, SinceTs: f.since, UntilTs: f.until,
			PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, Recheck: recheck, PageLimit: f.pageLimit,
		})
		if err != nil {
			return nil, nil, err
		}
		keys := make([]sqlc.ListPacketsBySitesScopedRow, len(r))
		for i, x := range r {
			keys[i] = sqlc.ListPacketsBySitesScopedRow(x)
		}
		return siteRowsForKeys(ctx, q, keys)
	}
	keys, err := q.ListPacketsBySitesScoped(ctx, sqlc.ListPacketsBySitesScopedParams{
		Iatas: sites, ScopeNames: f.scopes, CursorTs: f.cursorTS, CursorHash: f.cursorHash, ScanDepth: scanDepth, SinceTs: f.since,
		UntilTs: f.until, PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, Recheck: recheck, PageLimit: f.pageLimit,
	})
	if err != nil {
		return nil, nil, err
	}
	return siteRowsForKeys(ctx, q, keys)
}

// siteRowsForKeys splits a bounded statement's keys from its floor and reads the page rows for the keys.
func siteRowsForKeys(ctx context.Context, q sqlc.Querier, keys []sqlc.ListPacketsBySitesScopedRow) ([]siteRow, *siteKey, error) {
	var floor *siteKey
	var hashes [][]byte
	var ats []pgtype.Timestamptz
	for _, k := range keys {
		if k.IsFloor {
			floor = &siteKey{at: k.SiteHeardAt.Time, hash: k.PacketHash}
			continue
		}
		hashes = append(hashes, k.PacketHash)
		ats = append(ats, k.SiteHeardAt)
	}
	if len(hashes) == 0 {
		return []siteRow{}, floor, nil
	}
	r, err := q.ListPacketsBySiteKeys(ctx, sqlc.ListPacketsBySiteKeysParams{PacketHashes: hashes, SiteHeardAts: ats})
	return convertSiteRows(r, func(x sqlc.ListPacketsBySiteKeysRow) siteRow { return siteRow(x) }), floor, err
}

func convertSiteRows[R any](rows []R, conv func(R) siteRow) []siteRow {
	out := make([]siteRow, len(rows))
	for i, r := range rows {
		out[i] = conv(r)
	}
	return out
}

// listPacketsByRegion serves a region page. Until this process trusts packet_sites, it runs today's statement; a
// pageCursor sent alone then pages by its millisecond, which is the cursor the same row would have produced, so a
// walk never restarts. Trusted, it pages by the exact (site_heard_at, packet_hash) key and returns nextPageCursor.
func (s *Store) listPacketsByRegion(ctx context.Context, payloadTypes, routeTypes []int16, iatas, scopes []string, since, until time.Time, cursor int64, pageCursor *api.PacketCursor, limit int32) (api.PacketPage, error) {
	if !s.readModels.packetSitesTrusted() {
		c := cursor
		if c <= 0 && pageCursor != nil {
			c = pageCursor.At.UnixMilli()
		}
		page, err := s.listPacketsByIATAs(ctx, payloadTypes, routeTypes, iatas, scopes, since, until, c, limit)
		return api.PacketPage{Page: page}, err
	}
	empty := api.PacketPage{Page: api.Page[api.PacketSummary]{Items: []api.PacketSummary{}}}
	f, ok := newPacketFilter(payloadTypes, routeTypes, scopes, since, until, cursor, pageCursor, limit)
	if !ok {
		return empty, nil
	}
	rows, floor, err := listSiteRows(ctx, s.q, f, dedupeSites(iatas))
	if err != nil {
		return api.PacketPage{}, err
	}
	hasMore := len(rows) > int(limit)
	if hasMore {
		rows = rows[:limit]
	}
	var next *siteKey
	switch {
	case hasMore:
		last := rows[len(rows)-1]
		next = &siteKey{at: last.SiteHeardAt.Time, hash: last.PacketHash}
	case floor != nil:
		// A bounded read stopped above the end of a site's history: the next page starts at the floor.
		hasMore, next = true, floor
	}
	packetRows := make([]packetRow, len(rows))
	for i, r := range rows {
		packetRows[i] = siteToPacketRow(r)
	}
	page := api.PacketPage{Page: api.Page[api.PacketSummary]{Items: s.packetSummaries(ctx, packetRows), HasMore: hasMore}}
	if next != nil {
		ms := next.at.UnixMilli()
		pc := api.PacketCursor{At: next.at, Hash: next.hash}.String()
		page.NextCursor = &ms
		page.NextPageCursor = &pc
	}
	return page, nil
}
