// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
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

// siteTypedPairs is the most (payload type, route type) pairs the typed statement walks as separate ranges: it reads
// at most siteTypedPairs x (limit+1) entries per site, no more than one bounded read. More pairs read each site
// range once, bounded, instead.
const siteTypedPairs = 8

// siteScanDepthPerRow is how many entries of each range the bounded statements read per page row, as deep as the
// observation statement reads each site's observations.
var siteScanDepthPerRow int32 = 8 // a var only so tests can shrink the reads

// siteBoundedRounds is how many bounded reads one request makes before today's statement answers it instead: a
// filter that matches little keeps reading from the floor until the page is full, within this budget.
var siteBoundedRounds = 8 // a var only so tests can shrink the budget

// errSiteBudget means the bounded reads of one request ran out of their budget before the page was full.
var errSiteBudget = errors.New("bounded region reads exhausted their budget")

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
// f.pageLimit rows in (site_heard_at DESC, packet_hash DESC) order, and how many keys the page holds (hasMore is
// keys > limit). rows is nil when the pre-filter leaves no site. A bounded family reads on from its floor within the
// request until the page is full; errSiteBudget means it gave up. Shared by the store and the first-time
// verification.
func listSiteRows(ctx context.Context, q sqlc.Querier, f packetFilter, sites []string) (rows []siteRow, keys int, err error) {
	fam := siteFamilyOf(f)
	if fam != sitePlain {
		if sites, err = q.FilterKnownSites(ctx, sites); err != nil || len(sites) == 0 {
			return nil, 0, err
		}
	}
	hasCursor := f.cursorTS.InfinityModifier != pgtype.Infinity
	switch siteStatement(fam, len(sites), hasCursor) {
	case "ListPacketsBySite":
		rows, err = q.ListPacketsBySite(ctx, sqlc.ListPacketsBySiteParams{Iata: sites[0], CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit})
	case "ListPacketsBySites":
		var r []sqlc.ListPacketsBySitesRow
		r, err = q.ListPacketsBySites(ctx, sqlc.ListPacketsBySitesParams{Iatas: sites, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit})
		rows = convertSiteRows(r, func(x sqlc.ListPacketsBySitesRow) siteRow { return siteRow(x) })
	case "ListPacketsBySitesFirst":
		var r []sqlc.ListPacketsBySitesFirstRow
		r, err = q.ListPacketsBySitesFirst(ctx, sqlc.ListPacketsBySitesFirstParams{Iatas: sites, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit})
		rows = convertSiteRows(r, func(x sqlc.ListPacketsBySitesFirstRow) siteRow { return siteRow(x) })
	case "ListPacketsBySitesTyped":
		var r []sqlc.ListPacketsBySitesTypedRow
		r, err = q.ListPacketsBySitesTyped(ctx, sqlc.ListPacketsBySitesTypedParams{Iatas: sites, PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit})
		rows = convertSiteRows(r, func(x sqlc.ListPacketsBySitesTypedRow) siteRow { return siteRow(x) })
	case "ListPacketsBySitesTypedFirst":
		var r []sqlc.ListPacketsBySitesTypedFirstRow
		r, err = q.ListPacketsBySitesTypedFirst(ctx, sqlc.ListPacketsBySitesTypedFirstParams{Iatas: sites, PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit})
		rows = convertSiteRows(r, func(x sqlc.ListPacketsBySitesTypedFirstRow) siteRow { return siteRow(x) })
	default:
		return boundedSiteRows(ctx, q, fam, f, sites, hasCursor)
	}
	return rows, len(rows), err
}

// boundedSiteRows runs a bounded family: each read covers at most scan depth entries per range and returns the
// page keys above its floor; while the page is short and a floor remains, the next read starts at the floor (with
// the re-check, so no packet repeats), up to siteBoundedRounds reads. Then it reads the rows for the keys.
func boundedSiteRows(ctx context.Context, q sqlc.Querier, fam siteFamily, f packetFilter, sites []string, hasCursor bool) ([]siteRow, int, error) {
	cursorTS, cursorHash := f.cursorTS, f.cursorHash
	var hashes [][]byte
	var ats []pgtype.Timestamptz
	for round := 0; ; round++ {
		recheck := len(sites) > 1 && (hasCursor || round > 0)
		need := f.pageLimit - int32(len(hashes))
		var keys []sqlc.ListPacketsBySitesScopedRow
		var err error
		if fam == siteScoped {
			keys, err = q.ListPacketsBySitesScoped(ctx, sqlc.ListPacketsBySitesScopedParams{
				Iatas: sites, ScopeNames: f.scopes, CursorTs: cursorTS, CursorHash: cursorHash, ScanDepth: f.pageLimit * siteScanDepthPerRow,
				SinceTs: f.since, UntilTs: f.until, PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, Recheck: recheck, PageLimit: need,
			})
		} else {
			var r []sqlc.ListPacketsBySitesSinceRow
			r, err = q.ListPacketsBySitesSince(ctx, sqlc.ListPacketsBySitesSinceParams{
				Iatas: sites, CursorTs: cursorTS, CursorHash: cursorHash, ScanDepth: f.pageLimit * siteScanDepthPerRow, SinceTs: f.since,
				UntilTs: f.until, PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, Recheck: recheck, PageLimit: need,
			})
			keys = make([]sqlc.ListPacketsBySitesScopedRow, len(r))
			for i, x := range r {
				keys[i] = sqlc.ListPacketsBySitesScopedRow(x)
			}
		}
		if err != nil {
			return nil, 0, err
		}
		var floor *siteKey
		for _, k := range keys {
			if k.IsFloor {
				floor = &siteKey{at: k.SiteHeardAt.Time, hash: k.PacketHash}
				continue
			}
			hashes = append(hashes, k.PacketHash)
			ats = append(ats, k.SiteHeardAt)
		}
		if int32(len(hashes)) >= f.pageLimit || floor == nil {
			break
		}
		if round+1 >= siteBoundedRounds {
			return nil, 0, errSiteBudget
		}
		cursorTS, cursorHash = pgtype.Timestamptz{Time: floor.at, Valid: true}, floor.hash
	}
	if len(hashes) == 0 {
		return []siteRow{}, 0, nil
	}
	r, err := q.ListPacketsBySiteKeys(ctx, sqlc.ListPacketsBySiteKeysParams{PacketHashes: hashes, SiteHeardAts: ats})
	return convertSiteRows(r, func(x sqlc.ListPacketsBySiteKeysRow) siteRow { return siteRow(x) }), len(hashes), err
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
// walk never restarts. Trusted, it pages by the exact (site_heard_at, packet_hash) key and returns nextPageCursor;
// a request whose bounded reads run out of budget is answered by today's statement the same way.
func (s *Store) listPacketsByRegion(ctx context.Context, payloadTypes, routeTypes []int16, iatas, scopes []string, since, until time.Time, cursor int64, pageCursor *api.PacketCursor, limit int32) (api.PacketPage, error) {
	legacy := func() (api.PacketPage, error) {
		c := cursor
		if c <= 0 && pageCursor != nil {
			c = pageCursor.At.UnixMilli()
		}
		page, err := s.listPacketsByIATAs(ctx, payloadTypes, routeTypes, iatas, scopes, since, until, c, limit)
		return api.PacketPage{Page: page}, err
	}
	if !s.readModels.packetSitesTrusted() {
		return legacy()
	}
	empty := api.PacketPage{Page: api.Page[api.PacketSummary]{Items: []api.PacketSummary{}}}
	f, ok := newPacketFilter(payloadTypes, routeTypes, scopes, since, until, cursor, pageCursor, limit)
	if !ok {
		return empty, nil
	}
	rows, keys, err := listSiteRows(ctx, s.q, f, dedupeSites(iatas))
	if errors.Is(err, errSiteBudget) {
		slog.Debug("region page: bounded reads ran out of budget; the original statement answers", "component", "db", "sites", len(iatas))
		switch {
		case pageCursor != nil:
			// From the page cursor's exact position, so the walk keeps every row once.
			return s.listPacketsByIATAsAfter(ctx, payloadTypes, routeTypes, iatas, scopes, since, until, f.cursorTS, f.cursorHash, limit)
		case cursor <= 0:
			// Page 1: from the top, with an exact next cursor, so the walk can stay exact.
			return s.listPacketsByIATAsAfter(ctx, payloadTypes, routeTypes, iatas, scopes, since, until, f.cursorTS, f.cursorHash, limit)
		}
		return legacy() // a millisecond cursor alone: today's behaviour
	}
	if err != nil {
		return api.PacketPage{}, err
	}
	// hasMore counts the keys: a packet deleted between the keys and their rows does not end the walk (unless every
	// row went, which only retention at the oldest edge does).
	hasMore := keys > int(limit) && len(rows) > 0
	if len(rows) > int(limit) {
		rows = rows[:limit]
	}
	packetRows := make([]packetRow, len(rows))
	for i, r := range rows {
		packetRows[i] = siteToPacketRow(r)
	}
	page := api.PacketPage{Page: api.Page[api.PacketSummary]{Items: s.packetSummaries(ctx, packetRows), HasMore: hasMore}}
	if hasMore && len(rows) > 0 {
		last := rows[len(rows)-1]
		ms := last.SiteHeardAt.Time.UnixMilli()
		pc := api.PacketCursor{At: last.SiteHeardAt.Time, Hash: last.PacketHash}.String()
		page.NextCursor = &ms
		page.NextPageCursor = &pc
	}
	return page, nil
}

// listPacketsByIATAsAfter is today's region statement from an exact page cursor position. It pages on by the exact
// key: a full page ends at its last row; a page today's scan cut short ends at its scan floor, whose own millisecond
// rows it leaves to the next page, which starts just above that floor.
func (s *Store) listPacketsByIATAsAfter(ctx context.Context, payloadTypes, routeTypes []int16, iatas, scopes []string, since, until time.Time, cursorTS pgtype.Timestamptz, cursorHash []byte, limit int32) (api.PacketPage, error) {
	var sinceTS, untilTS pgtype.Timestamptz
	if !since.IsZero() {
		sinceTS = pgtype.Timestamptz{Time: since, Valid: true}
	}
	if !until.IsZero() {
		untilTS = pgtype.Timestamptz{Time: until, Valid: true}
	}
	r, err := s.q.ListPacketsByIATAsAfter(ctx, sqlc.ListPacketsByIATAsAfterParams{
		Iatas: iatas, CursorTs: cursorTS, CursorHash: cursorHash, PayloadTypes: payloadTypes,
		RouteTypes: routeTypes, SinceTs: sinceTS, UntilTs: untilTS, ScopeNames: scopes, ScanDepth: (limit + 1) * 8, PageLimit: limit + 1,
	})
	if err != nil {
		return api.PacketPage{}, err
	}
	var next *api.PacketCursor
	hasMore := len(r) > int(limit)
	if hasMore {
		r = r[:limit]
	}
	if len(r) > 0 && r[0].ScanSaturated && r[0].ScanFloor.Valid {
		// The scan stopped at its floor: keys above it are complete, keys at it may not be. Today's statement says
		// more from here; the rows at the floor move to the next page, which starts at the floor's microsecond above
		// every hash.
		floor := r[0].ScanFloor.Time
		kept := r
		for len(kept) > 0 && !kept[len(kept)-1].SiteHeardAt.Time.After(floor) {
			kept = kept[:len(kept)-1]
		}
		switch {
		case len(kept) == 0:
			hasMore = true // every row sits at the floor: page on from the last one
		case len(kept) < len(r) || !hasMore:
			r, hasMore = kept, true
			next = &api.PacketCursor{At: floor, Hash: bytes.Repeat([]byte{0xff}, maxCursorHashBytes)}
		}
	}
	if hasMore && next == nil && len(r) > 0 {
		last := r[len(r)-1]
		next = &api.PacketCursor{At: last.SiteHeardAt.Time, Hash: last.PacketHash}
	}
	packetRows := make([]packetRow, len(r))
	for i, x := range r {
		packetRows[i] = packetRow{
			PacketHash: x.PacketHash, PayloadType: x.PayloadType, Summary: x.Summary, RouteType: x.RouteType,
			FirstHeardAt: x.FirstHeardAt, LastHeardAt: x.LastHeardAt, ScopeID: x.ScopeID, ScopeName: x.ScopeName,
			ObservationCount: x.ObservationCount, LatestObserverID: x.LatestObserverID, LatestObserverName: x.LatestObserverName,
			LatestObserverIata: x.LatestObserverIata, LatestObserverPathLengthByte: x.LatestObserverPathLengthByte,
			LatestObserverHashSize: x.LatestObserverHashSize, LatestObserverHopCount: x.LatestObserverHopCount,
			LatestObserverPathBytes: x.LatestObserverPathBytes, RawPayload: x.RawPayload, OriginPubkey: x.OriginPubkey,
		}
	}
	page := api.PacketPage{Page: api.Page[api.PacketSummary]{Items: s.packetSummaries(ctx, packetRows), HasMore: hasMore}}
	if hasMore && next != nil {
		ms := next.At.UnixMilli()
		str := next.String()
		page.NextCursor = &ms
		page.NextPageCursor = &str
	}
	return page, nil
}

// maxCursorHashBytes is a hash longer than any stored one: (floor, that) sorts above every key at floor.
const maxCursorHashBytes = 64
