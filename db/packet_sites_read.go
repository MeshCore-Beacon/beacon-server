// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"errors"
	"fmt"
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
	sitePlain       siteFamily = iota // no filter: one exact range per site
	siteTyped                         // a few (payload type, route type) pairs: one exact range per site and pair
	siteMany                          // many type pairs: bounded reads of each site range, filtered, then the pairs' ranges
	siteWindow                        // a first-heard window: each site range from the page start down to the window
	siteWindowTyped                   // a window and types: each (site, type pair) range, as siteWindow
	siteScoped                        // named scopes: each (site, scope) range, as siteWindow
	siteScopedTyped                   // named scopes and types: each (site, scope, type pair) range, as siteWindow
)

// siteTypedPairs is the most (payload type, route type) pairs the typed statement walks as separate ranges on the
// first try: it reads at most siteTypedPairs x (limit+1) entries per site, no more than one bounded read. More pairs
// read each site range bounded instead, and walk the pairs' ranges only when that runs out of budget.
const siteTypedPairs = 8

// siteScanDepthPerRow is how many entries of each range the bounded statement reads per page row, as deep as the
// observation statement reads each site's observations.
var siteScanDepthPerRow int32 = 8 // a var only so tests can shrink the reads

// siteBoundedRounds is how many bounded reads one many-pair request makes before the pairs' ranges answer it
// instead: a filter that matches little keeps reading from the floor until the page is full, within this budget.
var siteBoundedRounds = 8 // a var only so tests can shrink the budget

// siteSkew is the lag the partial indexes idx_packet_sites_late and idx_packet_sites_early (migrations 054 and 055)
// are built on: a site row heard more than this after, or before, its packet was first heard is in one of them.
// Every other row first heard in [since, until] was heard at the site in [since - siteSkew, until + siteSkew].
const siteSkew = 30 * time.Minute

// maxCursorHashBytes is a hash longer than any stored one: (t, that) sorts above every key at t.
const maxCursorHashBytes = 64

// siteFamilyOf picks the family: scopes, then a window, then types. A type list that names all 64 pairs filters
// nothing, so it counts as no type filter.
func siteFamilyOf(f packetFilter) siteFamily {
	typed := f.typed && len(f.payloadTypes)*len(f.routeTypes) < payloadTypeCount*routeTypeCount
	switch {
	case len(f.scopes) > 0 && typed:
		return siteScopedTyped
	case len(f.scopes) > 0:
		return siteScoped
	case f.windowed && typed:
		return siteWindowTyped
	case f.windowed:
		return siteWindow
	case f.typed && len(f.payloadTypes)*len(f.routeTypes) > siteTypedPairs:
		return siteMany
	case f.typed:
		return siteTyped
	}
	return sitePlain
}

// siteStatement names the first statement for a family after the pre-filter: the re-check form only with a cursor
// and two or more sites.
func siteStatement(fam siteFamily, sites int, hasCursor bool) string {
	recheck := hasCursor && sites > 1
	switch fam {
	case siteTyped:
		if recheck {
			return "ListPacketsBySitesTyped"
		}
		return "ListPacketsBySitesTypedFirst"
	case siteMany:
		return "ListPacketsBySitesSince"
	case siteWindow:
		return "ListPacketsBySitesWindow"
	case siteWindowTyped:
		return "ListPacketsBySitesTypedWindow"
	case siteScoped:
		return "ListPacketsBySitesScopedWindow"
	case siteScopedTyped:
		return "ListPacketsBySitesScopedTypedWindow"
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

// errSiteBudget means the bounded reads of one request ran out of their budget before the page was full.
var errSiteBudget = errors.New("bounded region reads exhausted their budget")

// listSiteRows pre-filters unknown sites (every family but plain), dispatches and returns up to f.pageLimit rows in
// (site_heard_at DESC, packet_hash DESC) order, and how many keys the page holds (hasMore is keys > limit). rows is
// nil when the pre-filter leaves no site. Every family reads a bounded part of the table: plain and typed one
// range per site (and pair) to at most a page; window and scoped each range (with types, each type pair's range) from
// the page start down to the window and at most a page; many-pair the bounded reads and then, if they run out of
// budget, the pairs' ranges. A filter therefore never makes a range read past what the page and its window need.
// Shared by the store and the first-time verification.
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
	case "ListPacketsBySitesTyped", "ListPacketsBySitesTypedFirst":
		rows, err = typedSiteRows(ctx, q, f, sites, hasCursor)
	case "ListPacketsBySitesSince":
		rows, keys, err = boundedSiteRows(ctx, q, f, sites, hasCursor)
		if !errors.Is(err, errSiteBudget) {
			return rows, keys, err
		}
		// Every pair's range from the cursor, each to at most a page: exact, and bounded by the pair count.
		rows, err = typedSiteRows(ctx, q, f, sites, hasCursor)
	default:
		return windowSiteRows(ctx, q, fam, f, sites, hasCursor)
	}
	return rows, len(rows), err
}

// typedSiteRows walks one range per (site, payload type, route type), with the re-check only with a cursor and two
// or more sites.
func typedSiteRows(ctx context.Context, q sqlc.Querier, f packetFilter, sites []string, hasCursor bool) ([]siteRow, error) {
	if hasCursor && len(sites) > 1 {
		r, err := q.ListPacketsBySitesTyped(ctx, sqlc.ListPacketsBySitesTypedParams{Iatas: sites, PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit})
		return convertSiteRows(r, func(x sqlc.ListPacketsBySitesTypedRow) siteRow { return siteRow(x) }), err
	}
	r, err := q.ListPacketsBySitesTypedFirst(ctx, sqlc.ListPacketsBySitesTypedFirstParams{Iatas: sites, PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit})
	return convertSiteRows(r, func(x sqlc.ListPacketsBySitesTypedFirstRow) siteRow { return siteRow(x) }), err
}

// boundedSiteRows runs the many-pair family: each read covers at most scan depth entries per site and returns the
// page keys above its floor; while the page is short and a floor remains, the next read starts at the floor (with
// the re-check, so no packet repeats), up to siteBoundedRounds reads. Then it reads the rows for the keys.
func boundedSiteRows(ctx context.Context, q sqlc.Querier, f packetFilter, sites []string, hasCursor bool) ([]siteRow, int, error) {
	cursorTS, cursorHash := f.cursorTS, f.cursorHash
	var hashes [][]byte
	var ats []pgtype.Timestamptz
	for round := 0; ; round++ {
		keys, err := q.ListPacketsBySitesSince(ctx, sqlc.ListPacketsBySitesSinceParams{
			Iatas: sites, CursorTs: cursorTS, CursorHash: cursorHash, ScanDepth: f.pageLimit * siteScanDepthPerRow, SinceTs: f.since,
			UntilTs: f.until, PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, Recheck: len(sites) > 1 && (hasCursor || round > 0),
			PageLimit: f.pageLimit - int32(len(hashes)),
		})
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
	return siteRowsForKeys(ctx, q, hashes, ats)
}

// windowBounds returns the window and scoped statements' lo_ts, hi_ts and page start (main_ts, main_hash): lo is
// since - siteSkew and hi until + siteSkew (-infinity / infinity without that side), and the start is the cursor,
// lowered to just above every key at hi when the cursor is above hi. A side outside years 1-9999 is left unbounded,
// and a cursor outside them is passed as it is (the late range may then repeat main's rows, which the grouping
// drops): the statement still answers exactly, it only reads more.
func windowBounds(f packetFilter) (lo, hi, mainTS pgtype.Timestamptz, mainHash []byte) {
	plain := func(t pgtype.Timestamptz) bool {
		return t.Valid && t.InfinityModifier == pgtype.Finite && t.Time.Year() >= 1 && t.Time.Year() <= 9999
	}
	lo = pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}
	hi = pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true}
	if plain(f.since) {
		lo = pgtype.Timestamptz{Time: f.since.Time.Add(-siteSkew), Valid: true}
	}
	if plain(f.until) {
		hi = pgtype.Timestamptz{Time: f.until.Time.Add(siteSkew), Valid: true}
	}
	mainTS, mainHash = f.cursorTS, f.cursorHash
	above := f.cursorTS.InfinityModifier == pgtype.Infinity || (plain(f.cursorTS) && f.cursorTS.Time.After(hi.Time))
	if hi.InfinityModifier == pgtype.Finite && above {
		mainTS, mainHash = hi, bytes.Repeat([]byte{0xff}, maxCursorHashBytes)
	}
	return lo, hi, mainTS, mainHash
}

// windowSiteRows runs the window and scoped families: the page keys, then their rows.
func windowSiteRows(ctx context.Context, q sqlc.Querier, fam siteFamily, f packetFilter, sites []string, hasCursor bool) ([]siteRow, int, error) {
	lo, hi, mainTS, mainHash := windowBounds(f)
	recheck := hasCursor && len(sites) > 1
	var hashes [][]byte
	var ats []pgtype.Timestamptz
	switch fam {
	case siteScopedTyped:
		keys, err := q.ListPacketsBySitesScopedTypedWindow(ctx, sqlc.ListPacketsBySitesScopedTypedWindowParams{
			Iatas: sites, ScopeNames: f.scopes, MainTs: mainTS, MainHash: mainHash, LoTs: lo, HiTs: hi, SinceTs: f.since, UntilTs: f.until,
			PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, Recheck: recheck, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit,
		})
		if err != nil {
			return nil, 0, err
		}
		for _, k := range keys {
			hashes, ats = append(hashes, k.PacketHash), append(ats, k.SiteHeardAt)
		}
	case siteWindowTyped:
		keys, err := q.ListPacketsBySitesTypedWindow(ctx, sqlc.ListPacketsBySitesTypedWindowParams{
			Iatas: sites, MainTs: mainTS, MainHash: mainHash, LoTs: lo, HiTs: hi, SinceTs: f.since, UntilTs: f.until,
			PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, Recheck: recheck, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit,
		})
		if err != nil {
			return nil, 0, err
		}
		for _, k := range keys {
			hashes, ats = append(hashes, k.PacketHash), append(ats, k.SiteHeardAt)
		}
	case siteScoped:
		keys, err := q.ListPacketsBySitesScopedWindow(ctx, sqlc.ListPacketsBySitesScopedWindowParams{
			Iatas: sites, ScopeNames: f.scopes, MainTs: mainTS, MainHash: mainHash, LoTs: lo, HiTs: hi, SinceTs: f.since, UntilTs: f.until,
			PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, Recheck: recheck, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit,
		})
		if err != nil {
			return nil, 0, err
		}
		for _, k := range keys {
			hashes, ats = append(hashes, k.PacketHash), append(ats, k.SiteHeardAt)
		}
	default:
		keys, err := q.ListPacketsBySitesWindow(ctx, sqlc.ListPacketsBySitesWindowParams{
			Iatas: sites, MainTs: mainTS, MainHash: mainHash, LoTs: lo, HiTs: hi, SinceTs: f.since, UntilTs: f.until,
			PayloadTypes: f.payloadTypes, RouteTypes: f.routeTypes, Recheck: recheck, CursorTs: f.cursorTS, CursorHash: f.cursorHash, PageLimit: f.pageLimit,
		})
		if err != nil {
			return nil, 0, err
		}
		for _, k := range keys {
			hashes, ats = append(hashes, k.PacketHash), append(ats, k.SiteHeardAt)
		}
	}
	return siteRowsForKeys(ctx, q, hashes, ats)
}

// siteRowsForKeys reads the page rows for keys, in their order.
func siteRowsForKeys(ctx context.Context, q sqlc.Querier, hashes [][]byte, ats []pgtype.Timestamptz) ([]siteRow, int, error) {
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

// listPacketsByRegion serves a region page from packet_sites. Until this process trusts packet_sites (first fill,
// repair, a failed verification, or the operator turned it off) it answers api.ErrPagesNotReady. Trusted, it pages
// by the exact (site_heard_at, packet_hash) key and returns nextPageCursor.
func (s *Store) listPacketsByRegion(ctx context.Context, payloadTypes, routeTypes []int16, iatas, scopes []string, since, until time.Time, cursor int64, pageCursor *api.PacketCursor, limit int32) (api.PacketPage, error) {
	if !s.readModels.packetSitesTrusted() {
		return api.PacketPage{}, fmt.Errorf("region packets: %w", api.ErrPagesNotReady)
	}
	empty := api.PacketPage{Page: api.Page[api.PacketSummary]{Items: []api.PacketSummary{}}}
	f, ok := newPacketFilter(payloadTypes, routeTypes, scopes, since, until, cursor, pageCursor, limit)
	if !ok {
		return empty, nil
	}
	rows, keys, err := listSiteRows(ctx, s.q, f, dedupeSites(iatas))
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
