// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5/pgtype"
)

// Domains enforced by the packets_payload_type_domain and
// packets_route_type_domain check constraints. A value outside them matches no
// row, so dropping it from a filter list cannot change the result.
const (
	payloadTypeCount = 16 // payload_type BETWEEN 0 AND 15
	routeTypeCount   = 4  // route_type BETWEEN 0 AND 3
)

// packetFilter is a global /packets request after normalisation.
type packetFilter struct {
	payloadTypes []int16            // deduplicated, in 0..15, order kept; all 16 when the request gave none
	routeTypes   []int16            // deduplicated, in 0..3, order kept; all 4 when the request gave none
	typed        bool               // the request gave a payload or route type list
	scopes       []string           // deduplicated, order kept, values as given (no trim)
	windowed     bool               // since or until was given
	since, until pgtype.Timestamptz // -infinity / infinity when absent
	cursorTS     pgtype.Timestamptz // infinity on page 1
	cursorHash   []byte             // never nil; empty on page 1 and for a legacy cursor
	pageLimit    int32              // limit + 1
}

// newPacketFilter normalises a request. ok is false when no row can match: a type list that held values and lost
// all of them to the domain drop. The caller then returns an empty page without a query.
func newPacketFilter(payloadTypes, routeTypes []int16, scopes []string, since, until time.Time, cursor int64, pageCursor *api.PacketCursor, limit int32) (f packetFilter, ok bool) {
	var ptOK, rtOK bool
	f.payloadTypes, ptOK = normaliseTypes(payloadTypes, payloadTypeCount)
	f.routeTypes, rtOK = normaliseTypes(routeTypes, routeTypeCount)
	if !ptOK || !rtOK {
		return packetFilter{}, false
	}
	f.typed = len(payloadTypes) > 0 || len(routeTypes) > 0

	f.scopes = dedupeStrings(scopes)

	// since after until is not short-circuited: the statement compares the encoded values, which wrap for
	// out-of-range times exactly as they always have, so it alone decides which rows match.
	f.since = pgtype.Timestamptz{InfinityModifier: pgtype.NegativeInfinity, Valid: true}
	f.until = pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true}
	if !since.IsZero() {
		f.since = pgtype.Timestamptz{Time: since, Valid: true}
		f.windowed = true
	}
	if !until.IsZero() {
		f.until = pgtype.Timestamptz{Time: until, Valid: true}
		f.windowed = true
	}

	switch {
	case pageCursor != nil:
		f.cursorTS = pgtype.Timestamptz{Time: pageCursor.At, Valid: true}
		f.cursorHash = pageCursor.Hash
	case cursor > 0:
		// Same encoding as the numeric cursor has always had, including the
		// wrap of out-of-range values to an early instant.
		f.cursorTS = pgtype.Timestamptz{Time: time.UnixMilli(cursor), Valid: true}
	default:
		f.cursorTS = pgtype.Timestamptz{InfinityModifier: pgtype.Infinity, Valid: true}
	}
	if f.cursorHash == nil {
		f.cursorHash = []byte{}
	}

	f.pageLimit = limit + 1
	return f, true
}

// normaliseTypes returns the distinct values of in that fall in 0..n-1, in
// their first-seen order, or all of 0..n-1 when in is empty. ok is false when
// in held values and none of them were in range.
func normaliseTypes(in []int16, n int16) (out []int16, ok bool) {
	if len(in) == 0 {
		out = make([]int16, n)
		for i := range out {
			out[i] = int16(i)
		}
		return out, true
	}
	var seen [16]bool
	for _, v := range in {
		if v < 0 || v >= n || seen[v] {
			continue
		}
		seen[v] = true
		out = append(out, v)
	}
	return out, len(out) > 0
}

// dedupeStrings returns the distinct values of in, in first-seen order, or nil when
// in is empty.
func dedupeStrings(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, v := range in {
		if _, dup := seen[v]; dup {
			continue
		}
		seen[v] = struct{}{}
		out = append(out, v)
	}
	return out
}
