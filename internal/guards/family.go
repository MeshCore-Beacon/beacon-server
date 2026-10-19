// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package guards applies per-family cost guards to public API requests: a
// per-client concurrency cap, a database deadline per request class, a
// pool-acquire limit and a shadow database-time budget. A family only refuses
// requests when it is listed in guards.enforce; otherwise every guard just
// counts what it would have refused.
package guards

import (
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
)

// Family groups API requests whose database cost behaves alike.
type Family string

// The guard families.
const (
	Routes          Family = "routes"
	ChannelMessages Family = "channel_messages"
	PacketsGlobal   Family = "packets_global"
	PacketsRegion   Family = "packets_region"
	PacketsWindow   Family = "packets_window"
	PacketDetail    Family = "packet_detail"
	Stats           Family = "stats"
	Other           Family = "other"
)

// Families lists every family in display order.
var Families = []Family{Routes, ChannelMessages, PacketsGlobal, PacketsRegion, PacketsWindow, PacketDetail, Stats, Other}

// ErrUnknownFamily is returned for a guards.enforce entry that names no family.
var ErrUnknownFamily = errors.New("guards.enforce entries must be families: routes, channel_messages, packets_global, packets_region, packets_window, packet_detail, stats, other")

// ParseFamilies trims and deduplicates names and returns them in Families order.
func ParseFamilies(names []string) ([]Family, error) {
	seen := make(map[Family]bool, len(names))
	for _, n := range names {
		f := Family(strings.TrimSpace(n))
		if !slices.Contains(Families, f) {
			return nil, fmt.Errorf("%w (got %q)", ErrUnknownFamily, n)
		}
		seen[f] = true
	}
	out := make([]Family, 0, len(seen))
	for _, f := range Families {
		if seen[f] {
			out = append(out, f)
		}
	}
	return out, nil
}

// Classify maps a request to its family and class. It expects the path after
// StripSlashes, with or without the /api/v1 prefix.
func Classify(r *http.Request) (Family, Class) {
	p := strings.Trim(strings.TrimPrefix(r.URL.Path, "/api/v1"), "/")
	segs := strings.Split(p, "/")
	q := r.URL.Query()
	set := func(keys ...string) bool {
		for _, k := range keys {
			for _, v := range q[k] {
				if v != "" {
					return true
				}
			}
		}
		return false
	}
	region := func() bool { return set("iata", "iatas", "region", "regionId") }
	n := len(segs)
	switch segs[0] {
	case "packets":
		switch {
		case n == 1 && set("since", "until"):
			return PacketsWindow, Aggregation
		case n == 1 || (n == 2 && segs[1] == "backfill"):
			if region() {
				return PacketsRegion, List
			}
			return PacketsGlobal, List
		case n == 2:
			return PacketDetail, Entity
		}
	case "routes":
		switch {
		case n == 1:
			return Routes, List
		case n == 2 && (segs[1] == "cross" || segs[1] == "search"):
			return Routes, Aggregation
		case n == 4 && segs[3] == "observations":
			return Routes, Entity
		}
	case "messages":
		if n == 1 || (n == 2 && segs[1] == "backfill") {
			return ChannelMessages, List
		}
	case "stats":
		if n >= 2 {
			return Stats, Aggregation
		}
	case "channels":
		switch {
		case n == 1:
			return Other, List
		case n == 2:
			return Other, Entity
		case n == 3 && segs[2] == "messages":
			return ChannelMessages, List
		}
	case "nodes":
		switch {
		case n == 1, n == 3 && segs[2] == "observations":
			return Other, List
		case n == 2, n == 3 && segs[2] == "neighbors":
			return Other, Entity
		}
	case "observers":
		switch {
		case n == 1, n == 3 && segs[2] == "adverts":
			return Other, List
		case n == 2 && segs[1] == "directory":
			if region() {
				return Other, List
			}
			return Other, Aggregation
		case n == 3 && segs[2] == "activity":
			return Other, Aggregation
		case n == 2, n == 3 && segs[2] == "telemetry":
			return Other, Entity
		}
	case "traces":
		switch n {
		case 1:
			return Other, List
		case 2:
			return Other, Entity
		}
	}
	return Other, Entity
}
