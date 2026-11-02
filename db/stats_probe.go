// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"strings"
	"time"
)

// probeIATALimit is the widest distinct IATA list the per-(hour, IATA) probe statements take; wider
// lists read today's statements, which scan the window once.
const probeIATALimit = 32

// distinctIATAs counts the codes as Postgres bpchar compares them: trailing blanks ignored, case kept.
func distinctIATAs(iatas []string) int {
	seen := make(map[string]struct{}, len(iatas))
	for _, s := range iatas {
		seen[strings.TrimRight(s, " ")] = struct{}{}
	}
	return len(seen)
}

// useStatsProbe reports whether a rollup aggregate may use its probe statement: at most probeIATALimit
// distinct codes, and every bound on a UTC hour (the probes step hour by hour from since).
func useStatsProbe(iatas []string, bounds ...time.Time) bool {
	if distinctIATAs(iatas) > probeIATALimit {
		return false
	}
	for _, b := range bounds {
		if !b.Equal(b.Truncate(time.Hour)) {
			return false
		}
	}
	return true
}
