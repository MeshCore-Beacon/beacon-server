// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"bytes"
	"sort"
)

const (
	// HysteresisDB is how much better a challenger must be to displace the poller.
	HysteresisDB = 3.0
	// MissLimit is the miss count at which the current poller loses the slot.
	MissLimit = 3
)

// Candidate is an observer that could poll one repeater.
type Candidate struct {
	Observer      []byte
	EvidenceSNR   *float64 // mean zero-hop advert SNR, nil when Beacon has none
	EvidenceCount int
	ClaimedSNR    *float32
	State         string
	Misses        int
	Blocked       bool
}

// Assignment is one observer's place in the order for a repeater. Rank 0 polls.
type Assignment struct {
	Observer      []byte
	Rank          int
	EvidenceSNR   *float64
	EvidenceCount int
}

func eligible(c Candidate) bool {
	return !c.Blocked && c.State != "admin_stop" && c.State != "not_direct"
}

// better reports whether a ranks ahead of b: evidence first, then claimed SNR
// for those without, with the key bytes as the final tie-break.
func better(a, b Candidate) bool {
	switch {
	case a.EvidenceSNR != nil && b.EvidenceSNR == nil:
		return true
	case a.EvidenceSNR == nil && b.EvidenceSNR != nil:
		return false
	case a.EvidenceSNR != nil:
		if *a.EvidenceSNR != *b.EvidenceSNR {
			return *a.EvidenceSNR > *b.EvidenceSNR
		}
		if a.EvidenceCount != b.EvidenceCount {
			return a.EvidenceCount > b.EvidenceCount
		}
	default:
		switch {
		case a.ClaimedSNR != nil && b.ClaimedSNR == nil:
			return true
		case a.ClaimedSNR == nil && b.ClaimedSNR != nil:
			return false
		case a.ClaimedSNR != nil && *a.ClaimedSNR != *b.ClaimedSNR:
			return *a.ClaimedSNR > *b.ClaimedSNR
		}
	}
	return bytes.Compare(a.Observer, b.Observer) < 0
}

// Elect orders the eligible candidates; rank 0 polls and the rest stand by.
// The current poller keeps rank 0 unless it is failing or clearly beaten.
func Elect(cands []Candidate, currentPoller []byte) []Assignment {
	ranked := make([]Candidate, 0, len(cands))
	for _, c := range cands {
		if eligible(c) {
			ranked = append(ranked, c)
		}
	}
	sort.SliceStable(ranked, func(i, j int) bool { return better(ranked[i], ranked[j]) })

	if len(currentPoller) > 0 && len(ranked) > 1 {
		ci := -1
		for i, c := range ranked {
			if bytes.Equal(c.Observer, currentPoller) {
				ci = i
				break
			}
		}
		if ci >= 0 {
			if !displaced(ranked[ci], bestOther(ranked, ci)) {
				cur := ranked[ci]
				copy(ranked[1:ci+1], ranked[:ci])
				ranked[0] = cur
			} else if ci == 0 {
				// A failing poller that still ranks first drops behind the challenger.
				ranked[0], ranked[1] = ranked[1], ranked[0]
			}
		}
	}

	out := make([]Assignment, len(ranked))
	for i, c := range ranked {
		out[i] = Assignment{Observer: c.Observer, Rank: i, EvidenceSNR: c.EvidenceSNR, EvidenceCount: c.EvidenceCount}
	}
	return out
}

// bestOther returns the best-ranked candidate other than index ci.
func bestOther(ranked []Candidate, ci int) Candidate {
	if ci == 0 {
		return ranked[1]
	}
	return ranked[0]
}

func displaced(cur, best Candidate) bool {
	if cur.Misses >= MissLimit {
		return true
	}
	if best.EvidenceSNR == nil {
		return false
	}
	if cur.EvidenceSNR == nil {
		return true
	}
	return *best.EvidenceSNR-*cur.EvidenceSNR >= HysteresisDB
}
