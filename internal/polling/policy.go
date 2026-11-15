// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

const (
	fwClient     = "meshcore.observer_fw"
	maxExcluded  = 16
	maxOneHopSec = 86400
)

// Caller is what the observer's query string told us about it.
type Caller struct {
	Observer      []byte // nil when the query value was not 64 hex
	Client        string
	ClientVersion string
	Blocked       bool
}

type RuntimeState struct {
	Paused      bool
	PausedUntil *time.Time
	Excluded    [][]byte
}

// Policy is a complete answer: the firmware resets missing fields to defaults.
type Policy struct {
	ReadingFormat    string         `json:"reading_format"`
	MQTTHosts        []string       `json:"mqtt_hosts"`
	DirectS          int            `json:"direct_s"`
	OneHopS          int            `json:"one_hop_s"`
	GraceS           int            `json:"grace_s"`
	MaxRequests      int            `json:"max_requests"`
	Version          string         `json:"version"`
	Paused           bool           `json:"paused"`
	PausedUntil      string         `json:"paused_until"`
	ObserverBlocked  bool           `json:"observer_blocked"`
	MinPluginVersion string         `json:"min_plugin_version"`
	VersionOK        bool           `json:"version_ok"`
	ExcludedNodes    []string       `json:"excluded_nodes"`
	NodeWindows      map[string]int `json:"node_windows"`
}

// policyContent is the caller-independent part that Version hashes.
type policyContent struct {
	ReadingFormat string         `json:"reading_format"`
	MQTTHosts     []string       `json:"mqtt_hosts"`
	DirectS       int            `json:"direct_s"`
	OneHopS       int            `json:"one_hop_s"`
	GraceS        int            `json:"grace_s"`
	MaxRequests   int            `json:"max_requests"`
	Paused        bool           `json:"paused"`
	PausedUntil   string         `json:"paused_until"`
	ExcludedNodes []string       `json:"excluded_nodes"`
	NodeWindows   map[string]int `json:"node_windows"`
}

func (p *Policy) setVersion() {
	b, _ := json.Marshal(policyContent{
		p.ReadingFormat, p.MQTTHosts, p.DirectS, p.OneHopS, p.GraceS, p.MaxRequests,
		p.Paused, p.PausedUntil, p.ExcludedNodes, p.NodeWindows,
	})
	sum := sha256.Sum256(b)
	p.Version = hex.EncodeToString(sum[:])[:12]
}

func oneHop(directS int) int {
	return min(directS*2, maxOneHopSec)
}

// BuildPolicy answers for one caller. Every field is always set.
func BuildPolicy(cfg config.ResolvedPolling, rt RuntimeState, c Caller, now time.Time) Policy {
	directS := int(cfg.DirectWindow / time.Second)
	p := Policy{
		ReadingFormat:   "telemetry",
		MQTTHosts:       append([]string{}, cfg.MQTTHosts...),
		DirectS:         directS,
		OneHopS:         oneHop(directS),
		GraceS:          int(cfg.Grace / time.Second),
		MaxRequests:     cfg.MaxRequests,
		ObserverBlocked: c.Blocked,
		VersionOK:       versionOK(cfg.MinClientVersion, c),
		ExcludedNodes:   mergeExcluded(cfg.ExcludedNodes, rt.Excluded),
		NodeWindows:     map[string]int{},
	}
	switch {
	case cfg.Paused:
		p.Paused = true
	case rt.Paused && rt.PausedUntil == nil:
		p.Paused = true
	case rt.Paused && rt.PausedUntil.After(now):
		p.Paused = true
		p.PausedUntil = rt.PausedUntil.UTC().Format(time.RFC3339)
	}
	p.setVersion()
	return p
}

// DisabledPolicy is served when polling is off: paused, defaults, no hosts.
func DisabledPolicy() Policy {
	p := Policy{
		ReadingFormat: "telemetry",
		MQTTHosts:     []string{},
		DirectS:       3600,
		OneHopS:       7200,
		GraceS:        1800,
		MaxRequests:   2,
		Paused:        true,
		VersionOK:     true,
		ExcludedNodes: []string{},
		NodeWindows:   map[string]int{},
	}
	p.setVersion()
	return p
}

// mergeExcluded keeps the first maxExcluded distinct keys so the policy fits the firmware. Config and admin
// validation keep the union within that; Service.Policy logs if it ever is not.
func mergeExcluded(lists ...[][]byte) []string {
	out := distinctExcluded(lists...)
	return out[:min(len(out), maxExcluded)]
}

func distinctExcluded(lists ...[][]byte) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, l := range lists {
		for _, b := range l {
			h := strings.ToLower(hex.EncodeToString(b))
			if !seen[h] {
				seen[h] = true
				out = append(out, h)
			}
		}
	}
	return out
}

// versionOK gates only the observer firmware family. A missing or odd
// version from that client fails when a minimum is set.
func versionOK(minVer string, c Caller) bool {
	if minVer == "" || c.Client != fwClient {
		return true
	}
	want, ok := parseSemver(minVer)
	if !ok {
		return false // config load rejects this; never fail open
	}
	have, ok := parseSemver(c.ClientVersion)
	if !ok {
		return false
	}
	for i := range want {
		if have[i] != want[i] {
			return have[i] > want[i]
		}
	}
	return true
}

// parseSemver reads major.minor.patch with an optional leading v and ignores any suffix.
func parseSemver(s string) ([3]int, bool) {
	var v [3]int
	s = strings.TrimPrefix(strings.TrimSpace(s), "v")
	parts := strings.SplitN(s, ".", 3)
	if len(parts) != 3 {
		return v, false
	}
	for i, p := range parts {
		end := 0
		for end < len(p) && p[end] >= '0' && p[end] <= '9' {
			end++
		}
		if end == 0 || (i < 2 && end != len(p)) {
			return v, false
		}
		n, err := strconv.Atoi(p[:end])
		if err != nil {
			return v, false
		}
		v[i] = n
	}
	return v, true
}

type OwnerRow struct {
	Target     []byte
	Rank       int // 0 = poller, 1.. = standby; -1 = no assignment yet
	Assigned   int // assignment rows the target has, from any observer
	PollerHops int
	LatestTS   *time.Time
	Excluded   bool
	// Requested: the observer is assigned the target as a requested one. ClaimedRequested: its claim names it as
	// one. PWOK: the entry may be offered (OfferSeats).
	Requested, ClaimedRequested, PWOK bool
	// TargetRequested: a request is in use for the target. A requested assignment without one is withdrawn.
	TargetRequested bool
	// The claim's own numbers, the same on every row.
	AcceptsRequests bool
	RequestSlots    int
	// The observer's flags and the password in use, read in the same statement as the rest of the row. Password is
	// nil unless the row is a requested assignment of an unblocked observer whose claim accepts requests.
	ObserverApproved, ObserverBlocked bool
	Password                          *RequestedPassword
}

// OfferSeats sets PWOK on the requested assignments the device can take in this answer. The firmware drops the
// requested targets an answer leaves out before it adds new ones, so its free seats are the claim's request_slots
// plus the held targets it is no longer given (unassigned, or no longer requested): the free-plus-held count the
// election gave seats from.
func OfferSeats(rows []OwnerRow) {
	free := 0
	var added []int
	for i := range rows {
		r := &rows[i]
		given := r.Requested && r.TargetRequested
		r.PWOK = r.AcceptsRequests && given && r.ClaimedRequested
		if i == 0 {
			free = min(max(r.RequestSlots, 0), MaxRequestSlots)
		}
		switch {
		case r.ClaimedRequested && !given:
			free++
		case given && !r.ClaimedRequested && r.AcceptsRequests:
			added = append(added, i)
		}
	}
	// Pollers first, should the seats run short.
	sort.SliceStable(added, func(a, b int) bool { return rows[added[a]].Rank < rows[added[b]].Rank })
	for _, i := range added {
		if free == 0 {
			break
		}
		rows[i].PWOK = true
		free--
	}
}

type OwnerEntry struct {
	Role        string  `json:"role"`
	Mine        bool    `json:"mine"`
	Excluded    bool    `json:"excluded"`
	StandbyRank int     `json:"standby_rank"`
	OwnerHops   int     `json:"owner_hops"`
	LatestTS    *string `json:"latest_ts"`
	// Requested marks a server-requested target; PW is the password sealed to this observer, when one is in use.
	Requested bool      `json:"requested,omitempty"`
	PW        *Envelope `json:"pw,omitempty"`
}

type OwnersAnswer struct {
	Nodes  map[string]OwnerEntry `json:"nodes"`
	Policy Policy                `json:"policy"`
}

func BuildOwners(rows []OwnerRow, p Policy) OwnersAnswer {
	nodes := make(map[string]OwnerEntry, len(rows))
	for _, r := range rows {
		// A device holding a requested target it is not assigned drops it when the answer leaves it out.
		if (r.ClaimedRequested && !r.Requested) || (r.Requested && !r.PWOK) {
			continue
		}
		e := OwnerEntry{Role: "polling", Mine: true, StandbyRank: 1, Requested: r.Requested}
		switch {
		case r.Excluded:
			e = OwnerEntry{Role: "excluded", Excluded: true, StandbyRank: 1}
		case r.Rank >= 1:
			e = OwnerEntry{Role: "standby", StandbyRank: r.Rank, Requested: r.Requested}
		}
		if r.LatestTS != nil {
			s := r.LatestTS.UTC().Truncate(time.Second).Format(time.RFC3339)
			e.LatestTS = &s
		}
		nodes[strings.ToLower(hex.EncodeToString(r.Target))] = e
	}
	return OwnersAnswer{Nodes: nodes, Policy: p}
}
