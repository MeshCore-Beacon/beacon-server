// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/hex"
	"errors"
	"time"
)

const (
	// MaxPollingExcludedNodes matches the config bound.
	MaxPollingExcludedNodes = 16
	MaxPollingNoteLen       = 200
	// MaxPollingTargets caps the admin targets list.
	MaxPollingTargets = 500
)

// PollingObserver is an operator's verdict on one observer.
type PollingObserver struct {
	PublicKey string `json:"publicKey"` // 64 lowercase hex
	Blocked   bool   `json:"blocked"`
	Approved  bool   `json:"approved"`
	Note      string `json:"note"`
}

// PollingAdminState is the operator view of repeater polling.
type PollingAdminState struct {
	Paused        bool     `json:"paused"`       // the stored pause, false once pausedUntil has passed
	ConfigPaused  bool     `json:"configPaused"` // polling.paused in config, which pauses regardless
	PausedUntil   *int64   `json:"pausedUntil"`  // epoch ms; null when not set
	ExcludedNodes []string `json:"excludedNodes"`
	// polling.excluded_nodes in config; the policy excludes the union of both lists, at most 16 distinct.
	ConfigExcludedNodes []string `json:"configExcludedNodes"`
	// polling.authority: evidence or approved.
	Authority string            `json:"authority"`
	Observers []PollingObserver `json:"observers"`
	// NoPasswordBlocked lists repeaters that grant admin to a blank login (64 hex each); no passwordless request is
	// accepted for them until the operator clears the flag.
	NoPasswordBlocked []string `json:"noPasswordBlocked"`
	// AdminStops are observers that were given admin access by a requested repeater and are never offered it again.
	AdminStops []PollingAdminStop `json:"adminStops"`
}

// PollingAdminStop is one observer that must not take one requested repeater.
type PollingAdminStop struct {
	TargetPublicKey   string `json:"targetPublicKey"`   // 64 lowercase hex
	ObserverPublicKey string `json:"observerPublicKey"` // 64 lowercase hex
	ReportedAt        int64  `json:"reportedAt"`        // epoch ms
}

// PollingTargetUpdate replaces what the operator can change on one repeater. Only false is accepted: it lifts the
// blank-login block.
type PollingTargetUpdate struct {
	NoPasswordBlocked bool `json:"noPasswordBlocked"`
}

// PollingTargetCandidate is one observer's place for a repeater: a live candidate, an assigned observer, or both.
type PollingTargetCandidate struct {
	ObserverPublicKey string   `json:"observerPublicKey"` // 64 lowercase hex
	Rank              *int     `json:"rank"`              // 0 polls; null when unassigned
	EvidenceSnr       *float64 `json:"evidenceSnr"`       // mean zero-hop advert SNR at the last election
	EvidenceCount     int      `json:"evidenceCount"`
	KeyMode           string   `json:"keyMode"`
	State             string   `json:"state"`
	Misses            int      `json:"misses"`
	LastOk            *int64   `json:"lastOk"` // epoch ms; null when never
	Approved          bool     `json:"approved"`
}

// PollingTarget is one repeater with a live claim or an assignment.
type PollingTarget struct {
	PublicKey            string                   `json:"publicKey"`            // 64 lowercase hex
	LatestTrustedReading *int64                   `json:"latestTrustedReading"` // epoch ms; only readings from an assigned observer with advert evidence
	LatestReading        *int64                   `json:"latestReading"`        // epoch ms, from any observer
	Candidates           []PollingTargetCandidate `json:"candidates"`
	// Requested is true while a telemetry request is in use for the repeater; Requests counts all its requests.
	Requested bool `json:"requested"`
	Requests  int  `json:"requests"`
}

// PollingTargetList is at most 500 targets, newest claim first.
type PollingTargetList struct {
	Targets []PollingTarget `json:"targets"`
}

// PollingStateRequest replaces the polling pause and exclusion state.
type PollingStateRequest struct {
	Paused        bool     `json:"paused"`
	PausedUntil   *int64   `json:"pausedUntil"` // epoch ms in the future, or null
	ExcludedNodes []string `json:"excludedNodes"`
}

// PollingObserverRequest replaces one observer's flags.
type PollingObserverRequest struct {
	Blocked  bool   `json:"blocked"`
	Approved bool   `json:"approved"`
	Note     string `json:"note"`
}

var (
	ErrPollingTooManyNodes = errors.New("excludedNodes: at most 16 entries")
	ErrPollingBadKey       = errors.New("keys must be 64 hex characters")
	ErrPollingPastPause    = errors.New("pausedUntil must be in the future")
	ErrPollingNoteLong     = errors.New("note: at most 200 characters")
)

// ParsePollingKey accepts 64 hex characters in either case.
func ParsePollingKey(s string) ([]byte, error) {
	if len(s) != 64 {
		return nil, ErrPollingBadKey
	}
	b, err := hex.DecodeString(s)
	if err != nil {
		return nil, ErrPollingBadKey
	}
	return b, nil
}

// Parse validates the request and returns the values to store.
func (r PollingStateRequest) Parse(now time.Time) (*time.Time, [][]byte, error) {
	nodes := make([][]byte, 0, len(r.ExcludedNodes))
	seen := make(map[string]bool, len(r.ExcludedNodes))
	for _, h := range r.ExcludedNodes {
		b, err := ParsePollingKey(h)
		if err != nil {
			return nil, nil, err
		}
		if !seen[string(b)] {
			seen[string(b)] = true
			nodes = append(nodes, b)
		}
	}
	if len(nodes) > MaxPollingExcludedNodes {
		return nil, nil, ErrPollingTooManyNodes
	}
	var until *time.Time
	if r.PausedUntil != nil {
		t := time.UnixMilli(*r.PausedUntil).UTC()
		if !t.After(now) {
			return nil, nil, ErrPollingPastPause
		}
		until = &t
	}
	return until, nodes, nil
}

// Validate checks the note length in characters.
func (r PollingObserverRequest) Validate() error {
	if len([]rune(r.Note)) > MaxPollingNoteLen {
		return ErrPollingNoteLong
	}
	return nil
}
