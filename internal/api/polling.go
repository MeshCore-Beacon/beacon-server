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
	Paused        bool              `json:"paused"`
	PausedUntil   *int64            `json:"pausedUntil"` // epoch ms; null when not set
	ExcludedNodes []string          `json:"excludedNodes"`
	Observers     []PollingObserver `json:"observers"`
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
	if len(r.ExcludedNodes) > MaxPollingExcludedNodes {
		return nil, nil, ErrPollingTooManyNodes
	}
	nodes := make([][]byte, 0, len(r.ExcludedNodes))
	for _, h := range r.ExcludedNodes {
		b, err := ParsePollingKey(h)
		if err != nil {
			return nil, nil, err
		}
		nodes = append(nodes, b)
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
