// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

// maxPageCursorLen bounds every pageCursor before it is split or decoded.
const maxPageCursorLen = 160

// maxPacketCursorHash is the longest packet hash a cursor may carry, in bytes.
const maxPacketCursorHash = 64

var errPageCursor = errors.New("invalid pageCursor")

// PacketCursor is a boundary in (last_heard_at DESC, packet_hash DESC) order.
type PacketCursor struct {
	At   time.Time
	Hash []byte
}

func (c PacketCursor) String() string {
	return fmt.Sprintf("v1:%d:%x", c.At.UnixMicro(), c.Hash)
}

// ParsePacketCursor parses the opaque, versioned cursor returned by PacketPage.
// The returned Hash is never nil.
func ParsePacketCursor(raw string) (*PacketCursor, error) {
	at, tail, err := splitPageCursor(raw)
	if err != nil {
		return nil, err
	}
	if tail == "" || len(tail) > 2*maxPacketCursorHash {
		return nil, errPageCursor
	}
	hash, err := hex.DecodeString(tail)
	if err != nil || len(hash) == 0 {
		return nil, errPageCursor
	}
	return &PacketCursor{At: at, Hash: hash}, nil
}

// NodeCursor is a boundary in (last_seen DESC, id DESC) order.
type NodeCursor struct {
	LastSeen time.Time
	ID       uuid.UUID
}

func (c NodeCursor) String() string {
	return fmt.Sprintf("v1:%d:%s", c.LastSeen.UnixMicro(), c.ID)
}

// ParseNodeCursor parses the opaque, versioned cursor returned by NodePage.
func ParseNodeCursor(raw string) (*NodeCursor, error) {
	at, id, err := parseUUIDCursor(raw)
	if err != nil {
		return nil, err
	}
	return &NodeCursor{LastSeen: at, ID: id}, nil
}

// ObserverCursor is a boundary in (last_seen DESC, id DESC) order.
type ObserverCursor struct {
	LastSeen time.Time
	ID       uuid.UUID
}

func (c ObserverCursor) String() string {
	return fmt.Sprintf("v1:%d:%s", c.LastSeen.UnixMicro(), c.ID)
}

// ParseObserverCursor parses the opaque, versioned cursor returned by ObserverPage.
func ParseObserverCursor(raw string) (*ObserverCursor, error) {
	at, id, err := parseUUIDCursor(raw)
	if err != nil {
		return nil, err
	}
	return &ObserverCursor{LastSeen: at, ID: id}, nil
}

// parseUUIDCursor accepts only the canonical 36-character hyphenated UUID form.
func parseUUIDCursor(raw string) (time.Time, uuid.UUID, error) {
	at, tail, err := splitPageCursor(raw)
	if err != nil {
		return time.Time{}, uuid.Nil, err
	}
	if len(tail) != 36 {
		return time.Time{}, uuid.Nil, errPageCursor
	}
	id, err := uuid.Parse(tail)
	if err != nil {
		return time.Time{}, uuid.Nil, errPageCursor
	}
	return at, id, nil
}

// splitPageCursor checks the "v1:<unix micros>:<tail>" frame, with micros in
// canonical decimal form, and returns the UTC time and the unparsed tail.
func splitPageCursor(raw string) (time.Time, string, error) {
	if len(raw) > maxPageCursorLen {
		return time.Time{}, "", errPageCursor
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 3 || parts[0] != "v1" {
		return time.Time{}, "", errPageCursor
	}
	micros, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil || strconv.FormatInt(micros, 10) != parts[1] {
		return time.Time{}, "", errPageCursor // also rejects "+1", "01", "-0"
	}
	at := time.UnixMicro(micros).UTC()
	if at.Year() < 1 || at.Year() > 9999 {
		return time.Time{}, "", errPageCursor
	}
	return at, parts[2], nil
}
