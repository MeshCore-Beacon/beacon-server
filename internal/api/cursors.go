// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/base64"
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

// maxNodeSortCursorLen bounds a NodeSortCursor, which carries a node name.
const maxNodeSortCursorLen = 512

// NodeSortCursor is a boundary in a NodeSort order: the last row's sort keys.
type NodeSortCursor struct {
	Sort NodeSort
	Null int32  // 1 for an unnamed node in name order
	Text string // the name, in name order
	Num  int64  // the type, neighbour count or last_seen in unix micros, negated when descending
	Seen int64  // last_seen in negated unix micros
	ID   uuid.UUID
}

func (c NodeSortCursor) String() string {
	order := "a"
	if c.Sort.Descending {
		order = "d"
	}
	return fmt.Sprintf("s1:%s:%s:%d:%d:%d:%s:%s", c.Sort.By, order, c.Null, c.Num, c.Seen, c.ID,
		base64.RawURLEncoding.EncodeToString([]byte(c.Text)))
}

// ParseNodeSortCursor parses the opaque cursor a sorted NodePage returns.
func ParseNodeSortCursor(raw string) (*NodeSortCursor, error) {
	if len(raw) > maxNodeSortCursorLen {
		return nil, errPageCursor
	}
	parts := strings.Split(raw, ":")
	if len(parts) != 8 || parts[0] != "s1" || !ValidNodeSort(parts[1]) || (parts[2] != "a" && parts[2] != "d") {
		return nil, errPageCursor
	}
	if parts[3] != "0" && parts[3] != "1" {
		return nil, errPageCursor
	}
	var null int32
	if parts[3] == "1" {
		null = 1
	}
	var nums [2]int64
	for i, part := range parts[4:6] {
		n, err := strconv.ParseInt(part, 10, 64)
		if err != nil || strconv.FormatInt(n, 10) != part {
			return nil, errPageCursor
		}
		nums[i] = n
	}
	if len(parts[6]) != 36 {
		return nil, errPageCursor
	}
	id, err := uuid.Parse(parts[6])
	if err != nil {
		return nil, errPageCursor
	}
	text, err := base64.RawURLEncoding.Strict().DecodeString(parts[7])
	if err != nil {
		return nil, errPageCursor
	}
	return &NodeSortCursor{
		Sort: NodeSort{By: parts[1], Descending: parts[2] == "d"},
		Null: null, Num: nums[0], Seen: nums[1], ID: id, Text: string(text),
	}, nil
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
