// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

func TestPacketCursorRoundTrip(t *testing.T) {
	c := PacketCursor{At: time.Date(2026, 10, 19, 20, 43, 45, 19_123_000, time.UTC), Hash: []byte{0xab, 0x01}}
	if got, want := c.String(), "v1:1792442625019123:ab01"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
	got, err := ParsePacketCursor(c.String())
	if err != nil {
		t.Fatalf("ParsePacketCursor: %v", err)
	}
	if !got.At.Equal(c.At) || !bytes.Equal(got.Hash, c.Hash) {
		t.Fatalf("round trip = %+v, want %+v", got, c)
	}
	if got.String() != c.String() {
		t.Fatalf("re-format = %q, want %q", got.String(), c.String())
	}
	upper, err := ParsePacketCursor("v1:1792442625019123:AB01")
	if err != nil || !bytes.Equal(upper.Hash, []byte{0xab, 0x01}) {
		t.Fatalf("upper-case hash = %+v, %v", upper, err)
	}
	for _, at := range []time.Time{
		time.UnixMicro(0), time.UnixMicro(-1),
		time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC),
		time.Date(9999, 12, 31, 23, 59, 59, 999999000, time.UTC),
		time.Date(2026, 9, 8, 12, 0, 0, 123456000, time.FixedZone("offset", 3600)),
	} {
		hash := bytes.Repeat([]byte{0xff}, 64)
		got, err := ParsePacketCursor(PacketCursor{At: at, Hash: hash}.String())
		if err != nil || !got.At.Equal(at) || !bytes.Equal(got.Hash, hash) {
			t.Fatalf("round trip at %v: %+v, %v", at, got, err)
		}
	}
}

// A nil []byte goes to Postgres as NULL, and a row comparison against NULL
// matches nothing, so a parsed cursor must always carry a non-nil hash.
func TestParsePacketCursorHashNeverNil(t *testing.T) {
	got, err := ParsePacketCursor("v1:1:00")
	if err != nil {
		t.Fatal(err)
	}
	if got.Hash == nil || !bytes.Equal(got.Hash, []byte{0}) {
		t.Fatalf("hash = %#v, want non-nil []byte{0}", got.Hash)
	}
}

func TestParsePacketCursorRejects(t *testing.T) {
	for _, raw := range []string{
		"", "v1", "v2:1:ab", "v1:x:ab", "v1:1:", "v1:1:abc", "v1:1:zz",
		"v1:1:" + strings.Repeat("ab", 65),
		"v1:253402300800000000:ab", "v1:-62135596800000001:ab",
		"v1:1:ab:cd", strings.Repeat("9", 161),
		"v1:1.5:ab", "v1: 1:ab", "v1:1: ab", "v1:9223372036854775808:ab",
		"v1:+1:ab", "v1:01:ab", "v1:-0:ab", "v1::ab",
	} {
		got, err := ParsePacketCursor(raw)
		if err == nil || got != nil {
			t.Errorf("accepted invalid cursor %q: %+v", raw, got)
			continue
		}
		if err.Error() != "invalid pageCursor" {
			t.Errorf("%q: error %q, want %q", raw, err, "invalid pageCursor")
		}
	}
	// Longest valid cursor sits well under the 160-character cap.
	longest := "v1:-62135596800000000:" + strings.Repeat("ab", 64)
	if _, err := ParsePacketCursor(longest); err != nil || len(longest) > 160 {
		t.Fatalf("longest valid cursor (%d chars) rejected: %v", len(longest), err)
	}
}

func TestNodeAndObserverCursors(t *testing.T) {
	id := uuid.MustParse("3f8a2c1e-9b7d-4e6f-a1b2-c3d4e5f60718")
	at := time.Date(2026, 10, 19, 20, 43, 45, 19_123_000, time.UTC)

	n := NodeCursor{LastSeen: at, ID: id}
	if got, want := n.String(), "v1:1792442625019123:3f8a2c1e-9b7d-4e6f-a1b2-c3d4e5f60718"; got != want {
		t.Fatalf("NodeCursor.String() = %q, want %q", got, want)
	}
	gotN, err := ParseNodeCursor(n.String())
	if err != nil || !gotN.LastSeen.Equal(at) || gotN.ID != id || gotN.String() != n.String() {
		t.Fatalf("node round trip = %+v, %v", gotN, err)
	}

	o := ObserverCursor{LastSeen: at, ID: id}
	if o.String() != n.String() {
		t.Fatalf("ObserverCursor.String() = %q, want %q", o.String(), n.String())
	}
	gotO, err := ParseObserverCursor(o.String())
	if err != nil || !gotO.LastSeen.Equal(at) || gotO.ID != id || gotO.String() != o.String() {
		t.Fatalf("observer round trip = %+v, %v", gotO, err)
	}

	nilRaw := "v1:1:" + uuid.Nil.String()
	if c, err := ParseNodeCursor(nilRaw); err != nil || c.ID != uuid.Nil {
		t.Fatalf("nil uuid node cursor: %+v, %v", c, err)
	}
	if c, err := ParseObserverCursor(nilRaw); err != nil || c.ID != uuid.Nil {
		t.Fatalf("nil uuid observer cursor: %+v, %v", c, err)
	}

	for _, raw := range []string{
		"", "v1", "v2:1:" + id.String(), "v1:x:" + id.String(), "v1:1:",
		"v1:1:not-a-uuid",
		"v1:1:{" + id.String() + "}",
		"v1:1:3f8a2c1e9b7d4e6fa1b2c3d4e5f60718",
		"v1:1:urn:uuid:" + id.String(),
		"v1:253402300800000000:" + id.String(),
		"v1:-62135596800000001:" + id.String(),
		"v1:1:" + id.String() + ":x",
		strings.Repeat("9", 161),
	} {
		if c, err := ParseNodeCursor(raw); err == nil || c != nil || err.Error() != "invalid pageCursor" {
			t.Errorf("node accepted %q: %+v, %v", raw, c, err)
		}
		if c, err := ParseObserverCursor(raw); err == nil || c != nil || err.Error() != "invalid pageCursor" {
			t.Errorf("observer accepted %q: %+v, %v", raw, c, err)
		}
	}
}

func TestPageCursorJSON(t *testing.T) {
	cur := "v1:1:ab"
	for name, v := range map[string]any{
		"packet":   PacketPage{Page: Page[PacketSummary]{Items: []PacketSummary{}}},
		"node":     NodePage{Page: Page[NodeSummary]{Items: []NodeSummary{}}},
		"observer": ObserverPage{Page: Page[ObserverSummary]{Items: []ObserverSummary{}}},
	} {
		b, err := json.Marshal(v)
		if err != nil || string(b) != `{"items":[],"hasMore":false}` {
			t.Errorf("%s empty page = %s, %v", name, b, err)
		}
	}
	b, _ := json.Marshal(PacketPage{Page: Page[PacketSummary]{Items: []PacketSummary{}, HasMore: true}, NextPageCursor: &cur})
	if string(b) != `{"items":[],"hasMore":true,"nextPageCursor":"v1:1:ab"}` {
		t.Errorf("packet page with cursor = %s", b)
	}
}

func TestNodeSortCursor(t *testing.T) {
	id := uuid.MustParse("01234567-89ab-cdef-0123-456789abcdef")
	for _, c := range []NodeSortCursor{
		{Sort: NodeSort{By: NodeSortName}, Text: "🏔️ Peak: 9/10", Seen: -1767225600000000, ID: id},
		{Sort: NodeSort{By: NodeSortName, Descending: true}, Null: 1, Seen: -1, ID: id},
		{Sort: NodeSort{By: NodeSortNeighbors, Descending: true}, Num: -42, Seen: -1767225600123456, ID: id},
		{Sort: NodeSort{By: NodeSortType}, Num: 5, ID: id},
	} {
		got, err := ParseNodeSortCursor(c.String())
		if err != nil || *got != c {
			t.Fatalf("%s: %+v, %v", c.String(), got, err)
		}
	}
	valid := NodeSortCursor{Sort: NodeSort{By: NodeSortName}, Text: "x", ID: id}.String()
	for _, raw := range []string{
		"", "v1:1:" + id.String(), valid + ":", strings.Replace(valid, "s1:", "s2:", 1),
		strings.Replace(valid, ":name:", ":radio:", 1), strings.Replace(valid, ":a:", ":x:", 1),
		"s1:name:a:2:0:0:" + id.String() + ":eA", // null is 0 or 1
		"s1:name:a:01:0:0:" + id.String() + ":eA", "s1:name:a:-0:0:0:" + id.String() + ":eA",
		"s1:name:a:0:01:0:" + id.String() + ":eA", "s1:name:a:0:0:+1:" + id.String() + ":eA",
		"s1:name:a:0:0:0:" + strings.ReplaceAll(id.String(), "-", "") + ":eA",
		"s1:name:a:0:0:0:" + id.String() + ":eA==", "s1:name:a:0:0:0:" + id.String() + ":e+A",
		"s1:name:a:0:0:0:" + id.String() + ":" + strings.Repeat("eA", 300),
	} {
		if _, err := ParseNodeSortCursor(raw); err == nil {
			t.Fatalf("accepted %q", raw)
		}
	}
}
