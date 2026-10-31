// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"slices"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/jackc/pgx/v5/pgtype"
)

func domain(n int16) []int16 {
	out := make([]int16, n)
	for i := range out {
		out[i] = int16(i)
	}
	return out
}

func TestPacketFilterNormalisation(t *testing.T) {
	full16, full4 := domain(16), domain(4)
	var zero time.Time
	t0 := time.UnixMilli(1792442625019).UTC()
	t1 := t0.Add(time.Minute)
	epoch := time.UnixMilli(0)
	for _, tc := range []struct {
		name              string
		pt, rt            []int16
		scopes            []string
		since, until      time.Time
		wantOK, wantTyped bool
		wantPT, wantRT    []int16
		wantScopes        []string
		wantWindowed      bool
	}{
		{"absent", nil, nil, nil, zero, zero, true, false, full16, full4, nil, false},
		{"empty after parse", []int16{}, []int16{}, []string{}, zero, zero, true, false, full16, full4, nil, false},
		{"dup types", []int16{4, 4}, []int16{0, 0, 1}, nil, zero, zero, true, true, []int16{4}, []int16{0, 1}, nil, false},
		{"order kept", []int16{9, 2, 4, 2}, nil, nil, zero, zero, true, true, []int16{9, 2, 4}, full4, nil, false},
		{"drop 99", []int16{99, 4}, nil, nil, zero, zero, true, true, []int16{4}, full4, nil, false},
		{"drop negative and 16", []int16{-3, 16, 15}, nil, nil, zero, zero, true, true, []int16{15}, full4, nil, false},
		{"all dropped", []int16{99}, nil, nil, zero, zero, false, false, nil, nil, nil, false},
		{"unknown name", []int16{-1}, nil, nil, zero, zero, false, false, nil, nil, nil, false},
		{"route 9", nil, []int16{9}, nil, zero, zero, false, false, nil, nil, nil, false},
		{"route 3,7", nil, []int16{3, 7}, nil, zero, zero, true, true, full16, []int16{3}, nil, false},
		{"route given payload dropped", []int16{99}, []int16{1}, nil, zero, zero, false, false, nil, nil, nil, false},
		{"scope dups kept untrimmed", nil, nil, []string{"#a", " #b", "#a"}, zero, zero, true, false, full16, full4, []string{"#a", " #b"}, false},
		{"scope blank singular kept", nil, nil, []string{" "}, zero, zero, true, false, full16, full4, []string{" "}, false},
		{"since > until is left to the statement", nil, nil, nil, t1, t0, true, false, full16, full4, nil, true},
		{"since = until", nil, nil, nil, t0, t0, true, false, full16, full4, nil, true},
		{"since < until", nil, nil, nil, t0, t1, true, false, full16, full4, nil, true},
		{"until only", nil, nil, nil, zero, t0, true, false, full16, full4, nil, true},
		{"since only", nil, nil, nil, t0, zero, true, false, full16, full4, nil, true},
		{"since epoch is a bound", nil, nil, nil, epoch, zero, true, false, full16, full4, nil, true},
		{"since epoch > until", nil, nil, nil, epoch, time.UnixMilli(-1), true, false, full16, full4, nil, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, ok := newPacketFilter(tc.pt, tc.rt, tc.scopes, tc.since, tc.until, 0, nil, 50)
			if ok != tc.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tc.wantOK)
			}
			if !ok {
				return
			}
			if f.typed != tc.wantTyped {
				t.Errorf("typed = %v, want %v", f.typed, tc.wantTyped)
			}
			if !slices.Equal(f.payloadTypes, tc.wantPT) {
				t.Errorf("payloadTypes = %v, want %v", f.payloadTypes, tc.wantPT)
			}
			if !slices.Equal(f.routeTypes, tc.wantRT) {
				t.Errorf("routeTypes = %v, want %v", f.routeTypes, tc.wantRT)
			}
			if !slices.Equal(f.scopes, tc.wantScopes) {
				t.Errorf("scopes = %q, want %q", f.scopes, tc.wantScopes)
			}
			if f.windowed != tc.wantWindowed {
				t.Errorf("windowed = %v, want %v", f.windowed, tc.wantWindowed)
			}
			checkBound(t, "since", f.since, tc.since, pgtype.NegativeInfinity)
			checkBound(t, "until", f.until, tc.until, pgtype.Infinity)
			if f.pageLimit != 51 {
				t.Errorf("pageLimit = %d, want 51", f.pageLimit)
			}
			if f.cursorHash == nil {
				t.Error("cursorHash is nil")
			}
		})
	}

	// The caller's slices are not modified.
	in := []int16{4, 4, 99}
	newPacketFilter(in, nil, nil, zero, zero, 0, nil, 1)
	if !slices.Equal(in, []int16{4, 4, 99}) {
		t.Errorf("input mutated: %v", in)
	}
}

func checkBound(t *testing.T, name string, got pgtype.Timestamptz, in time.Time, absent pgtype.InfinityModifier) {
	t.Helper()
	if !got.Valid {
		t.Errorf("%s not valid", name)
		return
	}
	if in.IsZero() {
		if got.InfinityModifier != absent {
			t.Errorf("%s = %+v, want infinity modifier %v", name, got, absent)
		}
		return
	}
	if got.InfinityModifier != pgtype.Finite || !got.Time.Equal(in) {
		t.Errorf("%s = %+v, want %v", name, got, in)
	}
}

func TestPacketFilterCursor(t *testing.T) {
	var zero time.Time
	at := time.UnixMicro(1792442625019123).UTC()
	hash := []byte{0xde, 0xad, 0xbe, 0xef}
	for _, tc := range []struct {
		name     string
		cursor   int64
		pc       *api.PacketCursor
		wantInf  bool
		wantTime time.Time
		wantHash []byte
	}{
		{"page 1", 0, nil, true, zero, []byte{}},
		{"negative cursor", -5, nil, true, zero, []byte{}},
		{"legacy cursor", 1792442625019, nil, false, time.UnixMilli(1792442625019), []byte{}},
		{"legacy max int64 wraps as today", 9223372036854775807, nil, false, time.UnixMilli(9223372036854775807), []byte{}},
		{"pageCursor", 0, &api.PacketCursor{At: at, Hash: hash}, false, at, hash},
		{"pageCursor wins over cursor", 5, &api.PacketCursor{At: at, Hash: hash}, false, at, hash},
		{"pageCursor with nil hash", 0, &api.PacketCursor{At: at}, false, at, []byte{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f, ok := newPacketFilter(nil, nil, nil, zero, zero, tc.cursor, tc.pc, 100)
			if !ok {
				t.Fatal("ok = false")
			}
			if f.cursorHash == nil {
				t.Fatal("cursorHash is nil")
			}
			if !bytes.Equal(f.cursorHash, tc.wantHash) {
				t.Errorf("cursorHash = %x, want %x", f.cursorHash, tc.wantHash)
			}
			if !f.cursorTS.Valid {
				t.Fatal("cursorTS not valid")
			}
			if tc.wantInf {
				if f.cursorTS.InfinityModifier != pgtype.Infinity {
					t.Errorf("cursorTS = %+v, want infinity", f.cursorTS)
				}
			} else if f.cursorTS.InfinityModifier != pgtype.Finite || !f.cursorTS.Time.Equal(tc.wantTime) {
				t.Errorf("cursorTS = %+v, want %v", f.cursorTS, tc.wantTime)
			}
			if f.pageLimit != 101 {
				t.Errorf("pageLimit = %d, want 101", f.pageLimit)
			}
		})
	}
}
