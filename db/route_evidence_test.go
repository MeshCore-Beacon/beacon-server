// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package db

import (
	"bytes"
	"testing"
)

func TestSavedRoutePath(t *testing.T) {
	size, path, ok := savedRoutePath([][]byte{{0xaa}, {0xbb}}, 2)
	if !ok || size != 1 || !bytes.Equal(path, []byte{0xaa, 0xbb}) {
		t.Fatal("valid path lost")
	}
	for _, prefixes := range [][][]byte{nil, {{1}}, {{1}, {2, 3}}, {{}, {2}}, {{1, 2, 3, 4}, {5, 6, 7, 8}}} {
		if _, _, ok := savedRoutePath(prefixes, int32(len(prefixes))); ok {
			t.Fatalf("accepted malformed path %x", prefixes)
		}
	}
	if _, _, ok := savedRoutePath([][]byte{{1}, {2}}, 3); ok {
		t.Fatal("accepted inconsistent hop count")
	}
}
