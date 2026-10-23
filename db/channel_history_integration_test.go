// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package db

import (
	"bytes"
	"testing"
)

func TestChannelHistoryPagesPostgres(t *testing.T) {
	f := newPsFixture(t)
	for i := range 501 {
		f.psPacket(i)
	}
	var after []byte
	seen := 0
	for range 3 {
		page, err := f.store.ListChannelHistoryPage(f.ctx, after, 10000)
		if err != nil {
			t.Fatal(err)
		}
		if len(page) > 500 {
			t.Fatal("history work was not bounded")
		}
		for _, p := range page {
			if bytes.Compare(p.PacketHash, after) <= 0 {
				t.Fatal("history cursor did not advance")
			}
			after = p.PacketHash
			seen++
		}
		if len(page) == 0 {
			break
		}
	}
	if seen != 501 {
		t.Fatalf("history candidate coverage=%d", seen)
	}
}
