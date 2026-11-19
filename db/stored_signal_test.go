// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import "testing"

func TestStoredSignal(t *testing.T) {
	r, zeroR := int16(-90), int16(0)
	s, zeroS := float32(4.5), float32(0)
	if gotR, gotS := storedSignal(&zeroR, &zeroS); gotR != nil || gotS != nil {
		t.Fatal("the old 0/0 placeholder was shown as a reading")
	}
	if gotR, gotS := storedSignal(&r, &zeroS); gotR != &r || gotS != &zeroS {
		t.Fatal("a real 0 dB SNR was hidden")
	}
	if gotR, gotS := storedSignal(nil, &s); gotR != nil || gotS != &s {
		t.Fatal("an SNR without RSSI was changed")
	}
	if gotR, gotS := storedSignal(&r, nil); gotR != &r || gotS != nil {
		t.Fatal("an RSSI without SNR was changed")
	}
	if gotR, gotS := storedSignal(nil, nil); gotR != nil || gotS != nil {
		t.Fatal("nothing became a reading")
	}
}
