// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package ingest

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
)

// signalNumber preserves missing/invalid values as missing, including quoted
// non-finite values from observers. A real zero remains a measurement.
func signalNumber(raw json.RawMessage, lo, hi float64) (float64, bool) {
	if len(raw) == 0 || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
		return 0, false
	}
	var n float64
	if err := json.Unmarshal(raw, &n); err != nil {
		var text string
		if json.Unmarshal(raw, &text) != nil {
			return 0, false
		}
		var err error
		n, err = strconv.ParseFloat(text, 64)
		if err != nil {
			return 0, false
		}
	}
	return n, !math.IsNaN(n) && !math.IsInf(n, 0) && n >= lo && n <= hi
}

func signalSNR(raw json.RawMessage) *float32 {
	n, ok := signalNumber(raw, -50, 50)
	if !ok {
		return nil
	}
	v := float32(n)
	return &v
}

func signalRSSI(raw json.RawMessage) *int16 {
	n, ok := signalNumber(raw, -200, 0)
	if !ok {
		return nil
	}
	v := int16(n)
	return &v
}
