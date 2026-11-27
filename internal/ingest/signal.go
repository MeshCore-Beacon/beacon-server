// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"bytes"
	"encoding/json"
	"math"
	"strconv"
)

// What a LoRa receiver can report. Packet SNR is a signed byte of quarter dB, so -32 to 31.75 (32 once rounded).
// Packet RSSI is below 0 dBm on every chip and no lower than -164 dBm, the bottom of the widest register formula.
const (
	snrMin, snrMax   = -32, 32
	rssiMin, rssiMax = -164, -1
)

// signalNumber reads a bare or quoted JSON number; ok is false when it is missing, not a number or not finite.
func signalNumber(raw json.RawMessage) (float64, bool) {
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
	return n, !math.IsNaN(n) && !math.IsInf(n, 0)
}

// validSNR returns snr when a receiver could have measured it, nil otherwise. Zero is a measurement.
func validSNR(snr *float32) *float32 {
	if snr == nil || math.IsNaN(float64(*snr)) || *snr < snrMin || *snr > snrMax {
		return nil
	}
	return snr
}

// observationSignal reads a packet report's RSSI and SNR; nil is not measured. An SNR of exactly 0 counts only
// beside a valid RSSI: a receiver reports both, and 0 with no RSSI is how observers write "unknown".
func observationSignal(rawRSSI, rawSNR json.RawMessage) (*int16, *float32) {
	var rssi *int16
	if n, ok := signalNumber(rawRSSI); ok && n >= rssiMin && n <= rssiMax {
		v := int16(n)
		rssi = &v
	}
	var snr *float32
	if n, ok := signalNumber(rawSNR); ok {
		v := float32(n)
		snr = validSNR(&v)
	}
	if snr != nil && *snr == 0 && rssi == nil {
		snr = nil
	}
	return rssi, snr
}
