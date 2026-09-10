// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package lora computes LoRa airtime for MeshCore frames, so an observation
// can be costed in milliseconds of channel occupancy.
package lora

import "math"

const (
	minSF = 7
	maxSF = 12
	minCR = 5
	maxCR = 8
)

// PreambleSymbols mirrors MeshCore's preambleLengthForSF: 32 symbols at SF<=8, 16 above.
func PreambleSymbols(sf int) int {
	if sf <= 8 {
		return 32
	}
	return 16
}

// FrameLength is the on-air byte count of a MeshCore frame rebuilt from its stored parts.
func FrameLength(transportCodes bool, pathLen, payloadLen int) int {
	n := 1 + 1 + pathLen + payloadLen // header byte + path length byte
	if transportCodes {
		n += 4
	}
	return n
}

// TimeOnAirMs is the Semtech LoRa time-on-air (RadioLib getTimeOnAir: CRC on, explicit header,
// LDRO auto at t_sym >= 16 ms). ok is false when any parameter is outside the costable range.
func TimeOnAirMs(frameLen, sf int, bwKHz float64, cr int) (ms float64, ok bool) {
	if frameLen <= 0 || sf < minSF || sf > maxSF || bwKHz <= 0 || cr < minCR || cr > maxCR {
		return 0, false
	}
	tSym := math.Exp2(float64(sf)) / bwKHz
	de := 0
	if tSym >= 16 {
		de = 1
	}
	n := float64(PreambleSymbols(sf)) + 4.25 + float64(symbols(frameLen, sf, cr, de))
	return n * tSym, true
}

// symbols is the Semtech payload symbol count with explicit header and CRC on.
func symbols(frameLen, sf, cr, de int) int {
	n := math.Ceil(float64(8*frameLen-4*sf+44)/float64(4*(sf-2*de))) * float64(cr)
	if n < 0 {
		n = 0
	}
	return 8 + int(n)
}
