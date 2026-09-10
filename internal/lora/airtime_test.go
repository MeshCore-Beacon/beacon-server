// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package lora

import (
	"math"
	"testing"
)

func TestPreambleSymbols(t *testing.T) {
	cases := []struct {
		sf   int
		want int
	}{
		{7, 32},
		{8, 32},
		{9, 16},
		{12, 16},
	}
	for _, tc := range cases {
		if got := PreambleSymbols(tc.sf); got != tc.want {
			t.Errorf("PreambleSymbols(%d) = %d, want %d", tc.sf, got, tc.want)
		}
	}
}

func TestFrameLength(t *testing.T) {
	cases := []struct {
		name           string
		transportCodes bool
		pathLen        int
		payloadLen     int
		want           int
	}{
		{"no transport codes, no path", false, 0, 10, 12},
		{"transport codes with path", true, 3, 10, 19},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := FrameLength(tc.transportCodes, tc.pathLen, tc.payloadLen); got != tc.want {
				t.Errorf("FrameLength(%v, %d, %d) = %d, want %d", tc.transportCodes, tc.pathLen, tc.payloadLen, got, tc.want)
			}
		})
	}
}

func TestSymbols(t *testing.T) {
	cases := []struct {
		name     string
		frameLen int
		sf       int
		cr       int
		de       int
		want     int
	}{
		{"sf7 no ldro", 50, 7, 5, 0, 83},
		{"sf10 ldro", 50, 10, 5, 1, 73},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := symbols(tc.frameLen, tc.sf, tc.cr, tc.de); got != tc.want {
				t.Errorf("symbols(%d, %d, %d, %d) = %d, want %d", tc.frameLen, tc.sf, tc.cr, tc.de, got, tc.want)
			}
		})
	}
}

func TestTimeOnAirMs(t *testing.T) {
	cases := []struct {
		name     string
		frameLen int
		sf       int
		bwKHz    float64
		cr       int
		want     float64
	}{
		{"sf7 bw62.5", 50, 7, 62.5, 5, 244.224},
		{"sf10 bw62.5 ldro", 50, 10, 62.5, 5, 1527.808},
		{"sf7 bw250", 50, 7, 250, 5, 61.056},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := TimeOnAirMs(tc.frameLen, tc.sf, tc.bwKHz, tc.cr)
			if !ok {
				t.Fatalf("TimeOnAirMs(%d, %d, %v, %d) not costable, want ok", tc.frameLen, tc.sf, tc.bwKHz, tc.cr)
			}
			if math.Abs(got-tc.want) > 0.001 {
				t.Errorf("TimeOnAirMs(%d, %d, %v, %d) = %v, want %v", tc.frameLen, tc.sf, tc.bwKHz, tc.cr, got, tc.want)
			}
		})
	}
}

func TestTimeOnAirMsNotCostable(t *testing.T) {
	cases := []struct {
		name     string
		frameLen int
		sf       int
		bwKHz    float64
		cr       int
	}{
		{"sf zero", 50, 0, 62.5, 5},
		{"sf below range", 50, 6, 62.5, 5},
		{"sf above range", 50, 13, 62.5, 5},
		{"bw zero", 50, 7, 0, 5},
		{"cr below range", 50, 7, 62.5, 4},
		{"cr above range", 50, 7, 62.5, 9},
		{"empty frame", 0, 7, 62.5, 5},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := TimeOnAirMs(tc.frameLen, tc.sf, tc.bwKHz, tc.cr)
			if ok {
				t.Errorf("TimeOnAirMs(%d, %d, %v, %d) = %v, ok; want not costable", tc.frameLen, tc.sf, tc.bwKHz, tc.cr, got)
			}
			if got != 0 {
				t.Errorf("TimeOnAirMs(%d, %d, %v, %d) = %v, want 0 ms when not costable", tc.frameLen, tc.sf, tc.bwKHz, tc.cr, got)
			}
		})
	}
}
