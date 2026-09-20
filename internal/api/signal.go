// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

// SignalStats describes a materialized reception snapshot in [Since, Until).
// Since and Until are the effective UTC-hour boundaries, rounded down from input.
// A reception is a stored observation, not a unique packet or a lost-packet estimate.
type SignalStats struct {
	Since      int64        `json:"since"`
	Until      int64        `json:"until"`
	Receptions int64        `json:"receptions"`
	SNR        SignalMetric `json:"snr"`
	RSSI       SignalMetric `json:"rssi"`
	Hourly     []SignalHour `json:"hourly"`
}

// SignalMetric counts available finite samples; Average is null without samples.
type SignalMetric struct {
	Samples   int64       `json:"samples"`
	Average   *float64    `json:"average"`
	Histogram []SignalBin `json:"histogram"`
}

// SignalBin includes Lower and excludes Upper. Null denotes an unbounded end.
// SNR uses 5 dB bins from -30 to 30; RSSI uses 10 dBm bins from -140 to 0,
// both with underflow and overflow bins. These are display bins, not quality ratings.
type SignalBin struct {
	Lower *float64 `json:"lower"`
	Upper *float64 `json:"upper"`
	Count int64    `json:"count"`
}

// SignalHour is a complete UTC bucket in the effective window. Missing hours are omitted.
type SignalHour struct {
	Hour        int64    `json:"hour"`
	Receptions  int64    `json:"receptions"`
	SNRSamples  int64    `json:"snrSamples"`
	SNRAverage  *float64 `json:"snrAverage"`
	RSSISamples int64    `json:"rssiSamples"`
	RSSIAverage *float64 `json:"rssiAverage"`
}
