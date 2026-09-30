// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"encoding/json"
	"testing"
)

func TestStatusStatsUsable(t *testing.T) {
	cases := []struct {
		name string
		raw  string
		want bool
	}{
		{"full stats", `{"uptime_secs":1093083,"noise_floor":-96,"tx_air_secs":7935,"rx_air_secs":47484,"recv_errors":93689}`, true},
		{"missing stats", `{}`, false},
		{"uptime only", `{"uptime_secs":1096701,"noise_floor":0,"tx_air_secs":0,"rx_air_secs":0,"recv_errors":0}`, false},
		{"uptime and recv errors only", `{"uptime_secs":3834495,"recv_errors":325529}`, false},
		{"fresh boot, no airtime yet", `{"uptime_secs":60,"noise_floor":-110,"tx_air_secs":0,"rx_air_secs":0}`, true},
		{"no noise floor reported", `{"uptime_secs":5000,"tx_air_secs":3,"rx_air_secs":40}`, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var s statusStats
			if err := json.Unmarshal([]byte(c.raw), &s); err != nil {
				t.Fatal(err)
			}
			if got := s.usable(); got != c.want {
				t.Errorf("usable() = %v, want %v", got, c.want)
			}
		})
	}
}
