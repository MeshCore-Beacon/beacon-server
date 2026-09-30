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

func TestStripNULs(t *testing.T) {
	fw := "v1.17.1-d929643\x00\x00\x00"
	name := "Canadaverse\x00 RemoteTerm"
	model := "Heltec Wireless Paper"
	got := stripNULs(map[string]*string{"firmware_version": &fw, "origin": &name, "model": &model})
	if fw != "v1.17.1-d929643" || name != "Canadaverse RemoteTerm" || model != "Heltec Wireless Paper" {
		t.Errorf("stripped to %q %q %q", fw, name, model)
	}
	if len(got) != 2 || got[0] != "firmware_version" || got[1] != "origin" {
		t.Errorf("reported fields %v, want [firmware_version origin]", got)
	}
}

func TestStripJSONNULs(t *testing.T) {
	clean := []byte(`{"origin":"a","stats":{"uptime_secs":12}}`)
	if out, changed := stripJSONNULs(clean); changed || string(out) != string(clean) {
		t.Errorf("clean payload rewritten: %s", out)
	}

	dirty := []byte(`{"firmware_version":"v1.17\u0000\u0000","extra":{"list":["x\u0000y"],"n":1.50}}`)
	out, changed := stripJSONNULs(dirty)
	if !changed {
		t.Fatal("expected payload to be cleaned")
	}
	var v struct {
		FirmwareVersion string `json:"firmware_version"`
		Extra           struct {
			List []string    `json:"list"`
			N    json.Number `json:"n"`
		} `json:"extra"`
	}
	if err := json.Unmarshal(out, &v); err != nil {
		t.Fatal(err)
	}
	if v.FirmwareVersion != "v1.17" || v.Extra.List[0] != "xy" || v.Extra.N != "1.50" {
		t.Errorf("cleaned to %s", out)
	}
}
