// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"encoding/json"
	"strconv"
	"testing"
)

func TestObservationSignal(t *testing.T) {
	none := "-"
	for _, tc := range []struct {
		rssi, snr         string // raw JSON, "" for a field that was not sent
		wantRSSI, wantSNR string // "-" for not measured
	}{
		{"-108", "5.25", "-108", "5.25"},
		{`"-108"`, `"-7.25"`, "-108", "-7.25"},
		{"-107.5", "-0.0", "-107", "0"},
		{"-90", "0", "-90", "0"},   // a real 0 dB
		{"-90", `"0"`, "-90", "0"}, // quoted
		{"0", "0", none, none},     // how observers write "unknown"
		{"", "0", none, none},      // 0 with no RSSI is not a measurement
		{"null", "0", none, none},  //
		{"12", "0", none, none},    // nor beside an impossible RSSI
		{"", "3.5", none, "3.5"},   // a non-zero SNR stands alone
		{"0", "-4", none, "-4"},    // RSSI 0 is never a measurement
		{"-1", "31.75", "-1", "31.75"},
		{"-164", "-32", "-164", "-32"},
		{"-90", "32", "-90", "32"}, // 31.75 rounded by the observer
		{"-165", "32.25", none, none},
		{"1", "-32.25", none, none},
		{"-0.5", "4", none, "4"},
		{`"NaN"`, `"NaN"`, none, none},
		{`"Inf"`, `"-Inf"`, none, none},
		{`"1e999"`, `"1e999"`, none, none},
		{"32768", "51", none, none},
		{"true", "{}", none, none},
		{`""`, " null ", none, none},
		{"-90", "", "-90", none},
	} {
		rssi, snr := observationSignal([]byte(tc.rssi), []byte(tc.snr))
		gotRSSI, gotSNR := none, none
		if rssi != nil {
			gotRSSI = fmtNumber(float64(*rssi))
		}
		if snr != nil {
			gotSNR = fmtNumber(float64(*snr))
		}
		if gotRSSI != tc.wantRSSI || gotSNR != tc.wantSNR {
			t.Errorf("RSSI %q SNR %q: got %s / %s, want %s / %s", tc.rssi, tc.snr, gotRSSI, gotSNR, tc.wantRSSI, tc.wantSNR)
		}
	}
}

func fmtNumber(v float64) string {
	if v == 0 {
		return "0" // -0 too
	}
	return strconv.FormatFloat(v, 'f', -1, 32)
}

func snrTestValue(v float32) *float32 { return &v }

// The live event carries rssi and snr only when they were measured; a real 0 dB SNR is sent.
func TestLivePacketObservationSignal(t *testing.T) {
	for _, tc := range []struct {
		name, fields      string // extra envelope fields
		wantRSSI, wantSNR string // the event's JSON, "" for a field that is left out
	}{
		{"measured", `"RSSI":-97,"SNR":6.25`, "-97", "6.25"},
		{"zero SNR", `"RSSI":"-97","SNR":"0"`, "-97", "0"},
		{"not sent", ``, "", ""},
		{"unknown written as zeros", `"RSSI":0,"SNR":0`, "", ""},
		{"SNR missing", `"RSSI":-97`, "-97", ""},
		{"RSSI impossible", `"RSSI":20,"SNR":-3`, "", "-3"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := newRepeatHarness(t, false)
			r.db.observationInserted = true
			var env map[string]json.RawMessage
			if err := json.Unmarshal(packetEnvelope(t, buildGrpTxtPacket(t, 0x1a, make([]byte, 16))), &env); err != nil {
				t.Fatal(err)
			}
			var extra map[string]json.RawMessage
			if err := json.Unmarshal([]byte("{"+tc.fields+"}"), &extra); err != nil {
				t.Fatal(err)
			}
			for k, v := range extra {
				env[k] = v
			}
			raw, _ := json.Marshal(env)
			r.w.handlePacket(r.ctx, "YOW", "0102", raw)
			select {
			case evt := <-r.client.Send:
				var got struct {
					Observation map[string]json.RawMessage `json:"observation"`
				}
				if err := json.Unmarshal(evt.Payload, &got); err != nil || got.Observation == nil {
					t.Fatalf("event %s: %v", evt.Payload, err)
				}
				if rssi, snr := string(got.Observation["rssi"]), string(got.Observation["snr"]); rssi != tc.wantRSSI || snr != tc.wantSNR {
					t.Fatalf("rssi %q snr %q, want %q %q", rssi, snr, tc.wantRSSI, tc.wantSNR)
				}
			case <-r.ctx.Done():
				t.Fatal("no event")
			}
		})
	}
}
