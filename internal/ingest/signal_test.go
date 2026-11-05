// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package ingest

import "testing"

func TestSignalValidityPreservesMeasuredZero(t *testing.T) {
	for _, raw := range []string{"", "null", " null ", `""`, `"NaN"`, `"Inf"`, `"-Inf"`, `"1e999"`, `true`, `{}`, `51`, `-51`} {
		if v := signalSNR([]byte(raw)); v != nil {
			t.Errorf("SNR %q became %v", raw, *v)
		}
	}
	for _, raw := range []string{"0", `"0"`, "-0.0"} {
		if v := signalSNR([]byte(raw)); v == nil || *v != 0 {
			t.Errorf("SNR zero %q lost", raw)
		}
		if v := signalRSSI([]byte(raw)); v == nil || *v != 0 {
			t.Errorf("RSSI zero %q lost", raw)
		}
	}
	if v := signalSNR([]byte(`"-7.25"`)); v == nil || *v != -7.25 {
		t.Fatal("quoted SNR lost")
	}
	if v := signalRSSI([]byte(`"-108"`)); v == nil || *v != -108 {
		t.Fatal("quoted RSSI lost")
	}
	for _, raw := range []string{`1`, `-201`, `"NaN"`, `"32768"`, `null`} {
		if v := signalRSSI([]byte(raw)); v != nil {
			t.Errorf("RSSI %q became %v", raw, *v)
		}
	}
}

func snrTestValue(v float32) *float32 { return &v }
