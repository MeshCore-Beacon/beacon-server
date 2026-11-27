// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"strings"
	"testing"
)

func TestParseReadingsClearsRepeatedIntegerFields(t *testing.T) {
	for _, missing := range []string{"null", "9223372036854775808"} {
		t.Run(missing, func(t *testing.T) {
			doc := string(readFile(t, "v2_status.json"))
			doc = strings.Replace(doc, `"battery_mv":4038`, `"battery_mv":4038,"battery_mv":`+missing, 1)
			doc = strings.Replace(doc, `"packets_recv":81234`, `"packets_recv":81234,"packets_recv":`+missing, 1)
			rows, err := ParseReadings([]byte(doc))
			if err != nil {
				t.Fatal(err)
			}
			if rows[0].BatteryMV != nil || rows[0].Stats.PacketsRecv != nil {
				t.Fatal("an explicitly absent/overflowed field retained its earlier value")
			}
		})
	}
	doc := strings.Replace(string(readFile(t, "v2_status.json")), `"packets_recv":81234`, `"packets_recv":81234,"packets_recv":0`, 1)
	rows, err := ParseReadings([]byte(doc))
	if err != nil || rows[0].Stats.PacketsRecv == nil || *rows[0].Stats.PacketsRecv != 0 {
		t.Fatalf("an explicit zero counter must stay present: %v", err)
	}
}

func TestParseClaimClearsRepeatedRequestSlots(t *testing.T) {
	doc := strings.Replace(string(readFile(t, "claim_extended.json")), `"request_slots": 3`, `"request_slots": 3, "request_slots": null`, 1)
	claim, err := ParseClaim([]byte(doc))
	if err != nil {
		t.Fatal(err)
	}
	if claim.RequestSlots != 0 {
		t.Fatalf("request slots = %d, want the absent-field default 0", claim.RequestSlots)
	}
}
