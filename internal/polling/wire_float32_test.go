// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package polling

import (
	"strings"
	"testing"
)

func TestParseReadingsDropsSNRThatOverflowsStoredFloat32(t *testing.T) {
	for _, value := range []string{"1e100", "-1e100"} {
		t.Run(value, func(t *testing.T) {
			doc := string(readFile(t, "v2_status.json"))
			doc = strings.Replace(doc, `"snr":6.5`, `"snr":`+value, 1)
			doc = strings.Replace(doc, `"last_snr":1.25`, `"last_snr":`+value, 1)
			rows, err := ParseReadings([]byte(doc))
			if err != nil {
				t.Fatal(err)
			}
			if rows[0].PollSNR != nil || rows[0].Stats.LastSNR != nil {
				t.Fatal("a float64 value that overflows PostgreSQL real must not be stored")
			}
			if rows[0].BatteryMV == nil || *rows[0].BatteryMV != 4038 {
				t.Fatal("a bad SNR must not discard the rest of the reading")
			}
		})
	}
	for _, value := range []string{"0", "6.5", "-6.5"} {
		doc := strings.Replace(string(readFile(t, "v2_status.json")), `"snr":6.5`, `"snr":`+value, 1)
		rows, err := ParseReadings([]byte(doc))
		if err != nil || rows[0].PollSNR == nil {
			t.Fatalf("valid SNR %s was lost: %v", value, err)
		}
	}
}
