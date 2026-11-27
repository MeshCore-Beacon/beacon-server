// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import "testing"

func TestSentenceTemperatureUnit(t *testing.T) {
	// value is tenths of a degree C
	if got := Sentence(KindTemperature, "R1", 355, "", "c"); got != "R1 temperature is 35.5 C, outside your limits." {
		t.Fatal(got)
	}
	if got := Sentence(KindTemperature, "R1", 355, "", "f"); got != "R1 temperature is 95.9 F, outside your limits." {
		t.Fatal(got)
	}
	if got := Sentence(KindTemperature, "R1", -400, "", "f"); got != "R1 temperature is -40.0 F, outside your limits." {
		t.Fatal(got)
	}
	if got := Sentence(KindTemperature, "R1", 355, "", ""); got != "R1 temperature is 35.5 C, outside your limits." {
		t.Fatal(got)
	}
}
