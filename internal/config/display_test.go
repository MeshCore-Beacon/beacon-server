// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"strings"
	"testing"
)

func TestDisplayResolve(t *testing.T) {
	got, err := DisplayConfig{}.Resolve()
	if err != nil || got != (ResolvedDisplay{"c", "metric"}) {
		t.Errorf("defaults: %+v, %v", got, err)
	}
	got, err = DisplayConfig{TemperatureUnit: " F ", DistanceUnit: "Imperial"}.Resolve()
	if err != nil || got != (ResolvedDisplay{"f", "imperial"}) {
		t.Errorf("normalized: %+v, %v", got, err)
	}
	if _, err = (DisplayConfig{TemperatureUnit: "kelvin"}).Resolve(); err == nil || !strings.Contains(err.Error(), "display.temperature_unit must be c or f") {
		t.Errorf("kelvin: %v", err)
	}
	if _, err = (DisplayConfig{DistanceUnit: "miles"}).Resolve(); err == nil || !strings.Contains(err.Error(), "display.distance_unit must be metric or imperial") {
		t.Errorf("miles: %v", err)
	}
}
