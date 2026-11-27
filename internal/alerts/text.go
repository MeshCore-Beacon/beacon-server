// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"fmt"
	"time"
)

// Notice texts hold only the radio's sanitised name, a figure and fixed words: nothing secret, nothing a radio can
// shape beyond its capped name.

// Sentence is the one-sentence description of an alert, starting with the radio's name. tempUnit "f" reads
// temperatures in Fahrenheit; anything else is Celsius.
func Sentence(kind, name string, value int64, detail, tempUnit string) string {
	switch kind {
	case KindGoneQuiet:
		return fmt.Sprintf("%s has not been heard for %s.", name, plural(value, "hour"))
	case KindBackOnline:
		return fmt.Sprintf("%s is back online.", name)
	case KindBatteryLow:
		return fmt.Sprintf("%s battery is low: %.2f V.", name, float64(value)/1000)
	case KindBatteryFalling:
		return fmt.Sprintf("%s battery is falling by about %.2f V a day.", name, float64(value)/1000)
	case KindRebooted:
		return fmt.Sprintf("%s rebooted.", name)
	case KindTemperature:
		if tempUnit == "f" {
			return fmt.Sprintf("%s temperature is %.1f F, outside your limits.", name, float64(value)/10*9/5+32)
		}
		return fmt.Sprintf("%s temperature is %.1f C, outside your limits.", name, float64(value)/10)
	case KindNoiseHigh:
		return fmt.Sprintf("%s noise floor is high: %d dBm.", name, value)
	case KindClock:
		return fmt.Sprintf("%s clock is off by %s.", name, duration(time.Duration(value)*time.Second))
	case KindObserverOff:
		return fmt.Sprintf("%s has not reported for %s.", name, plural(value, "minute"))
	case KindRequestAttention:
		switch detail {
		case "login_failed":
			return fmt.Sprintf("Telemetry request for %s: the repeater did not accept the guest password.", name)
		case "admin_stopped":
			return fmt.Sprintf("Telemetry request for %s: the password gave admin access and was deleted. Enter a guest password.", name)
		case "disabled":
			return fmt.Sprintf("Telemetry request for %s was turned off: no observer has heard the repeater for days.", name)
		}
		return fmt.Sprintf("Telemetry request for %s needs your attention.", name)
	}
	return fmt.Sprintf("%s needs your attention.", name)
}

func plural(n int64, unit string) string {
	if n == 1 {
		return fmt.Sprintf("1 %s", unit)
	}
	return fmt.Sprintf("%d %ss", n, unit)
}

func duration(d time.Duration) string {
	switch {
	case d >= 24*time.Hour:
		return plural(int64(d/(24*time.Hour)), "day")
	case d >= time.Hour:
		return plural(int64(d/time.Hour), "hour")
	}
	return plural(int64(d/time.Minute), "minute")
}
