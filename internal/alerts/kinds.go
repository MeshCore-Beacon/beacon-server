// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package alerts holds the alert rules: which kinds exist and what they accept, the evaluation of a rule against
// fresh data, and the delivery of what it produces. The decision logic is pure.
package alerts

import (
	"fmt"
	"slices"
	"time"
)

// Alert kinds, in the order clients list them.
const (
	KindGoneQuiet      = "gone_quiet"
	KindBackOnline     = "back_online"
	KindClock          = "clock"
	KindBatteryLow     = "battery_low"
	KindBatteryFalling = "battery_falling"
	KindRebooted       = "rebooted"
	KindTemperature    = "temperature"
	KindNoiseHigh      = "noise_high"
	KindObserverOff    = "observer_offline"
	// KindRequestAttention is announced for the user's telemetry requests; it is not a stored rule.
	KindRequestAttention = "request_attention"
)

// Kinds is every stored rule kind in display order.
var Kinds = []string{KindGoneQuiet, KindBackOnline, KindClock, KindBatteryLow, KindBatteryFalling, KindRebooted,
	KindTemperature, KindNoiseHigh, KindObserverOff}

// Target kinds.
const (
	TargetNode     = "node"
	TargetObserver = "observer"
)

const (
	nodeTypeRepeater   = 2
	nodeTypeRoomServer = 3
)

// DataWindow is how far back readings are looked at (seconds). It decides which readings describe the radio, not
// whether the radio is polled.
const DataWindow = 14 * 24 * 3600

// ObserverFresh is how recent an observer's status telemetry must be for it to count as a current source.
const ObserverFresh = 24 * time.Hour

// Caps is what a radio is and what Beacon has recently seen it report.
type Caps struct {
	// Observer: the key has an observer row. HasNode: it has a node row (NodeType is then its type). A radio that
	// is both (an observer that also adverts) has the capabilities of both, with the observer's own data kept apart
	// in the Obs fields so each alert can read from the source that has its data.
	Observer bool
	HasNode  bool
	NodeType int16 // nodes only
	// Polled: the radio has a current source: a polling assignment (nodes) or fresh status telemetry (observers).
	// The data flags below only count while it does, so readings from before the source went do not offer a kind.
	Polled                              bool
	Battery, Temperature, Noise, Uptime bool
	ObsBattery, ObsNoise, ObsUptime     bool
}

func (c Caps) isNode() bool { return c.HasNode || c.NodeType != 0 }

func (c Caps) repeaterLike() bool {
	return c.Observer || c.NodeType == nodeTypeRepeater || c.NodeType == nodeTypeRoomServer
}

// Source is the kind of row a rule of the kind reads its data from. A radio with both an observer and a node row
// reads each alert where its data is: observer telemetry for battery, noise and uptime when the observer reports
// them, the node for temperature and the clock, the observer for its own status. Otherwise it is the requested kind.
func (c Caps) Source(kind, requested string) string {
	if !c.Observer || !c.isNode() {
		return requested
	}
	switch kind {
	case KindObserverOff:
		return TargetObserver
	case KindClock, KindTemperature:
		return TargetNode
	case KindBatteryLow, KindBatteryFalling:
		return pickSource(c.ObsBattery)
	case KindNoiseHigh:
		return pickSource(c.ObsNoise)
	case KindRebooted:
		return pickSource(c.ObsUptime)
	}
	return requested
}

func pickSource(observerHasIt bool) string {
	if observerHasIt {
		return TargetObserver
	}
	return TargetNode
}

// Reason codes: a stable snake_case code for each reason a kind can be unavailable, so clients can translate it.
const (
	CodeObserverNoData     = "observer_no_data"     // an observer does not report this value
	CodeRepeatersOnly      = "repeaters_only"       // only repeaters report this value
	CodeNotPolled          = "not_polled"           // Beacon is not polling this repeater
	CodeNoData             = "no_data"              // a polled repeater does not report this value
	CodeObserverNoClock    = "observer_no_clock"    // observers have no clock drift value
	CodeClockRepeatersOnly = "clock_repeaters_only" // only repeaters and room servers report a clock
	CodeNeedsGoneQuiet     = "needs_gone_quiet"     // back_online needs an enabled gone_quiet rule (rule errors only)
	CodeNotApplicable      = "not_applicable"       // the kind has nothing to do with this radio (rule errors only)
)

// ReasonCodes is every code the server can emit.
var ReasonCodes = []string{CodeObserverNoData, CodeRepeatersOnly, CodeNotPolled, CodeNoData, CodeObserverNoClock,
	CodeClockRepeatersOnly, CodeNeedsGoneQuiet, CodeNotApplicable}

// KindUnavailable says why a kind cannot be used on a radio. Code is the stable form of Reason.
type KindUnavailable struct{ Reason, Code string }

func (e *KindUnavailable) Error() string { return e.Reason }

// InvalidThreshold says what is wrong with a threshold.
type InvalidThreshold struct{ Message string }

func (e *InvalidThreshold) Error() string { return e.Message }

// Availability lists the kinds the radio can have, in display order, the reason for each that it cannot, and the
// stable code of each reason (same keys).
func Availability(c Caps) (available []string, unavailable, codes map[string]string) {
	unavailable, codes = map[string]string{}, map[string]string{}
	for _, k := range Kinds {
		ok, code, why, relevant := kindStatus(c, k)
		switch {
		case ok:
			available = append(available, k)
		case relevant:
			unavailable[k], codes[k] = why, code
		}
	}
	return available, unavailable, codes
}

// kindStatus: ok when the kind can be used; otherwise the reason code and text, and whether the radio's user should hear about it
// (a kind that has nothing to do with the radio is left out of both lists).
func kindStatus(c Caps, kind string) (ok bool, code, why string, relevant bool) {
	data := func(has bool, what string) (bool, string, string, bool) {
		switch {
		case has:
			return true, "", "", true
		case c.Observer && !c.isNode():
			return false, CodeObserverNoData, "This observer does not report " + what + ".", true
		case !c.repeaterLike():
			return false, CodeRepeatersOnly, "Only repeaters report " + what + ".", true
		case !c.Polled:
			return false, CodeNotPolled, "Beacon is not polling this repeater. Request telemetry for it first.", true
		default:
			return false, CodeNoData, "This repeater does not report " + what + ".", true
		}
	}
	switch kind {
	case KindGoneQuiet, KindBackOnline:
		return true, "", "", true
	case KindObserverOff:
		return c.Observer, "", "", c.Observer
	case KindClock:
		if c.Observer && !c.isNode() {
			return false, CodeObserverNoClock, "Observers have no clock drift value.", true
		}
		if c.NodeType != nodeTypeRepeater && c.NodeType != nodeTypeRoomServer {
			return false, CodeClockRepeatersOnly, "Only repeaters and room servers report their clock.", true
		}
		return true, "", "", true
	case KindBatteryLow, KindBatteryFalling:
		return data(c.Battery, "a battery level")
	case KindRebooted:
		return data(c.Uptime, "its uptime")
	case KindTemperature:
		return data(c.Temperature, "a temperature")
	case KindNoiseHigh:
		return data(c.Noise, "a noise floor")
	}
	return false, "", "", false
}

var (
	goneQuietHours     = []int{12, 24, 48}
	goneQuietFastHours = []int{3, 6, 12, 24, 48}
	fallingPerDay      = []int{100, 200, 300}
	clockSeconds       = []int{300, 3600, 86400}
	observerOffMinutes = []int{30, 60, 180, 360}
)

const (
	batteryMin, batteryMax = 2500, 5000
	noiseMin, noiseMax     = -130, -50
	tempMin, tempMax       = -500, 1500 // tenths of a degree C
)

func oneOf(v int, set []int, what string) error {
	if slices.Contains(set, v) {
		return nil
	}
	return &InvalidThreshold{Message: fmt.Sprintf("%s must be one of %v.", what, set)}
}

func between(v, lo, hi int, what string) error {
	if v >= lo && v <= hi {
		return nil
	}
	return &InvalidThreshold{Message: fmt.Sprintf("%s must be between %d and %d.", what, lo, hi)}
}

// ValidateRule checks a rule for the radio and returns the thresholds to store. A rule that is being turned off is
// only checked for shape, so a rule whose data went away can still be switched off. hasGoneQuiet says whether the
// radio has an enabled gone_quiet rule, which back_online needs.
func ValidateRule(c Caps, kind string, th, high *int, enabled, hasGoneQuiet bool) (*int, *int, error) {
	if !slices.Contains(Kinds, kind) {
		return nil, nil, &InvalidThreshold{Message: "Unknown alert kind."}
	}
	if enabled {
		if ok, code, why, _ := kindStatus(c, kind); !ok {
			if why == "" {
				code, why = CodeNotApplicable, "This alert does not apply to this radio."
			}
			return nil, nil, &KindUnavailable{Reason: why, Code: code}
		}
		if kind == KindBackOnline && !hasGoneQuiet {
			return nil, nil, &KindUnavailable{Reason: "Turn on the gone-quiet alert for this radio first.", Code: CodeNeedsGoneQuiet}
		}
	}
	none := func() error {
		if th != nil || high != nil {
			return &InvalidThreshold{Message: "This alert takes no threshold."}
		}
		return nil
	}
	single := func() (int, error) {
		if high != nil {
			return 0, &InvalidThreshold{Message: "This alert takes one threshold."}
		}
		if th == nil {
			return 0, &InvalidThreshold{Message: "A threshold is required."}
		}
		return *th, nil
	}
	var err error
	switch kind {
	case KindBackOnline, KindRebooted:
		err = none()
	case KindGoneQuiet:
		var v int
		if v, err = single(); err == nil {
			set := goneQuietHours
			if !enabled || c.Observer || c.Polled {
				set = goneQuietFastHours
			}
			err = oneOf(v, set, "Hours")
		}
	case KindBatteryLow:
		var v int
		if v, err = single(); err == nil {
			err = between(v, batteryMin, batteryMax, "Millivolts")
		}
	case KindBatteryFalling:
		var v int
		if v, err = single(); err == nil {
			err = oneOf(v, fallingPerDay, "Millivolts per day")
		}
	case KindNoiseHigh:
		var v int
		if v, err = single(); err == nil {
			err = between(v, noiseMin, noiseMax, "dBm")
		}
	case KindClock:
		var v int
		if v, err = single(); err == nil {
			err = oneOf(v, clockSeconds, "Seconds")
		}
	case KindObserverOff:
		var v int
		if v, err = single(); err == nil {
			err = oneOf(v, observerOffMinutes, "Minutes")
		}
	case KindTemperature:
		switch {
		case th == nil && high == nil:
			err = &InvalidThreshold{Message: "Set a low limit, a high limit or both."}
		default:
			for _, p := range []*int{th, high} {
				if p != nil {
					if err = between(*p, tempMin, tempMax, "Tenths of a degree"); err != nil {
						break
					}
				}
			}
			if err == nil && th != nil && high != nil && *th >= *high {
				err = &InvalidThreshold{Message: "The low limit must be below the high limit."}
			}
			// Recovery needs a degree of room inside each limit, so a narrower range could never recover.
			if err == nil && enabled && th != nil && high != nil && *high-*th < 2*tempRecoverTenth {
				err = &InvalidThreshold{Message: fmt.Sprintf("The range must be at least %d degrees wide.", 2*tempRecoverTenth/10)}
			}
		}
	}
	if err != nil {
		return nil, nil, err
	}
	return th, high, nil
}
