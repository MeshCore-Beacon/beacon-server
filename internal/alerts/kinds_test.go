// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"errors"
	"slices"
	"testing"
	"time"
)

func ip(v int) *int { return &v }

var (
	polledRepeater = Caps{NodeType: 2, Polled: true, Battery: true, Noise: true, Uptime: true}
	plainRepeater  = Caps{NodeType: 2}
	companion      = Caps{NodeType: 1}
	observerCaps   = Caps{Observer: true, Polled: true, Battery: true, Noise: true, Uptime: true}
)

func TestAvailability(t *testing.T) {
	av, un, _ := Availability(polledRepeater)
	if want := []string{"gone_quiet", "back_online", "clock", "battery_low", "battery_falling", "rebooted", "noise_high"}; !slices.Equal(av, want) {
		t.Fatalf("polled repeater: %v", av)
	}
	if un["temperature"] != "This repeater does not report a temperature." || len(un) != 1 {
		t.Fatalf("%v", un)
	}

	av, un, _ = Availability(plainRepeater)
	if want := []string{"gone_quiet", "back_online", "clock"}; !slices.Equal(av, want) {
		t.Fatalf("unpolled repeater: %v", av)
	}
	for _, k := range []string{"battery_low", "battery_falling", "rebooted", "temperature", "noise_high"} {
		if un[k] == "" {
			t.Errorf("no reason for %s", k)
		}
	}

	av, un, _ = Availability(companion)
	if want := []string{"gone_quiet", "back_online"}; !slices.Equal(av, want) || un["clock"] == "" || un["battery_low"] == "" {
		t.Fatalf("companion: %v %v", av, un)
	}

	av, un, _ = Availability(observerCaps)
	if want := []string{"gone_quiet", "back_online", "battery_low", "battery_falling", "rebooted", "noise_high", "observer_offline"}; !slices.Equal(av, want) {
		t.Fatalf("observer: %v", av)
	}
	if un["temperature"] == "" || un["clock"] == "" {
		t.Fatalf("%v", un)
	}
	// observer_offline is for observers only and not mentioned for nodes.
	if _, ok := un["observer_offline"]; ok || slices.Contains(av, "observer_offline") != true {
		t.Fatal("observer_offline")
	}
	_, un, _ = Availability(polledRepeater)
	if _, ok := un["observer_offline"]; ok {
		t.Fatal("observer_offline listed for a node")
	}
}

func TestAvailabilityCodes(t *testing.T) {
	check := func(name string, c Caps, want map[string]string) {
		t.Helper()
		_, un, codes := Availability(c)
		if len(codes) != len(un) {
			t.Fatalf("%s: %d codes for %d reasons", name, len(codes), len(un))
		}
		for k, code := range want {
			if codes[k] != code {
				t.Errorf("%s %s: code %q, want %q", name, k, codes[k], code)
			}
		}
		for k := range un {
			if codes[k] == "" {
				t.Errorf("%s %s: no code", name, k)
			}
		}
	}
	check("polled", polledRepeater, map[string]string{"temperature": CodeNoData})
	check("plain", plainRepeater, map[string]string{"battery_low": CodeNotPolled, "noise_high": CodeNotPolled, "temperature": CodeNotPolled})
	check("companion", companion, map[string]string{"battery_low": CodeRepeatersOnly, "clock": CodeClockRepeatersOnly})
	check("observer", observerCaps, map[string]string{"temperature": CodeObserverNoData, "clock": CodeObserverNoClock})
	// Every code the server can emit is in the list clients are told about.
	for _, c := range []string{CodeNoData, CodeNotPolled, CodeRepeatersOnly, CodeClockRepeatersOnly, CodeObserverNoData,
		CodeObserverNoClock, CodeNeedsGoneQuiet, CodeNotApplicable} {
		if !slices.Contains(ReasonCodes, c) {
			t.Errorf("%s missing from ReasonCodes", c)
		}
	}
}

func TestValidateRuleReasonCodes(t *testing.T) {
	code := func(c Caps, kind string, gq bool) string {
		_, _, err := ValidateRule(c, kind, nil, nil, true, gq)
		var na *KindUnavailable
		if !errors.As(err, &na) {
			t.Fatalf("%s: %v", kind, err)
		}
		return na.Code
	}
	if got := code(plainRepeater, "back_online", false); got != CodeNeedsGoneQuiet {
		t.Errorf("back_online: %q", got)
	}
	if got := code(plainRepeater, "battery_low", false); got != CodeNotPolled {
		t.Errorf("battery_low: %q", got)
	}
	if got := code(polledRepeater, "observer_offline", false); got != CodeNotApplicable {
		t.Errorf("observer_offline: %q", got)
	}
}

func TestValidateRule(t *testing.T) {
	type tc struct {
		name         string
		caps         Caps
		kind         string
		th, high     *int
		enabled, gq  bool
		wantInvalid  bool
		wantNotAvail bool
	}
	on := func(name string, c Caps, kind string, th, high *int) tc {
		return tc{name: name, caps: c, kind: kind, th: th, high: high, enabled: true}
	}
	cases := []tc{
		on("gone quiet 24", plainRepeater, "gone_quiet", ip(24), nil),
		{name: "gone quiet 6 not polled", caps: plainRepeater, kind: "gone_quiet", th: ip(6), enabled: true, wantInvalid: true},
		on("gone quiet 6 polled", polledRepeater, "gone_quiet", ip(6), nil),
		on("gone quiet 3 observer", observerCaps, "gone_quiet", ip(3), nil),
		{name: "gone quiet 7", caps: polledRepeater, kind: "gone_quiet", th: ip(7), enabled: true, wantInvalid: true},
		{name: "gone quiet none", caps: polledRepeater, kind: "gone_quiet", enabled: true, wantInvalid: true},
		{name: "gone quiet high", caps: polledRepeater, kind: "gone_quiet", th: ip(24), high: ip(1), enabled: true, wantInvalid: true},
		on("battery 3500", polledRepeater, "battery_low", ip(3500), nil),
		{name: "battery 2499", caps: polledRepeater, kind: "battery_low", th: ip(2499), enabled: true, wantInvalid: true},
		{name: "battery 5001", caps: polledRepeater, kind: "battery_low", th: ip(5001), enabled: true, wantInvalid: true},
		on("battery 2500", polledRepeater, "battery_low", ip(2500), nil),
		on("falling 200", polledRepeater, "battery_falling", ip(200), nil),
		{name: "falling 150", caps: polledRepeater, kind: "battery_falling", th: ip(150), enabled: true, wantInvalid: true},
		on("rebooted", polledRepeater, "rebooted", nil, nil),
		{name: "rebooted with threshold", caps: polledRepeater, kind: "rebooted", th: ip(1), enabled: true, wantInvalid: true},
		on("noise -90", polledRepeater, "noise_high", ip(-90), nil),
		{name: "noise -131", caps: polledRepeater, kind: "noise_high", th: ip(-131), enabled: true, wantInvalid: true},
		{name: "noise -49", caps: polledRepeater, kind: "noise_high", th: ip(-49), enabled: true, wantInvalid: true},
		on("clock 3600", plainRepeater, "clock", ip(3600), nil),
		{name: "clock 60", caps: plainRepeater, kind: "clock", th: ip(60), enabled: true, wantInvalid: true},
		on("offline 60", observerCaps, "observer_offline", ip(60), nil),
		{name: "offline 45", caps: observerCaps, kind: "observer_offline", th: ip(45), enabled: true, wantInvalid: true},
		{name: "offline on a node", caps: polledRepeater, kind: "observer_offline", th: ip(60), enabled: true, wantNotAvail: true},
		{name: "clock on observer", caps: observerCaps, kind: "clock", th: ip(300), enabled: true, wantNotAvail: true},
		{name: "battery on companion", caps: companion, kind: "battery_low", th: ip(3500), enabled: true, wantNotAvail: true},
		{name: "temperature no data", caps: polledRepeater, kind: "temperature", th: ip(0), enabled: true, wantNotAvail: true},
		{name: "back online without gone quiet", caps: plainRepeater, kind: "back_online", enabled: true, wantNotAvail: true},
		{name: "back online with gone quiet", caps: plainRepeater, kind: "back_online", enabled: true, gq: true},
		{name: "unknown kind", caps: plainRepeater, kind: "nope", enabled: true, wantInvalid: true},
		// Turning a rule off needs no data and no sibling.
		{name: "off, data gone", caps: plainRepeater, kind: "battery_low", th: ip(3500), enabled: false},
		{name: "off, bad value", caps: plainRepeater, kind: "battery_low", th: ip(10), enabled: false, wantInvalid: true},
		{name: "back online off", caps: plainRepeater, kind: "back_online", enabled: false},
	}
	temp := Caps{NodeType: 2, Polled: true, Temperature: true}
	cases = append(cases,
		on("temp both", temp, "temperature", ip(0), ip(400)),
		on("temp low only", temp, "temperature", ip(50), nil),
		on("temp high only", temp, "temperature", nil, ip(400)),
		tc{name: "temp neither", caps: temp, kind: "temperature", enabled: true, wantInvalid: true},
		tc{name: "temp inverted", caps: temp, kind: "temperature", th: ip(400), high: ip(0), enabled: true, wantInvalid: true},
		tc{name: "temp out of range", caps: temp, kind: "temperature", th: ip(-501), enabled: true, wantInvalid: true},
	)
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			th, high, err := ValidateRule(c.caps, c.kind, c.th, c.high, c.enabled, c.gq)
			var inv *InvalidThreshold
			var na *KindUnavailable
			switch {
			case c.wantInvalid:
				if !errors.As(err, &inv) {
					t.Fatalf("want invalid threshold, got %v", err)
				}
			case c.wantNotAvail:
				if !errors.As(err, &na) || na.Reason == "" {
					t.Fatalf("want unavailable, got %v", err)
				}
			default:
				if err != nil || th != c.th || high != c.high {
					t.Fatalf("%v", err)
				}
			}
		})
	}
}

func TestCapsOfAnObserverThatIsAlsoANode(t *testing.T) {
	c := Caps{Observer: true, HasNode: true, NodeType: nodeTypeRepeater, Polled: true,
		Battery: true, Noise: true, Uptime: true, ObsBattery: true, ObsNoise: true, ObsUptime: true}
	avail, un, _ := Availability(c)
	for _, k := range []string{KindObserverOff, KindClock, KindBatteryLow, KindRebooted, KindNoiseHigh} {
		if !slices.Contains(avail, k) {
			t.Errorf("%s missing: %v %v", k, avail, un)
		}
	}
	for kind, want := range map[string]string{
		KindObserverOff: TargetObserver, KindClock: TargetNode, KindTemperature: TargetNode, KindBatteryLow: TargetObserver,
		KindNoiseHigh: TargetObserver, KindRebooted: TargetObserver, KindGoneQuiet: TargetNode,
	} {
		if got := c.Source(kind, TargetNode); got != want {
			t.Errorf("%s reads from %s, want %s", kind, got, want)
		}
	}
	c.ObsBattery = false // only the node reports a battery
	if got := c.Source(KindBatteryLow, TargetObserver); got != TargetNode {
		t.Errorf("battery from %s", got)
	}
	// A plain node or observer reads as asked.
	if got := (Caps{NodeType: nodeTypeRepeater}).Source(KindObserverOff, TargetNode); got != TargetNode {
		t.Errorf("plain node: %s", got)
	}
}

// A range narrower than twice the one degree of hysteresis has no temperature a firing rule could recover at.
func TestTemperatureRangeNeedsRoomToRecover(t *testing.T) {
	c := Caps{NodeType: nodeTypeRepeater, Polled: true, Temperature: true}
	for _, tc := range []struct {
		low, high int
		ok        bool
	}{{0, 10, false}, {0, 19, false}, {0, 20, true}, {-50, 400, true}, {100, 100, false}} {
		_, _, err := ValidateRule(c, KindTemperature, ip(tc.low), ip(tc.high), true, false)
		var bad *InvalidThreshold
		if tc.ok != (err == nil) || (!tc.ok && !errors.As(err, &bad)) {
			t.Errorf("%d to %d: %v", tc.low, tc.high, err)
		}
	}
	// One limit alone has no width, and a rule being turned off is not held to it.
	if _, _, err := ValidateRule(c, KindTemperature, ip(0), nil, true, false); err != nil {
		t.Errorf("low only: %v", err)
	}
	if _, _, err := ValidateRule(c, KindTemperature, ip(0), ip(10), false, false); err != nil {
		t.Errorf("off: %v", err)
	}
	// Every range that is accepted can recover: some reading satisfies the recovery condition.
	o := Evaluate(Rule{Kind: KindTemperature, Threshold: ip(0), High: ip(20), State: StateFiring},
		live(temp(time.Hour, 10), temp(2*time.Hour, -5)).withNow())
	if o.State != StateOK {
		t.Errorf("narrowest range did not recover: %+v", o)
	}
}
