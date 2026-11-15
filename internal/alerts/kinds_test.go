// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"errors"
	"slices"
	"testing"
)

func ip(v int) *int { return &v }

var (
	polledRepeater = Caps{NodeType: 2, Polled: true, Battery: true, Noise: true, Uptime: true}
	plainRepeater  = Caps{NodeType: 2}
	companion      = Caps{NodeType: 1}
	observerCaps   = Caps{Observer: true, Polled: true, Battery: true, Noise: true, Uptime: true}
)

func TestAvailability(t *testing.T) {
	av, un := Availability(polledRepeater)
	if want := []string{"gone_quiet", "back_online", "clock", "battery_low", "battery_falling", "rebooted", "noise_high"}; !slices.Equal(av, want) {
		t.Fatalf("polled repeater: %v", av)
	}
	if un["temperature"] != "This repeater does not report a temperature." || len(un) != 1 {
		t.Fatalf("%v", un)
	}

	av, un = Availability(plainRepeater)
	if want := []string{"gone_quiet", "back_online", "clock"}; !slices.Equal(av, want) {
		t.Fatalf("unpolled repeater: %v", av)
	}
	for _, k := range []string{"battery_low", "battery_falling", "rebooted", "temperature", "noise_high"} {
		if un[k] == "" {
			t.Errorf("no reason for %s", k)
		}
	}

	av, un = Availability(companion)
	if want := []string{"gone_quiet", "back_online"}; !slices.Equal(av, want) || un["clock"] == "" || un["battery_low"] == "" {
		t.Fatalf("companion: %v %v", av, un)
	}

	av, un = Availability(observerCaps)
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
	_, un = Availability(polledRepeater)
	if _, ok := un["observer_offline"]; ok {
		t.Fatal("observer_offline listed for a node")
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
