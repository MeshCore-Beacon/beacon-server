// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"testing"
	"time"
)

var now = time.Date(2026, 11, 15, 12, 0, 0, 0, time.UTC)

func ago(d time.Duration) time.Time { return now.Add(-d) }

func bat(at time.Duration, mv int) Reading   { return Reading{At: ago(at), BatteryMV: &mv} }
func noise(at time.Duration, db int) Reading { return Reading{At: ago(at), NoiseDBm: &db} }
func temp(at time.Duration, t int) Reading   { return Reading{At: ago(at), TempTenths: &t} }
func up(at time.Duration, s int64) Reading   { return Reading{At: ago(at), Uptime: &s} }

func live(rs ...Reading) Data {
	return Data{Now: now, Exists: true, Polled: true, Readings: rs, Uptimes: rs}
}

type step struct {
	name   string
	rule   Rule
	data   Data
	state  State
	notify bool
	value  int64
}

func run(t *testing.T, steps []step) {
	t.Helper()
	for _, s := range steps {
		t.Run(s.name, func(t *testing.T) {
			if s.data.Now.IsZero() {
				s.data.Now = now
			}
			got := Evaluate(s.rule, s.data)
			if got.State != s.state || got.Notify != s.notify || (s.notify && got.Value != s.value) {
				t.Fatalf("state %s notify %v value %d; want %s %v %d", got.State, got.Notify, got.Value, s.state, s.notify, s.value)
			}
		})
	}
}

func TestEvaluateGoneQuiet(t *testing.T) {
	r := func(st State) Rule { return Rule{Kind: KindGoneQuiet, Threshold: ip(24), State: st} }
	d := func(quiet time.Duration) Data { return Data{Exists: true, LastHeard: ago(quiet)} }
	run(t, []step{
		{"no data: row missing", r(StateOK), Data{}, StateInactive, false, 0},
		{"no data: never heard", r(StateOK), Data{Exists: true}, StateInactive, false, 0},
		{"heard recently", r(StateUnknown), d(time.Hour), StateOK, false, 0},
		{"just under", r(StateOK), d(24*time.Hour - time.Minute), StateOK, false, 0},
		{"fires", r(StateOK), d(25 * time.Hour), StateFiring, true, 25},
		{"fires from unknown", r(StateUnknown), d(30 * time.Hour), StateFiring, true, 30},
		{"fires after inactive", r(StateInactive), d(30 * time.Hour), StateFiring, true, 30},
		{"does not refire while firing", r(StateFiring), d(30 * time.Hour), StateFiring, false, 0},
		{"recovers when heard", r(StateFiring), d(time.Minute), StateOK, false, 0},
		{"six hour limit", Rule{Kind: KindGoneQuiet, Threshold: ip(24), State: StateOK, LastSentAt: ago(5 * time.Hour)}, d(30 * time.Hour), StateFiring, false, 0},
		{"limit over", Rule{Kind: KindGoneQuiet, Threshold: ip(24), State: StateOK, LastSentAt: ago(7 * time.Hour)}, d(30 * time.Hour), StateFiring, true, 30},
	})
	if o := Evaluate(r(StateFiring), d(time.Minute).withNow()); !o.Recovered || o.State != StateOK {
		t.Fatalf("recovery not flagged: %+v", o)
	}
	if o := Evaluate(r(StateOK), d(time.Minute).withNow()); o.Recovered {
		t.Fatal("recovery flagged without firing")
	}
}

func (d Data) withNow() Data { d.Now = now; return d }

func TestEvaluateObserverOffline(t *testing.T) {
	r := func(st State) Rule { return Rule{Kind: KindObserverOff, Threshold: ip(60), State: st} }
	d := func(quiet time.Duration) Data { return Data{Exists: true, StatusAt: ago(quiet)} }
	run(t, []step{
		{"never reported", r(StateOK), Data{Exists: true}, StateInactive, false, 0},
		{"reporting", r(StateUnknown), d(5 * time.Minute), StateOK, false, 0},
		{"fires", r(StateOK), d(2 * time.Hour), StateFiring, true, 120},
		{"no refire", r(StateFiring), d(3 * time.Hour), StateFiring, false, 0},
		{"recovers", r(StateFiring), d(time.Minute), StateOK, false, 0},
		{"limit", Rule{Kind: KindObserverOff, Threshold: ip(60), State: StateOK, LastSentAt: ago(time.Hour)}, d(2 * time.Hour), StateFiring, false, 0},
	})
}

func TestEvaluateBatteryLow(t *testing.T) {
	r := func(st State) Rule { return Rule{Kind: KindBatteryLow, Threshold: ip(3500), State: st} }
	run(t, []step{
		{"source gone", r(StateOK), Data{Now: now, Exists: true}, StateInactive, false, 0},
		{"row gone", r(StateOK), Data{Now: now, Polled: true}, StateInactive, false, 0},
		{"one reading", r(StateUnknown), live(bat(time.Hour, 3000)), StateUnknown, false, 0},
		{"inactive with one reading is unknown", r(StateInactive), live(bat(time.Hour, 3000)), StateUnknown, false, 0},
		{"two above", r(StateUnknown), live(bat(time.Hour, 3800), bat(2*time.Hour, 3800)), StateOK, false, 0},
		{"only the newest below", r(StateOK), live(bat(time.Hour, 3400), bat(2*time.Hour, 3800)), StateOK, false, 0},
		{"two below fires", r(StateOK), live(bat(time.Hour, 3400), bat(2*time.Hour, 3450)), StateFiring, true, 3400},
		{"no refire", r(StateFiring), live(bat(time.Hour, 3300), bat(2*time.Hour, 3350)), StateFiring, false, 0},
		{"inside the hysteresis band stays firing", r(StateFiring), live(bat(time.Hour, 3550), bat(2*time.Hour, 3300)), StateFiring, false, 0},
		{"at the band edge stays firing", r(StateFiring), live(bat(time.Hour, 3600), bat(2*time.Hour, 3300)), StateFiring, false, 0},
		{"recovers above threshold plus 100", r(StateFiring), live(bat(time.Hour, 3601), bat(2*time.Hour, 3300)), StateOK, false, 0},
		{"ignores readings without a battery", r(StateOK), live(noise(time.Hour, -100), bat(2*time.Hour, 3400), bat(3*time.Hour, 3400)), StateFiring, true, 3400},
		{"limit", Rule{Kind: KindBatteryLow, Threshold: ip(3500), State: StateOK, LastSentAt: ago(time.Hour)}, live(bat(time.Hour, 3400), bat(2*time.Hour, 3400)), StateFiring, false, 0},
	})
}

func TestEvaluateBatteryFalling(t *testing.T) {
	r := func(st State) Rule { return Rule{Kind: KindBatteryFalling, Threshold: ip(200), State: st} }
	with := func(newest, dayAgo int, gap time.Duration) Data {
		d := live(bat(time.Hour, newest))
		b := bat(time.Hour+gap, dayAgo)
		d.DayAgo = &b
		return d
	}
	run(t, []step{
		{"source gone", r(StateOK), Data{Now: now, Exists: true}, StateInactive, false, 0},
		{"no baseline yet", r(StateUnknown), live(bat(time.Hour, 3500)), StateUnknown, false, 0},
		{"steady", r(StateUnknown), with(3900, 3950, 24*time.Hour), StateOK, false, 0},
		{"exactly the limit", r(StateOK), with(3800, 4000, 24*time.Hour), StateOK, false, 0},
		{"falling fast fires", r(StateOK), with(3700, 4000, 24*time.Hour), StateFiring, true, 300},
		{"rate is per day (shorter gap)", r(StateOK), with(3800, 4000, 12*time.Hour), StateFiring, true, 400},
		{"rate is per day (longer gap)", r(StateOK), with(3850, 4000, 27*time.Hour), StateOK, false, 0},
		{"no refire", r(StateFiring), with(3600, 4000, 24*time.Hour), StateFiring, false, 0},
		{"recovers", r(StateFiring), with(3950, 4000, 24*time.Hour), StateOK, false, 0},
		{"charging is not falling", r(StateOK), with(4100, 3800, 24*time.Hour), StateOK, false, 0},
	})
}

func TestEvaluateRebooted(t *testing.T) {
	r := func(st State, cursor time.Duration) Rule {
		return Rule{Kind: KindRebooted, State: st, Cursor: ago(cursor)}
	}
	run(t, []step{
		{"source gone", r(StateOK, time.Hour), Data{Now: now, Exists: true}, StateInactive, false, 0},
		{"not enough data", r(StateUnknown, time.Hour), live(up(30*time.Minute, 100)), StateUnknown, false, 0},
		{"uptime grew", r(StateUnknown, time.Hour), live(up(30*time.Minute, 2000), up(90*time.Minute, 1000)), StateOK, false, 0},
		{"reboot fires", r(StateOK, time.Hour), live(up(30*time.Minute, 50), up(90*time.Minute, 9000)), StateFiring, true, 50},
		{"a second reboot notifies again", r(StateFiring, time.Hour), live(up(30*time.Minute, 50), up(90*time.Minute, 9000)), StateFiring, true, 50},
		{"already judged", r(StateFiring, 10*time.Minute), live(up(30*time.Minute, 50), up(90*time.Minute, 9000)), StateFiring, false, 0},
		{"a reboot before the rule existed is history", r(StateUnknown, 10*time.Minute), live(up(30*time.Minute, 50), up(90*time.Minute, 9000)), StateUnknown, false, 0},
		{"next reading clears it", r(StateFiring, time.Hour), live(up(10*time.Minute, 700), up(30*time.Minute, 50)), StateOK, false, 0},
		{"limit", Rule{Kind: KindRebooted, State: StateOK, Cursor: ago(time.Hour), LastSentAt: ago(time.Hour)}, live(up(30*time.Minute, 50), up(90*time.Minute, 9000)), StateFiring, false, 0},
	})
	o := Evaluate(r(StateOK, time.Hour), live(up(30*time.Minute, 50), up(90*time.Minute, 9000)).withNow())
	if !o.Cursor.Equal(ago(30 * time.Minute)) {
		t.Fatalf("cursor %v", o.Cursor)
	}
}

// A reboot between two passes is seen even when later readings show a steady uptime again.
func TestEvaluateRebootedBetweenPasses(t *testing.T) {
	r := Rule{Kind: KindRebooted, State: StateOK, Cursor: ago(5 * time.Hour)}
	// Newest first: 20, 10, then the 10000 from before the reboot.
	d := live(up(time.Hour, 20), up(2*time.Hour, 10), up(3*time.Hour, 10000))
	o := Evaluate(r, d.withNow())
	if !o.Notify || o.State != StateFiring || o.Value != 10 || !o.Cursor.Equal(ago(time.Hour)) {
		t.Fatalf("%+v", o)
	}
	// The baseline may be older than the cursor: the reading after it is what is new.
	r.Cursor = ago(150 * time.Minute)
	if o := Evaluate(r, d.withNow()); !o.Notify || o.Value != 10 {
		t.Fatalf("reboot right after the cursor: %+v", o)
	}
	// Nothing new since the cursor.
	r.Cursor = ago(time.Hour)
	if o := Evaluate(r, d.withNow()); o.Notify {
		t.Fatalf("judged again: %+v", o)
	}
	// Several unseen readings, no drop: no reboot.
	r.Cursor = ago(5 * time.Hour)
	if o := Evaluate(r, live(up(time.Hour, 3000), up(2*time.Hour, 2000), up(3*time.Hour, 1000)).withNow()); o.Notify || o.State != StateOK {
		t.Fatalf("steady uptime: %+v", o)
	}
}

func TestEvaluateTemperature(t *testing.T) {
	both := func(st State) Rule { return Rule{Kind: KindTemperature, Threshold: ip(0), High: ip(400), State: st} }
	highOnly := func(st State) Rule { return Rule{Kind: KindTemperature, High: ip(400), State: st} }
	run(t, []step{
		{"source gone", both(StateOK), Data{Now: now, Exists: true}, StateInactive, false, 0},
		{"one reading", both(StateUnknown), live(temp(time.Hour, 500)), StateUnknown, false, 0},
		{"in range", both(StateUnknown), live(temp(time.Hour, 200), temp(2*time.Hour, 210)), StateOK, false, 0},
		{"one outside", both(StateOK), live(temp(time.Hour, 450), temp(2*time.Hour, 300)), StateOK, false, 0},
		{"two above", both(StateOK), live(temp(time.Hour, 450), temp(2*time.Hour, 430)), StateFiring, true, 450},
		{"two below", both(StateOK), live(temp(time.Hour, -20), temp(2*time.Hour, -10)), StateFiring, true, -20},
		{"high only, low value is fine", highOnly(StateOK), live(temp(time.Hour, -200), temp(2*time.Hour, -300)), StateOK, false, 0},
		{"no refire", both(StateFiring), live(temp(time.Hour, 450), temp(2*time.Hour, 450)), StateFiring, false, 0},
		{"within a degree of the limit stays firing", both(StateFiring), live(temp(time.Hour, 395), temp(2*time.Hour, 450)), StateFiring, false, 0},
		{"a degree inside recovers", both(StateFiring), live(temp(time.Hour, 390), temp(2*time.Hour, 450)), StateOK, false, 0},
		{"a degree inside the low limit recovers", both(StateFiring), live(temp(time.Hour, 10), temp(2*time.Hour, -50)), StateOK, false, 0},
		{"limit", Rule{Kind: KindTemperature, High: ip(400), State: StateOK, LastSentAt: ago(time.Hour)}, live(temp(time.Hour, 450), temp(2*time.Hour, 450)), StateFiring, false, 0},
	})
}

func TestEvaluateNoiseHigh(t *testing.T) {
	r := func(st State) Rule { return Rule{Kind: KindNoiseHigh, Threshold: ip(-90), State: st} }
	run(t, []step{
		{"source gone", r(StateOK), Data{Now: now, Exists: true}, StateInactive, false, 0},
		{"one reading", r(StateUnknown), live(noise(time.Hour, -80)), StateUnknown, false, 0},
		{"quiet", r(StateUnknown), live(noise(time.Hour, -100), noise(2*time.Hour, -95)), StateOK, false, 0},
		{"one loud", r(StateOK), live(noise(time.Hour, -80), noise(2*time.Hour, -100)), StateOK, false, 0},
		{"two loud", r(StateOK), live(noise(time.Hour, -80), noise(2*time.Hour, -85)), StateFiring, true, -80},
		{"no refire", r(StateFiring), live(noise(time.Hour, -80), noise(2*time.Hour, -85)), StateFiring, false, 0},
		{"within 3 dB stays firing", r(StateFiring), live(noise(time.Hour, -92), noise(2*time.Hour, -80)), StateFiring, false, 0},
		{"3 dB below recovers", r(StateFiring), live(noise(time.Hour, -93), noise(2*time.Hour, -80)), StateOK, false, 0},
		{"limit", Rule{Kind: KindNoiseHigh, Threshold: ip(-90), State: StateOK, LastSentAt: ago(time.Hour)}, live(noise(time.Hour, -80), noise(2*time.Hour, -85)), StateFiring, false, 0},
	})
}

func TestEvaluateClock(t *testing.T) {
	r := func(st State, cursor time.Duration) Rule {
		return Rule{Kind: KindClock, Threshold: ip(300), State: st, Cursor: ago(cursor)}
	}
	// drift is the newest advert's drift, prev the one before it (0 means a good one, nil none).
	drift := func(s int64, prev *int64, at time.Duration) Data {
		d := Data{Now: now, Exists: true, Drifts: []Drift{{At: ago(at), Sec: s}}}
		if prev != nil {
			d.Drifts = append([]Drift{{At: ago(at + time.Minute), Sec: *prev}}, d.Drifts...)
		}
		return d
	}
	v := func(s int64) *int64 { return &s }
	run(t, []step{
		{"no advert", r(StateOK, time.Hour), Data{Now: now, Exists: true}, StateInactive, false, 0},
		{"row gone", r(StateOK, time.Hour), Data{Now: now}, StateInactive, false, 0},
		{"same advert again", r(StateOK, 10*time.Minute), drift(900, v(900), 10*time.Minute), StateOK, false, 0},
		{"first advert waits", r(StateOK, time.Hour), drift(900, nil, 10*time.Minute), StateOK, false, 0},
		{"high after a good one waits", r(StateOK, time.Hour), drift(900, v(10), 10*time.Minute), StateOK, false, 0},
		{"two in a row fires", r(StateOK, time.Hour), drift(-900, v(900), 10*time.Minute), StateFiring, true, 900},
		{"no refire", r(StateFiring, time.Hour), drift(900, v(900), 10*time.Minute), StateFiring, false, 0},
		{"one good advert recovers", r(StateFiring, time.Hour), drift(10, v(900), 10*time.Minute), StateOK, false, 0},
		{"limit", Rule{Kind: KindClock, Threshold: ip(300), State: StateOK, Cursor: ago(time.Hour), LastSentAt: ago(time.Hour)}, drift(900, v(900), 10*time.Minute), StateFiring, false, 0},
	})
	o := Evaluate(r(StateOK, time.Hour), drift(900, v(10), 10*time.Minute))
	if !o.Cursor.Equal(ago(10*time.Minute)) || o.Streak != 0 {
		t.Fatalf("streak %d cursor %v", o.Streak, o.Cursor)
	}
}

// Two adverts past the limit that recovered before the pass still fire once, and are not announced again.
func TestEvaluateClockBreachRecoveredBetweenPasses(t *testing.T) {
	d := Data{Now: now, Exists: true, Drifts: []Drift{{ago(5 * time.Hour), 0}, {ago(4 * time.Hour), 900}, {ago(3 * time.Hour), -900}, {ago(2 * time.Hour), 10}}}
	r := Rule{Kind: KindClock, Threshold: ip(300), State: StateOK, Cursor: ago(5 * time.Hour)}
	o := Evaluate(r, d)
	if !o.Notify || o.State != StateOK || o.Value != 900 || !o.Cursor.Equal(ago(2*time.Hour)) {
		t.Fatalf("%+v", o)
	}
	// Already firing: the recovery is silent.
	r.State = StateFiring
	if o := Evaluate(r, d); o.Notify || o.State != StateOK {
		t.Fatalf("firing rule: %+v", o)
	}
	// The next pass finds nothing new.
	r.State, r.Cursor = StateOK, ago(2*time.Hour)
	if o := Evaluate(r, d); o.Notify {
		t.Fatalf("announced twice: %+v", o)
	}
}

func TestEvaluateBackOnlinePairing(t *testing.T) {
	r := func(cursor time.Duration, sent time.Time) Rule {
		return Rule{Kind: KindBackOnline, State: StateOK, Cursor: ago(cursor), LastSentAt: sent}
	}
	// The sibling recovered after the rule last looked: notify once, and move the cursor.
	o := Evaluate(r(time.Hour, time.Time{}), Data{Now: now, Exists: true, SiblingEnabled: true, SiblingRecoveredAt: ago(10 * time.Minute)})
	if !o.Notify || o.State != StateOK || !o.Cursor.Equal(ago(10*time.Minute)) {
		t.Fatalf("%+v", o)
	}
	// The same recovery is not announced twice.
	o = Evaluate(r(10*time.Minute, time.Time{}), Data{Now: now, Exists: true, SiblingEnabled: true, SiblingRecoveredAt: ago(10 * time.Minute)})
	if o.Notify {
		t.Fatal("announced twice")
	}
	// No recovery, nothing; a recovery from before the rule was made is history.
	if Evaluate(r(time.Hour, time.Time{}), Data{Now: now, Exists: true, SiblingEnabled: true}).Notify || Evaluate(r(time.Hour, time.Time{}), Data{Now: now, Exists: true, SiblingEnabled: true, SiblingRecoveredAt: ago(2 * time.Hour)}).Notify {
		t.Fatal("notified without a new recovery")
	}
	// The six hour limit still applies, and the cursor still moves.
	o = Evaluate(r(time.Hour, ago(time.Hour)), Data{Now: now, Exists: true, SiblingEnabled: true, SiblingRecoveredAt: ago(10 * time.Minute)})
	if o.Notify || !o.Cursor.Equal(ago(10*time.Minute)) {
		t.Fatalf("%+v", o)
	}
	// A gone_quiet partner that is off or deleted makes the rule inactive, even with a recovery on record.
	o = Evaluate(r(time.Hour, time.Time{}), Data{Now: now, Exists: true, SiblingRecoveredAt: ago(10 * time.Minute)})
	if o.State != StateInactive || o.Notify {
		t.Fatalf("partner off: %+v", o)
	}
	// With the radio's row gone the rule has nothing to say but is not an error.
	if o := Evaluate(r(time.Hour, time.Time{}), Data{Now: now}); o.Notify {
		t.Fatal("notified for a missing radio")
	}
}

func TestEvaluateUnknownKindIsInactive(t *testing.T) {
	if o := Evaluate(Rule{Kind: "nope"}, Data{Now: now}); o.State != StateInactive || o.Notify {
		t.Fatalf("%+v", o)
	}
}
