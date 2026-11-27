// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import "time"

// State is where a rule stands.
type State string

const (
	StateOK       State = "ok"
	StateFiring   State = "firing"
	StateInactive State = "inactive" // the data the rule needs is gone
	StateUnknown  State = "unknown"  // not enough data yet
)

// UptimePage is how many readings after its cursor one pass reads for a reboot rule; a longer backlog takes more passes.
const UptimePage = 200

// MinResend is the shortest time between two notifications of one rule.
const MinResend = 6 * time.Hour

// Hysteresis: how far past the threshold a reading must be before the rule goes back to ok.
const (
	batteryRecoverMV = 100
	noiseRecoverDB   = 3
	tempRecoverTenth = 10
)

// Rule is the part of a stored rule the evaluation reads.
type Rule struct {
	Kind       string
	Threshold  *int
	High       *int
	State      State
	LastSentAt time.Time // zero when never sent
	// Cursor is the newest data already judged (reboots, adverts, recoveries); Streak counts breaches in a row.
	Cursor time.Time
	Streak int
}

// Reading is one trusted poll result or one observer telemetry row. Missing values are nil.
type Reading struct {
	At         time.Time
	BatteryMV  *int
	TempTenths *int
	NoiseDBm   *int
	Uptime     *int64
}

// Drift is the clock drift (seconds) one advert carried.
type Drift struct {
	At  time.Time
	Sec int64
}

// Data is what is known about the radio right now.
type Data struct {
	Now time.Time
	// Exists: the node or observer row is there.
	Exists bool
	// Polled: the radio has a current source (a polling assignment, or fresh status telemetry for an observer).
	Polled bool
	// LastHeard is nodes.last_seen (also counting a trusted reading) or observers.last_seen; zero when never.
	LastHeard time.Time
	// StatusAt is the observer's last status report.
	StatusAt time.Time
	// Readings are the newest first.
	Readings []Reading
	// Uptimes are the uptime readings a reboot rule judges, newest first: the one before its cursor (the baseline)
	// and then up to UptimePage readings after it, oldest first from the cursor, so the newest is where a full page
	// ended and the cursor only moves through what was read.
	Uptimes []Reading
	// DayAgo is the battery reading closest to 24 hours before the newest battery reading, within 3 hours of it.
	DayAgo *Reading
	// Drifts are the clock drift of the repeater's kept adverts, oldest first: the one at or before a clock rule's
	// cursor (the first half of a pair) and every advert after it.
	Drifts []Drift
	// SiblingRecoveredAt is when the radio's gone_quiet rule last went from firing to ok.
	SiblingRecoveredAt time.Time
	// SiblingEnabled: the radio has an enabled gone_quiet rule, which a back_online rule needs.
	SiblingEnabled bool
}

// Outcome is what the evaluation decided.
type Outcome struct {
	State State
	// Notify: send a notification; Value is the figure to put in it (unit of the kind).
	Notify bool
	Value  int64
	Cursor time.Time
	Streak int
	// Recovered: a gone_quiet rule went from firing to ok, for the back_online rule to see.
	Recovered bool
}

// Evaluate judges one rule against fresh data. A notification is produced only on a change into firing, at most
// once per MinResend; back_online and rebooted are events and notify on the event itself.
func Evaluate(r Rule, d Data) Outcome {
	o := Outcome{State: r.State, Cursor: r.Cursor, Streak: r.Streak}
	inactive := func() Outcome { o.State = StateInactive; return o }
	// keep: not enough data to judge; an inactive rule with data back is simply unknown again.
	keep := func() Outcome {
		if o.State == StateInactive {
			o.State = StateUnknown
		}
		return o
	}
	set := func(firing bool, value int64) Outcome {
		if firing {
			o.State = StateFiring
			if r.State != StateFiring {
				o.Notify, o.Value = true, value
			}
		} else {
			o.State = StateOK
		}
		return o
	}
	var out Outcome
	switch r.Kind {
	case KindGoneQuiet:
		if !d.Exists || d.LastHeard.IsZero() {
			return inactive()
		}
		quiet := d.Now.Sub(d.LastHeard)
		out = set(r.Threshold != nil && quiet >= time.Duration(*r.Threshold)*time.Hour, int64(quiet/time.Hour))
		out.Recovered = r.State == StateFiring && out.State == StateOK
	case KindObserverOff:
		if !d.Exists || d.StatusAt.IsZero() {
			return inactive()
		}
		quiet := d.Now.Sub(d.StatusAt)
		out = set(r.Threshold != nil && quiet >= time.Duration(*r.Threshold)*time.Minute, int64(quiet/time.Minute))
	case KindBatteryLow:
		if !d.Exists || !d.Polled {
			return inactive()
		}
		v := values(d.Readings, func(x Reading) *int { return x.BatteryMV })
		if len(v) < 2 || r.Threshold == nil {
			return keep()
		}
		th := *r.Threshold
		out = set(v[0] < th && v[1] < th || r.State == StateFiring && v[0] <= th+batteryRecoverMV, int64(v[0]))
	case KindBatteryFalling:
		if !d.Exists || !d.Polled {
			return inactive()
		}
		newest := first(d.Readings, func(x Reading) bool { return x.BatteryMV != nil })
		if newest == nil || d.DayAgo == nil || d.DayAgo.BatteryMV == nil || r.Threshold == nil {
			return keep()
		}
		hours := newest.At.Sub(d.DayAgo.At).Hours()
		if hours <= 0 {
			return keep()
		}
		rate := float64(*d.DayAgo.BatteryMV-*newest.BatteryMV) * 24 / hours
		out = set(rate > float64(*r.Threshold), int64(rate+0.5))
	case KindRebooted:
		if !d.Exists || !d.Polled {
			return inactive()
		}
		ups := firstN(d.Uptimes, len(d.Uptimes), func(x Reading) bool { return x.Uptime != nil })
		if len(ups) < 2 {
			return keep()
		}
		if !ups[0].At.After(r.Cursor) {
			return keep() // already judged
		}
		o.Cursor = ups[0].At
		// Every reading since the cursor is compared with the one before it (which may be older than the cursor),
		// so a reboot between two passes is not lost behind later readings. The newest reboot gives the figure.
		var reboot *Reading
		for i := 0; i+1 < len(ups) && ups[i].At.After(r.Cursor); i++ {
			if *ups[i].Uptime < *ups[i+1].Uptime {
				reboot = &ups[i]
				break
			}
		}
		out = set(reboot != nil, 0)
		// An event: notify on every new reboot, not only on the change into firing.
		out.Notify = reboot != nil
		if reboot != nil {
			out.Value = *reboot.Uptime
		}
	case KindTemperature:
		if !d.Exists || !d.Polled {
			return inactive()
		}
		v := values(d.Readings, func(x Reading) *int { return x.TempTenths })
		if len(v) < 2 || (r.Threshold == nil && r.High == nil) {
			return keep()
		}
		outside := func(x int) bool {
			return (r.Threshold != nil && x < *r.Threshold) || (r.High != nil && x > *r.High)
		}
		inside := func(x int) bool { // back inside by a degree
			return (r.Threshold == nil || x >= *r.Threshold+tempRecoverTenth) && (r.High == nil || x <= *r.High-tempRecoverTenth)
		}
		out = set(outside(v[0]) && outside(v[1]) || r.State == StateFiring && !inside(v[0]), int64(v[0]))
	case KindNoiseHigh:
		if !d.Exists || !d.Polled {
			return inactive()
		}
		v := values(d.Readings, func(x Reading) *int { return x.NoiseDBm })
		if len(v) < 2 || r.Threshold == nil {
			return keep()
		}
		th := *r.Threshold
		out = set(v[0] > th && v[1] > th || r.State == StateFiring && v[0] > th-noiseRecoverDB, int64(v[0]))
	case KindClock:
		if !d.Exists || len(d.Drifts) == 0 {
			return inactive()
		}
		newest := d.Drifts[len(d.Drifts)-1]
		if !newest.At.After(r.Cursor) || r.Threshold == nil {
			return keep()
		}
		o.Cursor = newest.At
		o.Streak = 0
		abs := func(v int64) int64 {
			if v < 0 {
				return -v
			}
			return v
		}
		over := func(v int64) bool { return abs(v) > int64(*r.Threshold) }
		// A breach is two adverts in a row past the limit. Every pair since the cursor counts, so a breach that
		// recovered between two passes still fires once.
		var breach *Drift
		for i := 1; i < len(d.Drifts); i++ {
			if d.Drifts[i].At.After(r.Cursor) && over(d.Drifts[i].Sec) && over(d.Drifts[i-1].Sec) {
				breach = &d.Drifts[i]
			}
		}
		last := len(d.Drifts) - 1
		out = set(last >= 1 && over(newest.Sec) && over(d.Drifts[last-1].Sec), abs(newest.Sec))
		if breach != nil && out.State == StateOK && r.State != StateFiring {
			out.Notify, out.Value = true, abs(breach.Sec)
		}
	case KindBackOnline:
		// Never firing itself: it announces the moment the radio's gone_quiet rule recovered. Without an enabled
		// gone_quiet partner there is nothing to announce.
		if !d.SiblingEnabled {
			return inactive()
		}
		o.State = StateOK
		if d.SiblingRecoveredAt.After(r.Cursor) {
			o.Cursor = d.SiblingRecoveredAt
			o.Notify = true
		}
		out = o
	default:
		return inactive()
	}
	if out.Notify && !r.LastSentAt.IsZero() && d.Now.Sub(r.LastSentAt) < MinResend {
		out.Notify = false
	}
	return out
}

// values returns the first two non-nil values of the readings, newest first.
func values(rs []Reading, get func(Reading) *int) []int {
	var out []int
	for _, r := range rs {
		if v := get(r); v != nil {
			out = append(out, *v)
			if len(out) == 2 {
				break
			}
		}
	}
	return out
}

func first(rs []Reading, ok func(Reading) bool) *Reading {
	if p := firstN(rs, 1, ok); len(p) > 0 {
		return &p[0]
	}
	return nil
}

func firstN(rs []Reading, n int, ok func(Reading) bool) []Reading {
	var out []Reading
	for _, r := range rs {
		if ok(r) {
			out = append(out, r)
			if len(out) == n {
				break
			}
		}
	}
	return out
}
