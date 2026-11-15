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
	// DayAgo is the battery reading closest to 24 hours before the newest battery reading, within 3 hours of it.
	DayAgo *Reading
	// DriftSec is the repeater's clock drift from its newest advert, at AdvertAt.
	DriftSec *int64
	AdvertAt time.Time
	// PrevDriftSec is the drift of the advert before that one; nil when there was none.
	PrevDriftSec *int64
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
		ups := firstN(d.Readings, len(d.Readings), func(x Reading) bool { return x.Uptime != nil })
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
		if !d.Exists || d.DriftSec == nil || d.AdvertAt.IsZero() {
			return inactive()
		}
		if !d.AdvertAt.After(r.Cursor) || r.Threshold == nil {
			return keep()
		}
		o.Cursor = d.AdvertAt
		o.Streak = 0
		over := func(v int64) bool {
			if v < 0 {
				v = -v
			}
			return v > int64(*r.Threshold)
		}
		// Two adverts in a row past the limit: the newest and the one before it, as the node row keeps them.
		both := over(*d.DriftSec) && d.PrevDriftSec != nil && over(*d.PrevDriftSec)
		drift := *d.DriftSec
		if drift < 0 {
			drift = -drift
		}
		out = set(both, drift)
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
