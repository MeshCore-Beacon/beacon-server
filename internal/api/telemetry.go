// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"math"
	"time"
)

// Fastest a counter can honestly climb, per second of wall time: radio airtime can't
// outrun the clock, and real receive errors peak well under 1/s.
const (
	airtimeRate = 1.0
	errorRate   = 10.0
)

// counter turns a since-boot running total into increments, rejecting readings
// that can't be real: dips (partial stats) and jumps faster than rate allows
// (garbage values). Three rejections in a row mean the baseline itself was bad.
// Growth across a gap longer than maxGap can't be placed in a bucket, so it's dropped.
type counter struct {
	rate    float64
	maxGap  int64
	last    float64
	lastT   int64
	seeded  bool
	rejects int
}

func (c *counter) add(v *float64, t int64, rebooted bool) float64 {
	if v == nil {
		return 0
	}
	if !c.seeded || t-c.lastT > c.maxGap {
		c.last, c.lastT, c.seeded, c.rejects = *v, t, true, 0
		return 0
	}
	d := *v - c.last
	if rebooted && d < 0 {
		d = *v // restarted with the device; some firmware keeps counting instead
	}
	// Rows are truncated to the telemetry resolution, so allow twice the gap.
	if d < 0 || d > c.rate*2*float64(t-c.lastT)/1000 {
		if c.rejects++; c.rejects < 3 {
			return 0
		}
		d = 0
	}
	c.last, c.lastT, c.rejects = *v, t, 0
	return d
}

type mean struct {
	sum float64
	n   int
}

func (m *mean) add(v *float64) {
	if v != nil {
		m.sum += *v
		m.n++
	}
}

type telemetryBucket struct {
	start                time.Time
	tx, rx, errs         float64
	hasTx, hasRx, hasErr bool
	battery, noise, qlen mean
	uptime               *int64
}

// BucketTelemetry folds raw telemetry rows (ascending, with any rows before since
// serving only as counter baselines) into fixed UTC buckets. Counters are summed
// increments; battery, noise floor and queue are averaged; uptime is the max.
func BucketTelemetry(points []ObserverTelemetryPoint, since time.Time, size time.Duration) []ObserverTelemetryPoint {
	maxGap := size.Milliseconds()
	tx := counter{rate: airtimeRate, maxGap: maxGap}
	rx := counter{rate: airtimeRate, maxGap: maxGap}
	errs := counter{rate: errorRate, maxGap: maxGap}
	var last *ObserverTelemetryPoint
	var buckets []*telemetryBucket

	for i := range points {
		p := &points[i]
		rebooted := false
		if last != nil && p.UptimeSeconds != nil && last.UptimeSeconds != nil && *p.UptimeSeconds < *last.UptimeSeconds {
			if *p.UptimeSeconds > 2*(p.T-last.T)/1000 {
				continue // an old snapshot replayed, not a reboot
			}
			rebooted = true
		}
		last = p

		dTx := tx.add(f64(p.AirtimeTxSecs), p.T, rebooted)
		dRx := rx.add(f64(p.AirtimeRxSecs), p.T, rebooted)
		dErr := errs.add(i32f(p.ReceiveErrors), p.T, rebooted)
		t := time.UnixMilli(p.T).UTC()
		if t.Before(since) {
			continue
		}
		start := t.Truncate(size)
		if len(buckets) == 0 || !buckets[len(buckets)-1].start.Equal(start) {
			buckets = append(buckets, &telemetryBucket{start: start})
		}
		b := buckets[len(buckets)-1]
		b.tx += dTx
		b.rx += dRx
		b.errs += dErr
		b.hasTx = b.hasTx || p.AirtimeTxSecs != nil
		b.hasRx = b.hasRx || p.AirtimeRxSecs != nil
		b.hasErr = b.hasErr || p.ReceiveErrors != nil
		b.battery.add(i32f(p.BatteryMV))
		b.noise.add(f64(p.NoiseFloorDB))
		b.qlen.add(i32f(p.QueueLength))
		if p.UptimeSeconds != nil && (b.uptime == nil || *p.UptimeSeconds > *b.uptime) {
			b.uptime = p.UptimeSeconds
		}
	}

	out := make([]ObserverTelemetryPoint, 0, len(buckets))
	for _, b := range buckets {
		pt := ObserverTelemetryPoint{T: b.start.UnixMilli(), UptimeSeconds: b.uptime}
		if b.hasTx {
			pt.AirtimeTxSecs = ptr(float32(b.tx))
		}
		if b.hasRx {
			pt.AirtimeRxSecs = ptr(float32(b.rx))
		}
		if b.hasErr {
			pt.ReceiveErrors = ptr(int32(b.errs))
		}
		if b.battery.n > 0 {
			pt.BatteryMV = ptr(int32(math.Round(b.battery.sum / float64(b.battery.n))))
		}
		if b.noise.n > 0 {
			pt.NoiseFloorDB = ptr(float32(b.noise.sum / float64(b.noise.n)))
		}
		if b.qlen.n > 0 {
			pt.QueueLength = ptr(int32(math.Round(b.qlen.sum / float64(b.qlen.n))))
		}
		out = append(out, pt)
	}
	return out
}

func f64(v *float32) *float64 {
	if v == nil {
		return nil
	}
	f := float64(*v)
	return &f
}

func i32f(v *int32) *float64 {
	if v == nil {
		return nil
	}
	f := float64(*v)
	return &f
}

func ptr[T any](v T) *T { return &v }
