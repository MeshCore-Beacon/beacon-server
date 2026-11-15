// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
)

var nodeBase = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func at(h float64) time.Time { return nodeBase.Add(time.Duration(h * float64(time.Hour))) }

func statusReading(h float64, uptime, recv int64) NodeReading {
	return NodeReading{
		SampledAt: at(h), Method: "status", Trusted: true, UptimeSeconds: ptr(uptime), PacketsReceived: ptr(recv),
		AirtimeTxSecs: ptr(int64(0)), AirtimeRxSecs: ptr(int64(0)), PacketsSent: ptr(int64(0)), ReceiveErrors: ptr(int64(0)),
	}
}

func TestBucketNodeReadingsAverages(t *testing.T) {
	rs := []NodeReading{
		{SampledAt: at(1), BatteryMV: ptr(int32(4000)), TemperatureC: ptr(float32(10))},
		{SampledAt: at(2), BatteryMV: ptr(int32(4200)), TemperatureC: ptr(float32(12))},
	}
	got := BucketNodeReadings(rs, nodeBase, 6*time.Hour)
	if len(got) != 1 {
		t.Fatalf("points = %d, want 1", len(got))
	}
	p := got[0]
	if p.T != nodeBase.UnixMilli() || p.Readings != 2 {
		t.Errorf("t=%d readings=%d", p.T, p.Readings)
	}
	if *p.BatteryMV != 4100 || *p.TemperatureC != 11 {
		t.Errorf("battery %v temp %v, want 4100 and 11", *p.BatteryMV, *p.TemperatureC)
	}
}

func TestBucketNodeReadingsMixedMethods(t *testing.T) {
	rs := []NodeReading{
		{SampledAt: at(1), BatteryMV: ptr(int32(4100)), TemperatureC: ptr(float32(15))},
		{SampledAt: at(2), BatteryMV: ptr(int32(4000)), NoiseFloorDB: ptr(int32(-97)), UptimeSeconds: ptr(int64(500))},
	}
	p := BucketNodeReadings(rs, nodeBase, 6*time.Hour)[0]
	if *p.BatteryMV != 4050 || *p.TemperatureC != 15 || *p.NoiseFloorDB != -97 || *p.UptimeSeconds != 500 || p.Readings != 2 {
		t.Errorf("point = %+v", p)
	}
}

func TestBucketNodeReadingsRounding(t *testing.T) {
	rs := []NodeReading{
		{SampledAt: at(1), TemperatureC: ptr(float32(16.4)), LastSNRDB: ptr(float32(11.9)), LastRSSIDB: ptr(int32(-21)), QueueLength: ptr(int32(0))},
		{SampledAt: at(2), TemperatureC: ptr(float32(16.8)), LastSNRDB: ptr(float32(12.3)), LastRSSIDB: ptr(int32(-20)), QueueLength: ptr(int32(1))},
	}
	p := BucketNodeReadings(rs, nodeBase, 6*time.Hour)[0]
	if *p.TemperatureC != 16.6 || *p.LastSNRDB != 12.1 {
		t.Errorf("temp %v snr %v, want 16.6 and 12.1", *p.TemperatureC, *p.LastSNRDB)
	}
	// Means of -20.5 and 0.5 round half away from zero.
	if *p.LastRSSIDB != -21 || *p.QueueLength != 1 {
		t.Errorf("rssi %v queue %v, want -21 and 1", *p.LastRSSIDB, *p.QueueLength)
	}
}

func TestBucketNodeReadingsCounterIncrements(t *testing.T) {
	rs := []NodeReading{
		statusReading(-6, 90000, 1000), // baseline before since
		statusReading(1, 90100, 1100),
		statusReading(2, 90200, 1250),
	}
	got := BucketNodeReadings(rs, nodeBase, 6*time.Hour)
	if len(got) != 1 || *got[0].PacketsReceived != 250 {
		t.Fatalf("got %+v, want one point with packetsReceived 250", got)
	}
	if got[0].Readings != 2 {
		t.Errorf("readings = %d, baseline must not count", got[0].Readings)
	}
}

func TestBucketNodeReadingsCounterReset(t *testing.T) {
	prev := steady(1) // every counter above zero, so a restart makes them all drop
	boot := NodeReading{SampledAt: at(7), Method: "status", UptimeSeconds: ptr(int64(40)), AirtimeTxSecs: ptr(int64(1)), AirtimeRxSecs: ptr(int64(2)),
		PacketsReceived: ptr(int64(30)), PacketsSent: ptr(int64(1)), ReceiveErrors: ptr(int64(0))}
	got := BucketNodeReadings([]NodeReading{prev, boot}, nodeBase, 6*time.Hour)
	if len(got) != 2 || *got[1].PacketsReceived != 30 {
		t.Fatalf("got %+v, want second bucket packetsReceived 30", got)
	}
	if got[0].PacketsReceived != nil {
		t.Errorf("first reading ever has increment %d, want none", *got[0].PacketsReceived)
	}
}

func TestBucketNodeReadingsSparseStatusAcrossFineBuckets(t *testing.T) {
	// Status polls six hours apart with 1h buckets: the increment is kept, in the later bucket.
	rs := []NodeReading{statusReading(0, 1000, 100), statusReading(6, 22600, 700)}
	got := BucketNodeReadings(rs, nodeBase, time.Hour)
	if len(got) != 2 || *got[1].PacketsReceived != 600 {
		t.Fatalf("got %+v, want 600 in the second bucket", got)
	}
}

func TestBucketNodeReadingsDailyPollKept(t *testing.T) {
	rs := []NodeReading{statusReading(0, 1000, 100), statusReading(24, 87400, 5000)}
	got := BucketNodeReadings(rs, nodeBase, 24*time.Hour)
	if *got[len(got)-1].PacketsReceived != 4900 {
		t.Fatalf("got %+v, want 4900 across a 24h poll interval", got)
	}
}

func TestBucketNodeReadingsLongGapDropped(t *testing.T) {
	// Nothing for three days: the growth cannot be placed in one bucket, so it is dropped, not credited.
	rs := []NodeReading{statusReading(0, 1000, 100), statusReading(72, 260000, 90000)}
	got := BucketNodeReadings(rs, nodeBase, 24*time.Hour)
	if len(got) != 2 || got[1].PacketsReceived != nil {
		t.Fatalf("got %+v, want the increment across 72h dropped", got)
	}
}

func TestBucketNodeReadingsRateCap(t *testing.T) {
	// 10 million packets in six hours is garbage, not traffic.
	rs := []NodeReading{statusReading(0, 1000, 100), statusReading(6, 22600, 10_000_000)}
	got := BucketNodeReadings(rs, nodeBase, 6*time.Hour)
	if last := got[len(got)-1]; last.PacketsReceived != nil || last.CounterSpanSecs != nil {
		t.Fatalf("got %+v, want the spiked pair to contribute nothing, not a zero", got)
	}
}

func TestBucketNodeReadingsGap(t *testing.T) {
	rs := []NodeReading{
		{SampledAt: at(1), BatteryMV: ptr(int32(4000))},
		{SampledAt: at(19), BatteryMV: ptr(int32(4100))},
	}
	got := BucketNodeReadings(rs, nodeBase, 6*time.Hour)
	if len(got) != 2 || got[0].T != at(0).UnixMilli() || got[1].T != at(18).UnixMilli() {
		t.Fatalf("got %+v, want exactly two points, no fill between", got)
	}
}

func TestBucketNodeReadingsOmitsAbsentFields(t *testing.T) {
	rs := []NodeReading{{SampledAt: at(1), BatteryMV: ptr(int32(4000)), TemperatureC: ptr(float32(9))}}
	b, err := json.Marshal(BucketNodeReadings(rs, nodeBase, 6*time.Hour)[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"noiseFloorDb", "uptimeSeconds", "packetsReceived", "packetsSent", "receiveErrors", "airtimeTxSecs", "airtimeRxSecs", "lastRssiDb", "lastSnrDb", "queueLength"} {
		if strings.Contains(string(b), k) {
			t.Errorf("%s present in %s", k, b)
		}
	}
	if !strings.Contains(string(b), `"readings":1`) {
		t.Errorf("readings missing in %s", b)
	}
}

func TestBucketNodeReadingsEmpty(t *testing.T) {
	for _, rs := range [][]NodeReading{nil, {statusReading(-3, 5, 5)}} {
		got := BucketNodeReadings(rs, nodeBase, 6*time.Hour)
		if got == nil || len(got) != 0 {
			t.Errorf("got %#v, want empty non-nil", got)
		}
	}
}

// steady is a status reading of a repeater on air 10% of the time, h hours after nodeBase.
func steady(h float64) NodeReading {
	secs := int64((h + 24) * 3600) // counters are never negative, so count from a day before the earliest reading
	return NodeReading{
		SampledAt: at(h), Method: "status", Trusted: true, UptimeSeconds: ptr(1_000_000 + secs),
		AirtimeTxSecs: ptr(secs / 20), AirtimeRxSecs: ptr(secs / 10),
		PacketsReceived: ptr(secs / 100), PacketsSent: ptr(secs / 360), ReceiveErrors: ptr(secs / 3600),
	}
}

func steadyEvery6h(from, to float64) []NodeReading {
	var rs []NodeReading
	for h := from; h <= to; h += 6 {
		rs = append(rs, steady(h))
	}
	return rs
}

// wantRate fails unless the point's rx airtime over its span is the true 10%.
func wantRate(t *testing.T, p NodeTelemetryPoint, span int64) {
	t.Helper()
	if p.CounterSpanSecs == nil || *p.CounterSpanSecs != span || p.AirtimeRxSecs == nil {
		t.Fatalf("point %+v, want counterSpanSecs %d with counters", p, span)
	}
	if got := *p.AirtimeRxSecs / float64(*p.CounterSpanSecs); math.Abs(got-0.1) > 1e-6 {
		t.Errorf("rate %v, want 0.1 (point %+v)", got, p)
	}
}

func TestCounterSpanHourlyBuckets(t *testing.T) {
	got := BucketNodeReadings(steadyEvery6h(-6, 48), nodeBase, time.Hour)
	if len(got) != 9 {
		t.Fatalf("points = %d, want 9", len(got))
	}
	for _, p := range got {
		wantRate(t, p, 21600)
	}
}

func TestCounterSpanSameRateAcrossBucketSizes(t *testing.T) {
	for size, span := range map[time.Duration]int64{6 * time.Hour: 21600, 24 * time.Hour: 86400} {
		got := BucketNodeReadings(steadyEvery6h(-6, 48), nodeBase, size)
		for _, p := range got[:len(got)-1] {
			wantRate(t, p, span)
		}
	}
}

func TestCounterSpanAfterOutage(t *testing.T) {
	rs := append(steadyEvery6h(-6, 24), steady(54)) // a 30h outage, then the next poll
	got := BucketNodeReadings(rs, nodeBase, time.Hour)
	last := got[len(got)-1]
	if last.T != at(54).UnixMilli() {
		t.Fatalf("last point %+v, want the bucket at 54h", last)
	}
	wantRate(t, last, 108000)
}

func TestCounterSpanAfterReboot(t *testing.T) {
	rs := []NodeReading{steady(-6), steady(0), steady(6)}
	boot := steady(12)
	boot.UptimeSeconds = ptr(int64(3600)) // booted an hour before this poll
	boot.AirtimeRxSecs, boot.AirtimeTxSecs = ptr(int64(360)), ptr(int64(180))
	boot.PacketsReceived, boot.PacketsSent, boot.ReceiveErrors = ptr(int64(36)), ptr(int64(10)), ptr(int64(1))
	got := BucketNodeReadings(append(rs, boot), nodeBase, 6*time.Hour)
	wantRate(t, got[len(got)-1], 3600)
}

func TestCounterSpanRestartCappedAtGap(t *testing.T) {
	prev := steady(1)
	boot := NodeReading{SampledAt: at(2), Method: "status", UptimeSeconds: ptr(int64(3000)), AirtimeTxSecs: ptr(int64(0)), AirtimeRxSecs: ptr(int64(0)),
		PacketsReceived: ptr(int64(0)), PacketsSent: ptr(int64(0)), ReceiveErrors: ptr(int64(0))} // booted 3000s before a 3600s gap
	got := BucketNodeReadings([]NodeReading{prev, boot}, nodeBase, time.Hour)
	if last := got[len(got)-1]; last.CounterSpanSecs == nil || *last.CounterSpanSecs != 3000 {
		t.Errorf("point %+v, want span 3000", last)
	}
	boot.UptimeSeconds = ptr(int64(3500))
	boot.SampledAt = prev.SampledAt.Add(3400 * time.Second) // uptime above the gap: the gap bounds the span
	got = BucketNodeReadings([]NodeReading{prev, boot}, nodeBase, time.Hour)
	if last := got[len(got)-1]; last.CounterSpanSecs == nil || *last.CounterSpanSecs != 3400 {
		t.Errorf("point %+v, want span capped at 3400", last)
	}
}

func TestCounterSpanRebootWithSurvivingTotals(t *testing.T) {
	// Review I2: rx grows 10000 -> 12160 over six hours (10%) and uptime falls to 3600, but the
	// totals kept counting through the reboot, so the span is the whole gap.
	a := NodeReading{SampledAt: at(0), Method: "status", UptimeSeconds: ptr(int64(1_000_000)), AirtimeRxSecs: ptr(int64(10000))}
	b := NodeReading{SampledAt: at(6), Method: "status", UptimeSeconds: ptr(int64(3600)), AirtimeRxSecs: ptr(int64(12160))}
	got := BucketNodeReadings([]NodeReading{a, b}, at(1), 6*time.Hour)
	if len(got) != 1 {
		t.Fatalf("points = %+v", got)
	}
	if got[0].AirtimeRxSecs == nil || *got[0].AirtimeRxSecs != 2160 {
		t.Fatalf("point %+v, want rx 2160", got[0])
	}
	wantRate(t, got[0], 21600)
}

func TestCounterSpanMixedRebootIsInconsistent(t *testing.T) {
	// One counter dropped and another grew across a reboot: nothing can be trusted.
	a := NodeReading{SampledAt: at(0), Method: "status", UptimeSeconds: ptr(int64(1_000_000)), AirtimeRxSecs: ptr(int64(1000)), PacketsReceived: ptr(int64(100))}
	b := NodeReading{SampledAt: at(6), Method: "status", UptimeSeconds: ptr(int64(60)), AirtimeRxSecs: ptr(int64(1100)), PacketsReceived: ptr(int64(5))}
	got := BucketNodeReadings([]NodeReading{a, b}, at(1), 6*time.Hour)
	if got[0].CounterSpanSecs != nil || got[0].AirtimeRxSecs != nil || got[0].PacketsReceived != nil {
		t.Errorf("point %+v, want no counters", got[0])
	}
}

func TestCounterDropWithoutRebootIsInconsistent(t *testing.T) {
	a := NodeReading{SampledAt: at(0), Method: "status", UptimeSeconds: ptr(int64(1_000_000)), PacketsReceived: ptr(int64(100))}
	b := NodeReading{SampledAt: at(6), Method: "status", UptimeSeconds: ptr(int64(1_021_600)), PacketsReceived: ptr(int64(50))}
	got := BucketNodeReadings([]NodeReading{a, b}, at(1), 6*time.Hour)
	if got[0].CounterSpanSecs != nil || got[0].PacketsReceived != nil {
		t.Errorf("point %+v, want no counters", got[0])
	}
}

func TestCounterRateCapHasNoTolerance(t *testing.T) {
	// 1.5 airtime seconds per second is over the cap of 1, however the old code rounded it.
	a := NodeReading{SampledAt: at(0), Method: "status", AirtimeRxSecs: ptr(int64(0))}
	b := NodeReading{SampledAt: at(1), Method: "status", AirtimeRxSecs: ptr(int64(5400))}
	got := BucketNodeReadings([]NodeReading{a, b}, at(0), 6*time.Hour)
	if got[0].AirtimeRxSecs != nil {
		t.Errorf("point %+v, want the pair rejected", got[0])
	}
	b.AirtimeRxSecs = ptr(int64(3600)) // exactly 1/s is allowed
	got = BucketNodeReadings([]NodeReading{a, b}, at(0), 6*time.Hour)
	if got[0].AirtimeRxSecs == nil || *got[0].AirtimeRxSecs != 3600 {
		t.Errorf("point %+v, want 3600", got[0])
	}
}

func TestCounterSpanLongGapDropped(t *testing.T) {
	rs := []NodeReading{steady(0), steady(72)}
	got := BucketNodeReadings(rs, nodeBase, 24*time.Hour)
	for _, p := range got {
		if p.CounterSpanSecs != nil || p.AirtimeRxSecs != nil || p.PacketsReceived != nil || p.AirtimeTxSecs != nil || p.PacketsSent != nil || p.ReceiveErrors != nil {
			t.Errorf("point %+v, want no counters and no span", p)
		}
	}
}

func TestCounterSpanFirstReadingEver(t *testing.T) {
	got := BucketNodeReadings([]NodeReading{steady(1)}, nodeBase, 6*time.Hour)
	if len(got) != 1 || got[0].CounterSpanSecs != nil || got[0].AirtimeRxSecs != nil {
		t.Errorf("got %+v, want a point with neither counters nor span", got)
	}
}

func TestCounterSpanTelemetryOnlyBucket(t *testing.T) {
	rs := []NodeReading{steady(-6), {SampledAt: at(1), BatteryMV: ptr(int32(4100))}}
	got := BucketNodeReadings(rs, nodeBase, 6*time.Hour)
	b, _ := json.Marshal(got[0])
	if len(got) != 1 || strings.Contains(string(b), "counterSpanSecs") || got[0].AirtimeRxSecs != nil {
		t.Errorf("got %s, want no counters and no span", b)
	}
}

func TestCounterSpanRejectedReadingAddsNone(t *testing.T) {
	bad := steady(6)
	bad.AirtimeTxSecs, bad.AirtimeRxSecs = ptr(int64(9_000_000)), ptr(int64(9_000_000))
	bad.PacketsReceived, bad.PacketsSent, bad.ReceiveErrors = ptr(int64(9_000_000)), ptr(int64(9_000_000)), ptr(int64(9_000_000))
	rs := []NodeReading{steady(-6), steady(0), bad, steady(12), steady(18)}
	got := BucketNodeReadings(rs, nodeBase, 6*time.Hour)
	if len(got) != 4 {
		t.Fatalf("points = %d, want 4", len(got))
	}
	// Both pairs touching the bad reading contribute nothing, and nothing turns into a zero.
	for _, p := range got[1:3] {
		if p.CounterSpanSecs != nil || p.AirtimeRxSecs != nil || p.PacketsReceived != nil {
			t.Errorf("point %+v, want no span and no counters", p)
		}
	}
	// The next pair stands on its own, covering six hours.
	wantRate(t, got[3], 21600)
}

func TestCounterPartialRejectionThenRecovery(t *testing.T) {
	// Review I1: only rx is garbage at hour 6 and recovers at hour 12. No pair is credited with a
	// span it did not cover, and no rejected value shows as a zero.
	odd := steady(6)
	odd.AirtimeRxSecs = ptr(int64(9_000_000))
	got := BucketNodeReadings([]NodeReading{steady(0), odd, steady(12)}, at(1), 6*time.Hour)
	if len(got) != 2 {
		t.Fatalf("points = %+v", got)
	}
	for _, p := range got {
		if p.CounterSpanSecs != nil || p.AirtimeRxSecs != nil || p.AirtimeTxSecs != nil || p.PacketsReceived != nil {
			t.Errorf("point %+v, want no counters", p)
		}
	}
}

func TestCounterMissingCounterOmittedFromBucket(t *testing.T) {
	// Every pair in the bucket must carry a counter for it to be emitted.
	noRx := steady(2)
	noRx.AirtimeRxSecs = nil
	got := BucketNodeReadings([]NodeReading{steady(0), steady(1), noRx}, at(1), 6*time.Hour)
	p := got[0]
	if p.CounterSpanSecs == nil || *p.CounterSpanSecs != 7200 || p.AirtimeTxSecs == nil || p.PacketsReceived == nil {
		t.Fatalf("point %+v, want span 7200 with tx and packets", p)
	}
	if p.AirtimeRxSecs != nil {
		t.Errorf("rx %v, want omitted: one pair lacked it", *p.AirtimeRxSecs)
	}
}

func TestCounterSameResultForAnyRange(t *testing.T) {
	// Review I3: a corrupt reading at hour 6 must not change what the readings after it show,
	// whichever range is asked for.
	bad := steady(6)
	bad.AirtimeRxSecs, bad.PacketsReceived = ptr(int64(9_000_000)), ptr(int64(9_000_000))
	rs := []NodeReading{steady(0), bad, steady(12), steady(18), steady(24)}
	wide := BucketNodeReadings(rs, at(-1), 6*time.Hour)
	narrow := BucketNodeReadings(rs, at(7), 6*time.Hour) // the bad reading is the only predecessor
	byT := map[int64]NodeTelemetryPoint{}
	for _, p := range wide {
		byT[p.T] = p
	}
	for _, p := range narrow {
		w, _ := json.Marshal(byT[p.T])
		n, _ := json.Marshal(p)
		if string(w) != string(n) {
			t.Errorf("bucket %d differs by range:\n wide   %s\n narrow %s", p.T, w, n)
		}
	}
	if len(narrow) != 3 {
		t.Fatalf("narrow points = %d, want 3", len(narrow))
	}
	wantRate(t, narrow[1], 21600)
}

// A status reading that carries no counters still stands between its neighbours: the pair is taken across real
// consecutive status readings, so a wide range and a narrow one (whose baseline is that reading) agree.
func TestCounterPairsUseRealConsecutiveStatusReadings(t *testing.T) {
	bare := NodeReading{SampledAt: at(4), Method: "status", Trusted: true, BatteryMV: ptr(int32(4000))}
	wide := []NodeReading{statusReading(0, 100000, 100), bare, statusReading(6, 106000, 150)}
	narrow := []NodeReading{bare, statusReading(6, 106000, 150)}
	w := BucketNodeReadings(wide, at(0), 12*time.Hour)
	n := BucketNodeReadings(narrow, at(2), 12*time.Hour)
	last := func(ps []NodeTelemetryPoint) NodeTelemetryPoint { return ps[len(ps)-1] }
	if last(w).CounterSpanSecs != nil || last(w).PacketsReceived != nil {
		t.Errorf("wide range bridged a counterless status reading: %+v", last(w))
	}
	if last(n).CounterSpanSecs != nil || last(n).PacketsReceived != nil {
		t.Errorf("narrow range: %+v", last(n))
	}
	// A telemetry reading with a stray counter is not a status reading and breaks nothing.
	tele := NodeReading{SampledAt: at(4), Method: "telemetry", Trusted: true, PacketsReceived: ptr(int64(7))}
	p := last(BucketNodeReadings([]NodeReading{statusReading(0, 100000, 100), tele, statusReading(6, 106000, 150)}, at(0), 12*time.Hour))
	if p.PacketsReceived == nil || *p.PacketsReceived != 50 || *p.CounterSpanSecs != 21600 {
		t.Errorf("telemetry reading interfered: %+v", p)
	}
}

func TestCounterPairsRejectNegativeOperands(t *testing.T) {
	// Six hours apart: 100 -> -1 with the uptime restarted reads as a reboot whose increment is the current value.
	only := func(h float64, uptime, recv int64) NodeReading {
		return NodeReading{SampledAt: at(h), Method: "status", UptimeSeconds: ptr(uptime), PacketsReceived: ptr(recv)}
	}
	rs := []NodeReading{only(0, 20000, 100), only(6, 3600, -1)}
	p := BucketNodeReadings(rs, at(0), 12*time.Hour)
	if got := p[len(p)-1]; got.PacketsReceived != nil || got.CounterSpanSecs != nil {
		t.Fatalf("a negative counter produced %+v", got)
	}
	// A negative earlier value is just as impossible.
	rs = []NodeReading{statusReading(0, 20000, -5), statusReading(6, 30000, 10)}
	p = BucketNodeReadings(rs, at(0), 12*time.Hour)
	if got := p[len(p)-1]; got.PacketsReceived != nil {
		t.Fatalf("a negative earlier counter produced %+v", got)
	}
}
