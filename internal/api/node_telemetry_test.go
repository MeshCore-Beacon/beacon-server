// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

var nodeBase = time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)

func at(h float64) time.Time { return nodeBase.Add(time.Duration(h * float64(time.Hour))) }

func statusReading(h float64, uptime, recv int64) NodeReading {
	return NodeReading{
		SampledAt: at(h), Trusted: true, UptimeSeconds: ptr(uptime), PacketsReceived: ptr(recv),
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
	if *p.LastRSSIDB != -21 && *p.LastRSSIDB != -20 || *p.QueueLength != 1 && *p.QueueLength != 0 {
		t.Errorf("rssi %v queue %v not integers near the mean", *p.LastRSSIDB, *p.QueueLength)
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
	rs := []NodeReading{
		statusReading(1, 100000, 5000),
		statusReading(7, 40, 40), // rebooted since the last reading
	}
	got := BucketNodeReadings(rs, nodeBase, 6*time.Hour)
	if len(got) != 2 || *got[1].PacketsReceived != 40 {
		t.Fatalf("got %+v, want second bucket packetsReceived 40", got)
	}
	if *got[0].PacketsReceived < 0 {
		t.Errorf("negative increment %d", *got[0].PacketsReceived)
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
	if len(got) != 2 || *got[1].PacketsReceived != 0 {
		t.Fatalf("got %+v, want the increment across 72h dropped", got)
	}
}

func TestBucketNodeReadingsRateCap(t *testing.T) {
	// 10 million packets in six hours is garbage, not traffic.
	rs := []NodeReading{statusReading(0, 1000, 100), statusReading(6, 22600, 10_000_000)}
	got := BucketNodeReadings(rs, nodeBase, 6*time.Hour)
	if *got[len(got)-1].PacketsReceived != 0 {
		t.Fatalf("got %+v, want the spike rejected", got)
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
