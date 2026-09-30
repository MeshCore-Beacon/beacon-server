// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"testing"
	"time"
)

var telemetryBase = time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC)

type row struct {
	hour   int
	tx, rx float32
	errs   int32
	noise  float32
	uptime int64
}

func telemetryPoints(rows ...row) []ObserverTelemetryPoint {
	pts := make([]ObserverTelemetryPoint, 0, len(rows))
	for _, r := range rows {
		tx, rx, errs, noise, uptime := r.tx, r.rx, r.errs, r.noise, r.uptime
		pts = append(pts, ObserverTelemetryPoint{
			T:             telemetryBase.Add(time.Duration(r.hour) * time.Hour).UnixMilli(),
			AirtimeTxSecs: &tx,
			AirtimeRxSecs: &rx,
			ReceiveErrors: &errs,
			NoiseFloorDB:  &noise,
			UptimeSeconds: &uptime,
		})
	}
	return pts
}

func bucketAt(t *testing.T, buckets []ObserverTelemetryPoint, hour int) ObserverTelemetryPoint {
	t.Helper()
	want := telemetryBase.Add(time.Duration(hour) * time.Hour).UnixMilli()
	for _, b := range buckets {
		if b.T == want {
			return b
		}
	}
	t.Fatalf("no bucket at hour %d in %+v", hour, buckets)
	return ObserverTelemetryPoint{}
}

func checkCounters(t *testing.T, b ObserverTelemetryPoint, tx, rx float32, errs int32) {
	t.Helper()
	if *b.AirtimeTxSecs != tx || *b.AirtimeRxSecs != rx || *b.ReceiveErrors != errs {
		t.Errorf("counters = tx %v rx %v errs %v, want tx %v rx %v errs %v",
			*b.AirtimeTxSecs, *b.AirtimeRxSecs, *b.ReceiveErrors, tx, rx, errs)
	}
}

func TestBucketTelemetry_SumsIncrements(t *testing.T) {
	pts := telemetryPoints(
		row{0, 100, 1000, 50, -90, 10000},
		row{1, 110, 1060, 55, -92, 13600},
		row{2, 125, 1100, 60, -94, 17200},
	)
	b := BucketTelemetry(pts, telemetryBase, 6*time.Hour)
	if len(b) != 1 {
		t.Fatalf("got %d buckets, want 1", len(b))
	}
	checkCounters(t, b[0], 25, 100, 10)
	if *b[0].NoiseFloorDB != -92 || *b[0].UptimeSeconds != 17200 {
		t.Errorf("noise %v uptime %v, want -92 17200", *b[0].NoiseFloorDB, *b[0].UptimeSeconds)
	}
}

// Observer e61a62a7 on 2026-09-30: an uptime-only status stored zeros mid-run.
func TestBucketTelemetry_ZeroedRowIsNotARestart(t *testing.T) {
	pts := telemetryPoints(
		row{5, 7935, 47484, 93689, -96, 1093083},
		row{6, 0, 0, 0, 0, 1096701},
		row{7, 7951, 47586, 93884, -97, 1100319},
	)
	b := BucketTelemetry(pts, telemetryBase.Add(5*time.Hour), 6*time.Hour)
	checkCounters(t, bucketAt(t, b, 6), 16, 102, 195)
}

func TestBucketTelemetry_SingleCounterDip(t *testing.T) {
	pts := telemetryPoints(
		row{0, 3108, 19536, 38083, -108, 473828},
		row{1, 3160, 19837, 0, -92, 477447},
		row{2, 3208, 20145, 39211, -94, 481065},
	)
	b := BucketTelemetry(pts, telemetryBase, 6*time.Hour)
	checkCounters(t, b[0], 100, 609, 1128)
}

func TestBucketTelemetry_RebootCountsSinceBoot(t *testing.T) {
	pts := telemetryPoints(
		row{0, 50, 500, 10, -90, 10000},
		row{1, 60, 560, 12, -90, 13600},
		row{2, 5, 40, 3, -90, 1200},
		row{3, 9, 70, 4, -90, 4800},
	)
	b := BucketTelemetry(pts, telemetryBase, 6*time.Hour)
	checkCounters(t, b[0], 10+5+4, 60+40+30, 2+3+1)
}

// Observer 50350675 replays an old snapshot between live rows.
func TestBucketTelemetry_StaleReplaySkipped(t *testing.T) {
	pts := telemetryPoints(
		row{0, 121, 4788, 0, -120, 225259},
		row{1, 123, 4849, 0, -120, 228860},
		row{2, 88, 3879, 0, -50, 184130},
		row{3, 126, 4972, 0, -120, 236061},
		row{4, 88, 3879, 0, -50, 184130},
	)
	b := BucketTelemetry(pts, telemetryBase, 6*time.Hour)
	checkCounters(t, b[0], 5, 184, 0)
	if *b[0].NoiseFloorDB != -120 || *b[0].UptimeSeconds != 236061 {
		t.Errorf("stale row leaked into averages: noise %v uptime %v", *b[0].NoiseFloorDB, *b[0].UptimeSeconds)
	}
}

func TestBucketTelemetry_LookbackSeedsBaseline(t *testing.T) {
	pts := telemetryPoints(
		row{5, 100, 1000, 50, -90, 10000},
		row{6, 110, 1030, 52, -90, 13600},
		row{12, 130, 1090, 58, -90, 35200},
	)
	b := BucketTelemetry(pts, telemetryBase.Add(6*time.Hour), 6*time.Hour)
	if len(b) != 2 {
		t.Fatalf("got %d buckets, want 2 (lookback row must not get its own)", len(b))
	}
	checkCounters(t, bucketAt(t, b, 6), 10, 30, 2)
	checkCounters(t, bucketAt(t, b, 12), 20, 60, 6)
}

// Some firmware keeps counting airtime across a reboot (ed5a20b6).
func TestBucketTelemetry_RebootWithPersistentCounters(t *testing.T) {
	pts := telemetryPoints(
		row{0, 900, 178000, 10, -90, 500000},
		row{1, 910, 178400, 12, -90, 503600},
		row{2, 915, 178485, 1, -90, 2250},
		row{3, 920, 178589, 3, -90, 5851},
	)
	b := BucketTelemetry(pts, telemetryBase, 6*time.Hour)
	checkCounters(t, b[0], 20, 589, 2+1+2)
}

// Observer 408159b8 reported one-off garbage between sane readings.
func TestBucketTelemetry_GarbageSpikeRejected(t *testing.T) {
	pts := telemetryPoints(
		row{0, 96, 164, 144, -89, 145911},
		row{1, 101, 172, 146, -89, 149536},
		row{2, 111, 201329470, 2080374784, -90, 156847},
		row{3, 114, 197, 160, -89, 160464},
	)
	b := BucketTelemetry(pts, telemetryBase, 6*time.Hour)
	checkCounters(t, b[0], 18, 33, 16)
}

func TestBucketTelemetry_GarbageBaselineRecovers(t *testing.T) {
	pts := telemetryPoints(
		row{0, 100, 2080374784, 50, -90, 10000},
		row{1, 110, 200, 55, -90, 13600},
		row{2, 120, 210, 60, -90, 17200},
		row{3, 130, 220, 65, -90, 20800},
		row{4, 140, 235, 70, -90, 24400},
	)
	b := BucketTelemetry(pts, telemetryBase, 6*time.Hour)
	checkCounters(t, b[0], 40, 15, 20)
}

func TestBucketTelemetry_GapLongerThanBucketDropped(t *testing.T) {
	pts := telemetryPoints(
		row{0, 100, 22018, 50, -90, 500000},
		row{1, 110, 22100, 55, -90, 503600},
		row{20, 400, 39295, 900, -90, 572000},
		row{21, 410, 39384, 905, -90, 575600},
	)
	b := BucketTelemetry(pts, telemetryBase, 6*time.Hour)
	checkCounters(t, bucketAt(t, b, 18), 10, 89, 5)
}

func TestBucketTelemetry_AveragesAndNils(t *testing.T) {
	b1, b2 := int32(3700), int32(3705)
	q := int32(2)
	pts := []ObserverTelemetryPoint{
		{T: telemetryBase.UnixMilli(), BatteryMV: &b1, QueueLength: &q},
		{T: telemetryBase.Add(time.Hour).UnixMilli(), BatteryMV: &b2},
	}
	b := BucketTelemetry(pts, telemetryBase, 24*time.Hour)
	if len(b) != 1 {
		t.Fatalf("got %d buckets, want 1", len(b))
	}
	if *b[0].BatteryMV != 3703 || *b[0].QueueLength != 2 {
		t.Errorf("battery %v queue %v, want 3703 2", *b[0].BatteryMV, *b[0].QueueLength)
	}
	if b[0].AirtimeTxSecs != nil || b[0].NoiseFloorDB != nil || b[0].UptimeSeconds != nil {
		t.Errorf("fields with no data should stay nil: %+v", b[0])
	}
}
