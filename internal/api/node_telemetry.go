// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"math"
	"time"

	"github.com/google/uuid"
)

// Packets a radio can't beat per second; a higher claimed rate is a garbage reading.
const packetRate = 50.0

// Counters only arrive on status readings, which a poller may take up to 24h apart. A gap
// of two such intervals still places growth in a bucket; a longer one can't be placed, so it
// is dropped. Independent of the bucket size, which can be much finer than the poll interval.
const nodeCounterMaxGap = 48 * time.Hour

// NodeReading is one stored reading of a polled node, ascending by SampledAt.
type NodeReading struct {
	SampledAt       time.Time
	Trusted         bool
	BatteryMV       *int32
	TemperatureC    *float32
	NoiseFloorDB    *int32
	LastRSSIDB      *int32
	LastSNRDB       *float32
	UptimeSeconds   *int64
	QueueLength     *int32
	AirtimeTxSecs   *int64
	AirtimeRxSecs   *int64
	PacketsReceived *int64
	PacketsSent     *int64
	ReceiveErrors   *int64
}

// NodeTelemetryPoint is one bucket of a node's readings. Counters are increments within the
// bucket, uptime is the bucket maximum, the rest are averages. A field is omitted when no
// reading in the bucket carried it.
type NodeTelemetryPoint struct {
	T               int64    `json:"t"` // epoch ms, bucket start
	Readings        int      `json:"readings"`
	BatteryMV       *int32   `json:"batteryMv,omitempty"`
	TemperatureC    *float32 `json:"temperatureC,omitempty"`
	NoiseFloorDB    *int32   `json:"noiseFloorDb,omitempty"`
	LastRSSIDB      *int32   `json:"lastRssiDb,omitempty"`
	LastSNRDB       *float32 `json:"lastSnrDb,omitempty"`
	UptimeSeconds   *int64   `json:"uptimeSeconds,omitempty"`
	QueueLength     *int32   `json:"queueLength,omitempty"`
	AirtimeTxSecs   *float64 `json:"airtimeTxSecs,omitempty"`
	AirtimeRxSecs   *float64 `json:"airtimeRxSecs,omitempty"`
	PacketsReceived *int64   `json:"packetsReceived,omitempty"`
	PacketsSent     *int64   `json:"packetsSent,omitempty"`
	ReceiveErrors   *int64   `json:"receiveErrors,omitempty"`
}

// NodeTelemetry is the telemetry response for a polled node.
type NodeTelemetry struct {
	Range             string               `json:"range"`
	Interval          string               `json:"interval"`
	Polled            bool                 `json:"polled"`            // any reading has ever been stored for the node
	UntrustedReadings int                  `json:"untrustedReadings"` // readings in range left out because include=untrusted was not set
	Points            []NodeTelemetryPoint `json:"points"`
}

// incSum is one counter's running increment total inside a bucket.
type incSum struct {
	sum float64
	has bool
}

func (s *incSum) take(d float64, present bool) {
	s.sum += d
	s.has = s.has || present
}

type nodeBucket struct {
	start                      time.Time
	readings                   int
	battery, temp, noise, rssi mean
	snr, qlen                  mean
	uptime                     *int64
	tx, rx, recv, sent, errs   incSum
}

// BucketNodeReadings folds readings (ascending, with any before since serving only as counter
// baselines) into fixed UTC buckets of size. Buckets with no readings are not emitted.
func BucketNodeReadings(readings []NodeReading, since time.Time, size time.Duration) []NodeTelemetryPoint {
	maxGap := nodeCounterMaxGap.Milliseconds()
	tx := counter{rate: airtimeRate, maxGap: maxGap}
	rx := counter{rate: airtimeRate, maxGap: maxGap}
	errs := counter{rate: errorRate, maxGap: maxGap}
	recv := counter{rate: packetRate, maxGap: maxGap}
	sent := counter{rate: packetRate, maxGap: maxGap}

	var lastUptime *int64
	var lastT int64
	var buckets []*nodeBucket
	for i := range readings {
		r := &readings[i]
		t := r.SampledAt.UnixMilli()
		rebooted := false
		if r.UptimeSeconds != nil {
			if lastUptime != nil && *r.UptimeSeconds < *lastUptime {
				if *r.UptimeSeconds > 2*(t-lastT)/1000 {
					continue // an old snapshot replayed, not a reboot
				}
				rebooted = true
			}
			lastUptime, lastT = r.UptimeSeconds, t
		}

		dTx := tx.add(i64f(r.AirtimeTxSecs), t, rebooted)
		dRx := rx.add(i64f(r.AirtimeRxSecs), t, rebooted)
		dErr := errs.add(i64f(r.ReceiveErrors), t, rebooted)
		dRecv := recv.add(i64f(r.PacketsReceived), t, rebooted)
		dSent := sent.add(i64f(r.PacketsSent), t, rebooted)
		if r.SampledAt.Before(since) {
			continue
		}

		start := r.SampledAt.UTC().Truncate(size)
		if len(buckets) == 0 || !buckets[len(buckets)-1].start.Equal(start) {
			buckets = append(buckets, &nodeBucket{start: start})
		}
		cur := buckets[len(buckets)-1]
		cur.readings++
		cur.tx.take(dTx, r.AirtimeTxSecs != nil)
		cur.rx.take(dRx, r.AirtimeRxSecs != nil)
		cur.errs.take(dErr, r.ReceiveErrors != nil)
		cur.recv.take(dRecv, r.PacketsReceived != nil)
		cur.sent.take(dSent, r.PacketsSent != nil)
		cur.battery.add(i32f(r.BatteryMV))
		cur.temp.add(f64(r.TemperatureC))
		cur.noise.add(i32f(r.NoiseFloorDB))
		cur.rssi.add(i32f(r.LastRSSIDB))
		cur.snr.add(f64(r.LastSNRDB))
		cur.qlen.add(i32f(r.QueueLength))
		if r.UptimeSeconds != nil && (cur.uptime == nil || *r.UptimeSeconds > *cur.uptime) {
			cur.uptime = r.UptimeSeconds
		}
	}

	out := make([]NodeTelemetryPoint, 0, len(buckets))
	for _, cur := range buckets {
		p := NodeTelemetryPoint{T: cur.start.UnixMilli(), Readings: cur.readings, UptimeSeconds: cur.uptime}
		p.BatteryMV = roundedMean32(cur.battery)
		p.NoiseFloorDB = roundedMean32(cur.noise)
		p.LastRSSIDB = roundedMean32(cur.rssi)
		p.QueueLength = roundedMean32(cur.qlen)
		p.TemperatureC = tenthsMean(cur.temp)
		p.LastSNRDB = tenthsMean(cur.snr)
		if cur.tx.has {
			p.AirtimeTxSecs = ptr(cur.tx.sum)
		}
		if cur.rx.has {
			p.AirtimeRxSecs = ptr(cur.rx.sum)
		}
		if cur.recv.has {
			p.PacketsReceived = ptr(int64(cur.recv.sum))
		}
		if cur.sent.has {
			p.PacketsSent = ptr(int64(cur.sent.sum))
		}
		if cur.errs.has {
			p.ReceiveErrors = ptr(int64(cur.errs.sum))
		}
		out = append(out, p)
	}
	return out
}

func roundedMean32(m mean) *int32 {
	if m.n == 0 {
		return nil
	}
	return ptr(int32(math.Round(m.sum / float64(m.n))))
}

func tenthsMean(m mean) *float32 {
	if m.n == 0 {
		return nil
	}
	return ptr(float32(math.Round(m.sum/float64(m.n)*10) / 10))
}

func i64f(v *int64) *float64 {
	if v == nil {
		return nil
	}
	f := float64(*v)
	return &f
}

// NodePollingObserver is an observer assigned to poll a node. Rank is 0 for the poller (omitted) and 1+ for standbys.
type NodePollingObserver struct {
	ObserverID uuid.UUID `json:"observerId"`
	Name       string    `json:"name"`
	Rank       int       `json:"rank,omitempty"`
}

// NodePolling says who polls a node and when it last got a trusted reading.
type NodePolling struct {
	Polled          bool                  `json:"polled"`          // any reading has ever been stored for the node
	LatestReadingAt *int64                `json:"latestReadingAt"` // newest trusted reading, epoch ms
	Poller          *NodePollingObserver  `json:"poller"`          // null when nobody is assigned
	Standbys        []NodePollingObserver `json:"standbys"`
}
