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

// Counters only arrive on status readings, which a poller may take up to 24h apart. A pair
// two such intervals apart still places growth in a bucket; a longer one can't be placed, so it
// is dropped. Independent of the bucket size, which can be much finer than the poll interval.
const nodeCounterMaxGap = 48 * time.Hour

// NodeReading is one stored reading of a polled node, ascending by SampledAt.
type NodeReading struct {
	SampledAt time.Time
	// Method is how the reading was taken, "status" or "telemetry". Only consecutive status readings make counter
	// pairs, whatever their counters hold.
	Method          string
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
// bucket, covering counterSpanSecs of elapsed time, uptime is the bucket maximum, the rest are averages. A field is omitted when no
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
	// Seconds of elapsed time the counter increments cover; divide an increment by it for a rate.
	// Omitted, along with the counters, when no increment in the bucket could be placed in time.
	CounterSpanSecs *int64 `json:"counterSpanSecs,omitempty"`
}

// NodeTelemetry is the telemetry response for a polled node.
type NodeTelemetry struct {
	Range             string               `json:"range"`
	Interval          string               `json:"interval"`
	Polled            bool                 `json:"polled"`            // any reading has ever been stored for the node
	UntrustedReadings int                  `json:"untrustedReadings"` // readings in range left out because include=untrusted was not set
	Points            []NodeTelemetryPoint `json:"points"`
}

// nodeCounters are the five since-boot counters on a status reading, each with the fastest
// rate per second it can honestly climb.
var nodeCounters = [5]struct {
	get func(*NodeReading) *int64
	cap float64
}{
	{func(r *NodeReading) *int64 { return r.AirtimeTxSecs }, airtimeRate},
	{func(r *NodeReading) *int64 { return r.AirtimeRxSecs }, airtimeRate},
	{func(r *NodeReading) *int64 { return r.PacketsReceived }, packetRate},
	{func(r *NodeReading) *int64 { return r.PacketsSent }, packetRate},
	{func(r *NodeReading) *int64 { return r.ReceiveErrors }, errorRate},
}

const (
	ctrTx = iota
	ctrRx
	ctrRecv
	ctrSent
	ctrErrs
)

// isStatusReading goes by the reading's method, not its counters: a status reply too short to carry any is still the
// status reading its neighbours pair with, which is also how the store picks the baseline before a range.
func isStatusReading(r *NodeReading) bool { return r.Method == "status" }

// counterPair is what one pair of consecutive status readings contributes.
type counterPair struct {
	inc    [5]int64
	has    [5]bool
	spanMS int64
}

// pairIncrements reads only prev and cur, so a pair gives the same answer whatever range was
// asked for. It reports false when the pair can't be trusted to say anything.
func pairIncrements(prev, cur *NodeReading) (counterPair, bool) {
	var p counterPair
	elapsed := cur.SampledAt.Sub(prev.SampledAt).Milliseconds()
	if elapsed <= 0 || elapsed > nodeCounterMaxGap.Milliseconds() {
		return p, false
	}
	rebooted := prev.UptimeSeconds != nil && cur.UptimeSeconds != nil &&
		(*cur.UptimeSeconds < *prev.UptimeSeconds || *cur.UptimeSeconds*1000 < elapsed)
	var drops, rises int
	var vals [5][2]int64
	for i, c := range nodeCounters {
		pv, cv := c.get(prev), c.get(cur)
		if pv == nil || cv == nil {
			continue
		}
		if *pv < 0 || *cv < 0 {
			return p, false // a counter cannot be negative; whatever sent one is not to be believed
		}
		vals[i] = [2]int64{*pv, *cv}
		p.has[i] = true
		if *cv < *pv {
			drops++
		} else {
			rises++
		}
	}
	if drops+rises == 0 {
		return p, false
	}
	p.spanMS = elapsed
	switch {
	case rebooted && drops > 0 && rises == 0: // the counters restarted with the device
		if up := *cur.UptimeSeconds * 1000; up < elapsed {
			p.spanMS = up
		}
		for i := range p.inc {
			p.inc[i] = vals[i][1]
		}
	case drops == 0: // counting on, through a reboot or not
		for i := range p.inc {
			p.inc[i] = vals[i][1] - vals[i][0]
		}
	default:
		return p, false
	}
	if p.spanMS < 1000 {
		return p, false
	}
	secs := float64(p.spanMS) / 1000
	for i, c := range nodeCounters {
		if p.has[i] && float64(p.inc[i])/secs > c.cap {
			return p, false
		}
	}
	return p, true
}

type nodeBucket struct {
	start                      time.Time
	readings                   int
	battery, temp, noise, rssi mean
	snr, qlen                  mean
	uptime                     *int64
	pairs                      int
	inc                        [5]int64
	carried                    [5]int // contributing pairs that had the counter
	spanMS                     int64
}

func (b *nodeBucket) addPair(p counterPair) {
	b.pairs++
	b.spanMS += p.spanMS
	for i := range p.inc {
		if p.has[i] {
			b.inc[i] += p.inc[i]
			b.carried[i]++
		}
	}
}

// BucketNodeReadings folds readings (ascending, with any before since serving only as the first
// pair's predecessor) into fixed UTC buckets of size. Buckets with no readings are not emitted.
// Counter increments come from consecutive pairs of status readings, credited to the bucket of
// the later one.
func BucketNodeReadings(readings []NodeReading, since time.Time, size time.Duration) []NodeTelemetryPoint {
	var prevStatus *NodeReading
	var buckets []*nodeBucket
	for i := range readings {
		r := &readings[i]
		var pair counterPair
		var paired bool
		if isStatusReading(r) {
			if prevStatus != nil {
				pair, paired = pairIncrements(prevStatus, r)
			}
			prevStatus = r
		}
		if r.SampledAt.Before(since) {
			continue
		}

		start := r.SampledAt.UTC().Truncate(size)
		if len(buckets) == 0 || !buckets[len(buckets)-1].start.Equal(start) {
			buckets = append(buckets, &nodeBucket{start: start})
		}
		cur := buckets[len(buckets)-1]
		cur.readings++
		if paired {
			cur.addPair(pair)
		}
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
		if cur.pairs > 0 {
			p.CounterSpanSecs = ptr(cur.spanMS / 1000)
			// A counter missing from any contributing pair would understate its rate, so it is left out.
			full := func(i int) bool { return cur.carried[i] == cur.pairs }
			if full(ctrTx) {
				p.AirtimeTxSecs = ptr(float64(cur.inc[ctrTx]))
			}
			if full(ctrRx) {
				p.AirtimeRxSecs = ptr(float64(cur.inc[ctrRx]))
			}
			if full(ctrRecv) {
				p.PacketsReceived = ptr(cur.inc[ctrRecv])
			}
			if full(ctrSent) {
				p.PacketsSent = ptr(cur.inc[ctrSent])
			}
			if full(ctrErrs) {
				p.ReceiveErrors = ptr(cur.inc[ctrErrs])
			}
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
