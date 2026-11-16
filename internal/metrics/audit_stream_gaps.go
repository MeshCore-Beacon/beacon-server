// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package metrics

import "github.com/prometheus/client_golang/prometheus"

var StreamDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "beacon_stream_dropped_total", Help: "Live events dropped by queue (global, client, repeat).",
}, []string{"queue"})

func init() { Registry.MustRegister(StreamDropped) }
