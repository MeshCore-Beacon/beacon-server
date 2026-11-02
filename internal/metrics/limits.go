// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package metrics

import "github.com/prometheus/client_golang/prometheus"

var (
	CacheRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_cache_requests_total", Help: "Cache lookups by bounded outcome (hit, miss, invalid, error).",
	}, []string{"outcome"})
	IngestRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_ingest_rejected_total", Help: "Input rejected before processing, by bounded reason.",
	}, []string{"reason"})
	StreamDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "beacon_stream_dropped_total", Help: "Live events dropped by queue (global, client, repeat).",
	}, []string{"queue"})
)

func init() { Registry.MustRegister(CacheRequests, IngestRejected, StreamDropped) }
