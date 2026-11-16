// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package metrics

import "github.com/prometheus/client_golang/prometheus"

var CacheRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "beacon_cache_requests_total", Help: "Cache lookups by bounded outcome (hit, miss, invalid, error).",
}, []string{"outcome"})

func init() { Registry.MustRegister(CacheRequests) }
