// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package metrics

import "github.com/prometheus/client_golang/prometheus"

var IngestRejected = prometheus.NewCounterVec(prometheus.CounterOpts{
	Name: "beacon_ingest_rejected_total", Help: "Input rejected before processing, by bounded reason.",
}, []string{"reason"})

func init() { Registry.MustRegister(IngestRejected) }
