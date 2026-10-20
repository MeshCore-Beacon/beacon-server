// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
)

// Models the pages read, as beacon_api_unavailable_total labels them.
const (
	modelPacketSites  = "packet_sites"
	modelCmi          = "cmi"
	modelPayloadTypes = "observation_payload_types"
)

// respondUnavailableFor is respondUnavailable for a page that reads model; it counts each 503 it writes.
func respondUnavailableFor(w http.ResponseWriter, err error, model string) bool {
	if !respondUnavailable(w, err) {
		return false
	}
	metrics.APIUnavailable.WithLabelValues("slow_pool_busy", model).Inc()
	return true
}

// slowWarnEvery spaces the busy-pool warning; beacon_api_unavailable_total counts every refusal.
const slowWarnEvery = time.Minute

var slowWarnAt atomic.Int64 // unix ns of the last busy-pool warning

func warnSlowPoolBusy() {
	now := time.Now().UnixNano()
	if last := slowWarnAt.Load(); (last == 0 || now-last >= int64(slowWarnEvery)) && slowWarnAt.CompareAndSwap(last, now) {
		slog.Warn("api: slow requests refused, every slow connection is busy (counted in beacon_api_unavailable_total)", "component", "api")
	}
}
