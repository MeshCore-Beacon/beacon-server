// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"

	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
)

// Models the pages read, as beacon_api_unavailable_total labels them.
const (
	modelPacketSites  = "packet_sites"
	modelCmi          = "cmi"
	modelPayloadTypes = "observation_payload_types"
	modelNone         = "none" // a page that reads no read model
)

// respondUnavailableFor is respondUnavailable for a page that reads model; it counts each 503 it writes.
func respondUnavailableFor(w http.ResponseWriter, err error, model string) bool {
	if !respondUnavailable(w, err) {
		return false
	}
	reason := "not_ready"
	if isQueryTimeout(err) {
		reason = "timeout"
	}
	metrics.APIUnavailable.WithLabelValues(reason, model).Inc()
	return true
}
