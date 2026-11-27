// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
)

func TestTelemetryRequestReasonCode(t *testing.T) {
	off := NewTelemetryRequest(polling.RequestRow{Reason: polling.ReasonOffline(3)})
	if off.StatusReasonCode != "offline" || off.StatusReasonDays == nil || *off.StatusReasonDays != 3 {
		t.Fatalf("%+v", off)
	}
	none := NewTelemetryRequest(polling.RequestRow{})
	b, _ := json.Marshal(none)
	if none.StatusReasonCode != "" || !strings.Contains(string(b), `"statusReasonCode":""`) || strings.Contains(string(b), "statusReasonDays") {
		t.Fatalf("%s", b)
	}
}
