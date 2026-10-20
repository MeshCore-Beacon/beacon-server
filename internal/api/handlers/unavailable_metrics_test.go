// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// Each 503 a page answers is counted by reason and by the model the page reads; the busy-pool warning is logged at
// most once a minute, not once a request.
func TestUnavailableCountedByReasonAndModel(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	slowWarnAt.Store(0)

	models := map[string]string{
		"region packets":           "packet_sites",
		"messages across channels": "cmi",
		"channel messages":         "cmi",
		"observer adverts":         "observation_payload_types",
	}
	for _, c := range unavailablePages {
		before := testutil.ToFloat64(metrics.APIUnavailable.WithLabelValues("slow_pool_busy", models[c.name]))
		for range 2 {
			rec := httptest.NewRecorder()
			c.mount(failingReader(fmt.Errorf("x: %w", api.ErrSlowPoolBusy))).ServeHTTP(rec, httptest.NewRequest("GET", c.path, nil))
			if rec.Code != 503 {
				t.Fatalf("%s: status %d", c.name, rec.Code)
			}
		}
		if got := testutil.ToFloat64(metrics.APIUnavailable.WithLabelValues("slow_pool_busy", models[c.name])) - before; got != 2 {
			t.Errorf("%s: counted %v slow_pool_busy, want 2", c.name, got)
		}
	}
	if n := strings.Count(logs.String(), "every slow connection is busy"); n != 1 {
		t.Errorf("busy warning logged %d times for 8 refusals, want 1", n)
	}
}
