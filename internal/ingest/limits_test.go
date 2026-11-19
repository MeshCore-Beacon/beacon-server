// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"fmt"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"strings"
	"testing"
)

func TestEnvelopeLimitBeforeQueueAdmission(t *testing.T) {
	q := newMessageQueue(1, 1, 2*maxEnvelopeBytes, func(context.Context, mqtt.Message) { t.Error("oversized input reached handler") })
	defer q.close(context.Background())
	m := queuedTestMessage{malformedTopicMessage{"meshcore/SEA/" + strings.Repeat("a", 64) + "/packets"}, make([]byte, maxEnvelopeBytes+1)}
	if q.enqueue(m) {
		t.Fatal("oversized envelope admitted")
	}
	m.payload = []byte(`{}`)
	m.malformedTopicMessage = malformedTopicMessage{strings.Repeat("x", 257)}
	if q.enqueue(m) {
		t.Fatal("oversized topic admitted")
	}
}

// One bad entry costs only itself: the valid neighbors and the observer's own scope are still stored.
func TestNeighborReportSkipsBadEntries(t *testing.T) {
	w, base := newTestWorker()
	d := &neighborCaptureDB{stubDB: base}
	w.db = d
	entry := func(key string) string {
		return `{"pubkey":"` + key + `","status":"responded","scopes":"#` + key[:2] + `"}`
	}
	entries := []string{entry(strings.Repeat("02", 32)), entry("03"), entry("zz"), entry(strings.Repeat("04", 32))}
	raw := `{"self":{"scopes":"#self"},"neighbors":[` + strings.Join(entries, ",") + `]}`
	w.handleNeighbors(context.Background(), "YOW", strings.Repeat("01", 32), []byte(raw))
	if d.selfScope != "#self" || len(d.scopes) != 2 || d.scopes[0] != "#02" || d.scopes[1] != "#04" {
		t.Fatalf("self=%q neighbors=%q", d.selfScope, d.scopes)
	}

	d.scopes = nil
	many := make([]string, maxNeighbors+5)
	for i := range many {
		many[i] = entry(fmt.Sprintf("%02x", i%200+16) + strings.Repeat("aa", 31))
	}
	w.handleNeighbors(context.Background(), "YOW", strings.Repeat("01", 32), []byte(`{"self":{},"neighbors":[`+strings.Join(many, ",")+`]}`))
	if len(d.scopes) != maxNeighbors {
		t.Fatalf("stored %d neighbors, want the first %d", len(d.scopes), maxNeighbors)
	}
}
