// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package ingest

import (
	"context"
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
