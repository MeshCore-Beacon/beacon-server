// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/google/uuid"
)

type observerCountDB struct {
	*frameCaptureDB
	observers int
}

func (s *observerCountDB) UpsertObserver(context.Context, []byte, string) (uuid.UUID, string, error) {
	s.observers++
	return uuid.Nil, "", nil
}

func TestTopicPubkeyMustBe32Bytes(t *testing.T) {
	var envelope map[string]string
	if err := json.Unmarshal(packetEnvelope(t, buildGrpTxtPacket(t, 0x1a, make([]byte, 16))), &envelope); err != nil {
		t.Fatal(err)
	}
	envelope["timestamp"] = time.Now().UTC().Add(-time.Hour).Format(time.RFC3339) // takes the clamp log path
	body, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"ab", strings.Repeat("ab", 31), strings.Repeat("ab", 33), strings.Repeat("zz", 32)} {
		for _, sub := range []string{"packets", "status", "neighbors"} {
			t.Run(sub+"/"+key, func(t *testing.T) {
				w, base := newTestWorker()
				db := &observerCountDB{frameCaptureDB: &frameCaptureDB{stubDB: base}}
				w.db = db
				w.handleMessage(queuedTestMessage{malformedTopicMessage{"meshcore/YOW/" + key + "/" + sub}, body})
				if db.observers != 0 || len(db.packets) != 0 {
					t.Fatalf("malformed pubkey processed: %d observers, %d packets", db.observers, len(db.packets))
				}
			})
		}
	}
	w, base := newTestWorker()
	db := &observerCountDB{frameCaptureDB: &frameCaptureDB{stubDB: base}}
	w.db = db
	w.handleMessage(queuedTestMessage{malformedTopicMessage{"meshcore/YOW/" + strings.Repeat("AB", 32) + "/packets"}, body})
	if len(db.packets) != 1 {
		t.Fatal("valid pubkey dropped")
	}
}

func TestQueueSurvivesHandlerPanic(t *testing.T) {
	done := make(chan struct{})
	q := newMessageQueue(1, 8, 4096, func(_ context.Context, m mqtt.Message) {
		if string(m.Payload()) == "0" {
			panic("bad message")
		}
		close(done)
	})
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = q.close(ctx)
	})
	q.enqueue(queueMessage("aa", "packets", 0))
	q.enqueue(queueMessage("aa", "packets", 1))
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("lane stopped after a panic")
	}
}
