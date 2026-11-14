// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"strings"
	"testing"
)

type sinkCall struct {
	kind, key string
	raw       []byte
}

type fakeSink struct{ calls []sinkCall }

func (f *fakeSink) HandleClaim(_ context.Context, key string, raw []byte) {
	f.calls = append(f.calls, sinkCall{"claim", key, raw})
}

func (f *fakeSink) HandleReading(_ context.Context, key string, raw []byte) {
	f.calls = append(f.calls, sinkCall{"reading", key, raw})
}

func pollingMsg(iata, key, sub string) queuedTestMessage {
	return queuedTestMessage{malformedTopicMessage{"meshcore/" + iata + "/" + key + "/" + sub}, []byte(`{"x":1}`)}
}

func TestPollingTopicsReachSink(t *testing.T) {
	key := strings.Repeat("aB", 32)
	for sub, kind := range map[string]string{"polling": "claim", "telemetry": "reading", "battery": "reading"} {
		t.Run(sub, func(t *testing.T) {
			w, _ := newTestWorker()
			w.db = nil // the sink path must not touch the DB
			sink := &fakeSink{}
			w.SetPolling(sink)
			w.handleMessage(pollingMsg("YOW", key, sub))
			if len(sink.calls) != 1 {
				t.Fatalf("got %d sink calls, want 1", len(sink.calls))
			}
			c := sink.calls[0]
			if c.kind != kind || c.key != key || string(c.raw) != `{"x":1}` {
				t.Fatalf("unexpected call %+v", c)
			}
		})
	}
}

func TestPollingTopicsIgnoredWithoutSink(t *testing.T) {
	w, _ := newTestWorker()
	w.db = nil
	for _, sub := range []string{"polling", "telemetry", "battery"} {
		w.handleMessage(pollingMsg("YOW", strings.Repeat("ab", 32), sub))
	}
}

func TestPollingTopicsRespectIATAFilter(t *testing.T) {
	w, _ := newTestWorker()
	w.cfg.AllowedIATAs = map[string]struct{}{"SEA": {}}
	sink := &fakeSink{}
	w.SetPolling(sink)
	w.handleMessage(pollingMsg("YOW", strings.Repeat("ab", 32), "polling"))
	if len(sink.calls) != 0 {
		t.Fatalf("sink called for filtered IATA: %+v", sink.calls)
	}
}

func TestPollingTopicBadKey(t *testing.T) {
	w, _ := newTestWorker()
	sink := &fakeSink{}
	w.SetPolling(sink)
	w.handleMessage(pollingMsg("YOW", strings.Repeat("ab", 31)+"a", "polling"))
	if len(sink.calls) != 0 {
		t.Fatalf("sink called for bad key: %+v", sink.calls)
	}
}
