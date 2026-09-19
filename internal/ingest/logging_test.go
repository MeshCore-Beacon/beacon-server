// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"strconv"
	"strings"
	"testing"
)

type malformedTopicMessage struct{ topic string }

func (m malformedTopicMessage) Duplicate() bool   { return false }
func (m malformedTopicMessage) Qos() byte         { return 0 }
func (m malformedTopicMessage) Retained() bool    { return false }
func (m malformedTopicMessage) Topic() string     { return m.topic }
func (m malformedTopicMessage) MessageID() uint16 { return 0 }
func (m malformedTopicMessage) Payload() []byte   { return nil }
func (m malformedTopicMessage) Ack()              {}

func TestMalformedIATAWarnNamesSource(t *testing.T) {
	for _, format := range []string{"json", "text"} {
		for _, iata := range []string{"XX", "XX\r\nforged=true"} {
			t.Run(format+"/"+strconv.Quote(iata), func(t *testing.T) {
				var output bytes.Buffer
				var handler slog.Handler = slog.NewJSONHandler(&output, nil)
				if format == "text" {
					handler = slog.NewTextHandler(&output, nil)
				}
				previous := slog.Default()
				slog.SetDefault(slog.New(handler))
				t.Cleanup(func() { slog.SetDefault(previous) })
				worker, _ := newTestWorker()
				topic := "meshcore/" + iata + "/0011223344556677/packets"
				worker.handleMessage(malformedTopicMessage{topic: topic})
				if bytes.Count(output.Bytes(), []byte("\n")) != 1 || bytes.ContainsRune(output.Bytes(), '\r') {
					t.Fatalf("warning did not remain one escaped record: %q", output.String())
				}
				if format == "json" {
					var record map[string]any
					if err := json.Unmarshal(output.Bytes(), &record); err != nil {
						t.Fatal(err)
					}
					if record["iata"] != iata || record["topic"] != topic || record["msg"] != "dropped packet with malformed IATA" {
						t.Fatalf("warning lost its source: %v", record)
					}
				} else {
					expectedIATA, expectedTopic := iata, topic
					if strings.ContainsAny(iata, "\r\n") {
						expectedIATA, expectedTopic = strconv.Quote(iata), strconv.Quote(topic)
					}
					if !strings.Contains(output.String(), "iata="+expectedIATA) || !strings.Contains(output.String(), "topic="+expectedTopic) {
						t.Fatalf("warning lost its source: %q", output.String())
					}
				}
			})
		}
	}
}
