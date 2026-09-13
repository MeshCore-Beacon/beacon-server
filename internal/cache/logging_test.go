// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package cache

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"log/slog"
	"strings"
	"testing"
	"time"
)

func TestCacheDiagnostics(t *testing.T) {
	previous, writer, flags := slog.Default(), log.Writer(), log.Flags()
	t.Cleanup(func() { slog.SetDefault(previous); log.SetOutput(writer); log.SetFlags(flags) })
	var out bytes.Buffer
	slog.SetDefault(slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: slog.LevelDebug})))
	c, mr := newTestClient(t)
	key, value := "private-cache-key", "private-cache-value"
	calls := 0
	fetch := func() (string, error) { calls++; return value, nil }
	for i := 0; i < 2; i++ {
		got, err := getOrSet(context.Background(), c, key, time.Minute, fetch)
		if err != nil || got != value {
			t.Fatalf("cache result changed: %v", err)
		}
	}
	if calls != 1 {
		t.Fatalf("fetches=%d, want 1", calls)
	}
	mr.Set(key, "invalid JSON")
	if _, err := getOrSet(context.Background(), c, key, time.Minute, fetch); err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(out.Bytes()), []byte("\n"))
	want := []string{"cache miss", "cache hit", "cache invalid entry"}
	if len(lines) != len(want) {
		t.Fatalf("got %d records, want %d", len(lines), len(want))
	}
	for i, line := range lines {
		var record map[string]any
		if err := json.Unmarshal(line, &record); err != nil {
			t.Fatal(err)
		}
		if record["component"] != "cache" || record["level"] != "DEBUG" || record["msg"] != want[i] {
			t.Fatalf("unexpected record: %v", record)
		}
	}
	if strings.Contains(out.String(), key) || strings.Contains(out.String(), value) {
		t.Fatal("cache key or payload leaked")
	}
	out.Reset()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&out, nil)))
	if _, err := getOrSet(context.Background(), c, key, time.Minute, fetch); err != nil {
		t.Fatal(err)
	}
	if out.Len() != 0 {
		t.Fatal("debug diagnostics appeared at info level")
	}
}
