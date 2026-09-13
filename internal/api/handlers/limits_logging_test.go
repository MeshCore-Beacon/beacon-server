// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"bytes"
	"encoding/json"
	"log"
	"log/slog"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestLimitLogOmitsRequestValues(t *testing.T) {
	var out bytes.Buffer
	previous, writer, flags := slog.Default(), log.Writer(), log.Flags()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&out, nil)))
	t.Cleanup(func() { slog.SetDefault(previous); log.SetOutput(writer); log.SetFlags(flags) })
	req := httptest.NewRequest("GET", "/private-path%0Aforged?limit=1000", nil)
	req.RemoteAddr = "private-peer"
	limit, err := parseLimit(req, 50)
	if err != nil || limit != 200 {
		t.Fatalf("limit=%d error=%v", limit, err)
	}
	if strings.Contains(out.String(), "private-") || strings.Contains(out.String(), "forged") {
		t.Fatal("limit log retained request identity")
	}
	var record map[string]any
	if err := json.Unmarshal(out.Bytes(), &record); err != nil {
		t.Fatal(err)
	}
	if record["requested_limit"] != float64(1000) || record["limit"] != float64(200) || record["component"] != "api" {
		t.Fatalf("clamp diagnostics missing: %v", record)
	}
}
