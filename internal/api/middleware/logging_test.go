// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"bytes"
	"encoding/json"
	"log"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
)

func TestRequestLogging(t *testing.T) {
	for _, tc := range []struct {
		status    int
		panic     bool
		level     slog.Level
		wantLevel string
	}{
		{200, false, slog.LevelInfo, "INFO"}, {429, false, slog.LevelWarn, "WARN"},
		{500, false, slog.LevelError, "ERROR"}, {200, false, slog.LevelWarn, ""}, {500, true, slog.LevelError, "ERROR"},
	} {
		t.Run(tc.level.String()+"/"+http.StatusText(tc.status), func(t *testing.T) {
			var out bytes.Buffer
			previous, writer, flags := slog.Default(), log.Writer(), log.Flags()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&out, &slog.HandlerOptions{Level: tc.level})))
			t.Cleanup(func() { slog.SetDefault(previous); log.SetOutput(writer); log.SetFlags(flags) })
			r := chi.NewRouter()
			r.Use(chimw.RequestID)
			r.Use(TrustedProxyIP([]netip.Prefix{netip.MustParsePrefix("192.0.2.0/24")}))
			r.Use(RequestLogger)
			r.Use(chimw.Recoverer)
			r.Get("/items/{id}", func(w http.ResponseWriter, _ *http.Request) {
				if tc.panic {
					panic("test panic")
				}
				w.WriteHeader(tc.status)
			})
			req := httptest.NewRequest(http.MethodGet, "/items/private-id?token=private-token", nil)
			req.RemoteAddr = "192.0.2.1:4444"
			req.Header.Set("X-Real-IP", "203.0.113.7")
			req.Header.Set("True-Client-IP", "198.51.100.8")
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != tc.status {
				t.Fatalf("status=%d", w.Code)
			}
			if tc.wantLevel == "" {
				if out.Len() != 0 {
					t.Fatal("info request was not filtered")
				}
				return
			}
			if strings.Contains(out.String(), "private-id") || strings.Contains(out.String(), "private-token") || strings.Contains(out.String(), "198.51.100.8") {
				t.Fatal("request values leaked or spoofed identity used")
			}
			var completed bool
			for _, line := range bytes.Split(bytes.TrimSpace(out.Bytes()), []byte("\n")) {
				var record map[string]any
				if err := json.Unmarshal(line, &record); err != nil {
					t.Fatal(err)
				}
				if record["level"] != tc.wantLevel || record["component"] != "http" || record["client_ip"] != "203.0.113.7" || record["path"] != "/items/{id}" {
					t.Fatalf("bad record: %v", record)
				}
				if record["msg"] == "request complete" {
					completed = true
					if record["status"] != float64(tc.status) {
						t.Fatal("wrong logged status")
					}
				}
			}
			if !completed {
				t.Fatal("request completion missing")
			}
		})
	}
}
