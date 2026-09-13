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

func TestRequestLogInputBoundary(t *testing.T) {
	for _, format := range []string{"text", "json"} {
		for _, path := range []string{"/items/private-path", "/unmatched-private-path%0D%0Aforged"} {
			t.Run(format+path, func(t *testing.T) {
				var out bytes.Buffer
				previous, writer, flags := slog.Default(), log.Writer(), log.Flags()
				var handler slog.Handler = slog.NewTextHandler(&out, nil)
				if format == "json" {
					handler = slog.NewJSONHandler(&out, nil)
				}
				slog.SetDefault(slog.New(handler))
				t.Cleanup(func() { slog.SetDefault(previous); log.SetOutput(writer); log.SetFlags(flags) })
				r := chi.NewRouter()
				r.Use(chimw.RequestID, RequestLogger)
				r.Get("/items/{id}", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(200) })
				req := httptest.NewRequest(http.MethodGet, path+"?token=private-token", nil)
				req.RemoteAddr = "192.0.2.1:1234"
				req.Header.Set("X-Request-ID", "client\r\nforged")
				w := httptest.NewRecorder()
				r.ServeHTTP(w, req)
				wantPath, wantStatus := "/items/{id}", 200
				if strings.HasPrefix(path, "/unmatched") {
					wantPath, wantStatus = "unmatched", 404
				}
				if w.Code != wantStatus || strings.Count(out.String(), "\n") != 1 || strings.ContainsAny(strings.TrimSuffix(out.String(), "\n"), "\r\n") {
					t.Fatalf("status or log boundary changed: %d %q", w.Code, out.String())
				}
				if strings.Contains(out.String(), "private-path") || strings.Contains(out.String(), "private-token") {
					t.Fatal("raw path or query was retained")
				}
				if format == "json" {
					var record map[string]any
					if err := json.Unmarshal(out.Bytes(), &record); err != nil {
						t.Fatal(err)
					}
					if record["path"] != wantPath || record["request_id"] != "clientforged" || record["client_ip"] != "192.0.2.1:1234" {
						t.Fatalf("unexpected request fields: %v", record)
					}
				} else if !strings.Contains(out.String(), "path="+wantPath) || !strings.Contains(out.String(), "request_id=clientforged") {
					t.Fatalf("unexpected text fields: %s", out.String())
				}
			})
		}
	}
}

func TestSingleLineLogValue(t *testing.T) {
	for _, tc := range []struct{ input, want string }{
		{"", ""}, {"GET", "GET"}, {"[2001:db8::1]:80", "[2001:db8::1]:80"},
		{"client\r\nentry\n", "cliententry"}, {"café", "café"},
	} {
		if got := singleLineLogValue(tc.input); got != tc.want {
			t.Errorf("got %q, want %q", got, tc.want)
		}
	}
}
