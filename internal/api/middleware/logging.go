// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/go-chi/chi/v5"
	chimw "github.com/go-chi/chi/v5/middleware"
)

// RequestLogger uses chi's response wrapper to preserve streaming/hijacking support.
// Place it after TrustedProxyIP and RequestID, and before Recoverer.
func RequestLogger(next http.Handler) http.Handler {
	return chimw.RequestLogger(requestLogFormatter{})(next)
}

type requestLogFormatter struct{}
type requestLogEntry struct{ request *http.Request }

func (requestLogFormatter) NewLogEntry(r *http.Request) chimw.LogEntry {
	return &requestLogEntry{request: r}
}

func (e *requestLogEntry) logger() *slog.Logger {
	path := "unmatched"
	if ctx := chi.RouteContext(e.request.Context()); ctx != nil && ctx.RoutePattern() != "" {
		path = ctx.RoutePattern()
	}
	return slog.Default().With("component", "http", "method", singleLineLogValue(e.request.Method), "path", path,
		"client_ip", singleLineLogValue(e.request.RemoteAddr), "request_id", singleLineLogValue(chimw.GetReqID(e.request.Context())))
}

// Strip line breaks before request fields reach a handler or downstream log consumer.
func singleLineLogValue(value string) string {
	value = strings.ReplaceAll(value, "\r", "")
	return strings.ReplaceAll(value, "\n", "")
}

func (e *requestLogEntry) Write(status, bytes int, header http.Header, elapsed time.Duration, _ any) {
	if status == 0 {
		status = http.StatusOK
	}
	level := requestLogLevel(status, header)
	if !slog.Default().Enabled(e.request.Context(), level) {
		return
	}
	e.logger().Log(e.request.Context(), level, "request complete", "status", status, "bytes", bytes, "duration_ms", float64(elapsed)/float64(time.Millisecond))
}

// requestLogLevel is INFO below 400, WARN for 4xx and ERROR for 5xx. A 503 with Retry-After is a refusal the client
// is told to retry (read model not ready, slow pool busy, guard admission); it heals itself and is already counted
// in metrics, so it logs at WARN. A 503 without Retry-After is unexpected and stays at ERROR.
func requestLogLevel(status int, header http.Header) slog.Level {
	switch {
	case status == http.StatusServiceUnavailable && header.Get("Retry-After") != "":
		return slog.LevelWarn
	case status >= 500:
		return slog.LevelError
	case status >= 400:
		return slog.LevelWarn
	}
	return slog.LevelInfo
}

func (e *requestLogEntry) Panic(value any, stack []byte) {
	e.logger().ErrorContext(e.request.Context(), "request panic", "panic", fmt.Sprint(value), "stack", string(stack))
}
