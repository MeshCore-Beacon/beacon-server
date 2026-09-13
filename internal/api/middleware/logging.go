// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"fmt"
	"log/slog"
	"net/http"
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
	path := e.request.URL.Path
	if ctx := chi.RouteContext(e.request.Context()); ctx != nil && ctx.RoutePattern() != "" {
		path = ctx.RoutePattern()
	}
	return slog.Default().With("component", "http", "method", e.request.Method, "path", path,
		"client_ip", e.request.RemoteAddr, "request_id", chimw.GetReqID(e.request.Context()))
}

func (e *requestLogEntry) Write(status, bytes int, _ http.Header, elapsed time.Duration, _ any) {
	if status == 0 {
		status = http.StatusOK
	}
	level := slog.LevelInfo
	if status >= 500 {
		level = slog.LevelError
	} else if status >= 400 {
		level = slog.LevelWarn
	}
	if !slog.Default().Enabled(e.request.Context(), level) {
		return
	}
	e.logger().Log(e.request.Context(), level, "request complete", "status", status, "bytes", bytes, "duration_ms", float64(elapsed)/float64(time.Millisecond))
}

func (e *requestLogEntry) Panic(value any, stack []byte) {
	e.logger().ErrorContext(e.request.Context(), "request panic", "panic", fmt.Sprint(value), "stack", string(stack))
}
