// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package main

import (
	"log/slog"
	"net/http"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
)

// newHTTPServer bounds how long a client may take to send headers and how long an idle
// keep-alive connection stays open. There is no read or write timeout: backup downloads and
// WebSocket connections run for as long as they need.
func newHTTPServer(addr string, h http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           h,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		ErrorLog:          slog.NewLogLogger(slog.Default().With("component", "http").Handler(), slog.LevelError),
	}
}

// metricsMux serves GET /metrics and nothing else.
func metricsMux() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", metrics.Handler())
	return mux
}
