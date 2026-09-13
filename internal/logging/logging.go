// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"

	"github.com/MeshCore-Beacon/beacon-server/internal/config"
)

// New resolves environment overrides, validates options and writes to out.
// Empty settings default to info/text; callers install the logger before starting workers.
func New(out io.Writer, cfg config.LogConfig) (*slog.Logger, error) {
	if value := os.Getenv("LOG_LEVEL"); value != "" {
		cfg.Level = value
	}
	if value := os.Getenv("LOG_FORMAT"); value != "" {
		cfg.Format = value
	}
	var level slog.Level
	switch strings.ToLower(strings.TrimSpace(cfg.Level)) {
	case "", "info":
		level = slog.LevelInfo
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		return nil, fmt.Errorf("log level must be debug, info, warn or error")
	}
	opts := &slog.HandlerOptions{Level: level}
	switch strings.ToLower(strings.TrimSpace(cfg.Format)) {
	case "", "text":
		return slog.New(slog.NewTextHandler(out, opts)), nil
	case "json":
		return slog.New(slog.NewJSONHandler(out, opts)), nil
	default:
		return nil, fmt.Errorf("log format must be text or json")
	}
}
