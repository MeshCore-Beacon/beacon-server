// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"errors"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

const maxListLimit int32 = 200

const clockSkewTolerance = 5 * time.Minute // client clocks run ahead; accept a small future 'until'

// parseLimit caps the requested result size while retaining each endpoint's default.
func parseLimit(r *http.Request, defaultLimit int32) (int32, error) {
	value := r.URL.Query().Get("limit")
	if value == "" {
		return defaultLimit, nil
	}
	limit, err := strconv.ParseInt(value, 10, 32)
	if err != nil {
		return 0, errors.New("limit must be an integer")
	}
	if limit <= 0 {
		return 0, errors.New("limit must be positive")
	}
	if limit > int64(maxListLimit) {
		slog.Info("api: clamped list limit", "component", "api", "requested_limit", limit, "limit", maxListLimit)
		return maxListLimit, nil
	}
	return int32(limit), nil
}
