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

// The map and neighbour graph load every node of a region; a node row is cheap (2000 rows cost about
// twice the database time of 200), so bigger pages cut a whole-mesh load from ~390 requests to ~40.
const maxNodeListLimit int32 = 2000

const clockSkewTolerance = 5 * time.Minute // client clocks run ahead; accept a small future 'until'

// parseLimit caps the requested result size while retaining each endpoint's default.
func parseLimit(r *http.Request, defaultLimit int32) (int32, error) {
	return parseLimitUpTo(r, defaultLimit, maxListLimit)
}

// parseLimitUpTo is parseLimit with an endpoint's own maximum.
func parseLimitUpTo(r *http.Request, defaultLimit, maxLimit int32) (int32, error) {
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
	if limit > int64(maxLimit) {
		slog.Info("api: clamped list limit", "component", "api", "requested_limit", limit, "limit", maxLimit)
		return maxLimit, nil
	}
	return int32(limit), nil
}
