// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package handlers provides HTTP route handlers for the Beacon REST API.
package handlers

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/guards"
)

// respondUnavailable answers a reader error the client should retry with 503 and Retry-After, in the same shape as
// the guards' refusals, and reports whether it did.
func respondUnavailable(w http.ResponseWriter, err error) bool {
	switch {
	case errors.Is(err, api.ErrPagesNotReady):
		guards.WriteRefusal(w, &guards.Refusal{Guard: "not_ready", Status: http.StatusServiceUnavailable, RetryAfter: 30,
			Code: "service_unavailable", Message: "This page is not available yet. Retry later."})
		return true
	case errors.Is(err, api.ErrSlowPoolBusy):
		warnSlowPoolBusy()
		guards.WriteRefusal(w, &guards.Refusal{Guard: "slow_pool", Status: http.StatusServiceUnavailable, RetryAfter: 5,
			Code: "service_unavailable", Message: "The server is busy with other slow requests. Retry later."})
		return true
	}
	return false
}

// APIError is the standard error response shape for all Beacon API endpoints.
// Code is a stable snake_case identifier derived from the HTTP status text,
// safe to match against in client code. Message is a human-readable description.
type APIError struct {
	Code    string `json:"code"`    // e.g. "not_found", "bad_request"
	Message string `json:"message"` // e.g. "channel not found"
}

// respond writes a JSON response with the given status code and data.
// Logs if encoding fails since the header is already written at that point.
func respond(w http.ResponseWriter, status int, data any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(data); err != nil {
		slog.Error("api: failed to encode response", "component", "api", "error", err)
	}
}

// respondError writes a standard JSON error response.
// The error code is derived automatically from the HTTP status text.
func respondError(w http.ResponseWriter, status int, message string) {
	if status >= 500 {
		slog.Error(fmt.Sprintf("api: error %d: %s", status, message), "component", "api")
	}
	code := strings.ToLower(strings.ReplaceAll(http.StatusText(status), " ", "_"))
	respond(w, status, map[string]APIError{"error": {Code: code, Message: message}})
}
