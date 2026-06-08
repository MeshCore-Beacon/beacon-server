// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: agpl

// Package handlers provides HTTP route handlers for the Beacon REST API.
package handlers

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"
)

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
		log.Printf("api: failed to encode response: %v", err)
	}
}

// respondError writes a standard JSON error response.
// The error code is derived automatically from the HTTP status text.
func respondError(w http.ResponseWriter, status int, message string) {
	if status >= 500 {
		log.Printf("api: error %d: %s", status, message)
	}
	code := strings.ToLower(strings.ReplaceAll(http.StatusText(status), " ", "_"))
	respond(w, status, map[string]APIError{"error": {Code: code, Message: message}})
}
