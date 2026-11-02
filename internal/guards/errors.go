// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package guards

import (
	"encoding/json"
	"net/http"
	"strconv"
)

// Refusal is the response a guard sends instead of the handler's.
type Refusal struct {
	Guard      string
	Status     int
	RetryAfter int
	Code       string
	Message    string
}

func (r *Refusal) Error() string { return "guard " + r.Guard + ": " + r.Message }

func deadlineRefusal() *Refusal {
	return &Refusal{Guard: "deadline", Status: http.StatusServiceUnavailable, RetryAfter: 5,
		Code: "deadline_exceeded", Message: "The request ran past its database time limit. Retry later."}
}

func admissionRefusal() *Refusal {
	return &Refusal{Guard: "admission", Status: http.StatusServiceUnavailable, RetryAfter: 2,
		Code: "service_unavailable", Message: "The server is busy. Retry later."}
}

func capRefusal() *Refusal {
	return &Refusal{Guard: "cap", Status: http.StatusTooManyRequests, RetryAfter: 1,
		Code: "rate_limited", Message: "Too many concurrent requests from this client. Retry later."}
}

// WriteRefusal writes r in the API's standard error envelope.
func WriteRefusal(w http.ResponseWriter, r *Refusal) {
	body, _ := json.Marshal(map[string]map[string]string{"error": {"code": r.Code, "message": r.Message}})
	w.Header().Set("Retry-After", strconv.Itoa(r.RetryAfter))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(r.Status)
	_, _ = w.Write(body)
}
