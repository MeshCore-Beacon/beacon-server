// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package router

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNodeTelemetryOptionalRoutesAndCapability(t *testing.T) {
	disabled := New(nil, nil, nil, Options{})
	response := httptest.NewRecorder()
	disabled.ServeHTTP(response, httptest.NewRequest("GET", "/api/v1/node-telemetry/availability", nil))
	if response.Code != 404 {
		t.Fatal("intake enabled without configuration")
	}
	enabled := New(nil, nil, nil, Options{NodeTelemetry: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/availability" {
			t.Errorf("incorrect gateway path: %s", r.URL.Path)
		}
		_, _ = io.WriteString(w, `{"items":[]}`)
	})})
	response = httptest.NewRecorder()
	enabled.ServeHTTP(response, httptest.NewRequest("GET", "/api/v1/node-telemetry/availability", nil))
	if response.Code != 200 || response.Body.String() != `{"items":[]}` {
		t.Fatal(response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	enabled.ServeHTTP(response, httptest.NewRequest("GET", "/api/v1/info", nil))
	if !strings.Contains(response.Body.String(), `"collectedNodeTelemetry":true`) {
		t.Fatal(response.Body.String())
	}
}
