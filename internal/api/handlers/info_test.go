// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

func TestGetInfo(t *testing.T) {
	version, web := "0.1.1", "2.0.3"
	for _, tc := range []struct {
		name string
		info api.Info
		want string
	}{
		{"unset", api.Info{ServerVersion: "2.0.2", Features: api.InfoFeatures{Accounts: "off"}}, `{"minAppVersion":null,"minWebVersion":null,"serverVersion":"2.0.2","features":{"polling":false,"accounts":"off","registration":false,"telemetryRequests":false,"alerts":false,"atlasTelemetry":false,"mapTelemetry":false},"display":{"temperatureUnit":"","distanceUnit":""},"limits":{"atlas":0}}`},
		{"set", api.Info{MinAppVersion: &version, MinWebVersion: &web, ServerVersion: "2.0.2", Features: api.InfoFeatures{Polling: true, Accounts: "off"}}, `{"minAppVersion":"0.1.1","minWebVersion":"2.0.3","serverVersion":"2.0.2","features":{"polling":true,"accounts":"off","registration":false,"telemetryRequests":false,"alerts":false,"atlasTelemetry":false,"mapTelemetry":false},"display":{"temperatureUnit":"","distanceUnit":""},"limits":{"atlas":0}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			w := httptest.NewRecorder()
			InfoRouter(tc.info).ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
			if w.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d", w.Code)
			}
			if got := w.Header().Get("Cache-Control"); got != "no-cache" {
				t.Fatalf("Cache-Control = %q, want no-cache", got)
			}
			if got := strings.TrimSpace(w.Body.String()); got != tc.want {
				t.Fatalf("body = %s, want %s", got, tc.want)
			}
		})
	}
}
