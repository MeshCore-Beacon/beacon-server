// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
)

func TestMalformedRouteAndTraceNumbersDoNotReadUnfilteredData(t *testing.T) {
	reads := 0
	reader := stubReader{
		listKnownRoutes: func(context.Context, []string, int32, time.Time, int64, int32) ([]api.KnownRoute, error) {
			reads++
			return nil, nil
		},
		listTraceTags: func(context.Context, []string, string, string, time.Time, time.Time, time.Time, string, int32) ([]api.TraceTagSummary, error) {
			reads++
			return nil, nil
		},
	}
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		params  []string
	}{
		{"routes", listKnownRoutes(reader), []string{"hopCount", "cursor"}},
		{"traces", listTraceTags(reader), []string{"since", "until", "cursor"}},
	} {
		for _, param := range tc.params {
			for _, value := range []string{"not-a-number", "9223372036854775808"} {
				t.Run(tc.name+"/"+param+"/"+value, func(t *testing.T) {
					w := httptest.NewRecorder()
					tc.handler(w, httptest.NewRequest(http.MethodGet, "/?"+param+"="+value, nil))
					if w.Code != http.StatusBadRequest {
						t.Fatalf("status = %d, want 400", w.Code)
					}
				})
			}
		}
	}
	if reads != 0 {
		t.Fatalf("invalid numeric filters reached the reader %d times", reads)
	}
}
