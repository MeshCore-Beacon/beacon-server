// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package middleware

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
)

func csvOf(n int) string {
	parts := make([]string, n)
	for i := range parts {
		parts[i] = strconv.Itoa(i % 100)
	}
	return strings.Join(parts, ",")
}

func serveListLimit(t *testing.T, rawQuery string) (*httptest.ResponseRecorder, bool) {
	t.Helper()
	reached := false
	h := LimitListParams(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reached = true
		w.WriteHeader(http.StatusNoContent)
	}))
	req := httptest.NewRequest(http.MethodGet, "/api/v1/packets?"+rawQuery, nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)
	return rec, reached
}

func TestLimitListParamsPerParam(t *testing.T) {
	for _, param := range []string{"iatas", "scopes", "payloadTypes", "routeTypes"} {
		t.Run(param, func(t *testing.T) {
			rec, reached := serveListLimit(t, param+"="+csvOf(MaxListParamValues))
			if !reached || rec.Code != http.StatusNoContent {
				t.Fatalf("%d values: status %d reached %v, want pass-through", MaxListParamValues, rec.Code, reached)
			}

			rec, reached = serveListLimit(t, param+"="+csvOf(MaxListParamValues+1))
			if reached {
				t.Fatal("handler reached with too many values")
			}
			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status %d, want 400", rec.Code)
			}
			if ct := rec.Header().Get("Content-Type"); ct != "application/json" {
				t.Fatalf("Content-Type %q", ct)
			}
			var body struct {
				Error struct {
					Code    string `json:"code"`
					Message string `json:"message"`
				} `json:"error"`
			}
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
				t.Fatalf("decode body %q: %v", rec.Body.String(), err)
			}
			if body.Error.Code != "bad_request" || body.Error.Message != param+": at most 1000 values" {
				t.Fatalf("body %+v", body.Error)
			}
		})
	}
}

func TestLimitListParamsCountsNonEmptyEntries(t *testing.T) {
	// Empty and blank entries are dropped by the handlers, so they do not count.
	q := "iatas=" + csvOf(MaxListParamValues) + strings.Repeat(",%20,", 500)
	if rec, reached := serveListLimit(t, q); !reached || rec.Code != http.StatusNoContent {
		t.Fatalf("status %d reached %v, want pass-through", rec.Code, reached)
	}
}

func TestLimitListParamsChecksEveryValue(t *testing.T) {
	q := "iatas=YOW&iatas=" + csvOf(MaxListParamValues+1)
	if rec, reached := serveListLimit(t, q); reached || rec.Code != http.StatusBadRequest {
		t.Fatalf("status %d reached %v, want 400", rec.Code, reached)
	}
}

func TestLimitListParamsLeavesOtherRequestsAlone(t *testing.T) {
	for _, q := range []string{"", "limit=50", "iata=YOW", "name=" + csvOf(MaxListParamValues+1), "iatas=YOW,YYZ&scopes=%23bc"} {
		if rec, reached := serveListLimit(t, q); !reached || rec.Code != http.StatusNoContent {
			t.Fatalf("query %.40q: status %d reached %v, want pass-through", q, rec.Code, reached)
		}
	}
}
