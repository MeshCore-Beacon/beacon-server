// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: agpl

package handlers

import (
	"net/http"
	"net/url"
	"testing"
)

func TestParseIATAs_Single(t *testing.T) {
	r := &http.Request{URL: &url.URL{RawQuery: "iata=yvr"}}
	result := parseIATAs(r)
	if len(result) != 1 || result[0] != "YVR" {
		t.Errorf("expected [YVR], got %v", result)
	}
}

func TestParseIATAs_Multiple(t *testing.T) {
	r := &http.Request{URL: &url.URL{RawQuery: "iatas=yvr,yyj,yyc"}}
	result := parseIATAs(r)
	if len(result) != 3 {
		t.Fatalf("expected 3 IATAs, got %d", len(result))
	}
	if result[0] != "YVR" || result[1] != "YYJ" || result[2] != "YYC" {
		t.Errorf("unexpected IATAs: %v", result)
	}
}

func TestParseIATAs_MultiplePreferredOverSingle(t *testing.T) {
	// iatas param takes precedence over iata
	r := &http.Request{URL: &url.URL{RawQuery: "iatas=yvr,yyj&iata=yyc"}}
	result := parseIATAs(r)
	if len(result) != 2 {
		t.Fatalf("expected 2 IATAs, got %d", len(result))
	}
}

func TestParseIATAs_Whitespace(t *testing.T) {
	r := &http.Request{URL: &url.URL{RawQuery: "iatas=yvr%2C+yyj"}}
	result := parseIATAs(r)
	if len(result) != 2 {
		t.Fatalf("expected 2 IATAs, got %d", len(result))
	}
	if result[1] != "YYJ" {
		t.Errorf("expected YYJ after trimming whitespace, got %s", result[1])
	}
}

func TestParseIATAs_Empty(t *testing.T) {
	r := &http.Request{URL: &url.URL{RawQuery: ""}}
	result := parseIATAs(r)
	if result != nil {
		t.Errorf("expected nil for empty query, got %v", result)
	}
}

func TestParseIATAs_Uppercase(t *testing.T) {
	r := &http.Request{URL: &url.URL{RawQuery: "iata=YVR"}}
	result := parseIATAs(r)
	if len(result) != 1 || result[0] != "YVR" {
		t.Errorf("expected [YVR], got %v", result)
	}
}
