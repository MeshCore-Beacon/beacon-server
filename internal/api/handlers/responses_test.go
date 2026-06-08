package handlers

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestRespond_StatusAndContentType(t *testing.T) {
	w := httptest.NewRecorder()
	respond(w, http.StatusOK, map[string]string{"hello": "world"})
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
	if ct := w.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("expected application/json, got %s", ct)
	}
}

func TestRespond_EncodesJSON(t *testing.T) {
	w := httptest.NewRecorder()
	respond(w, http.StatusOK, map[string]string{"key": "value"})
	var result map[string]string
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}
	if result["key"] != "value" {
		t.Errorf("expected value, got %s", result["key"])
	}
}

func TestRespondError_StatusCode(t *testing.T) {
	w := httptest.NewRecorder()
	respondError(w, http.StatusBadRequest, "invalid input")
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestRespondError_Shape(t *testing.T) {
	w := httptest.NewRecorder()
	respondError(w, http.StatusBadRequest, "invalid input")
	var result map[string]APIError
	if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	e, ok := result["error"]
	if !ok {
		t.Fatal("expected 'error' key in response")
	}
	if e.Code != "bad_request" {
		t.Errorf("expected code bad_request, got %s", e.Code)
	}
	if e.Message != "invalid input" {
		t.Errorf("expected message 'invalid input', got %s", e.Message)
	}
}

func TestRespondError_CodeDerivation(t *testing.T) {
	tests := []struct {
		status int
		want   string
	}{
		{http.StatusBadRequest, "bad_request"},
		{http.StatusNotFound, "not_found"},
		{http.StatusInternalServerError, "internal_server_error"},
		{http.StatusUnauthorized, "unauthorized"},
	}
	for _, tt := range tests {
		w := httptest.NewRecorder()
		respondError(w, tt.status, "msg")
		var result map[string]APIError
		if err := json.NewDecoder(w.Body).Decode(&result); err != nil {
			t.Fatalf("status %d: failed to decode: %v", tt.status, err)
		}
		if result["error"].Code != tt.want {
			t.Errorf("status %d: expected code %s, got %s", tt.status, tt.want, result["error"].Code)
		}
	}
}
