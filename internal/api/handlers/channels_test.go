// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: agpl

package handlers

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/go-chi/chi/v5"
)

func TestListChannels_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels", listChannels(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannels_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels", listChannels(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels?cursor=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannels_InvalidHash(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels", listChannels(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels?hash=nothex!!", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannels_HashNotSingleByte(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels", listChannels(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels?hash=aabb", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestGetChannel_InvalidID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels/{channelID}", getChannel(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels/notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannelMessages_InvalidChannelID(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels/{channelID}/messages", listChannelMessages(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels/notanint/messages", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannelMessages_InvalidLimit(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels/{channelID}/messages", listChannelMessages(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels/1/messages?limit=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannelMessages_InvalidSince(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels/{channelID}/messages", listChannelMessages(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels/1/messages?since=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}

func TestListChannelMessages_InvalidCursor(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels/{channelID}/messages", listChannelMessages(stubReader{}))
	req := httptest.NewRequest(http.MethodGet, "/channels/1/messages?cursor=notanint", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}
}
