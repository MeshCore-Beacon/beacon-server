// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
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

func TestListChannels_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels", listChannels(stubReader{
		listChannels: func(_ context.Context, _ int32, _ []byte, _ []string, _ int64) (api.Page[api.ChannelSummary], error) {
			return api.Page[api.ChannelSummary]{Items: []api.ChannelSummary{{ID: 1, ChannelHash: "ab"}}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/channels", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}

func TestListChannels_IATAParsing(t *testing.T) {
	cases := []struct {
		name  string
		query string
		want  []string
	}{
		{"single lowercased", "?iata=yow", []string{"YOW"}},
		{"multi csv", "?iatas=yow,%20yyz", []string{"YOW", "YYZ"}},
		{"none", "", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var got []string
			r := chi.NewRouter()
			r.Get("/channels", listChannels(stubReader{
				listChannels: func(_ context.Context, _ int32, _ []byte, iatas []string, _ int64) (api.Page[api.ChannelSummary], error) {
					got = iatas
					return api.Page[api.ChannelSummary]{}, nil
				},
			}))
			req := httptest.NewRequest(http.MethodGet, "/channels"+tc.query, nil)
			w := httptest.NewRecorder()
			r.ServeHTTP(w, req)
			if w.Code != http.StatusOK {
				t.Fatalf("expected 200, got %d", w.Code)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("expected iatas %v, got %v", tc.want, got)
			}
		})
	}
}

func TestGetChannel_OK(t *testing.T) {
	r := chi.NewRouter()
	r.Get("/channels/{channelID}", getChannel(stubReader{
		getChannel: func(_ context.Context, id int32) (*api.Channel, error) {
			return &api.Channel{ChannelSummary: api.ChannelSummary{ID: int(id)}}, nil
		},
	}))
	req := httptest.NewRequest(http.MethodGet, "/channels/1", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", w.Code)
	}
}
