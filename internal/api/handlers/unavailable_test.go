// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

// unavailableCase is one page whose reader fails with err; the handler must answer 503 with Retry-After.
type unavailableCase struct {
	name, path string
	mount      func(api.Reader) http.Handler
}

var unavailablePages = []unavailableCase{
	{"region packets", "/packets?iatas=YVR&since=1", func(r api.Reader) http.Handler {
		m := chi.NewRouter()
		m.Mount("/packets", PacketsRouter(r))
		return m
	}},
	{"messages across channels", "/messages?iatas=YVR&scope=%23bc", func(r api.Reader) http.Handler {
		m := chi.NewRouter()
		m.Mount("/messages", MessagesRouter(r))
		return m
	}},
	{"channel messages", "/channels/7/messages?iatas=YVR", func(r api.Reader) http.Handler {
		m := chi.NewRouter()
		m.Mount("/channels", ChannelsRouter(r))
		return m
	}},
	{"observer adverts", "/observers/" + uuid.Nil.String() + "/adverts", func(r api.Reader) http.Handler {
		m := chi.NewRouter()
		m.Mount("/observers", ObserversRouter(r))
		return m
	}},
}

func failingReader(err error) stubReader {
	return stubReader{
		listPackets: func(context.Context, []int16, []int16, []string, []string, time.Time, time.Time, int64, *api.PacketCursor, int32) (api.PacketPage, error) {
			return api.PacketPage{}, err
		},
		listChannelMessages: func(context.Context, *int32, time.Time, int32, []string, string, int64) (api.Page[api.ChannelMessage], error) {
			return api.Page[api.ChannelMessage]{}, err
		},
		listObserverAdverts: func(context.Context, uuid.UUID, int64, int32) (api.Page[api.AdvertObservation], error) {
			return api.Page[api.AdvertObservation]{}, err
		},
	}
}

func checkUnavailable(t *testing.T, err error, retryAfter, code string) {
	t.Helper()
	for _, c := range unavailablePages {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			c.mount(failingReader(fmt.Errorf("wrapped: %w", err))).ServeHTTP(rec, httptest.NewRequest("GET", c.path, nil))
			if rec.Code != http.StatusServiceUnavailable || rec.Header().Get("Retry-After") != retryAfter {
				t.Fatalf("status %d, Retry-After %q; want 503 and %s: %s", rec.Code, rec.Header().Get("Retry-After"), retryAfter, rec.Body)
			}
			var body map[string]APIError
			if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"].Code != code || body["error"].Message == "" {
				t.Errorf("body %s (%v), want error code %s", rec.Body, err, code)
			}
		})
	}
}

// A busy slow pool answers 503 with a short Retry-After, in the guards' error shape.
func TestSlowPoolBusyAnswers503(t *testing.T) {
	checkUnavailable(t, api.ErrSlowPoolBusy, "5", "service_unavailable")
}

// Any other reader error stays a 500.
func TestOtherReaderErrorsStay500(t *testing.T) {
	for _, c := range unavailablePages {
		rec := httptest.NewRecorder()
		c.mount(failingReader(fmt.Errorf("boom"))).ServeHTTP(rec, httptest.NewRequest("GET", c.path, nil))
		if rec.Code != http.StatusInternalServerError || rec.Header().Get("Retry-After") != "" {
			t.Errorf("%s: status %d, Retry-After %q", c.name, rec.Code, rec.Header().Get("Retry-After"))
		}
	}
}
