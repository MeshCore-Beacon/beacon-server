// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/testutil"
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

// A page whose read model is not ready answers 503 with Retry-After, in the guards' error shape, and is counted as
// not_ready for the model it reads.
func TestPagesNotReadyAnswers503(t *testing.T) {
	models := map[string]string{"region packets": "packet_sites", "messages across channels": "cmi", "channel messages": "cmi", "observer adverts": "observation_payload_types"}
	before := map[string]float64{}
	for _, m := range models {
		before[m] = testutil.ToFloat64(metrics.APIUnavailable.WithLabelValues("not_ready", m))
	}
	checkUnavailable(t, api.ErrPagesNotReady, "30", "service_unavailable")
	for m, n := range map[string]float64{"packet_sites": 1, "cmi": 2, "observation_payload_types": 1} {
		if got := testutil.ToFloat64(metrics.APIUnavailable.WithLabelValues("not_ready", m)) - before[m]; got != n {
			t.Errorf("not_ready %s counted %v, want %v", m, got, n)
		}
	}
}

// A statement timeout or deadline answers 503 with Retry-After and counts as timeout.
func TestQueryTimeoutAnswers503(t *testing.T) {
	models := map[string]string{"region packets": "packet_sites", "messages across channels": "cmi", "channel messages": "cmi", "observer adverts": "observation_payload_types"}
	before := map[string]float64{}
	for _, m := range models {
		before[m] = testutil.ToFloat64(metrics.APIUnavailable.WithLabelValues("timeout", m))
	}
	checkUnavailable(t, &pgconn.PgError{Code: "57014", Message: "canceling statement due to statement timeout"}, "10", "deadline_exceeded")
	checkUnavailable(t, context.DeadlineExceeded, "10", "deadline_exceeded")
	for m, n := range map[string]float64{"packet_sites": 2, "cmi": 4, "observation_payload_types": 2} {
		if got := testutil.ToFloat64(metrics.APIUnavailable.WithLabelValues("timeout", m)) - before[m]; got != n {
			t.Errorf("timeout %s counted %v, want %v", m, got, n)
		}
	}
	for _, err := range []error{nil, context.Canceled, fmt.Errorf("query: %w", context.Canceled), errors.New("boom"), &pgconn.PgError{Code: "23505"}} {
		if isQueryTimeout(err) {
			t.Errorf("isQueryTimeout(%v) = true", err)
		}
	}
}
