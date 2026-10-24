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

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// readerRoutes mounts every reader-backed router the way the API does.
func readerRoutes(reader api.Reader) http.Handler {
	m := chi.NewRouter()
	m.Mount("/channels", ChannelsRouter(reader))
	m.Mount("/iatas", IATAsRouter(reader))
	m.Mount("/messages", MessagesRouter(reader))
	m.Mount("/nodes", NodesRouter(reader))
	m.Mount("/observers", ObserversRouter(reader))
	m.Mount("/packets", PacketsRouter(reader))
	m.Mount("/regions", RegionsRouter(reader))
	m.Mount("/routes", RoutesRouter(reader))
	m.Mount("/scopes", ScopesRouter(reader, nil))
	m.Mount("/stats", StatsRouter(reader, StatsOptions{}))
	m.Mount("/traces", TracesRouter(reader))
	return m
}

const someID = "11111111-1111-1111-1111-111111111111"

// readerErrorPaths is one request per handler path that answers a failed read.
var readerErrorPaths = []string{
	"/channels",
	"/channels/7",
	"/channels/7/messages",
	"/iatas",
	"/iatas/YVR",
	"/iatas/YVR/border",
	"/messages",
	"/messages/backfill?afterId=1",
	"/nodes",
	"/nodes?regionId=1",
	"/nodes/" + someID,
	"/nodes/" + someID + "/observations",
	"/nodes/" + someID + "/neighbors",
	"/observers",
	"/observers/directory",
	"/observers/" + someID,
	"/observers/" + someID + "/adverts",
	"/observers/" + someID + "/telemetry",
	"/observers/" + someID + "/activity",
	"/packets",
	"/packets/backfill?afterObservationId=1",
	"/packets/0123456789abcdef",
	"/regions",
	"/regions/1",
	"/routes?iata=YVR",
	"/routes/search?iatas=YVR&from=ab&to=cd",
	"/routes/cross?fromHash=ab&toHash=cd&iatas=YVR,YYJ",
	"/routes/YVR/0123456789abcdef0123456789abcdef/observations",
	"/scopes",
	"/scopes/%23bc",
	"/stats/series?since=0&until=1",
	"/stats/overview",
	"/stats/observations",
	"/stats/signal?since=0&until=1",
	"/stats/paths?since=0&until=1",
	"/stats/observer-comparison?observerA=" + someID + "&observerB=22222222-2222-2222-2222-222222222222&since=0&until=1",
	"/stats/payload-breakdown",
	"/stats/top-nodes",
	"/stats/top-observers",
	"/stats/top-advertisers?sort=flood",
	"/stats/clock-drift",
	"/stats/top-talkers",
	"/stats/radio-presets",
	"/stats/scopes",
	"/stats/node-types",
	"/traces",
	"/traces/abcd",
}

// Every reader error answers the same way: a statement timeout or deadline is 503 deadline_exceeded with
// Retry-After 10, any other error a 500 without Retry-After.
func TestReaderErrorsAnswerTimeoutAs503(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		status     int
		retryAfter string
		code       string
	}{
		{"statement timeout", &pgconn.PgError{Code: "57014"}, http.StatusServiceUnavailable, "10", "deadline_exceeded"},
		{"deadline", context.DeadlineExceeded, http.StatusServiceUnavailable, "10", "deadline_exceeded"},
		{"other error", errors.New("boom"), http.StatusInternalServerError, "", "internal_server_error"},
	}
	for _, c := range cases {
		handler := readerRoutes(errReader{err: fmt.Errorf("wrapped: %w", c.err)})
		for _, path := range readerErrorPaths {
			t.Run(c.name+" "+path, func(t *testing.T) {
				rec := httptest.NewRecorder()
				handler.ServeHTTP(rec, httptest.NewRequest("GET", path, nil))
				if rec.Code != c.status || rec.Header().Get("Retry-After") != c.retryAfter {
					t.Fatalf("status %d, Retry-After %q; want %d and %q: %s", rec.Code, rec.Header().Get("Retry-After"), c.status, c.retryAfter, rec.Body)
				}
				var body map[string]APIError
				if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil || body["error"].Code != c.code || body["error"].Message == "" {
					t.Errorf("body %s (%v), want error code %s", rec.Body, err, c.code)
				}
			})
		}
	}
}

// timeoutsCounted sums beacon_api_unavailable_total{reason="timeout"} over every model a page reads.
func timeoutsCounted() float64 {
	var n float64
	for _, m := range []string{"none", "cmi", "packet_sites", "observation_payload_types"} {
		n += testutil.ToFloat64(metrics.APIUnavailable.WithLabelValues("timeout", m))
	}
	return n
}

// Every timeout 503 is counted once in beacon_api_unavailable_total: under its read model on a page that reads one,
// under none on every other page; a 500 is not counted.
func TestReaderTimeoutsAreCounted(t *testing.T) {
	for _, c := range []struct {
		err  error
		want float64
	}{
		{&pgconn.PgError{Code: "57014"}, float64(len(readerErrorPaths))},
		{errors.New("boom"), 0},
	} {
		handler := readerRoutes(errReader{err: fmt.Errorf("wrapped: %w", c.err)})
		before, none := timeoutsCounted(), testutil.ToFloat64(metrics.APIUnavailable.WithLabelValues("timeout", "none"))
		for _, path := range readerErrorPaths {
			handler.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest("GET", path, nil))
		}
		if got := timeoutsCounted() - before; got != c.want {
			t.Errorf("%v: %v timeouts counted over %d paths, want %v", c.err, got, len(readerErrorPaths), c.want)
		}
		if c.want > 0 && testutil.ToFloat64(metrics.APIUnavailable.WithLabelValues("timeout", "none"))-none < c.want-4 {
			t.Errorf("%v: pages that read no read model were not counted under none", c.err)
		}
	}
	// A handler's own time limit (the heavy stats and route searches) counts as a timeout too.
	before := timeoutsCounted()
	rec := httptest.NewRecorder()
	respondTimeout(rec, "comparison timed out; try a shorter time period")
	if rec.Code != http.StatusServiceUnavailable || timeoutsCounted()-before != 1 {
		t.Errorf("respondTimeout: status %d, counted %v", rec.Code, timeoutsCounted()-before)
	}
}
