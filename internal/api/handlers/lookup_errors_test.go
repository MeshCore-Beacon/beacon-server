// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// Lookups answer 404 only for a missing row; any other failure is a retryable 500.
func TestLookupErrors(t *testing.T) {
	id := uuid.NewString()
	for _, tc := range []struct {
		name    string
		router  func(error) http.Handler
		path    string
		missing int // status for pgx.ErrNoRows
	}{
		{"region", func(err error) http.Handler {
			return RegionsRouter(stubReader{getRegion: func(context.Context, int32) (*api.Region, error) { return nil, err }})
		}, "/1", http.StatusNotFound},
		{"regions", func(err error) http.Handler {
			return RegionsRouter(stubReader{listRegions: func(context.Context) ([]api.RegionSummary, error) { return nil, err }})
		}, "/", http.StatusInternalServerError},
		{"iata", func(err error) http.Handler {
			return IATAsRouter(stubReader{getIATA: func(context.Context, string) (*api.IATA, error) { return nil, err }})
		}, "/YOW", http.StatusNotFound},
		{"iata border", func(err error) http.Handler {
			return IATAsRouter(stubReader{getIATABorder: func(context.Context, string) (json.RawMessage, error) { return nil, err }})
		}, "/YOW/border", http.StatusNotFound},
		{"iatas", func(err error) http.Handler {
			return IATAsRouter(stubReader{listIATAs: func(context.Context) ([]api.IATA, error) { return nil, err }})
		}, "/", http.StatusInternalServerError},
		{"channel", func(err error) http.Handler {
			return ChannelsRouter(stubReader{getChannel: func(context.Context, int32) (*api.Channel, error) { return nil, err }})
		}, "/7", http.StatusNotFound},
		{"observer", func(err error) http.Handler {
			return ObserversRouter(stubReader{getObserver: func(context.Context, uuid.UUID) (*api.Observer, error) { return nil, err }})
		}, "/" + id, http.StatusNotFound},
		{"node", func(err error) http.Handler {
			return NodesRouter(stubReader{getNode: func(context.Context, uuid.UUID) (*api.Node, error) { return nil, err }})
		}, "/" + id, http.StatusNotFound},
		{"packet", func(err error) http.Handler {
			return PacketsRouter(stubReader{getPacket: func(context.Context, []byte) (*api.Packet, error) { return nil, err }})
		}, "/abcd", http.StatusNotFound},
	} {
		for _, c := range []struct {
			err  error
			want int
		}{
			{pgx.ErrNoRows, tc.missing},
			{context.DeadlineExceeded, http.StatusInternalServerError},
		} {
			t.Run(tc.name+"/"+c.err.Error(), func(t *testing.T) {
				w := httptest.NewRecorder()
				tc.router(c.err).ServeHTTP(w, httptest.NewRequest(http.MethodGet, tc.path, nil))
				if w.Code != c.want {
					t.Fatalf("status = %d, want %d", w.Code, c.want)
				}
				var body map[string]APIError
				if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["error"].Message == "" {
					t.Fatalf("not a JSON error: %s", w.Body.String())
				}
			})
		}
	}
}

// An unknown region filter is the caller's mistake; a failed lookup is retryable.
func TestRegionFilterErrors(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want int
	}{
		{pgx.ErrNoRows, http.StatusBadRequest},
		{context.DeadlineExceeded, http.StatusInternalServerError},
	} {
		reader := stubReader{getRegionBySlug: func(context.Context, string) (*api.Region, error) { return nil, tc.err }}
		for name, h := range map[string]http.Handler{
			"packets": PacketsRouter(reader),
			"nodes":   NodesRouter(reader),
			"traces":  TracesRouter(reader),
		} {
			t.Run(name+"/"+tc.err.Error(), func(t *testing.T) {
				w := httptest.NewRecorder()
				h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/?region=nowhere", nil))
				if w.Code != tc.want {
					t.Fatalf("status = %d, want %d", w.Code, tc.want)
				}
			})
		}
	}
}

func TestEmptyListsAreArrays(t *testing.T) {
	for name, h := range map[string]http.Handler{
		"regions": RegionsRouter(stubReader{}),
		"iatas":   IATAsRouter(stubReader{}),
	} {
		t.Run(name, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/", nil))
			if w.Code != http.StatusOK || w.Body.String() != "[]\n" {
				t.Fatalf("got %d %q, want 200 []", w.Code, w.Body.String())
			}
		})
	}
}
