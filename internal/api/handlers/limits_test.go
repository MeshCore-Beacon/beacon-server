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
	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
)

func TestListLimits(t *testing.T) {
	var got int32
	var calls int
	reader := stubReader{
		listChannels: func(ctx context.Context, limit int32, hash []byte, iatas []string, cursor int64) (api.Page[api.ChannelSummary], error) {
			calls++
			got = limit
			return api.Page[api.ChannelSummary]{}, nil
		},
		listChannelMessages: func(ctx context.Context, channelID *int32, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
			calls++
			got = limit
			return api.Page[api.ChannelMessage]{}, nil
		},
		listChannelMessagesByHash: func(ctx context.Context, hash []byte, since time.Time, limit int32, iatas []string, scope string, cursor int64) (api.Page[api.ChannelMessage], error) {
			calls++
			got = limit
			return api.Page[api.ChannelMessage]{}, nil
		},
		listMessagesAfterID: func(ctx context.Context, afterID int64, iatas []string, scope string, limit int32) ([]api.ChannelMessage, error) {
			calls++
			got = limit
			return nil, nil
		},
		listObservers: func(ctx context.Context, iatas []string, observerType, broker, status, name, scope string, cursor int64, limit int32) (api.Page[api.ObserverSummary], error) {
			calls++
			got = limit
			return api.Page[api.ObserverSummary]{}, nil
		},
		listObserverAdverts: func(ctx context.Context, observerID uuid.UUID, cursor int64, limit int32) (api.Page[api.AdvertObservation], error) {
			calls++
			got = limit
			return api.Page[api.AdvertObservation]{}, nil
		},
		listNodes: func(ctx context.Context, nodeType int16, iatas []string, supportsMultibytePaths, supportsMultibyteTraces *bool, pubkey []byte, pubkeyPrefix, name, scope string, cursor int64, limit int32, includeNeighbors bool) (api.Page[api.NodeSummary], error) {
			calls++
			got = limit
			return api.Page[api.NodeSummary]{}, nil
		},
		listNodeObservations: func(ctx context.Context, nodeID uuid.UUID, cursor int64, limit int32) (api.Page[api.PacketObservationSummary], error) {
			calls++
			got = limit
			return api.Page[api.PacketObservationSummary]{}, nil
		},
		listPackets: func(ctx context.Context, payloadTypes, routeTypes []int16, iatas []string, scopes []string, since, until time.Time, cursor int64, limit int32) (api.Page[api.PacketSummary], error) {
			calls++
			got = limit
			return api.Page[api.PacketSummary]{}, nil
		},
		listPacketsAfterID: func(ctx context.Context, afterObservationID int64, payloadType, routeType int16, iatas []string, scope string, limit int32) ([]api.PacketSummary, error) {
			calls++
			got = limit
			return nil, nil
		},
		getStatsTopNodes: func(ctx context.Context, iatas []string, limit int32) ([]api.TopNode, error) {
			calls++
			got = limit
			return nil, nil
		},
		getStatsTopObservers: func(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopObserver, error) {
			calls++
			got = limit
			return nil, nil
		},
		getStatsTopAdvertisers: func(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopAdvertiser, error) {
			calls++
			got = limit
			return nil, nil
		},
		getStatsClockDrift: func(ctx context.Context, iatas []string, limit int32) ([]api.ClockDriftEntry, error) {
			calls++
			got = limit
			return nil, nil
		},
		getStatsTopTalkers: func(ctx context.Context, iatas []string, since time.Time, limit int32) ([]api.TopTalker, error) {
			calls++
			got = limit
			return nil, nil
		},
		listTraceTags: func(ctx context.Context, iatas []string, scope, traceType string, since, until time.Time, cursor time.Time, limit int32) ([]api.TraceTagSummary, error) {
			calls++
			got = limit
			return nil, nil
		},
		listKnownRoutes: func(ctx context.Context, iata string, hopCount int32, cursor time.Time, limit int32) ([]api.KnownRoute, error) {
			calls++
			got = limit
			return nil, nil
		},
	}
	router := chi.NewRouter()
	router.Mount("/nodes", NodesRouter(reader))
	router.Mount("/observers", ObserversRouter(reader))
	router.Mount("/packets", PacketsRouter(reader))
	router.Mount("/channels", ChannelsRouter(reader))
	router.Mount("/messages", MessagesRouter(reader))
	router.Mount("/routes", RoutesRouter(reader))
	router.Mount("/traces", TracesRouter(reader))
	router.Mount("/stats", StatsRouter(reader))
	for _, endpoint := range []struct {
		path         string
		defaultLimit int32
	}{
		{"/nodes", 50},
		{"/nodes/00000000-0000-0000-0000-000000000001/observations", 50},
		{"/observers", 50},
		{"/observers/00000000-0000-0000-0000-000000000001/adverts", 50},
		{"/packets", 50},
		{"/packets/backfill?afterObservationId=1", 100},
		{"/channels", 50},
		{"/channels/1/messages", 50},
		{"/messages", 50},
		{"/messages?channelHash=11", 50},
		{"/messages/backfill?afterId=1", 100},
		{"/routes", 50},
		{"/traces", 50},
		{"/stats/top-nodes", 10},
		{"/stats/top-observers", 10},
		{"/stats/top-advertisers", 10},
		{"/stats/clock-drift", 10},
		{"/stats/top-talkers", 10},
	} {
		t.Run(endpoint.path, func(t *testing.T) {
			for _, tc := range []struct {
				value  string
				want   int32
				status int
			}{
				{"", endpoint.defaultLimit, 200}, {"1", 1, 200}, {"200", 200, 200},
				{"201", 200, 200}, {"1000000", 200, 200}, {"2147483647", 200, 200},
				{"0", 0, 400}, {"-1", 0, 400}, {"bad", 0, 400}, {"1.5", 0, 400},
				{"2147483648", 0, 400},
			} {
				t.Run(tc.value, func(t *testing.T) {
					got, calls = 0, 0
					req := httptest.NewRequest(http.MethodGet, endpoint.path, nil)
					query := req.URL.Query()
					query.Set("limit", tc.value)
					req.URL.RawQuery = query.Encode()
					rec := httptest.NewRecorder()
					router.ServeHTTP(rec, req)
					if rec.Code != tc.status {
						t.Fatalf("status %d, want %d: %s", rec.Code, tc.status, rec.Body.String())
					}
					if tc.status == http.StatusBadRequest {
						if calls != 0 {
							t.Fatal("invalid limit reached the reader")
						}
					} else if calls != 1 || got != tc.want {
						t.Fatalf("reader calls=%d limit=%d, want 1 and %d", calls, got, tc.want)
					}
				})
			}
		})
	}
}
