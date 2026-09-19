// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/borders"
	"github.com/MeshCore-Beacon/beacon-server/internal/cache"
	"github.com/alicebob/miniredis/v2"
	"github.com/google/uuid"
	"github.com/paulmach/orb"
)

type borderReader struct {
	api.Reader
	node                *api.Node
	page                api.Page[api.NodeSummary]
	getCalls, listCalls int
	err                 error
}

func (r *borderReader) GetNode(context.Context, uuid.UUID) (*api.Node, error) {
	r.getCalls++
	return r.node, r.err
}
func (r *borderReader) ListNodes(_ context.Context, _ int16, _ []string, _, _ *bool, _ []byte, _, _, _ string, _ int64, _ int32, _ bool) (api.Page[api.NodeSummary], error) {
	r.listCalls++
	return r.page, r.err
}

func TestLocalBordersAfterCache(t *testing.T) {
	ctx := context.Background()
	local, err := borders.New([]orb.Polygon{{{{10, 10}, {20, 10}, {20, 20}, {10, 20}, {10, 10}}}})
	if err != nil {
		t.Fatal(err)
	}
	other, err := borders.New([]orb.Polygon{{{{30, 30}, {40, 30}, {40, 40}, {30, 40}, {30, 30}}}})
	if err != nil {
		t.Fatal(err)
	}
	node := &api.Node{NodeSummary: api.NodeSummary{ID: uuid.New(), NodeType: 2, Latitude: new(15.0), Longitude: new(15.0), IATAs: []api.NodeIATA{{IATA: "ZZZ", LastHeard: 1}}}}
	base := &borderReader{node: node, page: api.Page[api.NodeSummary]{Items: []api.NodeSummary{node.NodeSummary}, HasMore: true, NextCursor: new(int64(123))}}
	mr := miniredis.RunT(t)
	client := cache.NewClient(mr.Addr(), "", 0)
	t.Cleanup(func() { client.Close() })
	cached := cache.NewCachedReader(base, client, cache.CacheTTLs{Nodes: time.Hour})
	first := api.WithLocalBorders(cached, local)
	second := api.WithLocalBorders(cached, other)
	if api.WithLocalBorders(cached, nil) != cached {
		t.Fatal("disabled decorator changed reader")
	}
	for _, tc := range []struct {
		reader api.Reader
		want   bool
	}{{first, false}, {second, true}} {
		got, err := tc.reader.GetNode(ctx, node.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.PossiblyForeign == nil || *got.PossiblyForeign != tc.want || got.IATAs[0].IATA != "ZZZ" {
			t.Fatalf("detail: %+v", got)
		}
		page, err := tc.reader.ListNodes(ctx, 2, []string{"ZZZ"}, nil, nil, nil, "", "", "", 0, 50, false)
		if err != nil {
			t.Fatal(err)
		}
		if !page.HasMore || *page.NextCursor != 123 || page.Items[0].PossiblyForeign == nil || *page.Items[0].PossiblyForeign != tc.want {
			t.Fatalf("page: %+v", page)
		}
		data, _ := json.Marshal(got)
		if !strings.Contains(string(data), `"possiblyForeign":`) {
			t.Fatal("known false was omitted")
		}
	}
	if base.getCalls != 1 || base.listCalls != 2 {
		t.Fatalf("extra reads: get=%d list=%d", base.getCalls, base.listCalls)
	}
	if node.PossiblyForeign != nil || base.page.Items[0].PossiblyForeign != nil {
		t.Fatal("shared reader data was mutated")
	}
	disabled, err := cached.GetNode(ctx, node.ID)
	if err != nil {
		t.Fatal(err)
	}
	data, _ := json.Marshal(disabled)
	if strings.Contains(string(data), "possiblyForeign") {
		t.Fatal("classification leaked into cached data")
	}
	// Existing ingest invalidation still supplies fresh coordinates to the wrapper.
	base.node.Longitude = new(35.0)
	cached.(*cache.CachedReader).InvalidateNode(ctx, node.ID)
	updated, err := first.GetNode(ctx, node.ID)
	if err != nil || updated.PossiblyForeign == nil || !*updated.PossiblyForeign || base.getCalls != 2 {
		t.Fatal("updated coordinates not classified")
	}
}

func TestLocalBordersPreserveReaderErrors(t *testing.T) {
	local, err := borders.New([]orb.Polygon{{{{10, 10}, {20, 10}, {20, 20}, {10, 20}, {10, 10}}}})
	if err != nil {
		t.Fatal(err)
	}
	want := errors.New("reader unavailable")
	base := &borderReader{err: want}
	reader := api.WithLocalBorders(base, local)
	if _, err := reader.GetNode(context.Background(), uuid.New()); !errors.Is(err, want) {
		t.Fatal(err)
	}
	if _, err := reader.ListNodes(context.Background(), 0, nil, nil, nil, nil, "", "", "", 0, 1, false); !errors.Is(err, want) {
		t.Fatal(err)
	}
	base.err = nil
	if node, err := reader.GetNode(context.Background(), uuid.New()); node != nil || err != nil {
		t.Fatal("nil result changed")
	}
}
