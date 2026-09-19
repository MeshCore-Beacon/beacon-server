// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

func TestObserverComparisonRejectsInvalidQuery(t *testing.T) {
	for _, query := range []string{
		"",
		"observerA=bad&observerB=bad&since=0&until=1",
		"observerA=11111111-1111-1111-1111-111111111111&observerB=11111111-1111-1111-1111-111111111111&since=0&until=1",
		"observerA=11111111-1111-1111-1111-111111111111&observerB=22222222-2222-2222-2222-222222222222&since=2&until=1",
		"observerA=11111111-1111-1111-1111-111111111111&observerB=22222222-2222-2222-2222-222222222222&since=no&until=1",
		"observerA=11111111-1111-1111-1111-111111111111&observerB=22222222-2222-2222-2222-222222222222&since=-1&until=1",
	} {
		t.Run(query, func(t *testing.T) {
			w := httptest.NewRecorder()
			StatsRouter(nil).ServeHTTP(w, httptest.NewRequest("GET", "/observer-comparison?"+query, nil))
			if w.Code != 400 {
				t.Fatalf("status %d; wanted validation error", w.Code)
			}
		})
	}
}

func TestObserverComparisonHTTP(t *testing.T) {
	a := uuid.MustParse("11111111-1111-1111-1111-111111111111")
	b := uuid.MustParse("22222222-2222-2222-2222-222222222222")
	query := "observerA=" + a.String() + "&observerB=" + b.String() + "&since=0&until=1000"
	for _, suffix := range []string{"&since=2", "&observerA=" + b.String(), "&until=253402300800000", "&observerB=00000000-0000-0000-0000-000000000000"} {
		w := httptest.NewRecorder()
		StatsRouter(nil).ServeHTTP(w, httptest.NewRequest("GET", "/observer-comparison?"+query+suffix, nil))
		if w.Code != 400 {
			t.Fatalf("invalid query returned %d", w.Code)
		}
	}
	for _, tc := range []struct {
		name, suffix string
		err          error
		status       int
		iatas        []string
	}{
		{"global", "", nil, 200, nil},
		{"IATA", "&iatas=yvr,yyj", nil, 200, []string{"YVR", "YYJ"}},
		{"region", "&region=coast", nil, 200, []string{"YVR"}},
		{"empty region", "&region=empty", nil, 200, []string{""}},
		{"unknown observer", "", pgx.ErrNoRows, 404, nil},
		{"timeout", "", context.DeadlineExceeded, 503, nil},
		{"database failure", "", errors.New("private database detail"), 500, nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			reader := stubReader{
				getRegionBySlug: func(_ context.Context, slug string) (*api.Region, error) {
					if slug == "empty" {
						return &api.Region{}, nil
					}
					return &api.Region{IATAs: []string{"YVR"}}, nil
				},
				getObserverComparison: func(ctx context.Context, first, second uuid.UUID, since, until time.Time, iatas []string) (*api.ObserverComparison, error) {
					if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 15*time.Second {
						t.Fatal("query has no bounded deadline")
					}
					if first != a || second != b || since.UnixMilli() != 0 || until.UnixMilli() != 1000 || !reflect.DeepEqual(iatas, tc.iatas) {
						t.Fatalf("unexpected arguments: %s %s %v %v %v", first, second, since, until, iatas)
					}
					return &api.ObserverComparison{ObserverA: a, ObserverB: b, Until: 1000, TotalPackets: 4, OnlyA: 1, OnlyB: 1, Both: 2}, tc.err
				},
			}
			w := httptest.NewRecorder()
			StatsRouter(reader).ServeHTTP(w, httptest.NewRequest("GET", "/observer-comparison?"+query+tc.suffix, nil))
			if w.Code != tc.status {
				t.Fatalf("status = %d, body = %s", w.Code, w.Body.String())
			}
			if tc.status == 200 {
				var got api.ObserverComparison
				if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
					t.Fatal(err)
				}
				if got.TotalPackets != 4 || got.ObserverA != a || got.Both != 2 {
					t.Fatalf("response = %+v", got)
				}
			}
		})
	}
}
