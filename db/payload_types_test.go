// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"testing"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"go.uber.org/mock/gomock"
)

func TestListObserverAdverts_Dispatch(t *testing.T) {
	ctx := context.Background()
	obs := uuid.MustParse("00000000-0000-0000-0000-000000000001")
	typedParams := sqlc.ListObserverAdvertsParams{ObserverID: obs, CursorID: 7, RowLimit: 3}
	gated := func(m sqlc.Querier) *Store {
		s := &Store{q: m}
		WithPayloadTypeGate()(s)
		return s
	}
	page := func(t *testing.T, s *Store) error {
		t.Helper()
		_, err := s.ListObserverAdverts(ctx, obs, 7, 2)
		return err
	}

	// A store without the gate (never the API store) serves the observation-typed statement at once.
	t.Run("no gate", func(t *testing.T) {
		m := mockdb.NewMockQuerier(gomock.NewController(t))
		m.EXPECT().ListObserverAdverts(gomock.Any(), typedParams).Return(nil, nil).Times(1)
		if err := page(t, &Store{q: m}); err != nil {
			t.Fatal(err)
		}
	})
	// Until the backfill state reads done, the page is not ready and no adverts statement runs.
	for name, state := range map[string]struct {
		raw json.RawMessage
		err error
	}{
		"no state":   {err: pgx.ErrNoRows},
		"not done":   {raw: json.RawMessage(`{"next_id":5,"done":false}`)},
		"read fails": {err: errors.New("boom")},
	} {
		t.Run(name, func(t *testing.T) {
			if state.err != nil && !errors.Is(state.err, pgx.ErrNoRows) {
				// The read error is logged by design; keep it out of the test output.
				prev := slog.Default()
				slog.SetDefault(slog.New(slog.DiscardHandler))
				t.Cleanup(func() { slog.SetDefault(prev) })
			}
			m := mockdb.NewMockQuerier(gomock.NewController(t))
			m.EXPECT().GetBackgroundState(gomock.Any(), "observation_payload_type").Return(state.raw, state.err).Times(2)
			s := gated(m)
			for range 2 {
				if err := page(t, s); !errors.Is(err, api.ErrPagesNotReady) {
					t.Fatalf("err = %v, want ErrPagesNotReady", err)
				}
			}
		})
	}
	t.Run("done", func(t *testing.T) {
		m := mockdb.NewMockQuerier(gomock.NewController(t))
		m.EXPECT().GetBackgroundState(gomock.Any(), "observation_payload_type").Return(json.RawMessage(`{"next_id":5,"done":true}`), nil).Times(1)
		m.EXPECT().ListObserverAdverts(gomock.Any(), typedParams).Return(nil, nil).Times(2)
		s := gated(m)
		for range 2 {
			if err := page(t, s); err != nil {
				t.Fatal(err)
			}
		}
	})
}
