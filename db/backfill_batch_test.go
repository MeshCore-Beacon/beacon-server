// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

func TestBackfillTimeout(t *testing.T) {
	if got := backfillTimeout(0, time.Now().Add(time.Minute)); got != backfillStatementTimeout {
		t.Fatalf("default timeout %v", got)
	}
	if got := backfillTimeout(time.Second, time.Now().Add(-time.Second)); got != time.Millisecond {
		t.Fatalf("expired budget disabled the PostgreSQL timeout: %v", got)
	}
	if got := backfillTimeout(time.Second, time.Now().Add(50*time.Millisecond)); got <= 0 || got > 50*time.Millisecond {
		t.Fatalf("remaining timeout %v", got)
	}
	err := &pgconn.PgError{Code: "57014"}
	if !backfillTimedOut(context.Background(), err) || backfillTimedOut(context.Background(), errors.New("transport failed")) {
		t.Fatal("only a PostgreSQL query cancellation permits a smaller retry")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if backfillTimedOut(ctx, err) {
		t.Fatal("a canceled caller must stop, not retry")
	}
}

func TestBackfillAdmissionCancellation(t *testing.T) {
	backfillWriteSlot <- struct{}{}
	defer func() { <-backfillWriteSlot }()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := backfillWrite(ctx, nil, time.Second, func() (int64, error) {
		t.Fatal("canceled waiter ran")
		return 0, nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting for admission: %v", err)
	}
}
