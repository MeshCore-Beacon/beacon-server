// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestCapabilityAlreadyRecorded(t *testing.T) {
	w, db := newTestWorker()
	id := uuid.New()
	for range 3 {
		w.runCapabilityDetection(context.Background(), 5, 2, []uuid.UUID{id, id})
	}
	if got := len(db.setCapabilityCalls); got != 1 {
		t.Fatalf("recorded the same capability %d times, want 1", got)
	}
	w.runCapabilityDetection(context.Background(), 9, 2, []uuid.UUID{id})
	w.runCapabilityDetection(context.Background(), 9, 2, []uuid.UUID{id})
	if got := len(db.setCapabilityCalls); got != 2 || !db.setCapabilityCalls[1].traces {
		t.Fatalf("trace capability was not recorded separately: %+v", db.setCapabilityCalls)
	}
}

type failingCapabilityDB struct {
	*stubDB
	fail bool
}

func (d *failingCapabilityDB) SetNodeCapability(ctx context.Context, id uuid.UUID, paths, traces bool) error {
	_ = d.stubDB.SetNodeCapability(ctx, id, paths, traces)
	if d.fail {
		return errors.New("write failed")
	}
	return nil
}

func TestCapabilityRetriesFailedWrite(t *testing.T) {
	w, base := newTestWorker()
	db := &failingCapabilityDB{stubDB: base, fail: true}
	w.db = db
	id := uuid.New()
	w.runCapabilityDetection(context.Background(), 5, 2, []uuid.UUID{id})
	db.fail = false
	w.runCapabilityDetection(context.Background(), 5, 2, []uuid.UUID{id})
	w.runCapabilityDetection(context.Background(), 5, 2, []uuid.UUID{id})
	if got := len(db.setCapabilityCalls); got != 2 {
		t.Fatalf("got %d writes, want the failed write and one successful retry", got)
	}
}

func TestCapabilityCacheEviction(t *testing.T) {
	w, db := newTestWorker()
	first := uuid.New()
	w.runCapabilityDetection(context.Background(), 5, 2, []uuid.UUID{first})
	for range capabilityCacheLimit {
		w.runCapabilityDetection(context.Background(), 5, 2, []uuid.UUID{uuid.New()})
	}
	if len(w.capabilities.nodes) > capabilityCacheLimit {
		t.Fatal("cache grew beyond limit")
	}
	before := len(db.setCapabilityCalls)
	w.runCapabilityDetection(context.Background(), 5, 2, []uuid.UUID{first})
	if len(db.setCapabilityCalls) != before+1 {
		t.Fatal("evicted capability was not recorded again")
	}
}
