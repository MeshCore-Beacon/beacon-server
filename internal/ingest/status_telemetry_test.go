// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

const statusPubkey = "aabbccddeeff00112233445566778899aabbccddeeff00112233445566778899"

func statusJSON(uptime int64) []byte {
	return fmt.Appendf(nil, `{"origin":"obs","stats":{"uptime_secs":%d,"noise_floor":-110,"tx_air_secs":3,"rx_air_secs":40}}`, uptime)
}

// statusWorker is a worker with a tracker and a clock the test moves.
func statusWorker() (*Worker, *stubDB, *time.Time) {
	w, db := newTestWorker()
	w.uptimes = NewUptimeTracker()
	now := time.Date(2026, 11, 26, 10, 30, 0, 0, time.UTC)
	w.now = func() time.Time { return now }
	return w, db, &now
}

func TestStatusFailedInsertKeepsTheReboot(t *testing.T) {
	w, db, now := statusWorker()
	ctx := context.Background()
	w.handleStatus(ctx, statusPubkey, statusJSON(10000))

	// The reboot status: broker one's copy fails to store, broker two's arrives a second later.
	*now = now.Add(10 * time.Minute)
	db.telemetryErrs = []error{errors.New("connection reset")}
	w.handleStatus(ctx, statusPubkey, statusJSON(20))
	if len(db.telemetry) != 1 {
		t.Fatalf("rows after the failed insert: %v", db.telemetry)
	}
	*now = now.Add(time.Second)
	w.handleStatus(ctx, statusPubkey, statusJSON(20))
	if len(db.telemetry) != 2 || db.telemetry[1].uptime != 20 || !db.telemetry[1].at.Equal(*now) {
		t.Fatalf("the reboot was not stored at its own time: %v", db.telemetry)
	}
	// A third copy does not store it again.
	*now = now.Add(time.Second)
	w.handleStatus(ctx, statusPubkey, statusJSON(20))
	if len(db.telemetry) != 2 {
		t.Fatalf("the reboot was stored twice: %v", db.telemetry)
	}
}

func TestStatusNegativeUptimeStoresNothing(t *testing.T) {
	w, db, now := statusWorker()
	ctx := context.Background()
	w.handleStatus(ctx, statusPubkey, statusJSON(10000))
	for i := range 5 {
		*now = now.Add(time.Minute)
		w.handleStatus(ctx, statusPubkey, statusJSON(int64(-1-i)))
	}
	if len(db.telemetry) != 1 {
		t.Fatalf("negative uptimes stored rows: %v", db.telemetry)
	}
	// The baseline is untouched: the next real status is no reboot.
	*now = now.Add(time.Minute)
	w.handleStatus(ctx, statusPubkey, statusJSON(10360))
	if len(db.telemetry) != 1 {
		t.Fatalf("rows = %v", db.telemetry)
	}
}

func TestStatusFallingUptimesStoreOneReboot(t *testing.T) {
	w, db, now := statusWorker()
	ctx := context.Background()
	w.handleStatus(ctx, statusPubkey, statusJSON(10000))
	for up := int64(30); up > 0; up-- {
		*now = now.Add(20 * time.Second)
		w.handleStatus(ctx, statusPubkey, statusJSON(up))
	}
	if len(db.telemetry) != 2 {
		t.Fatalf("want the window row and one reboot row, got %v", db.telemetry)
	}
}
