// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	mockdb "github.com/MeshCore-Beacon/beacon-server/db/sqlc/mock"
	"github.com/MeshCore-Beacon/beacon-server/internal/ingest"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/mock/gomock"
)

func TestTrustPolicy(t *testing.T) {
	tr := NewTrust("cmi_policy_test", true, CmiTrustPolicy)
	gauge := func() float64 { return testutil.ToFloat64(metrics.ReadModelTrusted.WithLabelValues("cmi_policy_test")) }
	step := func(label string, want bool) {
		t.Helper()
		if tr.Trusted() != want {
			t.Fatalf("%s: trusted %v, want %v", label, tr.Trusted(), want)
		}
		if g := gauge(); (g == 1) != want {
			t.Fatalf("%s: gauge %v, want trusted %v", label, g, want)
		}
	}
	step("new", false)
	tr.ObserveRun(5)
	step("a repairing run before earn", false)
	tr.Earn()
	step("earned", true)
	tr.ObserveRun(1)
	tr.ObserveRun(1)
	step("two repairing runs", true)
	tr.ObserveRun(1)
	step("three repairing runs", false)
	tr.ObserveRun(0)
	step("one clean run", false)
	tr.ObserveRun(0)
	step("two clean runs", true)
	tr.ObserveRun(1)
	tr.ObserveRun(0)
	tr.ObserveRun(1)
	tr.ObserveRun(1)
	step("dirty, clean, dirty, dirty", true)

	disabled := NewTrust("cmi_policy_test_disabled", false, CmiTrustPolicy)
	disabled.Earn()
	if disabled.Trusted() || disabled.Model() != "cmi_policy_test_disabled" {
		t.Fatal("a disabled model earned trust")
	}
	if (*Trust)(nil).Trusted() {
		t.Fatal("a nil Trust is trusted")
	}
	(*Trust)(nil).Invalidate("nil-safe")
	if (*ReadModels)(nil).cmiTrusted() {
		t.Fatal("nil ReadModels trusted")
	}
}

// A writer failure drops trust at once; only a reconcile run that began after the last failure restores it.
func TestTrustInvalidate(t *testing.T) {
	tr := NewTrust("cmi_invalidate_test", true, CmiTrustPolicy)
	before := tr.Generation()
	tr.Invalidate("failed add")
	tr.Earn()
	tr.Repaired(before) // the boot run began before the failure
	if tr.Trusted() {
		t.Fatal("a run that began before the failure restored trust")
	}
	gen := tr.Generation()
	tr.Repaired(gen)
	if !tr.Trusted() {
		t.Fatal("a run that began after the failure did not restore trust")
	}
	gen = tr.Generation()
	tr.Invalidate("unseeded message")
	if tr.Trusted() {
		t.Fatal("an invalidation left trust on")
	}
	tr.Repaired(gen) // began before this failure
	if tr.Trusted() {
		t.Fatal("restored by an older run")
	}
	tr.Repaired(tr.Generation())
	if !tr.Trusted() {
		t.Fatal("not restored")
	}
}

// The writers drop cmi trust when they leave a pair missing: a failed Add, or a message stored without its seed.
func TestCmiWriterFailuresDropTrust(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	tr := NewTrust("cmi_writer_test", true, CmiTrustPolicy)
	tr.Earn()
	s := &Store{q: mock}
	WithReadModels(&ReadModels{Cmi: tr})(s)

	mock.EXPECT().AddChannelMessageIATA(gomock.Any(), gomock.Any()).Return(false, errors.New("add failed"))
	if _, err := s.AddChannelMessageIATA(context.Background(), []byte{1}, "YUL"); err == nil {
		t.Fatal("Add error not returned")
	}
	if tr.Trusted() {
		t.Fatal("a failed Add left cmi trusted")
	}
	tr.Repaired(tr.Generation())

	mock.EXPECT().InsertChannelMessage(gomock.Any(), gomock.Any()).Return(sqlc.InsertChannelMessageRow{}, errors.New("seed failed"))
	mock.EXPECT().InsertChannelMessageUnseeded(gomock.Any(), gomock.Any()).DoAndReturn(func(context.Context, sqlc.InsertChannelMessageUnseededParams) (sqlc.InsertChannelMessageUnseededRow, error) {
		if tr.Trusted() {
			t.Error("the unseeded insert ran while cmi was still trusted: a page could miss the message")
		}
		return sqlc.InsertChannelMessageUnseededRow{ID: 9}, nil
	})
	if m, err := s.InsertChannelMessage(context.Background(), ingest.InsertChannelMessageParams{ChannelID: 1, PacketHash: []byte{2}}); err != nil || m == nil || m.ID != 9 {
		t.Fatalf("fallback = %+v, %v", m, err)
	}
	if tr.Trusted() {
		t.Fatal("a message stored without its seed left cmi trusted")
	}
	tr.Repaired(tr.Generation())

	// Both statements failing: the message is not stored here, but another lane's message may be in flight, and the
	// Add after a failed insert can find none. Trust drops until the next reconcile.
	mock.EXPECT().InsertChannelMessage(gomock.Any(), gomock.Any()).Return(sqlc.InsertChannelMessageRow{}, errors.New("seed failed"))
	mock.EXPECT().InsertChannelMessageUnseeded(gomock.Any(), gomock.Any()).Return(sqlc.InsertChannelMessageUnseededRow{}, errors.New("insert failed"))
	if _, err := s.InsertChannelMessage(context.Background(), ingest.InsertChannelMessageParams{ChannelID: 1, PacketHash: []byte{2}}); err == nil {
		t.Fatal("error not returned")
	}
	if tr.Trusted() {
		t.Fatal("a failed insert left cmi trusted")
	}
	tr.Repaired(tr.Generation())

	// A plain duplicate stores nothing and changes nothing.
	mock.EXPECT().InsertChannelMessage(gomock.Any(), gomock.Any()).Return(sqlc.InsertChannelMessageRow{}, pgx.ErrNoRows)
	if m, err := s.InsertChannelMessage(context.Background(), ingest.InsertChannelMessageParams{ChannelID: 1, PacketHash: []byte{2}}); err != nil || m != nil {
		t.Fatalf("duplicate = %+v, %v", m, err)
	}
	if !tr.Trusted() {
		t.Fatal("a duplicate dropped trust")
	}
}

// A trust with an expiry is untrusted from that instant on, read by read, and the gauge follows; earning again
// with a later expiry restores it.
func TestTrustExpiry(t *testing.T) {
	tr := NewTrust("cmi_expiry_test", true, CmiTrustPolicy)
	tr.Earn()
	tr.SetExpiry(time.Now().Add(30 * time.Millisecond))
	if !tr.Trusted() {
		t.Fatal("not trusted before the expiry")
	}
	time.Sleep(40 * time.Millisecond)
	if tr.Trusted() {
		t.Fatal("trusted after the expiry")
	}
	if g := testutil.ToFloat64(metrics.ReadModelTrusted.WithLabelValues("cmi_expiry_test")); g != 0 {
		t.Fatalf("gauge %v after the expiry", g)
	}
	tr.SetExpiry(time.Now().Add(time.Minute))
	if !tr.Trusted() || testutil.ToFloat64(metrics.ReadModelTrusted.WithLabelValues("cmi_expiry_test")) != 1 {
		t.Fatal("a later expiry did not restore trust")
	}
	tr.SetExpiry(time.Time{})
	if !tr.Trusted() {
		t.Fatal("no expiry: untrusted")
	}
}

// packet_sites trust: a writer's invalidation drops it at once until a reconcile that began after it finishes; one
// repairing run drops it until two clean runs follow.
func TestPacketSitesTrustPolicy(t *testing.T) {
	tr := NewTrust("packet_sites_policy_test", true, PacketSitesTrustPolicy)
	gauge := func() float64 {
		return testutil.ToFloat64(metrics.ReadModelTrusted.WithLabelValues("packet_sites_policy_test"))
	}
	step := func(label string, want bool) {
		t.Helper()
		if tr.Trusted() != want {
			t.Fatalf("%s: trusted %v, want %v", label, tr.Trusted(), want)
		}
		if g := gauge(); (g == 1) != want {
			t.Fatalf("%s: gauge %v, want trusted %v", label, g, want)
		}
	}
	// run is one periodic reconcile that began now and repaired n rows.
	run := func(n int64) {
		gen := tr.Generation()
		tr.ObserveRun(n)
		tr.Repaired(gen)
	}
	tr.Earn()
	step("earned", true)
	tr.Invalidate("x")
	step("invalidated", false)
	tr.Invalidate("x")
	step("invalidated twice", false)
	run(0)
	step("a clean run after the invalidation", true)

	tr.Invalidate("x")
	run(3)
	step("the run that repaired the bypass", false)
	run(0)
	step("one clean run", false)
	run(0)
	step("two clean runs", true)

	run(1)
	step("one repairing run while trusted", false)
	run(0)
	run(0)
	step("restored", true)

	(*Trust)(nil).Invalidate("nil-safe")
	(*ReadModels)(nil).invalidatePacketSites("nil-safe")
	(&ReadModels{}).invalidatePacketSites("nil trust")
	if (*ReadModels)(nil).packetSitesTrusted() || (&ReadModels{}).packetSitesTrusted() {
		t.Fatal("nil packet_sites trust is trusted")
	}
}

// Any failure of the observation insert with its site row drops packet_sites trust before anything else runs;
// only a failure that is neither a cancellation nor a missing packet falls back to the unsited statement.
func TestInsertObservationSiteFallbackDropsTrust(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name       string
		err        error
		cancel     bool
		fallback   bool
		fallbackOK bool
	}{
		{name: "site write fails", err: errors.New("site failed"), fallback: true, fallbackOK: true},
		{name: "both fail", err: errors.New("site failed"), fallback: true},
		{name: "packet gone", err: &pgconn.PgError{Code: "23503"}},
		{name: "cancelled", err: context.Canceled, cancel: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			mock := mockdb.NewMockQuerier(ctrl)
			tr := NewTrust("packet_sites_writer_test", true, PacketSitesTrustPolicy)
			tr.Earn()
			s := &Store{q: mock}
			WithReadModels(&ReadModels{PacketSites: tr})(s)
			c := ctx
			if tc.cancel {
				var cancel context.CancelFunc
				c, cancel = context.WithCancel(ctx)
				cancel()
			}
			mock.EXPECT().InsertObservation(gomock.Any(), gomock.Any()).Return(sqlc.InsertObservationRow{}, tc.err)
			var genAtFallback uint64
			if tc.fallback {
				mock.EXPECT().InsertObservationUnsited(gomock.Any(), gomock.Any()).DoAndReturn(
					func(context.Context, sqlc.InsertObservationUnsitedParams) (sqlc.InsertObservationUnsitedRow, error) {
						if tr.Trusted() {
							t.Error("trust still on when the unsited insert ran")
						}
						genAtFallback = tr.Generation()
						if tc.fallbackOK {
							return sqlc.InsertObservationUnsitedRow{Inserted: true, ObservationCount: 4}, nil
						}
						return sqlc.InsertObservationUnsitedRow{}, errors.New("fallback failed")
					})
			}
			ins, n, err := s.InsertObservation(c, ingest.InsertObservationParams{PacketHash: []byte{1}, IATA: "AAA"})
			if tc.fallbackOK {
				if err != nil || !ins || n != 4 {
					t.Fatalf("fallback = %v, %d, %v", ins, n, err)
				}
			} else if err == nil {
				t.Fatal("error not returned")
			}
			if tr.Trusted() {
				t.Fatal("trust survived a failed site write")
			}
			// A reconcile that began between the first invalidation and the fallback's commit must not restore
			// trust: the writer invalidates again once the fallback returned.
			if tc.fallback && tr.Generation() <= genAtFallback {
				t.Fatalf("generation %d after the fallback, %d during it: no invalidation after the unsited write", tr.Generation(), genAtFallback)
			}
		})
	}
}

// The message writer drops cmi trust again once the unseeded fallback has returned: a reconcile that took its
// snapshot between the first invalidation and the fallback's commit must not be the one that restores trust.
func TestCmiUnseededFallbackInvalidatesAfter(t *testing.T) {
	ctrl := gomock.NewController(t)
	mock := mockdb.NewMockQuerier(ctrl)
	tr := NewTrust("cmi_unseeded_after_test", true, CmiTrustPolicy)
	tr.Earn()
	s := &Store{q: mock}
	WithReadModels(&ReadModels{Cmi: tr})(s)
	var during uint64
	mock.EXPECT().InsertChannelMessage(gomock.Any(), gomock.Any()).Return(sqlc.InsertChannelMessageRow{}, errors.New("seed failed"))
	mock.EXPECT().InsertChannelMessageUnseeded(gomock.Any(), gomock.Any()).DoAndReturn(
		func(context.Context, sqlc.InsertChannelMessageUnseededParams) (sqlc.InsertChannelMessageUnseededRow, error) {
			during = tr.Generation()
			return sqlc.InsertChannelMessageUnseededRow{ID: 9}, nil
		})
	if m, err := s.InsertChannelMessage(context.Background(), ingest.InsertChannelMessageParams{ChannelID: 1, PacketHash: []byte{2}}); err != nil || m == nil {
		t.Fatalf("fallback = %+v, %v", m, err)
	}
	if tr.Generation() <= during {
		t.Fatalf("generation %d after the unseeded insert, %d during it: no invalidation after the fallback", tr.Generation(), during)
	}
}

// Restores after a drop and policy drops are logged; the first trust is not.
func TestTrustLogsRestoresAndPolicyDrops(t *testing.T) {
	var logs bytes.Buffer
	prev := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
	t.Cleanup(func() { slog.SetDefault(prev) })
	count := func(msg string) int {
		return strings.Count(logs.String(), `msg="`+msg+`" component=readmodel model=trust_log_test`)
	}
	const again, dropped = "read model trusted again: its pages are served from it", "read model untrusted: repair runs fixed rows the writers missed"

	tr := NewTrust("trust_log_test", true, PacketSitesTrustPolicy)
	tr.Earn()
	tr.Repaired(tr.Generation())
	if count(again) != 0 {
		t.Fatalf("first trust logged as a restore:\n%s", logs.String())
	}
	tr.Invalidate("failed insert")
	tr.Repaired(tr.Generation())
	if count(again) != 1 {
		t.Fatalf("restore after an invalidation logged %d times:\n%s", count(again), logs.String())
	}
	tr.ObserveRun(3)
	tr.ObserveRun(2)
	if count(dropped) != 1 || tr.Trusted() {
		t.Fatalf("policy drop logged %d times, trusted %v:\n%s", count(dropped), tr.Trusted(), logs.String())
	}
	tr.ObserveRun(0)
	tr.ObserveRun(0)
	tr.ObserveRun(0)
	if count(again) != 2 || !tr.Trusted() {
		t.Fatalf("restore after clean runs logged %d restores in total, trusted %v:\n%s", count(again), tr.Trusted(), logs.String())
	}
}
