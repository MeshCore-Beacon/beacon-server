// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/db"
	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
)

const (
	// PacketSitesInterval is how often packet_sites is reconciled.
	PacketSitesInterval = 5 * time.Minute
	// PacketSitesAuditInterval is how often one random slice below the reconcile watermark is refilled.
	PacketSitesAuditInterval = time.Hour
	// PacketSitesSweepInterval is how often the front sweep runs.
	PacketSitesSweepInterval = 30 * time.Second

	// PacketSitesSweepLookback is how far back the front sweep visits index entries.
	PacketSitesSweepLookback = 2 * time.Minute

	packetSitesModel        = "packet_sites"
	packetSitesBackfillStep = 30 * time.Second // one Backfill call; the session is kept between calls
	packetSitesStaleRuns    = 3                // no successful reconcile for this many intervals suspends trust
	packetSitesHourly       = time.Hour        // the sweep's plan check and the size gauge
	packetSitesBusyRetry    = 5 * time.Second
	// packetSitesReadOnlyMaxAge is how old the writer's saved state may be before a read-only server stops trusting
	// the table: two reconcile intervals.
	packetSitesReadOnlyMaxAge = 2 * PacketSitesInterval
)

// processStart is when this process started; a read-only server counts the watermark age from it until a writer
// has saved the packet_sites state.
var processStart = time.Now()

// PacketSitesSession is the part of db.PacketSitesSession the jobs use.
type PacketSitesSession interface {
	State() db.PacketSitesState
	Init(ctx context.Context) (db.PacketSitesState, error)
	Reconcile(ctx context.Context) (int64, error)
	Backfill(ctx context.Context, budget time.Duration) (db.PacketSitesState, error)
	Audit(ctx context.Context, start func(lo, hi int64) int64) (int64, error)
	Verify(ctx context.Context) (db.PacketSitesVerification, error)
	Close(ctx context.Context)
}

// PacketSitesJobs repairs packet_sites at boot and every Interval, audits it hourly, sweeps its index fronts, and
// decides this process's trust in it.
type PacketSitesJobs struct {
	Begin     func(ctx context.Context) (PacketSitesSession, bool, error)
	Wait      func(ctx context.Context) error // db.WaitForPreBootWriters bound to the boot timestamp
	Trust     *db.Trust
	Interval  time.Duration
	Sweep     func(ctx context.Context) (db.SweepResult, error)
	SweepPlan func(ctx context.Context) (bool, string, error)
	Bytes     func(ctx context.Context) (int64, error)

	booted    atomic.Bool
	failures  atomic.Int32 // reconcile runs in a row that failed
	lastPlan  atomic.Int64 // unix ns of the last sweep plan check
	lastBytes atomic.Int64 // unix ns of the last size read
	busyRetry time.Duration
}

// Boot waits until no transaction from before boot can still write, then repairs the table, finishes the backfill
// and, the first time ever, verifies it; then trust is earned. Until then region pages are served by the original
// statement. A failure is logged and the repair retried after Interval. Boot returns once the repair is done (trust
// earned, or a failed first verification left to the periodic task) or ctx ends; main runs it in a goroutine.
func (j *PacketSitesJobs) Boot(ctx context.Context) {
	// Until the first reconcile finishes, the watermark age counts from boot, so a boot that never finishes shows.
	metrics.SetReadModelWatermark(packetSitesModel, time.Now())
	for {
		err := j.Wait(ctx)
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, db.ErrPreBootWriters) {
			slog.Warn("packet_sites boot repair waits for transactions from before boot", "component", "readmodel", "error", err)
			continue
		}
		slog.Error("packet_sites boot repair: pre-boot writer check failed", "component", "readmodel", "error", err)
		if !sleep(ctx, j.Interval) {
			return
		}
	}
	for {
		done, err := j.bootOnce(ctx)
		if done {
			if err != nil {
				slog.Error("packet_sites boot verification failed; the periodic task retries it", "component", "readmodel", "error", err)
			}
			return
		}
		if ctx.Err() != nil {
			return
		}
		if err != nil {
			slog.Error("packet_sites boot repair failed; retrying", "component", "readmodel", "error", err)
		}
		if !sleep(ctx, j.Interval) {
			return
		}
	}
}

func (j *PacketSitesJobs) bootOnce(ctx context.Context) (bool, error) {
	s, err := j.begin(ctx, true)
	if err != nil || s == nil {
		return false, err
	}
	defer s.Close(context.Background())
	gen := j.Trust.Generation()
	st, err := s.Init(ctx)
	if err != nil {
		return false, err
	}
	if _, err := j.reconcile(ctx, s); err != nil {
		return false, err
	}
	for !st.Complete {
		if st, err = s.Backfill(ctx, packetSitesBackfillStep); err != nil {
			return false, err
		}
		slog.Info("packet_sites backfill progress", "component", "readmodel", "backfilled_down_to_id", st.BackfilledDownToID, "writer_from_id", st.WriterFromID)
	}
	j.booted.Store(true)
	if st.FirstVerifiedAt == nil {
		v, err := s.Verify(ctx)
		if err != nil {
			return true, fmt.Errorf("packet_sites verification: %w", err)
		}
		if !v.OK() {
			return true, nil // the task verifies again every interval
		}
	}
	j.Trust.Earn()
	j.Trust.Repaired(gen)
	slog.Info("packet_sites trusted: region packet pages read packet_sites", "component", "readmodel", "trusted", j.Trust.Trusted())
	return true, nil
}

// begin takes the lock; at boot it retries a busy lock, in the tasks it skips the run (nil session).
func (j *PacketSitesJobs) begin(ctx context.Context, retry bool) (PacketSitesSession, error) {
	for {
		s, ok, err := j.Begin(ctx)
		if err != nil {
			return nil, err
		}
		if ok {
			return s, nil
		}
		if !retry {
			return nil, nil
		}
		wait := j.busyRetry
		if wait == 0 {
			wait = packetSitesBusyRetry
		}
		if !sleep(ctx, wait) {
			return nil, ctx.Err()
		}
	}
}

// reconcile runs one reconcile and publishes its rows and watermark.
func (j *PacketSitesJobs) reconcile(ctx context.Context, s PacketSitesSession) (int64, error) {
	n, err := s.Reconcile(ctx)
	if err != nil {
		return n, err
	}
	metrics.ReadModelRepaired.WithLabelValues(packetSitesModel).Add(float64(n))
	metrics.SetReadModelWatermark(packetSitesModel, time.Now())
	if n > 0 {
		slog.Warn(fmt.Sprintf("packet_sites reconcile repaired %d row(s): a writer bypassed the read model", n), "component", "readmodel")
	}
	return n, nil
}

// Task reconciles every Interval once Boot has repaired the table. Its rows feed the trust policy; a run that began
// after a writer's failure restores trust that failure dropped; a saved state that turned incomplete or unverified
// drops trust until it is complete and verified again; packetSitesStaleRuns failed runs in a row drop trust until a
// run succeeds. A busy lock skips the run.
func (j *PacketSitesJobs) Task() Task {
	return Task{
		Name:     "readmodel.packet_sites",
		Interval: j.Interval,
		Run: func(ctx context.Context) error {
			if !j.booted.Load() {
				return nil
			}
			n, st, gen, err := j.reconcileRun(ctx)
			if err != nil {
				if j.failures.Add(1) >= packetSitesStaleRuns {
					j.Trust.Invalidate(fmt.Sprintf("no successful reconcile for %d intervals", packetSitesStaleRuns))
				}
				return err
			}
			if st == nil {
				return nil // busy
			}
			j.failures.Store(0)
			j.Trust.ObserveRun(n)
			s := st.session
			defer s.Close(context.Background())
			if !st.state.Complete {
				_, err := s.Backfill(ctx, packetSitesBackfillStep)
				return err
			}
			if st.state.FirstVerifiedAt == nil {
				v, err := s.Verify(ctx)
				if err != nil {
					return err
				}
				if !v.OK() {
					return nil
				}
				j.Trust.Earn()
			}
			j.Trust.Repaired(gen)
			return nil
		},
	}
}

type openRun struct {
	session PacketSitesSession
	state   db.PacketSitesState
}

// reconcileRun takes the lock and reconciles; the session stays open on success (nil run when the lock is busy).
func (j *PacketSitesJobs) reconcileRun(ctx context.Context) (int64, *openRun, uint64, error) {
	gen := j.Trust.Generation()
	s, err := j.begin(ctx, false)
	if err != nil || s == nil {
		return 0, nil, gen, err
	}
	st, err := s.Init(ctx)
	if err != nil {
		s.Close(context.Background())
		return 0, nil, gen, err
	}
	if !st.Complete || st.FirstVerifiedAt == nil {
		// Rebuilt or never verified since this process earned trust: serve today's statement until this run (or a
		// later one) completes and verifies it.
		j.Trust.Invalidate("the read model state is incomplete or unverified")
		gen = j.Trust.Generation()
	}
	n, err := j.reconcile(ctx, s)
	if err != nil {
		s.Close(context.Background())
		return n, nil, gen, err
	}
	return n, &openRun{session: s, state: s.State()}, gen, nil
}

// AuditTask refills one random slice below the reconcile watermark every PacketSitesAuditInterval. A repair there
// was missed by every writer and reconcile: it counts as a repairing run.
func (j *PacketSitesJobs) AuditTask() Task {
	return Task{
		Name:     "readmodel.packet_sites.audit",
		Interval: PacketSitesAuditInterval,
		Run: func(ctx context.Context) error {
			if !j.booted.Load() {
				return nil
			}
			s, err := j.begin(ctx, false)
			if err != nil || s == nil {
				return err
			}
			defer s.Close(context.Background())
			st, err := s.Init(ctx)
			if err != nil {
				return err
			}
			if !st.Complete {
				return nil // a rebuild is backfilling: its missing slices are not misses
			}
			n, err := s.Audit(ctx, func(lo, hi int64) int64 { return lo + rand.Int64N(hi-lo+1) })
			if err != nil {
				return err
			}
			if n > 0 {
				metrics.ReadModelRepaired.WithLabelValues(packetSitesModel).Add(float64(n))
				slog.Warn(fmt.Sprintf("packet_sites audit repaired %d row(s) below the watermark", n), "component", "readmodel")
				j.Trust.ObserveRun(n)
			}
			return nil
		},
	}
}

// SweepTask runs the front sweep every PacketSitesSweepInterval, whether or not this process trusts the table. It
// checks the sweep's plan on its first run and then hourly, and reads the table's size hourly.
func (j *PacketSitesJobs) SweepTask() Task {
	return Task{
		Name:     "packet_sites.sweep",
		Interval: PacketSitesSweepInterval,
		Run: func(ctx context.Context) error {
			now := time.Now()
			if last := j.lastPlan.Load(); last == 0 || now.Sub(time.Unix(0, last)) >= packetSitesHourly {
				ok, detail, err := j.SweepPlan(ctx)
				switch {
				case err != nil:
					slog.Error("packet_sites sweep plan check failed", "component", "readmodel", "error", err)
				default:
					j.lastPlan.Store(now.UnixNano())
					if ok {
						metrics.PacketSitesSweepPlanOK.Set(1)
					} else {
						metrics.PacketSitesSweepPlanOK.Set(0)
						slog.Error("packet_sites sweep plan sets no dead-entry hints; the indexes will bloat", "component", "readmodel", "detail", detail)
					}
				}
			}
			if last := j.lastBytes.Load(); last == 0 || now.Sub(time.Unix(0, last)) >= packetSitesHourly {
				if b, err := j.Bytes(ctx); err != nil {
					slog.Error("packet_sites size read failed", "component", "readmodel", "error", err)
				} else {
					j.lastBytes.Store(now.UnixNano())
					metrics.PacketSitesBytes.Set(float64(b))
				}
			}
			res, err := j.Sweep(ctx)
			if err != nil {
				return fmt.Errorf("packet_sites sweep: %w", err)
			}
			metrics.MarkPacketSitesSwept(time.Now())
			metrics.PacketSitesFrontLive.WithLabelValues("idx_packet_sites_site").Set(float64(res.SiteLive))
			metrics.PacketSitesFrontLive.WithLabelValues("idx_packet_sites_site_type").Set(float64(res.TypeLive))
			metrics.PacketSitesFrontLive.WithLabelValues("idx_packet_sites_site_scope").Set(float64(res.ScopeLive))
			metrics.JobRows.WithLabelValues("packet_sites.sweep").Add(float64(res.SiteLive))
			return nil
		},
	}
}

// PacketSitesReadOnlyTrust earns trust on a read-only server when the persisted state is complete and verified, its
// writer saved it within two reconcile intervals, and the exact check over the newest observations is clean;
// otherwise it drops trust. It publishes the writer's watermark age and writes nothing.
func PacketSitesReadOnlyTrust(ctx context.Context, check func(context.Context) (db.PacketSitesReadOnlyCheck, error), trust *db.Trust) {
	gen := trust.Generation()
	c, err := check(ctx)
	switch {
	case err == nil && c.Found:
		metrics.SetReadModelWatermark(packetSitesModel, time.Now().Add(-c.Age))
	case err == nil:
		// No writer has saved the state yet: the age counts from this process's start.
		metrics.SetReadModelWatermark(packetSitesModel, processStart)
	}
	st, v := c.State, c.Verification
	switch {
	case err != nil:
		slog.Error("packet_sites read-only check failed", "component", "readmodel", "error", err)
	case !st.Complete || st.FirstVerifiedAt == nil:
		slog.Info("packet_sites not trusted on this read-only server: backfill or first verification not done", "component", "readmodel", "complete", st.Complete)
	case c.Age > packetSitesReadOnlyMaxAge:
		slog.Warn("packet_sites not trusted on this read-only server: the writer has not saved its state recently", "component", "readmodel", "age", c.Age.Round(time.Second))
	case !v.OK():
		slog.Warn("packet_sites not trusted on this read-only server: the newest observations fail the exact check", "component", "readmodel", "missing", v.Missing, "wrong", v.Wrong, "extra", v.Extra)
	default:
		if !trust.Trusted() {
			slog.Info("packet_sites trusted on this read-only server: region packet pages read packet_sites", "component", "readmodel")
		}
		// Trust ends when the writer's state turns too old, not at the next check, unless that check finds it
		// saved again.
		trust.SetExpiry(time.Now().Add(packetSitesReadOnlyMaxAge - c.Age))
		trust.Earn()
		trust.Repaired(gen)
		return
	}
	trust.Invalidate("read-only check not clean")
}

// PacketSitesReadOnlyWatch runs PacketSitesReadOnlyTrust now and every interval until ctx ends.
func PacketSitesReadOnlyWatch(ctx context.Context, check func(context.Context) (db.PacketSitesReadOnlyCheck, error), trust *db.Trust, interval time.Duration) {
	for {
		PacketSitesReadOnlyTrust(ctx, check, trust)
		if !sleep(ctx, interval) {
			return
		}
	}
}
