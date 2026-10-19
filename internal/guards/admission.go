// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package guards

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/metrics"
)

// Guard limits.
const (
	// ClientCap is the number of requests per client that may run database statements at once.
	ClientCap = 4
	// ClientWait is how long a request may wait for one of its client's slots.
	ClientWait = 5 * time.Second
	// AcquireLimit is how long a statement may wait for a pooled connection.
	AcquireLimit = 5 * time.Second
	// BudgetBurst is the database time a client may spend before it must wait for refill.
	BudgetBurst = 30 * time.Second
)

var guardNames = []string{"deadline", "cap", "admission", "budget"}

func init() {
	// Create every series up front so dashboards read 0 rather than nothing.
	for _, f := range Families {
		for _, gn := range guardNames {
			metrics.GuardWouldRefuse.WithLabelValues(string(f), gn)
			metrics.GuardRefused.WithLabelValues(string(f), gn)
		}
	}
}

// Options configures a Guard.
type Options struct {
	// Enforce lists the families whose guards refuse; every other family observes.
	Enforce []Family
	// ForceObserve makes every family observe whatever Enforce says (used by the
	// single plan layout).
	ForceObserve bool
	// Budget is the budget mode; "shadow" is the only mode.
	Budget string
}

type enforceSet struct {
	list []Family
	set  map[Family]bool
}

func newEnforceSet(fs []Family) *enforceSet {
	s := &enforceSet{set: make(map[Family]bool, len(fs))}
	for _, f := range fs {
		s.set[f] = true
	}
	for _, f := range Families {
		if s.set[f] {
			s.list = append(s.list, f)
		}
	}
	return s
}

// Guard holds the per-client slots and budgets shared by all requests.
type Guard struct {
	enforce      atomic.Pointer[enforceSet]
	forceObserve bool
	budget       string
	begun        atomic.Int64

	mu    sync.Mutex
	slots map[string]*slotState

	bmu     sync.Mutex
	budgets map[string]*bucket
	debits  int
}

// New returns a Guard. Nothing refuses unless o.Enforce names its family.
func New(o Options) *Guard {
	g := &Guard{
		forceObserve: o.ForceObserve,
		budget:       o.Budget,
		slots:        make(map[string]*slotState),
		budgets:      make(map[string]*bucket),
	}
	if g.budget == "" {
		g.budget = "shadow"
	}
	g.enforce.Store(newEnforceSet(o.Enforce))
	return g
}

// SetEnforce replaces the enforcing families. It applies to requests that begin afterwards.
func (g *Guard) SetEnforce(f []Family) { g.enforce.Store(newEnforceSet(f)) }

// Enforce returns the enforcing families in Families order.
func (g *Guard) Enforce() []Family { return slices.Clone(g.enforce.Load().list) }

// ForceObserve reports whether every family observes regardless of Enforce.
func (g *Guard) ForceObserve() bool { return g.forceObserve }

// Begun returns the number of requests that have passed through Begin.
func (g *Guard) Begun() int64 { return g.begun.Load() }

// Slot states of a request.
const (
	slotNone      = iota // not admitted yet, or gave up waiting
	slotHolder           // holds one of its client's slots
	slotWaiting          // queued for a slot
	slotDiscarded        // an observed waiter dropped at handover
)

type stateKey struct{}

// reqState is the guard state of one request.
type reqState struct {
	g        *Guard
	family   Family
	class    Class
	key      string
	enforced bool

	// Guarded by g.mu.
	admitted bool
	done     bool
	slot     int
	waiter   *waiter

	// Guarded by mu.
	mu         sync.Mutex
	armedAt    time.Time
	deadline   time.Time
	lastFinish time.Time
	flagged    map[string]bool
	refusal    *Refusal
}

// count records guard at most once per request: as refused when refusal is
// set (which also becomes the request's refusal), else as would-refuse.
func (s *reqState) count(guard string, refusal *Refusal) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.flagged[guard] {
		return
	}
	if s.flagged == nil {
		s.flagged = make(map[string]bool)
	}
	s.flagged[guard] = true
	if refusal != nil {
		metrics.GuardRefused.WithLabelValues(string(s.family), guard).Inc()
		if s.refusal == nil {
			s.refusal = refusal
		}
		return
	}
	metrics.GuardWouldRefuse.WithLabelValues(string(s.family), guard).Inc()
}

func (s *reqState) currentRefusal() *Refusal {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.refusal
}

func stateFrom(ctx context.Context) *reqState {
	s, _ := ctx.Value(stateKey{}).(*reqState)
	return s
}

type slotState struct {
	holders int
	waiters []*waiter
}

type waiter struct {
	st      *reqState
	arrived time.Time
	ch      chan struct{} // closed when an enforced waiter is handed a slot or its request ends
}

// Begin attaches request state; the returned finish releases the client slot and records the request.
func (g *Guard) Begin(ctx context.Context, key string, f Family, c Class) (context.Context, func()) {
	g.begun.Add(1)
	es := g.enforce.Load()
	st := &reqState{g: g, family: f, class: c, key: key, enforced: es.set[f] && !g.forceObserve}
	var once sync.Once
	return context.WithValue(ctx, stateKey{}, st), func() { once.Do(func() { g.finish(st) }) }
}

func (g *Guard) finish(st *reqState) {
	g.mu.Lock()
	defer g.mu.Unlock()
	st.done = true
	switch st.slot {
	case slotHolder:
		st.slot = slotNone
		g.release(st.key)
	case slotWaiting:
		w := st.waiter
		g.dequeue(st.key, w)
		st.slot = slotNone
		if w.ch != nil {
			close(w.ch)
		} else if time.Since(w.arrived) > ClientWait {
			st.count("cap", nil)
		}
	}
}

// release gives up one holder of key and hands slots to waiters, oldest first. g.mu must be held.
func (g *Guard) release(key string) {
	s := g.slots[key]
	if s == nil {
		return
	}
	s.holders--
	now := time.Now()
	for s.holders < ClientCap && len(s.waiters) > 0 {
		w := s.waiters[0]
		s.waiters = s.waiters[1:]
		w.st.waiter = nil
		switch {
		case w.ch != nil:
			w.st.slot = slotHolder
			s.holders++
			close(w.ch)
		case now.Sub(w.arrived) > ClientWait:
			w.st.slot = slotDiscarded
			w.st.count("cap", nil)
		default:
			w.st.slot = slotHolder
			s.holders++
		}
	}
	g.tidy(key, s)
}

// dequeue removes w from key's queue. g.mu must be held.
func (g *Guard) dequeue(key string, w *waiter) {
	s := g.slots[key]
	if s == nil {
		return
	}
	if i := slices.Index(s.waiters, w); i >= 0 {
		s.waiters = slices.Delete(s.waiters, i, i+1)
	}
	w.st.waiter = nil
	g.tidy(key, s)
}

func (g *Guard) tidy(key string, s *slotState) {
	if s.holders <= 0 && len(s.waiters) == 0 {
		delete(g.slots, key)
	}
}

// slotsHeld returns the number of holders for key.
func (g *Guard) slotsHeld(key string) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	if s := g.slots[key]; s != nil {
		return s.holders
	}
	return 0
}

// Gate is what the statement router calls before each guarded statement.
type Gate interface {
	Admit(ctx context.Context) (Ticket, error)
}

// Ticket follows one statement through acquire, run and finish.
type Ticket interface {
	// AcquireTimeout is the pool-acquire limit: AcquireLimit when enforced, else 0 (no limit).
	AcquireTimeout() time.Duration
	// Acquired is called after the pool acquire with its error.
	Acquired(err error)
	// Start returns the statement context.
	Start(ctx context.Context) (context.Context, context.CancelFunc)
	// Finish is called after the statement, once its rows are closed.
	Finish()
}

type nopGate struct{}

func (nopGate) Admit(context.Context) (Ticket, error) { return nopTicket{}, nil }

// NopGate admits everything; its ticket is inert.
var NopGate Gate = nopGate{}

type nopTicket struct{}

func (nopTicket) AcquireTimeout() time.Duration { return 0 }
func (nopTicket) Acquired(error)                {}
func (nopTicket) Start(ctx context.Context) (context.Context, context.CancelFunc) {
	return ctx, func() {}
}
func (nopTicket) Finish() {}

// Admit takes the request's client slot on its first call and returns a ticket
// for one statement. Without request state, or after the request finished, the
// ticket is inert.
func (g *Guard) Admit(ctx context.Context) (Ticket, error) {
	st := stateFrom(ctx)
	if st == nil || st.g != g {
		return nopTicket{}, nil
	}
	g.mu.Lock()
	if st.done {
		g.mu.Unlock()
		return nopTicket{}, nil
	}
	if st.admitted {
		g.mu.Unlock()
		return g.newTicket(ctx, st), nil
	}
	st.admitted = true
	s := g.slots[st.key]
	if s == nil {
		s = &slotState{}
		g.slots[st.key] = s
	}
	if s.holders < ClientCap {
		s.holders++
		st.slot = slotHolder
		g.mu.Unlock()
		return g.newTicket(ctx, st), nil
	}
	w := &waiter{st: st, arrived: time.Now()}
	if !st.enforced {
		// Observed: a virtual wait, settled at handover or when the request ends.
		s.waiters = append(s.waiters, w)
		st.slot = slotWaiting
		st.waiter = w
		g.mu.Unlock()
		return g.newTicket(ctx, st), nil
	}
	w.ch = make(chan struct{})
	s.waiters = append(s.waiters, w)
	st.slot = slotWaiting
	st.waiter = w
	g.mu.Unlock()

	timer := time.NewTimer(ClientWait)
	defer timer.Stop()
	var waitErr error
	select {
	case <-w.ch:
	case <-timer.C:
		waitErr = capRefusal()
	case <-ctx.Done():
		waitErr = ctx.Err()
	}
	g.mu.Lock()
	if st.done {
		g.mu.Unlock()
		return nopTicket{}, nil
	}
	if st.slot == slotHolder {
		g.mu.Unlock()
		return g.newTicket(ctx, st), nil
	}
	g.dequeue(st.key, w)
	st.slot = slotNone
	g.mu.Unlock()
	var r *Refusal
	if errors.As(waitErr, &r) {
		st.count("cap", r)
	}
	return nil, waitErr
}

func (g *Guard) newTicket(ctx context.Context, st *reqState) *ticket {
	return &ticket{st: st, reqCtx: ctx, admittedAt: time.Now()}
}

type ticket struct {
	st         *reqState
	reqCtx     context.Context
	admittedAt time.Time
	stmtCtx    context.Context
	startedAt  time.Time
}

func (t *ticket) AcquireTimeout() time.Duration {
	if t.st.enforced {
		return AcquireLimit
	}
	return 0
}

func (t *ticket) Acquired(err error) {
	if t.st.enforced {
		if errors.Is(err, context.DeadlineExceeded) && t.reqCtx.Err() == nil {
			t.st.count("admission", admissionRefusal())
		}
		return
	}
	if time.Since(t.admittedAt) > AcquireLimit {
		t.st.count("admission", nil)
	}
}

func (t *ticket) Start(ctx context.Context) (context.Context, context.CancelFunc) {
	now := time.Now()
	st := t.st
	st.mu.Lock()
	if st.armedAt.IsZero() {
		st.armedAt = now
		st.deadline = now.Add(st.class.Deadline())
	}
	deadline := st.deadline
	st.mu.Unlock()
	t.startedAt = now
	if !st.enforced {
		t.stmtCtx = ctx
		return ctx, func() {}
	}
	sctx, cancel := context.WithDeadline(ctx, deadline)
	t.stmtCtx = sctx
	return sctx, cancel
}

func (t *ticket) Finish() {
	now := time.Now()
	st := t.st
	st.mu.Lock()
	st.lastFinish = now
	deadline := st.deadline
	st.mu.Unlock()
	if t.startedAt.IsZero() {
		return
	}
	if st.enforced {
		if errors.Is(t.stmtCtx.Err(), context.DeadlineExceeded) {
			st.count("deadline", deadlineRefusal())
		}
	} else if now.After(deadline) {
		st.count("deadline", nil)
	}
	if st.g.debit(st.key, now.Sub(t.startedAt), now) {
		st.count("budget", nil)
	}
}
