// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"testing"
	"time"

	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
	"github.com/MeshCore-Beacon/beacon-server/internal/config"
	"github.com/MeshCore-Beacon/beacon-server/internal/polling"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
)

// reqDevice is an observer with a real node key, so envelopes can be sealed to it and opened as the firmware does.
type reqDevice struct{ pub, prv []byte }

func newReqDevice(n byte) reqDevice {
	seed := bytes.Repeat([]byte{n}, 32)
	h := sha512.Sum512(seed)
	h[0] &= 248
	h[31] &= 63
	h[31] |= 64
	return reqDevice{pub: ed25519.NewKeyFromSeed(seed).Public().(ed25519.PublicKey), prv: h[:]}
}

// reqNode is one node entry of a claim.
func reqNode(target []byte, requested bool, state string) string {
	r := ""
	if requested {
		r = `,"requested":true`
	}
	return fmt.Sprintf(`{"node_id":%q,"key":"own","state":%q%s}`, hex.EncodeToString(target), state, r)
}

func reqClaimJSON(obs []byte, at time.Time, slots int, nodes ...string) []byte {
	return fmt.Appendf(nil, `{"v":2,"origin":"o","origin_id":%q,"timestamp":%q,"plugin":"fw","accepts_requests":true,"request_slots":%d,"nodes":[%s]}`,
		hex.EncodeToString(obs), at.Format(time.RFC3339), slots, strings.Join(nodes, ","))
}

type reqWorld struct {
	t      *testing.T
	ctx    context.Context
	pool   *pgxpool.Pool
	s      *Store
	svc    *polling.Service
	reqs   *polling.RequestService
	sealer *polling.Sealer
	now    time.Time
}

func newReqWorld(t *testing.T, mode string) *reqWorld {
	t.Helper()
	ctx, pool, s := pollStore(t)
	w := &reqWorld{t: t, ctx: ctx, pool: pool, s: s, now: time.Now().UTC().Truncate(time.Second)}
	var err error
	if w.sealer, err = polling.NewSealer(bytes.Repeat([]byte{9}, 32)); err != nil {
		t.Fatal(err)
	}
	if mode == "" {
		mode = config.RequestObserversAll
	}
	cfg := config.ResolvedPolling{Enabled: true, RequestObservers: mode}
	w.svc = polling.NewService(s, cfg, func() time.Time { return w.now }, slog.New(slog.DiscardHandler))
	w.svc.SetRequests(s, w.sealer)
	w.reqs = polling.NewRequestService(s, w.sealer, w.svc, 3, func() time.Time { return w.now }, slog.New(slog.DiscardHandler))
	return w
}

func (w *reqWorld) node(key byte) uuid.UUID {
	w.t.Helper()
	var id uuid.UUID
	if err := w.pool.QueryRow(w.ctx, `INSERT INTO nodes (public_key, node_type, name) VALUES ($1, 2, 'Hill') RETURNING id`, pollKey(key)).Scan(&id); err != nil {
		w.t.Fatal(err)
	}
	return id
}

func (w *reqWorld) claim(d reqDevice, slots int, nodes ...string) {
	w.t.Helper()
	w.svc.HandleClaim(w.ctx, hex.EncodeToString(d.pub), reqClaimJSON(d.pub, w.now, slots, nodes...))
}

func (w *reqWorld) owners(d reqDevice, target byte) (polling.OwnerEntry, bool) {
	w.t.Helper()
	a, err := w.svc.Owners(w.ctx, polling.Caller{Observer: d.pub})
	if err != nil {
		w.t.Fatal(err)
	}
	e, ok := a.Nodes[hex.EncodeToString(pollKey(target))]
	return e, ok
}

func (w *reqWorld) opens(d reqDevice, target byte, e polling.OwnerEntry) string {
	w.t.Helper()
	if e.PW == nil {
		w.t.Fatal("no pw")
	}
	pw, err := polling.OpenEnvelope(d.prv, d.pub, pollKey(target), *e.PW)
	if err != nil {
		w.t.Fatal(err)
	}
	return string(pw)
}

func (w *reqWorld) status(u uuid.UUID, id uuid.UUID) polling.RequestRow {
	w.t.Helper()
	rows, err := w.reqs.List(w.ctx, u)
	if err != nil {
		w.t.Fatal(err)
	}
	for _, r := range rows {
		if r.ID == id {
			return r
		}
	}
	w.t.Fatal("request gone")
	return polling.RequestRow{}
}

func TestRequestedTargetsEndToEndPostgres(t *testing.T) {
	w := newReqWorld(t, "")
	ctx, s, pool := w.ctx, w.s, w.pool
	alice, bob, carol := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob"), newUser(t, ctx, s, "carol")
	o1, o2, o3 := newReqDevice(1), newReqDevice(2), newReqDevice(3)
	n1, n2 := w.node(0xA1), w.node(0xA2)
	t1, t2 := byte(0xA1), byte(0xA2)
	for _, d := range []reqDevice{o1, o2, o3} {
		pollObserver(t, ctx, s, d.pub)
	}
	// Both targets are heard directly by o1 (better) and o2; o3 accepts requests but has never heard either.
	heard := w.now.Add(-time.Hour)
	for _, tg := range []byte{t1, t2} {
		pollHearAdvert(t, ctx, pool, pollKey(tg), pollObserver(t, ctx, s, o1.pub), heard, 9)
		pollHearAdvert(t, ctx, pool, pollKey(tg), pollObserver(t, ctx, s, o2.pub), heard, 4)
	}
	// One free seat each: the two observers can hold one requested target apiece.
	w.claim(o1, 1)
	w.claim(o2, 1)
	w.claim(o3, 3)

	// 1. A request with a password: the better observer polls, the other stands by, both get their own envelope.
	r1, err := w.reqs.Create(ctx, alice.ID, n1, []byte("alicepw"))
	if err != nil {
		t.Fatal(err)
	}
	if got := w.status(alice.ID, r1.ID); got.Status != polling.StatusActive || got.PollerID == nil {
		t.Fatalf("after create: %+v", got)
	}
	e1, ok := w.owners(o1, t1)
	if !ok || !e1.Requested || e1.Role != "polling" || !e1.Mine || e1.PW == nil {
		t.Fatalf("poller: %+v", e1)
	}
	if got := w.opens(o1, t1, e1); got != "alicepw" {
		t.Fatalf("poller opens %q", got)
	}
	e2, ok := w.owners(o2, t1)
	if !ok || !e2.Requested || e2.Role != "standby" || e2.StandbyRank != 1 || w.opens(o2, t1, e2) != "alicepw" {
		t.Fatalf("standby: %+v", e2)
	}
	if e1.PW.EPK == e2.PW.EPK {
		t.Fatal("two observers share an envelope")
	}
	if _, ok := w.owners(o3, t1); ok {
		t.Fatal("an observer with no advert evidence was handed the repeater")
	}
	ver1 := e1.PW.Ver

	// 2. Both seats are taken by the older request, so a second repeater waits, and says why.
	r2, err := w.reqs.Create(ctx, bob.ID, n2, []byte("bobpw"))
	if err != nil {
		t.Fatal(err)
	}
	if got := w.status(bob.ID, r2.ID); got.Status != polling.StatusWaiting || got.Reason != polling.ReasonNoSeat {
		t.Fatalf("beyond the seats: %+v", got)
	}
	if _, ok := w.owners(o1, t2); ok {
		t.Fatal("handed a repeater beyond its seats")
	}

	// 3. Devices that now hold T1 report it as requested. A claim states "ok": the poller is not failing.
	w.now = w.now.Add(time.Hour)
	w.claim(o1, 0, reqNode(pollKey(t1), true, "ok"))
	w.claim(o2, 0, reqNode(pollKey(t1), true, "pending"))
	w.claim(o3, 3)
	if e, ok := w.owners(o1, t1); !ok || e.PW == nil || w.opens(o1, t1, e) != "alicepw" {
		t.Fatalf("a holder with no free seats still needs its envelope: %+v", e)
	}

	// 4. A standby's login_failed does not count; the poller's does, and with no other password the repeater is
	// not requested any more, so both devices drop it.
	w.now = w.now.Add(time.Hour)
	w.claim(o2, 0, reqNode(pollKey(t1), true, "login_failed"))
	if got := w.status(alice.ID, r1.ID); got.Status == polling.StatusLoginFailed {
		t.Fatalf("standby's report counted: %+v", got)
	}
	w.claim(o1, 0, reqNode(pollKey(t1), true, "login_failed"))
	if got := w.status(alice.ID, r1.ID); got.Status != polling.StatusLoginFailed || !got.HasPassword {
		t.Fatalf("poller's report: %+v", got)
	}
	if _, ok := w.owners(o1, t1); ok {
		t.Fatal("a repeater that is not requested any more is still in the answer")
	}

	// 5. A new password revives it and raises ver, and the devices get the new one.
	if _, err := w.reqs.Update(ctx, alice.ID, r1.ID, polling.RequestUpdate{SetPassword: true, Password: []byte("newpw")}); err != nil {
		t.Fatal(err)
	}
	e1, ok = w.owners(o1, t1)
	if !ok || e1.PW == nil || w.opens(o1, t1, e1) != "newpw" || e1.PW.Ver <= ver1 {
		t.Fatalf("after a new password: %+v (old ver %d)", e1, ver1)
	}
	if got := w.status(alice.ID, r1.ID); got.Status != polling.StatusActive {
		t.Fatalf("after a new password: %+v", got)
	}

	// 6. An admin answer from any observer: the password is deleted, the stop recorded, and that observer is never
	// offered the repeater again.
	// o2's device fetched the new password before it logged in with it.
	if e, ok := w.owners(o2, t1); !ok || w.opens(o2, t1, e) != "newpw" {
		t.Fatalf("o2 was not given the new password: %+v", e)
	}
	w.now = w.now.Add(time.Hour)
	w.claim(o1, 0, reqNode(pollKey(t1), true, "ok"))
	w.claim(o2, 1, reqNode(pollKey(t1), true, "admin_stop"))
	if got := w.status(alice.ID, r1.ID); got.Status != polling.StatusAdminStopped || got.HasPassword {
		t.Fatalf("admin stop: %+v", got)
	}
	var ct []byte
	if err := pool.QueryRow(ctx, `SELECT password_ct FROM telemetry_requests WHERE id = $1`, r1.ID).Scan(&ct); err != nil || ct != nil {
		t.Fatalf("password still stored: %x %v", ct, err)
	}
	stops, err := s.ListPollingAdminStops(ctx)
	if err != nil || len(stops) != 1 || stops[0].ObserverPublicKey != hex.EncodeToString(o2.pub) {
		t.Fatalf("stops %+v %v", stops, err)
	}
	// With T1 no longer requested, T2 (bob) gets the seats: o2 is excluded from T1 only.
	if err := w.svc.ElectTarget(ctx, pollKey(t2)); err != nil {
		t.Fatal(err)
	}
	if got := w.status(bob.ID, r2.ID); got.Status != polling.StatusActive {
		t.Fatalf("T2 after T1 freed its seats: %+v", got)
	}

	// 7. A new request for T1 never goes to the observer that was given admin access, and the operator's clear
	// lifts that.
	rc, err := w.reqs.Create(ctx, carol.ID, n1, []byte("carolpw"))
	if err != nil {
		t.Fatal(err)
	}
	if got := w.status(carol.ID, rc.ID); got.Status != polling.StatusActive {
		t.Fatalf("carol: %+v", got)
	}
	if e, ok := w.owners(o1, t1); !ok || w.opens(o1, t1, e) != "carolpw" {
		t.Fatalf("o1 for carol: %+v", e)
	}
	if _, ok := w.owners(o2, t1); ok {
		t.Fatal("an observer given admin access was offered the repeater again")
	}
	if found, err := s.DeletePollingAdminStop(ctx, pollKey(t1), o2.pub); err != nil || !found {
		t.Fatal(found, err)
	}
	if found, _ := s.DeletePollingAdminStop(ctx, pollKey(t1), o2.pub); found {
		t.Fatal("cleared twice")
	}
	// Its device keeps its own stop until the operator runs poll enable there, which shows in its next claim.
	w.now = w.now.Add(time.Minute)
	w.claim(o2, 1)
	if e, ok := w.owners(o2, t1); !ok || e.Role != "standby" || w.opens(o2, t1, e) != "carolpw" {
		t.Fatalf("after the operator cleared the stop: %+v", e)
	}
}

func TestRequestedLocalTargetKeepsBeingLocalPostgres(t *testing.T) {
	w := newReqWorld(t, "")
	ctx, s := w.ctx, w.s
	alice := newUser(t, ctx, s, "alice")
	local, other := newReqDevice(1), newReqDevice(2)
	node := w.node(0xA1)
	pollHearAdvert(t, ctx, w.pool, pollKey(0xA1), pollObserver(t, ctx, s, local.pub), w.now.Add(-time.Hour), 2)
	pollHearAdvert(t, ctx, w.pool, pollKey(0xA1), pollObserver(t, ctx, s, other.pub), w.now.Add(-time.Hour), 9)
	w.claim(local, 3, reqNode(pollKey(0xA1), false, "ok"))
	w.claim(other, 3)

	r, err := w.reqs.Create(ctx, alice.ID, node, []byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	if got := w.status(alice.ID, r.ID); got.Status != polling.StatusActive {
		t.Fatalf("the request rides on the local poll: %+v", got)
	}
	if e, ok := w.owners(local, 0xA1); !ok || e.Requested || e.PW != nil || e.Role != "polling" {
		t.Fatalf("local claimant: %+v", e)
	}
	if _, ok := w.owners(other, 0xA1); ok {
		t.Fatal("nothing extra is assigned while a local claimant has the repeater")
	}
}

func TestRequestObserversApprovedPostgres(t *testing.T) {
	w := newReqWorld(t, config.RequestObserversApproved)
	ctx, s := w.ctx, w.s
	alice := newUser(t, ctx, s, "alice")
	o := newReqDevice(1)
	node := w.node(0xA1)
	pollHearAdvert(t, ctx, w.pool, pollKey(0xA1), pollObserver(t, ctx, s, o.pub), w.now.Add(-time.Hour), 5)
	w.claim(o, 3)
	r, err := w.reqs.Create(ctx, alice.ID, node, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := w.status(alice.ID, r.ID); got.Status != polling.StatusWaiting || got.Reason != polling.ReasonNoObserver {
		t.Fatalf("unapproved observer: %+v", got)
	}
	if err := s.SetPollingObserver(ctx, o.pub, false, true, ""); err != nil {
		t.Fatal(err)
	}
	if err := w.svc.ElectAll(ctx); err != nil {
		t.Fatal(err)
	}
	got := w.status(alice.ID, r.ID)
	if got.Status != polling.StatusActive || got.PollerID == nil {
		t.Fatalf("approved observer: %+v", got)
	}
	// The blank or hello option: requested, no envelope.
	if e, ok := w.owners(o, 0xA1); !ok || !e.Requested || e.PW != nil {
		t.Fatalf("blank option: %+v", e)
	}
}

func TestRequestedAdminStateAndOfflinePostgres(t *testing.T) {
	w := newReqWorld(t, "")
	ctx, s, pool := w.ctx, w.s, w.pool
	alice, bob := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob")
	o := newReqDevice(1)
	node := w.node(0xA1)
	pollHearAdvert(t, ctx, pool, pollKey(0xA1), pollObserver(t, ctx, s, o.pub), w.now.Add(-time.Hour), 5)
	w.claim(o, 3)
	r, err := w.reqs.Create(ctx, alice.ID, node, nil)
	if err != nil {
		t.Fatal(err)
	}

	// A blank login answered with admin access blocks every passwordless request until the operator clears it.
	w.now = w.now.Add(time.Hour)
	w.claim(o, 3, reqNode(pollKey(0xA1), true, "admin_stop"))
	if got := w.status(alice.ID, r.ID); got.Status != polling.StatusAdminStopped {
		t.Fatalf("%+v", got)
	}
	if _, err := w.reqs.Create(ctx, bob.ID, node, nil); err != polling.ErrNoPasswordBlocked {
		t.Fatalf("passwordless request on a blocked repeater: %v", err)
	}
	blocked, err := s.ListNoPasswordBlocked(ctx)
	if err != nil || len(blocked) != 1 || blocked[0] != hex.EncodeToString(pollKey(0xA1)) {
		t.Fatalf("%v %v", blocked, err)
	}
	targets, err := s.PollingTargetsAdmin(ctx, w.now.Add(-time.Hour), 10)
	if err != nil || len(targets) != 1 || targets[0].Requests != 1 || targets[0].Requested {
		t.Fatalf("admin view %+v %v", targets, err)
	}
	if found, err := s.ClearNoPasswordBlocked(ctx, pollKey(0xA1)); err != nil || !found {
		t.Fatal(found, err)
	}
	if found, _ := s.ClearNoPasswordBlocked(ctx, pollKey(0xEE)); found {
		t.Fatal("unknown repeater found")
	}
	if rb, err := w.reqs.Create(ctx, bob.ID, node, nil); err != nil || rb.Status == polling.StatusAdminStopped {
		t.Fatalf("after the operator cleared it: %+v %v", rb, err)
	}
	if targets, _ = s.PollingTargetsAdmin(ctx, w.now.Add(-time.Hour), 10); len(targets) != 1 || targets[0].Requests != 2 || !targets[0].Requested {
		t.Fatalf("admin view %+v", targets)
	}

	// Offline: not heard for the cutoff and enabled before it. A recently heard repeater is left alone.
	if _, err := pool.Exec(ctx, `UPDATE nodes SET last_seen = $1`, w.now.Add(-10*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE telemetry_requests SET enabled_at = $1`, w.now.Add(-9*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	cutoff := w.now.Add(-7 * 24 * time.Hour)
	if n, err := s.DisableOfflineRequests(ctx, cutoff, w.now, "Not heard for 7 days."); err != nil || n != 1 {
		t.Fatalf("disabled %d %v", n, err)
	}
	rows, _ := s.UserTelemetryRequests(ctx, bob.ID)
	if len(rows) != 1 || rows[0].Status != polling.StatusDisabled || rows[0].Reason != "Not heard for 7 days." {
		t.Fatalf("%+v", rows)
	}
	// Nothing resumes by itself; the user re-enables it by hand and the clock restarts.
	if n, _ := s.DisableOfflineRequests(ctx, cutoff, w.now, "x"); n != 0 {
		t.Fatalf("disabled again: %d", n)
	}
	if _, err := w.reqs.Update(ctx, bob.ID, rows[0].ID, polling.RequestUpdate{Enable: true}); err != nil {
		t.Fatal(err)
	}
	if n, _ := s.DisableOfflineRequests(ctx, cutoff, w.now, "x"); n != 0 {
		t.Fatalf("a re-enabled request was disabled again at once: %d", n)
	}
}

func TestPollingOwnerRowsRequestedPostgres(t *testing.T) {
	// A requested assignment is listed before the observer's claim names the target; that is how it learns of it.
	w := newReqWorld(t, "")
	ctx, s := w.ctx, w.s
	alice := newUser(t, ctx, s, "alice")
	o := newReqDevice(1)
	node := w.node(0xA1)
	pollHearAdvert(t, ctx, w.pool, pollKey(0xA1), pollObserver(t, ctx, s, o.pub), w.now.Add(-time.Hour), 5)
	w.claim(o, 2)
	if _, err := w.reqs.Create(ctx, alice.ID, node, []byte("x")); err != nil {
		t.Fatal(err)
	}
	rows, err := s.PollingOwnerRows(ctx, o.pub, w.now.Add(-time.Hour))
	if err != nil || len(rows) != 1 || !rows[0].Requested || rows[0].ClaimedRequested || !rows[0].AcceptsRequests || rows[0].RequestSlots != 2 || rows[0].Rank != 0 {
		t.Fatalf("%+v %v", rows, err)
	}
	// Once it holds the target its claim has no free seat left, and the envelope still goes out.
	w.now = w.now.Add(time.Minute)
	w.claim(o, 0, reqNode(pollKey(0xA1), true, "ok"))
	if rows, _ = s.PollingOwnerRows(ctx, o.pub, w.now.Add(-time.Hour)); len(rows) != 1 || !rows[0].ClaimedRequested || rows[0].RequestSlots != 0 || !rows[0].Requested {
		t.Fatalf("%+v", rows)
	}
	if e, ok := w.owners(o, 0xA1); !ok || e.PW == nil {
		t.Fatalf("held target not offered: %+v", e)
	}
	// Full and not holding it: it loses the seat at the next election, and the target leaves its answer.
	w.now = w.now.Add(time.Minute)
	w.claim(o, 0)
	if rows, _ = s.PollingOwnerRows(ctx, o.pub, w.now.Add(-time.Hour)); len(rows) != 0 {
		t.Fatalf("%+v", rows)
	}
}

func TestRequestedReportsFromAnUnassignedObserverAreIgnoredPostgres(t *testing.T) {
	w := newReqWorld(t, "")
	ctx, s, pool := w.ctx, w.s, w.pool
	alice := newUser(t, ctx, s, "alice")
	holder, stranger := newReqDevice(1), newReqDevice(2)
	node := w.node(0xA1)
	pollHearAdvert(t, ctx, pool, pollKey(0xA1), pollObserver(t, ctx, s, holder.pub), w.now.Add(-time.Hour), 5)
	pollObserver(t, ctx, s, stranger.pub)
	w.claim(holder, 3)
	r, err := w.reqs.Create(ctx, alice.ID, node, []byte("alicepw"))
	if err != nil {
		t.Fatal(err)
	}
	w.now = w.now.Add(time.Hour)
	// The stranger was never handed the repeater (no advert evidence) but claims an admin answer for it.
	w.claim(stranger, 3, reqNode(pollKey(0xA1), true, "admin_stop"))
	w.claim(stranger, 3, reqNode(pollKey(0xA1), true, "login_failed"))
	var ct []byte
	if err := pool.QueryRow(ctx, `SELECT password_ct FROM telemetry_requests WHERE id = $1`, r.ID).Scan(&ct); err != nil || ct == nil {
		t.Fatalf("password gone: %v", err)
	}
	if got := w.status(alice.ID, r.ID); got.Status != polling.StatusActive || !got.HasPassword {
		t.Fatalf("request touched: %+v", got)
	}
	if stops, _ := s.ListPollingAdminStops(ctx); len(stops) != 0 {
		t.Fatalf("stop recorded: %+v", stops)
	}
	// The assigned observer's own report still counts.
	w.now = w.now.Add(time.Minute)
	w.claim(holder, 3, reqNode(pollKey(0xA1), true, "admin_stop"))
	if got := w.status(alice.ID, r.ID); got.Status != polling.StatusAdminStopped || got.HasPassword {
		t.Fatalf("holder's report: %+v", got)
	}
}

func TestRequestedPasswordOnlyGoesToAnObserverApprovedNowPostgres(t *testing.T) {
	w := newReqWorld(t, config.RequestObserversApproved)
	ctx, s := w.ctx, w.s
	alice := newUser(t, ctx, s, "alice")
	o := newReqDevice(1)
	node := w.node(0xA1)
	pollHearAdvert(t, ctx, w.pool, pollKey(0xA1), pollObserver(t, ctx, s, o.pub), w.now.Add(-time.Hour), 5)
	w.claim(o, 3)
	if err := s.SetPollingObserver(ctx, o.pub, false, true, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := w.reqs.Create(ctx, alice.ID, node, []byte("pw")); err != nil {
		t.Fatal(err)
	}
	if e, ok := w.owners(o, 0xA1); !ok || e.PW == nil {
		t.Fatalf("approved observer: %+v", e)
	}
	// Approval withdrawn: nothing goes out even before the next election.
	if err := s.SetPollingObserver(ctx, o.pub, false, false, ""); err != nil {
		t.Fatal(err)
	}
	if e, ok := w.owners(o, 0xA1); ok {
		t.Fatalf("a password went to an observer no longer approved: %+v", e)
	}
}

// seatWorld has one observer with one free seat that has heard two requested repeaters; the older request is for the
// repeater whose key sorts second. Both requests exist before the observer's claim does.
func seatWorld(t *testing.T) (w *reqWorld, o reqDevice, oldReq, newReq polling.RequestRow, alice, bob users.User) {
	t.Helper()
	w = newReqWorld(t, "")
	ctx, s := w.ctx, w.s
	alice, bob = newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob")
	o = newReqDevice(1)
	nOld, nNew := w.node(0xA2), w.node(0xA1)
	obs := pollObserver(t, ctx, s, o.pub)
	for _, tg := range []byte{0xA1, 0xA2} {
		pollHearAdvert(t, ctx, w.pool, pollKey(tg), obs, w.now.Add(-time.Hour), 5)
	}
	var err error
	if oldReq, err = w.reqs.Create(ctx, alice.ID, nOld, []byte("old")); err != nil {
		t.Fatal(err)
	}
	w.now = w.now.Add(time.Minute)
	if newReq, err = w.reqs.Create(ctx, bob.ID, nNew, []byte("new")); err != nil {
		t.Fatal(err)
	}
	w.claim(o, 1)
	return
}

func assignedTargets(t *testing.T, w *reqWorld) []string {
	t.Helper()
	rows, err := w.pool.Query(w.ctx, `SELECT encode(target_public_key, 'hex') FROM polling_assignments ORDER BY 1`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var k string
		if err := rows.Scan(&k); err != nil {
			t.Fatal(err)
		}
		out = append(out, k)
	}
	return out
}

// Elections run in key order, so the younger request is seated first; the older request must still win the seat.
func TestRequestedSeatsGoToOlderRequestsInOrderPostgres(t *testing.T) {
	w, _, oldReq, newReq, alice, bob := seatWorld(t)
	if err := w.svc.ElectAll(w.ctx); err != nil {
		t.Fatal(err)
	}
	if got := assignedTargets(t, w); len(got) != 1 || got[0] != hex.EncodeToString(pollKey(0xA2)) {
		t.Fatalf("one seat, assignments %v", got)
	}
	if st := w.status(alice.ID, oldReq.ID); st.Status != polling.StatusActive {
		t.Fatalf("older request: %+v", st)
	}
	if st := w.status(bob.ID, newReq.ID); st.Status != polling.StatusWaiting || st.Reason != polling.ReasonNoSeat {
		t.Fatalf("younger request: %+v", st)
	}
}

// Two elections that run at the same time must not both take the one seat. A trigger holds each assignment insert
// open so both transactions have read their seats before either commits.
func TestRequestedSeatsTwoTargetRacePostgres(t *testing.T) {
	w, _, _, _, _, _ := seatWorld(t)
	if _, err := w.pool.Exec(w.ctx, `CREATE FUNCTION slow_assign() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.5); RETURN NULL; END $$;
		CREATE TRIGGER slow_assign AFTER INSERT ON polling_assignments FOR EACH ROW EXECUTE FUNCTION slow_assign()`); err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	for _, tg := range []byte{0xA1, 0xA2} {
		go func() { errs <- w.svc.ElectTarget(w.ctx, pollKey(tg)) }()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	if got := assignedTargets(t, w); len(got) != 1 || got[0] != hex.EncodeToString(pollKey(0xA2)) {
		t.Fatalf("one seat, assignments %v", got)
	}
}

// Two observers were given password A. The first reports an admin stop, which deletes A and brings in the fallback
// B. The second's report is about A, which it was given: it is recorded and must not delete B, which nobody has
// handed to it.
func TestRequestedAdminStopIsTiedToThePasswordServedPostgres(t *testing.T) {
	w := newReqWorld(t, "")
	ctx, s := w.ctx, w.s
	alice, bob := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob")
	o1, o2 := newReqDevice(1), newReqDevice(2)
	node := w.node(0xA1)
	for _, d := range []reqDevice{o1, o2} {
		pollHearAdvert(t, ctx, w.pool, pollKey(0xA1), pollObserver(t, ctx, s, d.pub), w.now.Add(-time.Hour), 5)
	}
	w.claim(o1, 2)
	w.claim(o2, 2)
	ra, err := w.reqs.Create(ctx, alice.ID, node, []byte("A"))
	if err != nil {
		t.Fatal(err)
	}
	w.now = w.now.Add(time.Minute)
	rb, err := w.reqs.Create(ctx, bob.ID, node, []byte("B"))
	if err != nil {
		t.Fatal(err)
	}
	for _, d := range []reqDevice{o1, o2} {
		if e, ok := w.owners(d, 0xA1); !ok || w.opens(d, 0xA1, e) != "A" {
			t.Fatalf("observer was not given A: %+v", e)
		}
	}
	w.now = w.now.Add(time.Hour)
	w.claim(o1, 2, reqNode(pollKey(0xA1), true, "admin_stop"))
	if got := w.status(alice.ID, ra.ID); got.Status != polling.StatusAdminStopped || got.HasPassword {
		t.Fatalf("first stop: %+v", got)
	}
	// B is in use now, and no owners answer has handed it to o2 yet.
	w.now = w.now.Add(time.Minute)
	w.claim(o2, 2, reqNode(pollKey(0xA1), true, "admin_stop"))
	if got := w.status(bob.ID, rb.ID); got.Status == polling.StatusAdminStopped || !got.HasPassword {
		t.Fatalf("B was deleted by a report about A: %+v", got)
	}
	stops, err := s.ListPollingAdminStops(ctx)
	if err != nil || len(stops) != 2 {
		t.Fatalf("stops %+v %v", stops, err)
	}
	// o1 was never given B either, but it was excluded by its own stop; a fresh answer tells o2 nothing new.
	if e, ok := w.owners(o2, 0xA1); ok {
		t.Fatalf("an observer given admin access is offered B: %+v", e)
	}
}

// offlineGap pauses the offline sweep after it has picked its targets.
type offlineGap struct {
	sqlc.Querier
	arrived chan struct{}
	release chan struct{}
}

func (g offlineGap) OfflineRequestTargets(ctx context.Context, cutoff pgtype.Timestamptz) ([][]byte, error) {
	r, err := g.Querier.OfflineRequestTargets(ctx, cutoff)
	g.arrived <- struct{}{}
	<-g.release
	return r, err
}

// A request created after the sweep chose its repeater is not offline, and must not be disabled.
func TestRequestedOfflineSweepRechecksUnderTheLockPostgres(t *testing.T) {
	w := newReqWorld(t, "")
	ctx, s, pool := w.ctx, w.s, w.pool
	alice, bob := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob")
	node := w.node(0xA1)
	old, err := w.reqs.Create(ctx, alice.ID, node, []byte("pw"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE telemetry_requests SET enabled_at = $1`, w.now.Add(-9*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `UPDATE nodes SET last_seen = $1`, w.now.Add(-10*24*time.Hour)); err != nil {
		t.Fatal(err)
	}
	g := offlineGap{Querier: s.q, arrived: make(chan struct{}, 1), release: make(chan struct{})}
	s.q = g
	done := make(chan error, 1)
	go func() {
		_, err := s.DisableOfflineRequests(ctx, w.now.Add(-7*24*time.Hour), w.now, "offline")
		done <- err
	}()
	<-g.arrived
	fresh, err := w.reqs.Create(ctx, bob.ID, node, []byte("pw2"))
	if err != nil {
		t.Fatal(err)
	}
	close(g.release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	for _, r := range []struct {
		u  uuid.UUID
		id uuid.UUID
	}{{alice.ID, old.ID}, {bob.ID, fresh.ID}} {
		if got := w.status(r.u, r.id); got.Status == polling.StatusDisabled {
			t.Fatalf("disabled a repeater that a new request made current: %+v", got)
		}
	}
}
