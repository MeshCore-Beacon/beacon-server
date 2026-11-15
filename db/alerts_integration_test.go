// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package db

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
	"github.com/MeshCore-Beacon/beacon-server/internal/api"
	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
)

func ipp(v int) *int { return &v }

func alertSvc(s *Store) *alerts.Service { return alerts.NewService(s, "push", time.Now) }

func pinAtlas(t *testing.T, ctx context.Context, s *Store, u users.User, base int64, pins ...api.StoredPin) {
	t.Helper()
	if _, ok, err := s.ReplaceUserAtlas(ctx, u.ID, base, "24h", pins); err != nil || !ok {
		t.Fatalf("atlas: ok=%v %v", ok, err)
	}
}

func insertNode(t *testing.T, ctx context.Context, s *Store, key []byte, nodeType int, name string) {
	t.Helper()
	if _, err := s.pool.Exec(ctx, `INSERT INTO nodes (public_key, node_type, name) VALUES ($1, $2, $3)`, key, nodeType, name); err != nil {
		t.Fatal(err)
	}
}

// assignPoller gives the repeater a polling assignment, which is what makes it polled.
func assignPoller(t *testing.T, ctx context.Context, s *Store, target []byte) {
	t.Helper()
	if _, err := s.pool.Exec(ctx, `INSERT INTO polling_assignments (target_public_key, observer_public_key, rank, assigned_at) VALUES ($1, $2, 0, now())`, target, atlasKey(0xee)); err != nil {
		t.Fatal(err)
	}
}

func insertReading(t *testing.T, ctx context.Context, s *Store, target []byte, at time.Time, battery int, temp *float64) {
	t.Helper()
	obs := atlasKey(0xee)
	if _, err := s.pool.Exec(ctx, `INSERT INTO repeater_readings (target_public_key, observer_public_key, sampled_at, received_at, method, tag, battery_mv, temperature_c, uptime_secs, noise_floor, trusted)
		VALUES ($1, $2, $3, $3, 'status', $4, $5, $6, 1000, -100, true)`, target, obs, at, at.UnixNano(), battery, temp); err != nil {
		t.Fatal(err)
	}
}

func TestAlertRulesPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	u := newUser(t, ctx, s, "alice")
	other := newUser(t, ctx, s, "bob")
	rep, comp, ghost := atlasKey(1), atlasKey(2), atlasKey(3)
	insertNode(t, ctx, s, rep, 2, "Hilltop")
	insertNode(t, ctx, s, comp, 1, "Phone")
	insertReading(t, ctx, s, rep, time.Now().Add(-time.Hour), 3900, nil)
	assignPoller(t, ctx, s, rep)
	pinAtlas(t, ctx, s, u, 0,
		api.StoredPin{Kind: "node", Key: rep, Name: "My hill"},
		api.StoredPin{Kind: "node", Key: comp},
		api.StoredPin{Kind: "observer", Key: ghost, Name: "Unknown"})

	// Not on the Atlas.
	_, err := svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: atlasKey(9), Kind: "gone_quiet", Threshold: ipp(24), Enabled: true})
	if !errors.Is(err, alerts.ErrNotOnAtlas) {
		t.Fatalf("not on atlas: %v", err)
	}
	// Kind not available: a companion has no battery, a polled repeater has no temperature.
	var na *alerts.KindUnavailable
	if _, err = svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: comp, Kind: "battery_low", Threshold: ipp(3500), Enabled: true}); !errors.As(err, &na) {
		t.Fatalf("companion battery: %v", err)
	}
	if _, err = svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: rep, Kind: "temperature", Threshold: ipp(0), Enabled: true}); !errors.As(err, &na) {
		t.Fatalf("temperature: %v", err)
	}
	// back_online needs gone_quiet first.
	if _, err = svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: rep, Kind: "back_online", Enabled: true}); !errors.As(err, &na) {
		t.Fatalf("back online alone: %v", err)
	}
	var bad *alerts.InvalidThreshold
	if _, err = svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: rep, Kind: "battery_low", Threshold: ipp(10), Enabled: true}); !errors.As(err, &bad) {
		t.Fatalf("threshold: %v", err)
	}

	// A disabled gone_quiet does not count either.
	if _, err = svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: rep, Kind: "gone_quiet", Threshold: ipp(6), Enabled: false}); err != nil {
		t.Fatal(err)
	}
	if _, err = svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: rep, Kind: "back_online", Enabled: true}); !errors.As(err, &na) || na.Code != alerts.CodeNeedsGoneQuiet {
		t.Fatalf("back online with a disabled gone quiet: %v", err)
	}
	gq, err := svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: rep, Kind: "gone_quiet", Threshold: ipp(6), Enabled: true})
	if err != nil || gq.State != "unknown" || gq.Threshold == nil || *gq.Threshold != 6 {
		t.Fatalf("gone quiet: %+v %v", gq, err)
	}
	if _, err = svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: rep, Kind: "back_online", Enabled: true}); err != nil {
		t.Fatalf("back online: %v", err)
	}
	if _, err = svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: rep, Kind: "battery_low", Threshold: ipp(3500), Enabled: true}); err != nil {
		t.Fatal(err)
	}

	// One rule per (radio, kind): a second put is an update with the same id, and an unchanged put keeps the state.
	if _, err := pool.Exec(ctx, `UPDATE user_alert_rules SET state = 'firing' WHERE id = $1`, gq.ID); err != nil {
		t.Fatal(err)
	}
	same, err := svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: rep, Kind: "gone_quiet", Threshold: ipp(6), Enabled: true})
	if err != nil || same.ID != gq.ID || same.State != "firing" {
		t.Fatalf("unchanged put: %+v %v", same, err)
	}
	changed, err := svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: rep, Kind: "gone_quiet", Threshold: ipp(12), Enabled: true})
	if err != nil || changed.ID != gq.ID || changed.State != "unknown" || *changed.Threshold != 12 {
		t.Fatalf("changed put: %+v %v", changed, err)
	}
	// Off keeps the stored thresholds and the rule.
	off, err := svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: rep, Kind: "gone_quiet", Threshold: ipp(12), Enabled: false})
	if err != nil || off.Enabled {
		t.Fatalf("off: %+v %v", off, err)
	}

	ov, err := svc.Overview(ctx, u.ID)
	if err != nil || ov.Delivery != "push" || ov.HasDevice || len(ov.Targets) != 3 {
		t.Fatalf("overview: %+v %v", ov, err)
	}
	hill := ov.Targets[0]
	if hill.PublicKey != hex.EncodeToString(rep) || hill.Name != "My hill" || hill.Kind != "node" || len(hill.Rules) != 3 {
		t.Fatalf("first target: %+v", hill)
	}
	if hill.ID != nodeIDOf(t, ctx, s, rep) || !hill.Caps.Polled || hill.UnavailableCodes["temperature"] != alerts.CodeNoData {
		t.Fatalf("id, polled, code: %+v", hill)
	}
	if ov.Targets[1].ID == uuid.Nil || ov.Targets[1].Caps.Polled || ov.Targets[1].UnavailableCodes["battery_low"] != alerts.CodeRepeatersOnly ||
		ov.Targets[2].ID != uuid.Nil || ov.Targets[2].Caps.Polled {
		t.Fatalf("other targets: %+v %+v", ov.Targets[1], ov.Targets[2])
	}
	if hill.Unavailable["temperature"] == "" || !contains(hill.Available, "battery_low") || !contains(hill.Available, "clock") {
		t.Fatalf("availability: %v %v", hill.Available, hill.Unavailable)
	}
	if ov.Targets[1].Name != "Phone" || ov.Targets[1].Unavailable["battery_low"] == "" || len(ov.Targets[1].Rules) != 0 {
		t.Fatalf("companion target: %+v", ov.Targets[1])
	}
	if ov.Targets[2].Kind != "observer" || ov.Targets[2].Name != "Unknown" || contains(ov.Targets[2].Available, "clock") || !contains(ov.Targets[2].Available, "observer_offline") {
		t.Fatalf("observer target: %+v", ov.Targets[2])
	}

	// Another user sees none of it and cannot delete it.
	if bov, _ := svc.Overview(ctx, other.ID); len(bov.Targets) != 0 {
		t.Fatalf("bob sees %d targets", len(bov.Targets))
	}
	if err := svc.DeleteRule(ctx, other.ID, gq.ID); !errors.Is(err, alerts.ErrRuleMissing) {
		t.Fatalf("foreign delete: %v", err)
	}

	// Taking the radio off the Atlas deletes its rules; an unrelated pin keeps its own.
	if _, err := svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: comp, Kind: "gone_quiet", Threshold: ipp(24), Enabled: true}); err != nil {
		t.Fatal(err)
	}
	pinAtlas(t, ctx, s, u, 1, api.StoredPin{Kind: "node", Key: comp})
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_alert_rules WHERE user_id = $1`, u.ID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("%d rules left, want 1: %v", n, err)
	}

	if err := svc.DeleteRule(ctx, u.ID, uuid.New()); !errors.Is(err, alerts.ErrRuleMissing) {
		t.Fatalf("unknown delete: %v", err)
	}
	// Deleting the account removes the rest.
	if err := s.DeleteUser(ctx, u.ID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_alert_rules`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d rules after account deletion", n)
	}
}

func nodeIDOf(t *testing.T, ctx context.Context, s *Store, key []byte) uuid.UUID {
	t.Helper()
	var id uuid.UUID
	if err := s.pool.QueryRow(ctx, `SELECT id FROM nodes WHERE public_key = $1`, key).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

func contains(l []string, s string) bool {
	for _, v := range l {
		if v == s {
			return true
		}
	}
	return false
}

func TestAlertRulesRaceWithAtlasWritePostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	u := newUser(t, ctx, s, "alice")
	key := atlasKey(1)
	insertNode(t, ctx, s, key, 2, "Hilltop")
	pinAtlas(t, ctx, s, u, 0, api.StoredPin{Kind: "node", Key: key})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 20; i++ {
			_, _ = svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: "node", PublicKey: key, Kind: "gone_quiet", Threshold: ipp(24), Enabled: true})
		}
	}()
	pinAtlas(t, ctx, s, u, 1) // empty Atlas
	<-done
	var pins, rules int
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM user_atlas_pins WHERE user_id = $1`, u.ID).Scan(&pins)
	_ = pool.QueryRow(ctx, `SELECT count(*) FROM user_alert_rules WHERE user_id = $1`, u.ID).Scan(&rules)
	if pins != 0 || rules != 0 {
		t.Fatalf("a rule outlived its pin: pins=%d rules=%d", pins, rules)
	}
}

func TestUserDevicesPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	u := newUser(t, ctx, s, "alice")
	s1 := newSession(t, ctx, s, u, "app", 1)
	s2 := newSession(t, ctx, s, u, "app", 2)

	if err := svc.RegisterDevice(ctx, u.ID, s1.ID, "ios", "token-A"); err != nil {
		t.Fatal(err)
	}
	if ov, _ := svc.Overview(ctx, u.ID); !ov.HasDevice {
		t.Fatal("no device after registering")
	}
	// A new token for the same session replaces the old one; the same token on another session moves.
	if err := svc.RegisterDevice(ctx, u.ID, s1.ID, "ios", "token-B"); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterDevice(ctx, u.ID, s2.ID, "android", "token-B"); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_devices WHERE user_id = $1`, u.ID).Scan(&n); err != nil || n != 1 {
		t.Fatalf("%d device rows, want 1: %v", n, err)
	}
	for _, bad := range [][2]string{{"windows", "x"}, {"ios", ""}, {"ios", "with space"}, {"ios", "a\nb"}} {
		if err := svc.RegisterDevice(ctx, u.ID, s1.ID, bad[0], bad[1]); !errors.Is(err, alerts.ErrBadDevice) {
			t.Errorf("%q: %v", bad, err)
		}
	}
	// Ending the session ends delivery to it.
	if err := s.EndSession(ctx, u.ID, s2.ID); err != nil {
		t.Fatal(err)
	}
	if ov, _ := svc.Overview(ctx, u.ID); ov.HasDevice {
		t.Fatal("an ended session still counts as a device")
	}
	// Removing is quiet when there is nothing to remove, and cascades with the session and the account.
	if err := svc.RemoveDevice(ctx, u.ID, s1.ID); err != nil {
		t.Fatal(err)
	}
	if err := svc.RemoveDevice(ctx, u.ID, s1.ID); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterDevice(ctx, u.ID, s1.ID, "ios", "token-C"); err != nil {
		t.Fatal(err)
	}
	if err := s.DeleteUser(ctx, u.ID); err != nil {
		t.Fatal(err)
	}
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_devices`).Scan(&n); err != nil || n != 0 {
		t.Fatalf("%d devices after account deletion", n)
	}
}

// One phone belongs to one account: a token registered by a new user leaves the previous user's session, and no
// answer carries a token.
func TestDeviceTokenMovesAcrossUsersPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	alice, bob := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob")
	sa, sb := newSession(t, ctx, s, alice, "app", 1), newSession(t, ctx, s, bob, "app", 2)
	const token = "possession-secret-token"
	if err := svc.RegisterDevice(ctx, alice.ID, sa.ID, "android", token); err != nil {
		t.Fatal(err)
	}
	if err := svc.RegisterDevice(ctx, bob.ID, sb.ID, "android", token); err != nil {
		t.Fatal(err)
	}
	var owner uuid.UUID
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*), min(user_id::text)::uuid FROM user_devices WHERE push_token = $1`, token).Scan(&n, &owner); err != nil || n != 1 || owner != bob.ID {
		t.Fatalf("%d registrations, owner %v: %v", n, owner, err)
	}
	if ov, _ := svc.Overview(ctx, alice.ID); ov.HasDevice {
		t.Fatal("the previous user still has the phone")
	}
	ov, _ := svc.Overview(ctx, bob.ID)
	if b, _ := json.Marshal(ov); strings.Contains(string(b), token) || !ov.HasDevice {
		t.Fatalf("overview %s", b)
	}
}

// Two registrations of one token that overlap in time leave one owner. Triggers hold each insert and delete open so
// both transactions are in flight before either one commits.
func TestDeviceTokenSimultaneousRegistrationsPostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	alice, bob := newUser(t, ctx, s, "alice"), newUser(t, ctx, s, "bob")
	sa, sb := newSession(t, ctx, s, alice, "app", 1), newSession(t, ctx, s, bob, "app", 2)
	if _, err := pool.Exec(ctx, `CREATE FUNCTION slow_device() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_sleep(0.5); RETURN NULL; END $$;
		CREATE TRIGGER slow_device AFTER INSERT ON user_devices FOR EACH ROW EXECUTE FUNCTION slow_device();
		CREATE TRIGGER slow_device_del AFTER DELETE ON user_devices FOR EACH STATEMENT EXECUTE FUNCTION slow_device()`); err != nil {
		t.Fatal(err)
	}
	const token = "shared-phone-token"
	errs := make(chan error, 2)
	for _, r := range []struct{ u, s uuid.UUID }{{alice.ID, sa.ID}, {bob.ID, sb.ID}} {
		go func() { errs <- svc.RegisterDevice(ctx, r.u, r.s, "android", token) }()
	}
	for range 2 {
		if err := <-errs; err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM user_devices WHERE push_token = $1`, token).Scan(&n); err != nil || n != 1 {
		t.Fatalf("%d owners of one token: %v", n, err)
	}
}

// A radio that is an observer and also has a node row keeps what its observer telemetry gives it, and each alert
// reads from the source that has its data.
func TestAlertObserverThatIsAlsoANodePostgres(t *testing.T) {
	ctx, pool, s := userStore(t)
	svc := alertSvc(s)
	u := newUser(t, ctx, s, "alice")
	key := atlasKey(5)
	var obsID uuid.UUID
	if err := pool.QueryRow(ctx, `INSERT INTO observers (public_key, display_name, last_seen, last_status_at) VALUES ($1, 'North', now(), now()) RETURNING id`, key).Scan(&obsID); err != nil {
		t.Fatal(err)
	}
	if _, err := pool.Exec(ctx, `INSERT INTO observer_telemetry (id, observer_id, reported_at, battery_voltage_mv, noise_floor_db, uptime_seconds)
		VALUES (1, $1, now(), 3900, -100, 5000)`, obsID); err != nil {
		t.Fatal(err)
	}
	pinAtlas(t, ctx, s, u, 0, api.StoredPin{Kind: "observer", Key: key})
	want := []string{"observer_offline", "battery_low", "battery_falling", "rebooted", "noise_high"}
	check := func(when string) alerts.TargetView {
		t.Helper()
		ov, err := svc.Overview(ctx, u.ID)
		if err != nil || len(ov.Targets) != 1 {
			t.Fatalf("%s: %+v %v", when, ov, err)
		}
		for _, k := range want {
			if !contains(ov.Targets[0].Available, k) {
				t.Fatalf("%s: %s is not available: %v %v", when, k, ov.Targets[0].Available, ov.Targets[0].Unavailable)
			}
		}
		return ov.Targets[0]
	}
	check("observer only")
	insertNode(t, ctx, s, key, 2, "North")
	tgt := check("after its own advert")
	if !contains(tgt.Available, "clock") {
		t.Fatalf("the node side lost its clock: %v", tgt.Available)
	}
	// The alert is created through the target as the overview lists it.
	for _, k := range []string{"observer_offline", "battery_low"} {
		th := 60
		if k == "battery_low" {
			th = 3500
		}
		if _, err := svc.PutRule(ctx, u.ID, alerts.RuleInput{TargetKind: tgt.Kind, PublicKey: key, Kind: k, Threshold: &th, Enabled: true}); err != nil {
			t.Fatalf("%s through the listed target: %v", k, err)
		}
	}
	// Each rule reads from the source that has its data.
	kinds := map[string]string{}
	rows, err := pool.Query(ctx, `SELECT kind, target_kind FROM user_alert_rules WHERE user_id = $1`, u.ID)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var k, tk string
		if err := rows.Scan(&k, &tk); err != nil {
			t.Fatal(err)
		}
		kinds[k] = tk
	}
	if kinds["observer_offline"] != "observer" || kinds["battery_low"] != "observer" {
		t.Fatalf("sources %v", kinds)
	}
}
