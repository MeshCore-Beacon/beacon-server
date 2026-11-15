// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

type fakeLock struct{ closed *bool }

func (l fakeLock) Close(context.Context) { *l.closed = true }

type fakeEval struct {
	locked      bool // another process holds the guard
	rules       []RuleRow
	targets     map[string]TargetData
	batches     int
	loads       int
	applies     int
	ups         []Update
	notes       []Notice
	attention   int
	unlocked    bool
	lastPassIDs map[uuid.UUID]bool
	cursor      uuid.UUID
	loadFails   int // LoadTargets fails this many times first
	begun, done int // passes begun and finished
}

func (f *fakeEval) BeginPass(context.Context, uuid.UUID) error  { f.begun++; return nil }
func (f *fakeEval) FinishPass(context.Context, uuid.UUID) error { f.done++; return nil }

func (f *fakeEval) BeginEvalLock(context.Context) (Unlocker, bool, error) {
	if f.locked {
		return nil, false, nil
	}
	return fakeLock{&f.unlocked}, true, nil
}

func (f *fakeEval) LoadCursor(context.Context) (uuid.UUID, error) { return f.cursor, nil }
func (f *fakeEval) SaveCursor(_ context.Context, id uuid.UUID) error {
	f.cursor = id
	return nil
}

func (f *fakeEval) RuleBatch(_ context.Context, after uuid.UUID, limit int) ([]RuleRow, error) {
	f.batches++
	var out []RuleRow
	for _, r := range f.rules {
		if r.ID.String() > after.String() && len(out) < limit {
			out = append(out, r)
		}
	}
	return out, nil
}

func (f *fakeEval) LoadTargets(context.Context, []RuleRow, time.Time) (map[string]TargetData, error) {
	f.loads++
	if f.loadFails > 0 {
		f.loadFails--
		return nil, errors.New("load failed")
	}
	return f.targets, nil
}

func (f *fakeEval) Apply(_ context.Context, pass uuid.UUID, ups []Update, notes []Notice) error {
	f.applies++
	f.ups, f.notes = append(f.ups, ups...), append(f.notes, notes...)
	if f.lastPassIDs == nil {
		f.lastPassIDs = map[uuid.UUID]bool{}
	}
	f.lastPassIDs[pass] = true
	return nil
}

func (f *fakeEval) AnnounceAttention(context.Context, uuid.UUID) error { f.attention++; return nil }

func quietRule(i int, key byte) RuleRow {
	id := uuid.MustParse("00000000-0000-0000-0000-000000000000")
	id[14], id[15] = byte(i>>8), byte(i)
	k := make([]byte, 32)
	k[0] = key
	return RuleRow{ID: id, UserID: uuid.New(), TargetKind: TargetNode, Key: k, Kind: KindGoneQuiet, Threshold: ip(24), State: StateUnknown, Cursor: ago(time.Hour)}
}

func TestTaskBatchesAndDoesNotQueryPerRule(t *testing.T) {
	f := &fakeEval{targets: map[string]TargetData{}}
	for i := 1; i <= 1200; i++ {
		f.rules = append(f.rules, quietRule(i, byte(i)))
	}
	// Every radio quiet for 3 days: every rule fires.
	for _, r := range f.rules {
		f.targets[TargetID(r.TargetKind, r.Key)] = TargetData{Name: "R", Data: Data{Exists: true, LastHeard: ago(72 * time.Hour)}}
	}
	if err := NewTask(f, func() time.Time { return now }).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.batches != 3 || f.loads != 3 || f.applies != 3 || len(f.notes) != 1200 || f.attention != 1 || !f.unlocked {
		t.Fatalf("batches=%d loads=%d applies=%d notes=%d attention=%d unlocked=%v", f.batches, f.loads, f.applies, len(f.notes), f.attention, f.unlocked)
	}
	if len(f.lastPassIDs) != 1 {
		t.Fatalf("one pass must share one id, got %d", len(f.lastPassIDs))
	}
}

func TestTaskDoesNothingWhileAnotherProcessHoldsTheGuard(t *testing.T) {
	f := &fakeEval{locked: true, rules: []RuleRow{quietRule(1, 1)}}
	if err := NewTask(f, nil).Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if f.batches != 0 || f.attention != 0 {
		t.Fatalf("evaluated without the guard: %+v", f)
	}
}

func TestEvaluateBatchStoresOnlyChanges(t *testing.T) {
	quiet, fine := quietRule(1, 1), quietRule(2, 2)
	fine.State = StateOK
	targets := map[string]TargetData{
		TargetID(quiet.TargetKind, quiet.Key): {Data: Data{Exists: true, LastHeard: ago(72 * time.Hour)}},
		TargetID(fine.TargetKind, fine.Key):   {Data: Data{Exists: true, LastHeard: ago(time.Hour)}},
	}
	ups, notes := evaluateBatch([]RuleRow{quiet, fine}, targets, now)
	if len(ups) != 1 || ups[0].ID != quiet.ID || ups[0].State != StateFiring || ups[0].SentAt != now || len(notes) != 1 {
		t.Fatalf("%+v %+v", ups, notes)
	}
	if notes[0].Kind != KindGoneQuiet || notes[0].Value != 72 || notes[0].UserID != quiet.UserID || notes[0].RuleID != quiet.ID {
		t.Fatalf("%+v", notes[0])
	}
}

func TestEvaluateBatchNamesAreSanitisedAndPinNameWins(t *testing.T) {
	r := quietRule(1, 1)
	targets := map[string]TargetData{TargetID(r.TargetKind, r.Key): {Name: "Radio\r\nBcc: x@evil.example", Data: Data{Exists: true, LastHeard: ago(72 * time.Hour)}}}
	_, notes := evaluateBatch([]RuleRow{r}, targets, now)
	if strings.ContainsAny(notes[0].Name, "\r\n") || notes[0].Name != "Radio Bcc: x@evil.example" {
		t.Fatalf("%q", notes[0].Name)
	}
	r.PinName = "My hill"
	_, notes = evaluateBatch([]RuleRow{r}, targets, now)
	if notes[0].Name != "My hill" {
		t.Fatalf("%q", notes[0].Name)
	}
}

func TestEvaluateBatchRecoveryThenBackOnline(t *testing.T) {
	gq := quietRule(1, 1)
	gq.State = StateFiring
	bo := quietRule(2, 1)
	bo.Kind, bo.Threshold, bo.State = KindBackOnline, nil, StateOK
	id := TargetID(gq.TargetKind, gq.Key)
	targets := map[string]TargetData{id: {Data: Data{Exists: true, LastHeard: ago(time.Minute)}}}
	ups, notes := evaluateBatch([]RuleRow{gq}, targets, now)
	if len(ups) != 1 || ups[0].RecoveredAt != now || len(notes) != 0 {
		t.Fatalf("gone_quiet recovery: %+v %+v", ups, notes)
	}
	// The next evaluation of the back_online rule sees the stored recovery time.
	bo.SiblingRecoveredAt, bo.SiblingEnabled = now, true
	ups, notes = evaluateBatch([]RuleRow{bo}, targets, now.Add(5*time.Minute))
	if len(notes) != 1 || notes[0].Kind != KindBackOnline || len(ups) != 1 {
		t.Fatalf("back online: %+v %+v", ups, notes)
	}
	bo.Cursor = now
	if _, notes = evaluateBatch([]RuleRow{bo}, targets, now.Add(10*time.Minute)); len(notes) != 0 {
		t.Fatal("back online announced twice")
	}
}

func TestSanitizeName(t *testing.T) {
	key := make([]byte, 32)
	key[0], key[1], key[2], key[3] = 0x0f, 0x3a, 0xbc, 0xde
	for in, want := range map[string]string{
		"Hilltop":                    "Hilltop",
		"  a \t b\n c ":              "a b c",
		"x\r\nBcc: evil@example.org": "x Bcc: evil@example.org",
		"bidi‮evil⁦":                 "bidievil",
		"nul\x00byte\x1b[31m":        "nulbyte[31m",
		"line sep para":              "line sep para",
		"":                           "0f3abcde",
		"\r\n":                       "0f3abcde",
		strings.Repeat("é", 100):     strings.Repeat("é", 40),
		"emoji 📡 ok":                 "emoji 📡 ok",
		"��":                         "0f3abcde",
	} {
		if got := SanitizeName(in, key); got != want {
			t.Errorf("%q: %q, want %q", in, got, want)
		}
	}
}

// bigEval holds many ordered rules and records the highest rule id any batch returned.
type bigEval struct {
	ids     [][16]byte
	visited map[int]bool
	cursor  uuid.UUID
}

func (b *bigEval) BeginEvalLock(context.Context) (Unlocker, bool, error) {
	closed := false
	return fakeLock{&closed}, true, nil
}

func (b *bigEval) BeginPass(context.Context, uuid.UUID) error    { return nil }
func (b *bigEval) FinishPass(context.Context, uuid.UUID) error   { return nil }
func (b *bigEval) LoadCursor(context.Context) (uuid.UUID, error) { return b.cursor, nil }
func (b *bigEval) SaveCursor(_ context.Context, id uuid.UUID) error {
	b.cursor = id
	return nil
}

func (b *bigEval) RuleBatch(_ context.Context, after uuid.UUID, limit int) ([]RuleRow, error) {
	i := sort.Search(len(b.ids), func(i int) bool { return bytes.Compare(b.ids[i][:], after[:]) > 0 })
	var out []RuleRow
	for ; i < len(b.ids) && len(out) < limit; i++ {
		b.visited[i] = true
		out = append(out, RuleRow{ID: b.ids[i], TargetKind: TargetNode, Key: make([]byte, 32), Kind: KindGoneQuiet, Threshold: ip(24), State: StateInactive})
	}
	return out, nil
}

func (b *bigEval) LoadTargets(context.Context, []RuleRow, time.Time) (map[string]TargetData, error) {
	return map[string]TargetData{}, nil
}
func (b *bigEval) Apply(context.Context, uuid.UUID, []Update, []Notice) error { return nil }
func (b *bigEval) AnnounceAttention(context.Context, uuid.UUID) error         { return nil }

// A table bigger than one pass's budget is still evaluated whole: each pass carries on where the last one ended.
func TestTaskPassesContinueWhereTheLastEnded(t *testing.T) {
	b := &bigEval{visited: map[int]bool{}}
	for i := 1; i <= BatchSize*maxBatches+1; i++ {
		var id [16]byte
		binary.BigEndian.PutUint32(id[8:], uint32(i))
		b.ids = append(b.ids, id)
	}
	task := NewTask(b, func() time.Time { return now })
	for pass := 1; pass <= 2; pass++ {
		if err := task.Run(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if len(b.visited) != len(b.ids) {
		t.Fatalf("%d of %d rules were reached in two passes", len(b.visited), len(b.ids))
	}
}

// A batch whose load or store failed was not evaluated: the next pass must start at it, not after it.
func TestTaskCursorStaysBehindAFailedBatch(t *testing.T) {
	f := &fakeEval{targets: map[string]TargetData{}, loadFails: 1}
	for i := 1; i <= BatchSize+1; i++ {
		r := quietRule(i, byte(i))
		f.rules = append(f.rules, r)
		f.targets[TargetID(r.TargetKind, r.Key)] = TargetData{Name: "R", Data: Data{Exists: true, LastHeard: ago(72 * time.Hour)}}
	}
	task := NewTask(f, func() time.Time { return now })
	if err := task.Run(context.Background()); err == nil {
		t.Fatal("the failed load was not reported")
	}
	if err := task.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(f.notes) != BatchSize+1 {
		t.Fatalf("%d of %d rules evaluated after the failed batch", len(f.notes), BatchSize+1)
	}
}

// A pass is marked finished however it ends, a failed one included: what it queued is complete for the sender.
func TestTaskMarksThePassFinished(t *testing.T) {
	f := &fakeEval{targets: map[string]TargetData{}, loadFails: 1, rules: []RuleRow{quietRule(1, 1)}}
	task := NewTask(f, func() time.Time { return now })
	_ = task.Run(context.Background())
	_ = task.Run(context.Background())
	if f.begun != 2 || f.done != 2 {
		t.Fatalf("begun %d finished %d", f.begun, f.done)
	}
	locked := &fakeEval{locked: true}
	_ = NewTask(locked, nil).Run(context.Background())
	if locked.begun != 0 {
		t.Fatal("a pass that did not run was recorded")
	}
}
