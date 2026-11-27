// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"context"
	"encoding/hex"
	"log/slog"
	"time"

	"github.com/google/uuid"
)

const (
	// EvalInterval is how often the evaluation pass runs.
	EvalInterval = 5 * time.Minute
	// BatchSize is how many rules one batch loads; each batch costs a fixed number of queries.
	BatchSize = 500
	// maxBatches bounds one pass, so a pass cannot run away on a huge table.
	maxBatches = 400
)

// RuleRow is a stored rule as the evaluation reads it.
type RuleRow struct {
	ID, UserID         uuid.UUID
	TargetKind         string
	Key                []byte
	Kind               string
	Threshold, High    *int
	State              State
	LastSentAt, Cursor time.Time
	Streak             int
	Version            int64
	PinName            string
	SiblingRecoveredAt time.Time
	// SiblingEnabled: the radio has an enabled gone_quiet rule (read for back_online).
	SiblingEnabled bool
}

// TargetData is what is known about one radio; Now and the sibling time are filled per rule.
type TargetData struct {
	Data
	Name string
}

// TargetID keys TargetData by radio kind and key.
func TargetID(kind string, key []byte) string { return kind + "/" + hex.EncodeToString(key) }

// Update is what to store for a rule; a zero time means none.
type Update struct {
	ID          uuid.UUID
	Version     int64
	State       State
	SentAt      time.Time
	Cursor      time.Time
	Streak      int
	RecoveredAt time.Time
}

// Notice is a notification to queue for delivery.
type Notice struct {
	RuleID, UserID uuid.UUID
	TargetKind     string
	Key            []byte
	Name           string
	Kind           string
	Value          int64
}

// Unlocker releases the single-instance guard.
type Unlocker interface{ Close(ctx context.Context) }

// EvalStore is what the evaluation needs from the database; *db.Store satisfies it through an adapter.
type EvalStore interface {
	// BeginEvalLock takes the guard that lets one process evaluate at a time; ok is false when another holds it.
	BeginEvalLock(ctx context.Context) (Unlocker, bool, error)
	// LoadCursor and SaveCursor keep where the last pass stopped, so the next one carries on from there; the nil
	// uuid means start at the beginning.
	LoadCursor(ctx context.Context) (uuid.UUID, error)
	SaveCursor(ctx context.Context, after uuid.UUID) error
	// BeginPass records that the pass has started and FinishPass that it has ended; the sender takes the notices of a
	// finished pass only (see PassTimeout).
	BeginPass(ctx context.Context, pass uuid.UUID) error
	FinishPass(ctx context.Context, pass uuid.UUID) error
	// RuleBatch returns up to limit enabled rules of live users with an id after the given one, by id.
	RuleBatch(ctx context.Context, after uuid.UUID, limit int) ([]RuleRow, error)
	// LoadTargets reads the data of every radio the rules name, in a fixed number of queries.
	LoadTargets(ctx context.Context, rules []RuleRow, now time.Time) (map[string]TargetData, error)
	// Apply stores the updates of rules not edited since they were read, and the notices of those rules.
	Apply(ctx context.Context, passID uuid.UUID, ups []Update, notes []Notice) error
	// AnnounceAttention queues a notice for each telemetry request that needs attention and was not announced.
	AnnounceAttention(ctx context.Context, passID uuid.UUID) error
}

// Task evaluates every enabled rule on a schedule.
type Task struct {
	store EvalStore
	now   func() time.Time
}

func NewTask(store EvalStore, now func() time.Time) *Task {
	if now == nil {
		now = time.Now
	}
	return &Task{store: store, now: now}
}

// Run is one pass. It does nothing when another process is already evaluating. A pass is bounded, so it starts
// where the previous one stopped and wraps to the start only after it has reached the last rule; every enabled
// rule is evaluated in the end however many there are.
func (t *Task) Run(ctx context.Context) error {
	lock, ok, err := t.store.BeginEvalLock(ctx)
	if err != nil || !ok {
		return err
	}
	defer lock.Close(ctx)
	pass, now := uuid.New(), t.now()
	if err := t.store.BeginPass(ctx, pass); err != nil {
		return err
	}
	// Written however the pass ends, shutdown included: what it queued is then complete for the sender.
	defer func() {
		if err := t.store.FinishPass(context.WithoutCancel(ctx), pass); err != nil {
			slog.Warn("alert pass not marked finished", "component", "alerts", "err", err)
		}
	}()
	after, err := t.store.LoadCursor(ctx)
	if err != nil {
		return err
	}
	// Where the next pass starts, saved however this one ends: rules already stored are not judged again first.
	defer func() {
		if err := t.store.SaveCursor(ctx, after); err != nil {
			slog.Warn("alert cursor not saved", "component", "alerts", "err", err)
		}
	}()
	var fired, evaluated int
	for i := 0; i < maxBatches; i++ {
		rows, err := t.store.RuleBatch(ctx, after, BatchSize)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			after = uuid.Nil // the end: the next pass starts over
			break
		}
		targets, err := t.store.LoadTargets(ctx, rows, now)
		if err != nil {
			return err
		}
		ups, notes := evaluateBatch(rows, targets, now)
		if err := t.store.Apply(ctx, pass, ups, notes); err != nil {
			return err
		}
		after = rows[len(rows)-1].ID // only a batch that was evaluated and stored is passed
		evaluated += len(rows)
		fired += len(notes)
		if len(rows) < BatchSize {
			after = uuid.Nil
			break
		}
		if ctx.Err() != nil {
			break
		}
	}
	if err := t.store.AnnounceAttention(ctx, pass); err != nil {
		return err
	}
	if fired > 0 {
		slog.Info("alerts evaluated", "component", "alerts", "rules", evaluated, "notices", fired)
	}
	return nil
}

// evaluateBatch is the pure part of a pass: rules and data in, updates and notices out. Only rules whose stored
// state would change get an update.
func evaluateBatch(rows []RuleRow, targets map[string]TargetData, now time.Time) ([]Update, []Notice) {
	var ups []Update
	var notes []Notice
	for _, row := range rows {
		td := targets[TargetID(row.TargetKind, row.Key)]
		d := td.Data
		d.Now, d.SiblingRecoveredAt, d.SiblingEnabled = now, row.SiblingRecoveredAt, row.SiblingEnabled
		out := Evaluate(Rule{Kind: row.Kind, Threshold: row.Threshold, High: row.High, State: row.State,
			LastSentAt: row.LastSentAt, Cursor: row.Cursor, Streak: row.Streak}, d)
		if out.State == row.State && !out.Notify && !out.Recovered && out.Streak == row.Streak && out.Cursor.Equal(row.Cursor) {
			continue
		}
		up := Update{ID: row.ID, Version: row.Version, State: out.State, Cursor: out.Cursor, Streak: out.Streak}
		if out.Recovered {
			up.RecoveredAt = now
		}
		if out.Notify {
			up.SentAt = now
			name := row.PinName
			if name == "" {
				name = td.Name
			}
			notes = append(notes, Notice{RuleID: row.ID, UserID: row.UserID, TargetKind: row.TargetKind, Key: row.Key,
				Name: SanitizeName(name, row.Key), Kind: row.Kind, Value: out.Value})
		}
		ups = append(ups, up)
	}
	return ups, notes
}
