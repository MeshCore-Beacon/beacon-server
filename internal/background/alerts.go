// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"

	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
)

// AlertTasks evaluates the alert rules every five minutes and drains the outbox every minute. Each takes its own
// advisory guard, so only one process does either at a time. Jobs run only on instances that own background work,
// never on a read-only one. A nil sender (delivery cannot send) leaves the rules unevaluated too: nothing would
// come of the notices.
func AlertTasks(store alerts.EvalStore, sender *alerts.Sender) []Task {
	if sender == nil {
		return nil
	}
	eval := alerts.NewTask(store, nil)
	return []Task{
		{Name: "alerts_eval", Interval: alerts.EvalInterval, Run: func(ctx context.Context) error { return eval.Run(ctx) }},
		{Name: "alerts_send", Interval: alerts.SendInterval, Run: sender.Run},
	}
}
