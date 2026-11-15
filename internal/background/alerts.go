// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"

	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
)

// AlertTasks evaluates the alert rules every five minutes. The pass takes its own advisory guard, so only one
// process evaluates at a time. Jobs run only on instances that own background work, never on a read-only one.
func AlertTasks(store alerts.EvalStore) []Task {
	eval := alerts.NewTask(store, nil)
	return []Task{{Name: "alerts_eval", Interval: alerts.EvalInterval, Run: func(ctx context.Context) error { return eval.Run(ctx) }}}
}
