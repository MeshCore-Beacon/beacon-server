// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"testing"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/alerts"
)

func TestAlertTasks(t *testing.T) {
	if tasks := AlertTasks(nil, nil); len(tasks) != 0 {
		t.Fatalf("tasks without a sender: %+v", tasks)
	}
	tasks := AlertTasks(nil, alerts.NewSender(nil, alerts.SenderConfig{}))
	if len(tasks) != 2 || tasks[0].Name != "alerts_eval" || tasks[0].Interval != 5*time.Minute || tasks[0].Run == nil ||
		tasks[1].Name != "alerts_send" || tasks[1].Interval != time.Minute || tasks[1].Run == nil {
		t.Fatalf("%+v", tasks)
	}
}
