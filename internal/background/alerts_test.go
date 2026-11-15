// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"testing"
	"time"
)

func TestAlertTasks(t *testing.T) {
	tasks := AlertTasks(nil)
	if len(tasks) != 1 || tasks[0].Name != "alerts_eval" || tasks[0].Interval != 5*time.Minute || tasks[0].Run == nil {
		t.Fatalf("%+v", tasks)
	}
}
