// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package background

import (
	"context"
	"errors"
	"testing"
	"time"
)

type fakeUsersMaint struct {
	at  []time.Time
	err error
}

func (f *fakeUsersMaint) DeleteExpiredAuth(_ context.Context, now time.Time) error {
	f.at = append(f.at, now)
	return f.err
}

func TestUsersCleanupRunsHourlyAndCallsDeleteExpiredAuth(t *testing.T) {
	now := time.Date(2026, 11, 14, 12, 0, 0, 0, time.UTC)
	prev := usersNow
	usersNow = func() time.Time { return now }
	t.Cleanup(func() { usersNow = prev })

	f := &fakeUsersMaint{}
	tasks := UsersTasks(f)
	if len(tasks) != 1 || tasks[0].Name != "users_cleanup" || tasks[0].Interval != time.Hour {
		t.Fatalf("tasks: %+v", tasks)
	}
	if err := tasks[0].Run(context.Background()); err != nil || len(f.at) != 1 || !f.at[0].Equal(now) {
		t.Fatalf("run: %v %v", err, f.at)
	}
	f.err = errors.New("db down")
	if err := tasks[0].Run(context.Background()); !errors.Is(err, f.err) {
		t.Fatalf("error not passed on: %v", err)
	}
}
