// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planpolicy_test

import (
	"reflect"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
)

// The two statements that served read-model pages before the fallbacks were removed route to the slow pool: if that
// removal is reverted they come back ungated, on their own connections, as on the old legacy pool.
func TestRestoredFallbacksRouteSlow(t *testing.T) {
	want := map[string]bool{"ListChannelMessages": true, "ListObserverAdvertsJoinFilter": true}
	if !reflect.DeepEqual(planpolicy.RestoredFallbacks, want) {
		t.Fatalf("RestoredFallbacks = %v, want %v", planpolicy.RestoredFallbacks, want)
	}
	for name := range want {
		if planpolicy.ClassOf(name) != planpolicy.ClassSlow || planpolicy.Cached[name] || planpolicy.Slow[name] {
			t.Errorf("%s: class %v, want ClassSlow and in no other set", name, planpolicy.ClassOf(name))
		}
	}
}
