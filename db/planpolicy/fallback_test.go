// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planpolicy_test

import (
	"reflect"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
)

// The per-channel message and adverts fallbacks route to the slow pool: ungated, on their own connections, as on the
// old legacy pool.
func TestReadModelFallbacksRouteSlow(t *testing.T) {
	want := map[string]bool{"ListChannelMessages": true, "ListObserverAdvertsJoinFilter": true}
	if !reflect.DeepEqual(planpolicy.ReadModelFallbacks, want) {
		t.Fatalf("ReadModelFallbacks = %v, want %v", planpolicy.ReadModelFallbacks, want)
	}
	for name := range want {
		if planpolicy.ClassOf(name) != planpolicy.ClassSlow || planpolicy.Cached[name] || planpolicy.Slow[name] {
			t.Errorf("%s: class %v, want ClassSlow and in no other set", name, planpolicy.ClassOf(name))
		}
	}
}
