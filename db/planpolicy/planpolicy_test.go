// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planpolicy_test

import (
	"reflect"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

// Every Legacy/Cached name is a statement in db/sqlc, so a stale entry fails CI; the two sets are disjoint.
func TestPlanPolicyNamesExist(t *testing.T) {
	q := reflect.TypeOf((*sqlc.Querier)(nil)).Elem()
	for _, set := range []map[string]bool{planpolicy.Legacy, planpolicy.Cached} {
		for name := range set {
			if _, ok := q.MethodByName(name); !ok {
				t.Errorf("%s is not a sqlc statement", name)
			}
			if planpolicy.Legacy[name] && planpolicy.Cached[name] {
				t.Errorf("%s is both Legacy and Cached", name)
			}
		}
	}
	if len(planpolicy.Legacy) != 9 || planpolicy.ClassOf("ListPacketsByIATAs") != planpolicy.ClassLegacy ||
		planpolicy.ClassOf("ListKnownRoutesGlobalHop") != planpolicy.ClassC || planpolicy.ClassOf("GetNodeByID") != planpolicy.ClassG ||
		planpolicy.ClassOf("") != planpolicy.ClassC {
		t.Fatal("ClassOf")
	}
}

func TestParseLayout(t *testing.T) {
	for _, tc := range []struct {
		in   string
		want planpolicy.Layout
	}{
		{"", planpolicy.CustomAll},
		{"custom_all", planpolicy.CustomAll},
		{"split", planpolicy.Split},
		{"single", planpolicy.Single},
	} {
		got, err := planpolicy.ParseLayout(tc.in)
		if err != nil || got != tc.want {
			t.Errorf("ParseLayout(%q) = %q, %v; want %q", tc.in, got, err, tc.want)
		}
	}
	for _, in := range []string{"Split", "x"} {
		_, err := planpolicy.ParseLayout(in)
		if err == nil || err.Error() != "database.api_plan_layout must be custom_all, split or single" {
			t.Errorf("ParseLayout(%q) error = %v", in, err)
		}
	}
}
