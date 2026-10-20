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
	// The rewritten stats, adverts and traces readers left Legacy; today's adverts text stays there as the
	// fallback served until every observation carries its payload type.
	for _, name := range []string{"GetStatsSeries", "GetPathStats", "GetTopNodes", "GetStatsTopAdvertisers", "ListObserverAdverts", "ListTraceTags"} {
		if c := planpolicy.ClassOf(name); c != planpolicy.ClassC && c != planpolicy.ClassG {
			t.Errorf("%s is class %d, want C or G", name, c)
		}
	}
	if planpolicy.ClassOf("ListObserverAdvertsJoinFilter") != planpolicy.ClassLegacy || planpolicy.ClassOf("GetTopNodesProbe") != planpolicy.ClassC {
		t.Error("ListObserverAdvertsJoinFilter must stay Legacy and GetTopNodesProbe class C")
	}
	if len(planpolicy.Legacy) != 4 || planpolicy.ClassOf("ListPacketsByIATAs") != planpolicy.ClassLegacy ||
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

// The channel message statements: the four whose plan is stable under every layout are generic-safe; the scoped,
// since and scope-only ones are planned with their values; today's two statements stay on the legacy pool.
func TestCmiPlanClasses(t *testing.T) {
	for name, want := range map[string]planpolicy.Class{
		"ListChannelMessagesByIATAs":        planpolicy.ClassG,
		"ListChannelMessagesPlain":          planpolicy.ClassG,
		"ListAllChannelMessagesByIATAs":     planpolicy.ClassG,
		"ListAllChannelMessagesPlain":       planpolicy.ClassG,
		"ListChannelMessagesByIATAsScoped":  planpolicy.ClassC,
		"ListChannelMessagesPlainScoped":    planpolicy.ClassC,
		"ListChannelMessagesSinceWalk":      planpolicy.ClassC,
		"ListAllChannelMessagesPlainScoped": planpolicy.ClassC,
		"ListAllChannelMessagesSince":       planpolicy.ClassC,
		"GetTransportScopeByName":           planpolicy.ClassC,
		"ListChannelMessages":               planpolicy.ClassLegacy,
		"ListAllChannelMessages":            planpolicy.ClassLegacy,
	} {
		if got := planpolicy.ClassOf(name); got != want {
			t.Errorf("ClassOf(%s) = %v, want %v", name, got, want)
		}
	}
}
