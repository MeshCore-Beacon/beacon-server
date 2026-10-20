// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planpolicy_test

import (
	"reflect"
	"testing"

	"github.com/MeshCore-Beacon/beacon-server/db/planpolicy"
	sqlc "github.com/MeshCore-Beacon/beacon-server/db/sqlc"
)

// Every Slow/Cached name is a statement in db/sqlc, so a stale entry fails CI; the two sets are disjoint.
func TestPlanPolicyNamesExist(t *testing.T) {
	q := reflect.TypeOf((*sqlc.Querier)(nil)).Elem()
	for _, set := range []map[string]bool{planpolicy.Slow, planpolicy.Cached} {
		for name := range set {
			if _, ok := q.MethodByName(name); !ok {
				t.Errorf("%s is not a sqlc statement", name)
			}
			if planpolicy.Slow[name] && planpolicy.Cached[name] {
				t.Errorf("%s is both Slow and Cached", name)
			}
		}
	}
	// The rewritten stats, adverts and traces readers are C or G.
	for _, name := range []string{"GetStatsSeries", "GetPathStats", "GetTopNodes", "GetStatsTopAdvertisers", "ListObserverAdverts", "ListTraceTags"} {
		if c := planpolicy.ClassOf(name); c != planpolicy.ClassC && c != planpolicy.ClassG {
			t.Errorf("%s is class %d, want C or G", name, c)
		}
	}
	if planpolicy.ClassOf("GetTopNodesProbe") != planpolicy.ClassC {
		t.Error("GetTopNodesProbe must be class C")
	}
	// Exactly the statements that may walk whole site histories run on the slow pool.
	if want := map[string]bool{"ListPacketsByIATAs": true, "ListPacketsByIATAsAfter": true, "ListAllChannelMessages": true}; !reflect.DeepEqual(planpolicy.Slow, want) {
		t.Errorf("Slow = %v, want %v", planpolicy.Slow, want)
	}
	if planpolicy.ClassOf("ListPacketsByIATAs") != planpolicy.ClassSlow ||
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
// since and scope-only ones are planned with their values; the all-channel statement for sites with a since or scope
// and the per-channel one (verification-only today) route to the slow pool.
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
		"ListChannelMessages":               planpolicy.ClassSlow,
		"ListAllChannelMessages":            planpolicy.ClassSlow,
	} {
		if got := planpolicy.ClassOf(name); got != want {
			t.Errorf("ClassOf(%s) = %v, want %v", name, got, want)
		}
	}
}

func TestPacketSitesPlanClasses(t *testing.T) {
	for name, want := range map[string]planpolicy.Class{
		"ListPacketsBySite":            planpolicy.ClassG,
		"ListPacketsBySites":           planpolicy.ClassG,
		"ListPacketsBySitesFirst":      planpolicy.ClassG,
		"ListPacketsBySitesTyped":      planpolicy.ClassG,
		"ListPacketsBySitesTypedFirst": planpolicy.ClassG,
		"ListPacketsBySitesSince":      planpolicy.ClassC,
		"ListPacketsBySitesScoped":     planpolicy.ClassC,
		"ListPacketsBySiteKeys":        planpolicy.ClassC,
		"FilterKnownSites":             planpolicy.ClassC,
		"ListPacketsByIATAs":           planpolicy.ClassSlow,
		"ListPacketsByIATAsAfter":      planpolicy.ClassSlow,
	} {
		if got := planpolicy.ClassOf(name); got != want {
			t.Errorf("ClassOf(%s) = %v, want %v", name, got, want)
		}
	}
}
