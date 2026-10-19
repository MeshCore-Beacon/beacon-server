// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"io/fs"
	"regexp"
	"slices"
	"strings"
	"testing"

	pgdata "github.com/MeshCore-Beacon/beacon-server/db/planguard"
	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
)

// planIDs matches internal plan identifiers, which must not appear in the corpus or its SQL files.
var planIDs = regexp.MustCompile(`\b(S[0-9]+[a-z]?|V[0-9]|P[0-9]|§[0-9])\b`)

// realIDs matches identifiers copied from a deployment: a UUID that is not one of the synthetic
// 00000000-0000-0000-0000-… ids, or a hex byte string of 8 bytes or more (packet hashes, public keys) other than
// the synthetic 0102030405060708. Cases name such rows through resolvers instead.
var (
	uuidRe     = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	longHexRe  = regexp.MustCompile(`\\x[0-9a-fA-F]{16,}`)
	synthUUID  = "00000000-0000-0000-0000-"
	synthBytes = `\x0102030405060708`
)

func TestRepoCorpusLoads(t *testing.T) {
	c, err := planguard.LoadCorpus(pgdata.Files, "corpus.yaml", repoSQLC(t))
	if err != nil {
		t.Fatal(err)
	}
	if len(c.Entries) == 0 {
		t.Fatal("no entries")
	}
	for name, st := range c.Statements {
		switch {
		case st.Pending == "" && strings.TrimSpace(st.SQL) == "":
			t.Errorf("statement %s has no text", name)
		case st.Pending != "" && st.File != "" && planguard.Normalize(st.SQL) == "":
			t.Errorf("pending statement %s: %s holds no statement", name, st.File)
		}
		if st.Pending != "" && st.Group != st.Pending {
			t.Errorf("pending statement %s: group %q, want its feature %q (the overlay search_path)", name, st.Group, st.Pending)
		}
	}

	used := map[string]bool{}
	for _, s := range c.Defaults.SortExempt {
		used[s] = true
	}
	for _, e := range c.Entries {
		st := c.Statements[e.Stmt]
		if (strings.HasPrefix(e.ID, "planted") || st.Plan == "harness") && !e.ExpectFail {
			t.Errorf("planted entry %s is not expect_fail", e.ID)
		}
		for _, s := range e.Expect.SortExempt {
			used[s] = true
		}
		for _, o := range e.Mode {
			if o != nil && o.DegradeTo != nil {
				for _, s := range o.DegradeTo.SortExempt {
					used[s] = true
				}
			}
		}
	}
	for name := range c.SortExemptSets {
		if !used[name] {
			t.Errorf("sort_exempt_sets %s is used by no entry", name)
		}
	}

	// No plan IDs in the corpus or the frozen SQL.
	err = fs.WalkDir(pgdata.Files, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() || !(p == "corpus.yaml" || strings.HasPrefix(p, "sql/")) {
			return err
		}
		body, err := fs.ReadFile(pgdata.Files, p)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(body), "\n") {
			if m := planIDs.FindString(line); m != "" {
				t.Errorf("%s:%d holds the plan ID %q: %s", p, i+1, m, strings.TrimSpace(line))
			}
			for _, u := range uuidRe.FindAllString(line, -1) {
				if !strings.HasPrefix(u, synthUUID) {
					t.Errorf("%s:%d holds the deployment UUID %s", p, i+1, u)
				}
			}
			for _, h := range longHexRe.FindAllString(line, -1) {
				if h != synthBytes {
					t.Errorf("%s:%d holds the deployment byte string %s", p, i+1, h)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Legacy readers run on api_legacy with automatic plan caching: their entries run custom and auto, and the auto
// mode carries a known issue until their rewrite lands, so a generic flip reads KNOWN_ISSUE.
func TestRepoCorpusLegacyAutoKnownIssue(t *testing.T) {
	c, err := planguard.LoadCorpus(pgdata.Files, "corpus.yaml", repoSQLC(t))
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, e := range c.Entries {
		switch e.Stmt {
		case "GetTopNodes", "GetStatsTopAdvertisers", "GetPathStats":
		default:
			continue
		}
		n++
		if !slices.Equal(c.ModesOf(e), []string{"custom", "auto"}) || e.KnownIssueIn("auto") == "" || e.KnownIssueIn("custom") != "" {
			t.Errorf("entry %s: modes %v, auto known issue %q, custom known issue %q", e.ID, c.ModesOf(e), e.KnownIssueIn("auto"), e.KnownIssueIn("custom"))
		}
	}
	if n != 3 {
		t.Errorf("%d legacy reader entries, want 3", n)
	}
}

// The three statements gen-policy admits carry every entry in every mode, and their shapes are tagged.
func TestRepoCorpusCachedEntries(t *testing.T) {
	c, err := planguard.LoadCorpus(pgdata.Files, "corpus.yaml", repoSQLC(t))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range c.Entries {
		st := c.Statements[e.Stmt]
		if st.Plan != "cached" || st.Pending != "" {
			continue
		}
		if len(e.Shapes) == 0 {
			t.Errorf("entry %s of cached %s has no shapes", e.ID, e.Stmt)
		}
		if !slices.Equal(c.ModesOf(e), []string{"custom", "generic", "auto"}) {
			t.Errorf("entry %s of cached %s runs %v", e.ID, e.Stmt, c.ModesOf(e))
		}
	}
}
