// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard_test

import (
	"bytes"
	"encoding/csv"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	pgdata "github.com/MeshCore-Beacon/beacon-server/db/planguard"
	"github.com/MeshCore-Beacon/beacon-server/internal/planguard"
)

// planIDs matches internal plan identifiers, which must not appear in the corpus or its SQL files.
var planIDs = regexp.MustCompile(`\b(S[0-9]+[a-z]?|V[0-9]|P[0-9]|§[0-9])\b`)

// Identifiers copied from a deployment must not ship: a UUID that is not one of the synthetic
// 00000000-0000-0000-0000-… ids, or a hex run of 16 digits or more holding a letter (a packet hash or public key,
// as a \x byte string or bare) unless it is synthetic: it starts with eight zeros, or it is 0102030405060708.
// All-digit runs are numbers. Cases name such rows through resolvers instead.
var (
	uuidRe    = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)
	longHexRe = regexp.MustCompile(`(\\x)?([0-9a-fA-F]{16,})`)
	letterRe  = regexp.MustCompile(`[a-fA-F]`)
	synthUUID = "00000000-0000-0000-0000-"
)

// deploymentIDs returns the copied identifiers on one line.
func deploymentIDs(line string) []string {
	var out []string
	for _, u := range uuidRe.FindAllString(line, -1) {
		if !strings.HasPrefix(u, synthUUID) {
			out = append(out, u)
		}
	}
	line = uuidRe.ReplaceAllString(line, "")
	for _, m := range longHexRe.FindAllStringSubmatch(line, -1) {
		h := m[2]
		if (m[1] != "" || letterRe.MatchString(h)) && !strings.HasPrefix(h, "00000000") && h != "0102030405060708" {
			out = append(out, m[0])
		}
	}
	return out
}

func TestDeploymentIDs(t *testing.T) {
	for line, want := range map[string]int{
		`params: ["\\x7c05039b3e8a663c"]`:              1,
		`"hash": "a33704fac0d3a216"`:                   1,
		`id: 7ea9ed3f-d6ce-4e54-a8b7-9f1e2e7d5179`:     1,
		`limit 9223372036854775807`:                    0,
		`"00000000000000a2" and \\x0102030405060708`:   0,
		`'00000000-0000-0000-0000-00000000000b'::uuid`: 0,
	} {
		if got := len(deploymentIDs(line)); got != want {
			t.Errorf("deploymentIDs(%q) found %d, want %d", line, got, want)
		}
	}
}

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

	// No plan IDs or deployment identifiers in the corpus, the frozen SQL, the fixture generator and its testdata,
	// or the baselines.
	scanned := map[string]int{}
	err = fs.WalkDir(pgdata.Files, ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		top, _, _ := strings.Cut(p, "/")
		scanned[top]++
		body, err := fs.ReadFile(pgdata.Files, p)
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(body), "\n") {
			if m := planIDs.FindString(line); m != "" {
				t.Errorf("%s:%d holds the plan ID %q: %s", p, i+1, m, strings.TrimSpace(line))
			}
			for _, id := range deploymentIDs(line) {
				t.Errorf("%s:%d holds the deployment identifier %s", p, i+1, id)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, top := range []string{"corpus.yaml", "sql", "fixture", "baseline"} {
		if scanned[top] == 0 {
			t.Errorf("no embedded file under %s was scanned", top)
		}
	}
}

// The engine's own test data holds no deployment identifiers either.
func TestTestdataHoldsNoDeploymentIDs(t *testing.T) {
	err := fs.WalkDir(os.DirFS("testdata"), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		body, err := os.ReadFile(filepath.Join("testdata", p))
		if err != nil {
			return err
		}
		for i, line := range strings.Split(string(body), "\n") {
			for _, id := range deploymentIDs(line) {
				t.Errorf("testdata/%s:%d holds the deployment identifier %s", p, i+1, id)
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

// The fixture's data files hold aggregates only: every cell is a count, a number (at most 15 integer digits, so a
// key cannot pass as one), a boolean, a timestamp or an IATA code. A name, key, UUID or any other text (a node, observer, channel, scope or broker name) fails, as does a
// file that is not CSV.
func TestFixtureDataHoldsOnlyAggregates(t *testing.T) {
	header := regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	cell := regexp.MustCompile(`^(-?[0-9]{1,15}(\.[0-9]+)?([eE][-+]?[0-9]+)?|t|f|true|false|[A-Z0-9]{3}|[0-9]{4}-[0-9]{2}-[0-9]{2}[ T][0-9:.]+([+-][0-9:]+|Z)?|)$`)
	n := 0
	err := fs.WalkDir(pgdata.Files, "fixture/testdata", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		n++
		if !strings.HasSuffix(p, ".csv") {
			t.Errorf("%s: only CSV files belong in the fixture data", p)
			return nil
		}
		body, err := fs.ReadFile(pgdata.Files, p)
		if err != nil {
			return err
		}
		recs, err := csv.NewReader(bytes.NewReader(body)).ReadAll()
		if err != nil {
			t.Errorf("%s: %v", p, err)
			return nil
		}
		if len(recs) < 2 {
			t.Errorf("%s: no data rows", p)
			return nil
		}
		for _, h := range recs[0] {
			if !header.MatchString(h) {
				t.Errorf("%s: header %q is not a column name", p, h)
			}
		}
		for i, rec := range recs[1:] {
			for j, c := range rec {
				if !cell.MatchString(c) {
					t.Errorf("%s:%d column %s holds %q: not a count, number, boolean, timestamp or IATA code", p, i+2, recs[0][j], c)
				}
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if n == 0 {
		t.Fatal("no fixture data files")
	}
}
