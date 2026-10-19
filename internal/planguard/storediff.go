// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// StoreDiffSpec is a storediff case file: API endpoints walked page by page on two servers.
type StoreDiffSpec struct {
	Cases []StoreCase `yaml:"cases"`
}

// StoreCase is one endpoint walked at each page size.
//
// The whole JSON body of every page is compared. Each relaxation is a named class that must be listed in Allowed:
//   - tie_order: within each run of items with equal Key, the new items are a permutation of the old ones (same
//     count, keys equal position by position);
//   - additive_keys: new objects may hold keys the old ones lack; each added path is listed.
//
// Any other difference fails.
type StoreCase struct {
	ID       string              `yaml:"id"`
	Path     string              `yaml:"path"` // /api/v1/...; {name} is replaced by Vars[name]
	Query    map[string]string   `yaml:"query"`
	WebLimit int                 `yaml:"web_limit"` // the client's page size; default 50
	Pages    int                 `yaml:"pages"`     // walk cap per limit; default 50
	Follow   []Follow            `yaml:"follow"`    // param ← nextPageCursor | nextCursor | last.<field>
	Allowed  []string            `yaml:"allowed"`   // tie_order (keyed by key) | additive_keys
	Key      []string            `yaml:"key"`       // item fields for tie_order
	Vars     map[string]StoreVar `yaml:"vars"`      // path values read from another endpoint
}

// Follow sets a query param of the next page from the old server's page.
type Follow struct {
	Param string `yaml:"param"`
	From  string `yaml:"from"`
}

// StoreVar reads one path value from an endpoint of both servers: From is first.<field> or last.<field> of its
// items. Both servers must give the same value.
type StoreVar struct {
	Path  string            `yaml:"path"`
	Query map[string]string `yaml:"query"`
	From  string            `yaml:"from"`
}

// StoreDiffReport is the outcome of RunStoreDiff: one result per case and page size.
type StoreDiffReport struct {
	Cases []StoreCaseResult
}

// StoreCaseResult is one case walked at one page size.
type StoreCaseResult struct {
	ID      string
	Limit   int
	Pages   int      // pages compared
	Verdict string   // identical | allowed | different | skip
	Classes []string // allowed classes that fired, as "name(count)"
	Detail  string   // "page 3: $.hasMore: old true, new false"
	Stuck   string   // "cursor did not advance at page N"
}

// Store diff classes, defaults and limits.
const (
	ClassAdditiveKeys = "additive_keys"

	DefaultStorePages    = 50
	DefaultStoreWebLimit = 50
	maxStoreBody         = 64 << 20
)

var (
	storeIDRe     = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
	storeVarRe    = regexp.MustCompile(`\{([a-z][a-z0-9_]*)\}`)
	storeParamRe  = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9_]*$`)
	storeFieldRe  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*$`)
	storeClassSet = map[string]bool{ClassTieOrder: true, ClassAdditiveKeys: true}
)

// LoadStoreDiffSpec reads and validates a storediff case file.
func LoadStoreDiffSpec(path string) (*StoreDiffSpec, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return ParseStoreDiffSpec(raw, path)
}

// ParseStoreDiffSpec parses and validates a storediff case file named name. Unknown fields are errors.
func ParseStoreDiffSpec(raw []byte, name string) (*StoreDiffSpec, error) {
	var spec StoreDiffSpec
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&spec); err != nil {
		if errors.Is(err, io.EOF) {
			return nil, fmt.Errorf("%s: empty storediff spec", name)
		}
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	var extra yaml.Node
	if err := dec.Decode(&extra); !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("%s: more than one YAML document", name)
	}
	if err := spec.Validate(); err != nil {
		return nil, fmt.Errorf("%s: %w", name, err)
	}
	return &spec, nil
}

// Validate checks every case.
func (s *StoreDiffSpec) Validate() error {
	if len(s.Cases) == 0 {
		return errors.New("no cases")
	}
	seen := map[string]bool{}
	for i, c := range s.Cases {
		if err := c.validate(); err != nil {
			return fmt.Errorf("case %d (%s): %w", i+1, c.ID, err)
		}
		if seen[c.ID] {
			return fmt.Errorf("case id %s is used twice", c.ID)
		}
		seen[c.ID] = true
	}
	return nil
}

func validStorePath(p string) error {
	if !strings.HasPrefix(p, "/api/v1/") || strings.ContainsAny(p, "?# ") {
		return fmt.Errorf("path %q: want /api/v1/... without a query", p)
	}
	return nil
}

func validFrom(from string, first bool) error {
	switch {
	case !first && (from == "nextCursor" || from == "nextPageCursor"):
		return nil
	case strings.HasPrefix(from, "last.") && storeFieldRe.MatchString(strings.TrimPrefix(from, "last.")):
		return nil
	case first && strings.HasPrefix(from, "first.") && storeFieldRe.MatchString(strings.TrimPrefix(from, "first.")):
		return nil
	}
	if first {
		return fmt.Errorf("from %q: want first.<field> or last.<field>", from)
	}
	return fmt.Errorf("from %q: want nextCursor, nextPageCursor or last.<field>", from)
}

func (c StoreCase) validate() error {
	if !storeIDRe.MatchString(c.ID) {
		return fmt.Errorf("id %q: want [a-z][a-z0-9_]*", c.ID)
	}
	if err := validStorePath(c.Path); err != nil {
		return err
	}
	used := map[string]bool{}
	for _, m := range storeVarRe.FindAllStringSubmatch(c.Path, -1) {
		if _, ok := c.Vars[m[1]]; !ok {
			return fmt.Errorf("path uses {%s}, but vars does not define it", m[1])
		}
		used[m[1]] = true
	}
	if strings.ContainsAny(storeVarRe.ReplaceAllString(c.Path, ""), "{}") {
		return fmt.Errorf("path %q: a {name} must be [a-z][a-z0-9_]*", c.Path)
	}
	for name, v := range c.Vars {
		if !used[name] {
			return fmt.Errorf("vars %s is not used in the path", name)
		}
		if err := validStorePath(v.Path); err != nil {
			return fmt.Errorf("vars %s: %w", name, err)
		}
		if storeVarRe.MatchString(v.Path) {
			return fmt.Errorf("vars %s: path %q must not use vars", name, v.Path)
		}
		if err := validFrom(v.From, true); err != nil {
			return fmt.Errorf("vars %s: %w", name, err)
		}
	}
	if c.WebLimit < 0 || c.Pages < 0 {
		return errors.New("web_limit and pages must not be negative")
	}
	params := map[string]bool{"limit": true}
	for _, f := range c.Follow {
		if !storeParamRe.MatchString(f.Param) {
			return fmt.Errorf("follow param %q is not a query param name", f.Param)
		}
		if params[f.Param] {
			return fmt.Errorf("follow param %s is set twice (limit is set by the walk)", f.Param)
		}
		params[f.Param] = true
		if err := validFrom(f.From, false); err != nil {
			return fmt.Errorf("follow %s: %w", f.Param, err)
		}
	}
	for k := range c.Query {
		if params[k] {
			return fmt.Errorf("query %s is set by the walk", k)
		}
	}
	allowed := map[string]bool{}
	for _, a := range c.Allowed {
		if !storeClassSet[a] {
			return fmt.Errorf("allowed %q: want tie_order or additive_keys", a)
		}
		allowed[a] = true
	}
	if allowed[ClassTieOrder] != (len(c.Key) > 0) {
		return errors.New("tie_order needs key, and key is only for tie_order")
	}
	for _, k := range c.Key {
		if !storeFieldRe.MatchString(k) {
			return fmt.Errorf("key %q is not a field name", k)
		}
	}
	return nil
}

// RunStoreDiff walks every case of spec on the old and new server at each page size of limits ("web" is the case's
// web_limit) and compares the pages.
//
// Each page is a GET of base+path?query&limit=L on both servers. Both bodies are decoded with UseNumber and compared
// as whole JSON values, so any change in shape counts; the first differing JSON path is reported. A non-200 answer
// on either side must match the other's status and body, and ends the walk. The next page's params come from
// Follow, read from the old page. The walk stops when hasMore is false on both (a bare array body has more when it
// holds limit items), at Pages pages, when a follow value is absent from the (equal) pages, or when the follow
// values repeat (Stuck, a difference), and at the first different page.
//
// The error is a request that could not be made (a refused connection, a cancelled context) or a bad argument.
func RunStoreDiff(ctx context.Context, hc *http.Client, oldBase, newBase string, spec *StoreDiffSpec, limits []string) (*StoreDiffReport, error) {
	if hc == nil {
		hc = http.DefaultClient
	}
	if err := spec.Validate(); err != nil {
		return nil, err
	}
	sizes, err := parseStoreLimits(limits)
	if err != nil {
		return nil, err
	}
	sd := &storeDiff{ctx: ctx, hc: hc, old: strings.TrimRight(oldBase, "/"), new: strings.TrimRight(newBase, "/")}
	rep := &StoreDiffReport{}
	for _, c := range spec.Cases {
		path, skip, detail, err := sd.resolvePath(c)
		if err != nil {
			return rep, fmt.Errorf("case %s: %w", c.ID, err)
		}
		for _, size := range sizes {
			limit := size
			if limit == 0 {
				limit = c.WebLimit
				if limit == 0 {
					limit = DefaultStoreWebLimit
				}
			}
			res := StoreCaseResult{ID: c.ID, Limit: limit}
			switch {
			case skip:
				res.Verdict, res.Detail = DiffSkip, detail
			case detail != "":
				res.Verdict, res.Detail = DiffDifferent, detail
			default:
				if err := sd.walk(c, path, limit, &res); err != nil {
					return rep, fmt.Errorf("case %s, limit %d: %w", c.ID, limit, err)
				}
			}
			rep.Cases = append(rep.Cases, res)
		}
	}
	return rep, nil
}

// parseStoreLimits reads page sizes: a positive integer, or web (0: the case's web_limit).
func parseStoreLimits(limits []string) ([]int, error) {
	if len(limits) == 0 {
		return []int{0}, nil
	}
	var out []int
	for _, l := range limits {
		l = strings.TrimSpace(l)
		if l == "web" {
			out = append(out, 0)
			continue
		}
		n, err := strconv.Atoi(l)
		if err != nil || n < 1 {
			return nil, fmt.Errorf("page size %q: want an integer ≥ 1 or web", l)
		}
		out = append(out, n)
	}
	return out, nil
}

type storeDiff struct {
	ctx      context.Context
	hc       *http.Client
	old, new string
}

// storePage is one server's answer.
type storePage struct {
	status int
	raw    []byte
	body   any
	json   bool
}

func (sd *storeDiff) get(base, path string, q url.Values) (storePage, error) {
	u := base + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	req, err := http.NewRequestWithContext(sd.ctx, http.MethodGet, u, nil)
	if err != nil {
		return storePage{}, err
	}
	req.Header.Set("Accept", "application/json")
	resp, err := sd.hc.Do(req)
	if err != nil {
		return storePage{}, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxStoreBody+1))
	if err != nil {
		return storePage{}, fmt.Errorf("GET %s: %w", u, err)
	}
	if len(raw) > maxStoreBody {
		return storePage{}, fmt.Errorf("GET %s: body exceeds %d bytes", u, maxStoreBody)
	}
	p := storePage{status: resp.StatusCode, raw: raw}
	p.body, p.json = decodeJSON(raw)
	return p, nil
}

// decodeJSON decodes exactly one JSON value with UseNumber.
func decodeJSON(raw []byte) (any, bool) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, false
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, false
	}
	return v, true
}

// samePage compares two non-200 answers (or bodies that are not JSON): equal status and body.
func samePage(o, n storePage) string {
	if o.status != n.status {
		return fmt.Sprintf("status: old %d, new %d", o.status, n.status)
	}
	if o.json && n.json {
		if d := jsonDiff(o.body, n.body, "$", nil); d != nil {
			return d.String()
		}
		return ""
	}
	if !bytes.Equal(o.raw, n.raw) {
		return fmt.Sprintf("body: old %s, new %s", clip(string(o.raw)), clip(string(n.raw)))
	}
	return ""
}

// resolvePath fills the path's {name}s from both servers. skip: a var found no value; detail: the servers disagree.
func (sd *storeDiff) resolvePath(c StoreCase) (path string, skip bool, detail string, err error) {
	path = c.Path
	for _, name := range slices.Sorted(maps.Keys(c.Vars)) {
		v := c.Vars[name]
		q := url.Values{}
		for k, val := range v.Query {
			q.Set(k, val)
		}
		var vals [2]string
		for i, base := range []string{sd.old, sd.new} {
			p, err := sd.get(base, v.Path, q)
			if err != nil {
				return "", false, "", fmt.Errorf("vars %s: %w", name, err)
			}
			side := [2]string{"old", "new"}[i]
			if p.status != http.StatusOK || !p.json {
				return "", false, fmt.Sprintf("vars %s: %s server answered %d", name, side, p.status), nil
			}
			val, ok, err := pickValue(p.body, v.From)
			if err != nil {
				return "", false, fmt.Sprintf("vars %s: %s server: %v", name, side, err), nil
			}
			if !ok {
				return "", true, fmt.Sprintf("vars %s: %s %s found no value", name, v.Path, v.From), nil
			}
			vals[i] = val
		}
		if vals[0] != vals[1] {
			return "", false, fmt.Sprintf("vars %s: old %s, new %s", name, vals[0], vals[1]), nil
		}
		path = strings.ReplaceAll(path, "{"+name+"}", url.PathEscape(vals[0]))
	}
	return path, false, "", nil
}

// pageItems returns a page's items: the body when it is an array, else its items field.
func pageItems(body any) ([]any, bool) {
	switch b := body.(type) {
	case []any:
		return b, true
	case map[string]any:
		items, ok := b["items"].([]any)
		return items, ok
	}
	return nil, false
}

// hasMore reads a page's hasMore; a bare array has more when it is full.
func hasMore(body any, limit int) bool {
	switch b := body.(type) {
	case []any:
		return len(b) >= limit
	case map[string]any:
		more, _ := b["hasMore"].(bool)
		return more
	}
	return false
}

// pickValue reads first.<field>, last.<field>, nextCursor or nextPageCursor from a page. ok is false when there is
// no such item or the value is null or absent.
func pickValue(body any, from string) (string, bool, error) {
	var v any
	switch {
	case strings.HasPrefix(from, "first."), strings.HasPrefix(from, "last."):
		items, ok := pageItems(body)
		if !ok {
			return "", false, errors.New("the page has no items")
		}
		if len(items) == 0 {
			return "", false, nil
		}
		item := items[0]
		field := strings.TrimPrefix(from, "first.")
		if strings.HasPrefix(from, "last.") {
			item, field = items[len(items)-1], strings.TrimPrefix(from, "last.")
		}
		v = item
		for _, part := range strings.Split(field, ".") {
			m, ok := v.(map[string]any)
			if !ok {
				return "", false, fmt.Errorf("%s: not an object", from)
			}
			v = m[part]
		}
	default:
		m, ok := body.(map[string]any)
		if !ok {
			return "", false, fmt.Errorf("%s: the page is not an object", from)
		}
		v = m[from]
	}
	switch x := v.(type) {
	case nil:
		return "", false, nil
	case json.Number:
		return x.String(), true, nil
	case string:
		return x, true, nil
	case bool:
		return strconv.FormatBool(x), true, nil
	}
	return "", false, fmt.Errorf("%s is not a scalar", from)
}

func (sd *storeDiff) walk(c StoreCase, path string, limit int, res *StoreCaseResult) error {
	maxPages := c.Pages
	if maxPages == 0 {
		maxPages = DefaultStorePages
	}
	allowed := map[string]bool{}
	for _, a := range c.Allowed {
		allowed[a] = true
	}
	counts := map[string]int{}
	var notes []string
	res.Verdict = DiffIdentical
	seen := map[string]bool{}
	var follow []string
	for page := 1; ; page++ {
		q := url.Values{}
		for k, v := range c.Query {
			q.Set(k, v)
		}
		q.Set("limit", strconv.Itoa(limit))
		for i, f := range c.Follow {
			if follow != nil {
				q.Set(f.Param, follow[i])
			}
		}
		o, err := sd.get(sd.old, path, q)
		if err != nil {
			return fmt.Errorf("page %d: old: %w", page, err)
		}
		n, err := sd.get(sd.new, path, q)
		if err != nil {
			return fmt.Errorf("page %d: new: %w", page, err)
		}
		res.Pages = page
		if o.status != http.StatusOK || n.status != http.StatusOK || !o.json || !n.json {
			if d := samePage(o, n); d != "" {
				res.Verdict, res.Detail = DiffDifferent, fmt.Sprintf("page %d: %s", page, d)
				return nil
			}
			notes = append(notes, fmt.Sprintf("page %d: both answered %d", page, o.status))
			break
		}
		nb := n.body
		if allowed[ClassTieOrder] {
			var moved int
			nb, moved = alignTies(o.body, nb, c.Key, allowed[ClassAdditiveKeys])
			if moved > 0 {
				counts[ClassTieOrder] += moved
				notes = append(notes, fmt.Sprintf("page %d: tie_order: %d items hold another position within their run of equal %s", page, moved, strings.Join(c.Key, ", ")))
			}
		}
		var added []string
		var addedPtr *[]string
		if allowed[ClassAdditiveKeys] {
			addedPtr = &added
		}
		if d := jsonDiff(o.body, nb, "$", addedPtr); d != nil {
			res.Verdict, res.Detail = DiffDifferent, fmt.Sprintf("page %d: %s", page, d)
			return nil
		}
		if len(added) > 0 {
			counts[ClassAdditiveKeys] += len(added)
			if len(added) > 5 {
				added = append(added[:5], fmt.Sprintf("and %d more", len(added)-5))
			}
			notes = append(notes, fmt.Sprintf("page %d: additive_keys: %s", page, strings.Join(added, ", ")))
		}
		if !hasMore(o.body, limit) {
			break
		}
		if page >= maxPages {
			notes = append(notes, fmt.Sprintf("stopped at the %d-page cap", maxPages))
			break
		}
		if len(c.Follow) == 0 {
			notes = append(notes, "hasMore, but the case follows no cursor")
			break
		}
		next := make([]string, len(c.Follow))
		missing := ""
		for i, f := range c.Follow {
			v, ok, err := pickValue(o.body, f.From)
			if err != nil {
				res.Verdict, res.Detail = DiffDifferent, fmt.Sprintf("page %d: follow %s ← %s: %v", page, f.Param, f.From, err)
				return nil
			}
			if !ok && missing == "" {
				missing = fmt.Sprintf("%s ← %s", f.Param, f.From)
			}
			next[i] = v
		}
		if missing != "" {
			// Both pages are equal, so neither server gives a cursor: a client stops here too.
			notes = append(notes, fmt.Sprintf("page %d: hasMore, but %s is absent on both; the walk stops", page, missing))
			break
		}
		k := strings.Join(next, "\x00")
		if seen[k] {
			res.Stuck = fmt.Sprintf("cursor did not advance at page %d", page)
			res.Verdict, res.Detail = DiffDifferent, fmt.Sprintf("page %d: %s", page, res.Stuck)
			return nil
		}
		seen[k] = true
		follow = next
	}
	for _, cl := range []string{ClassTieOrder, ClassAdditiveKeys} {
		if n := counts[cl]; n > 0 {
			res.Classes = append(res.Classes, fmt.Sprintf("%s(%d)", cl, n))
		}
	}
	if len(res.Classes) > 0 {
		res.Verdict = DiffAllowed
	}
	res.Detail = strings.Join(notes, "; ")
	return nil
}

// alignTies returns newBody with its items reordered to old's order within runs of equal key, when the items
// differ only that way, and the number of items that moved. Otherwise it returns newBody unchanged.
func alignTies(oldBody, newBody any, key []string, additive bool) (any, int) {
	oi, ok1 := pageItems(oldBody)
	ni, ok2 := pageItems(newBody)
	if !ok1 || !ok2 || len(oi) != len(ni) {
		return newBody, 0
	}
	keyOf := func(item any) string {
		m, _ := item.(map[string]any)
		parts := make([]string, len(key))
		for i, k := range key {
			v := any(m)
			for _, p := range strings.Split(k, ".") {
				mm, _ := v.(map[string]any)
				v = mm[p]
			}
			b, _ := json.Marshal(v)
			parts[i] = string(b)
		}
		return strings.Join(parts, "\x00")
	}
	for i := range oi {
		if keyOf(oi[i]) != keyOf(ni[i]) {
			return newBody, 0
		}
	}
	equal := func(a, b any) bool {
		var added []string
		var p *[]string
		if additive {
			p = &added
		}
		return jsonDiff(a, b, "$", p) == nil
	}
	out := make([]any, len(ni))
	moved := 0
	for start := 0; start < len(oi); {
		end := start + 1
		k := keyOf(oi[start])
		for end < len(oi) && keyOf(oi[end]) == k {
			end++
		}
		used := make([]bool, end-start)
		for i := start; i < end; i++ {
			pick := -1
			if !used[i-start] && equal(oi[i], ni[i]) {
				pick = i
			} else {
				for j := start; j < end; j++ {
					if !used[j-start] && equal(oi[i], ni[j]) {
						pick = j
						break
					}
				}
			}
			if pick < 0 {
				return newBody, 0
			}
			if pick != i {
				moved++
			}
			used[pick-start] = true
			out[i] = ni[pick]
		}
		start = end
	}
	if moved == 0 {
		return newBody, 0
	}
	if _, isArray := newBody.([]any); isArray {
		return out, moved
	}
	m := map[string]any{}
	for k, v := range newBody.(map[string]any) {
		m[k] = v
	}
	m["items"] = out
	return m, moved
}

// jsonPathDiff is the first difference between two JSON values.
type jsonPathDiff struct {
	path     string
	old, new string
}

func (d *jsonPathDiff) String() string {
	return fmt.Sprintf("%s: old %s, new %s", d.path, d.old, d.new)
}

const absent = "(absent)"

// jsonDiff returns the first difference between a and b, walking object keys in sorted order and arrays by index.
// Numbers compare by their JSON text. With added non-nil, keys only b holds are appended to it instead of being a
// difference.
func jsonDiff(a, b any, path string, added *[]string) *jsonPathDiff {
	switch av := a.(type) {
	case map[string]any:
		bv, ok := b.(map[string]any)
		if !ok {
			break
		}
		keys := slices.Collect(maps.Keys(av))
		for k := range bv {
			if _, ok := av[k]; !ok {
				keys = append(keys, k)
			}
		}
		slices.Sort(keys)
		for _, k := range keys {
			p := path + jsonKey(k)
			x, inA := av[k]
			y, inB := bv[k]
			switch {
			case !inA && added != nil:
				*added = append(*added, p)
			case !inA:
				return &jsonPathDiff{p, absent, render(y)}
			case !inB:
				return &jsonPathDiff{p, render(x), absent}
			default:
				if d := jsonDiff(x, y, p, added); d != nil {
					return d
				}
			}
		}
		return nil
	case []any:
		bv, ok := b.([]any)
		if !ok {
			break
		}
		for i := 0; i < max(len(av), len(bv)); i++ {
			p := fmt.Sprintf("%s[%d]", path, i)
			switch {
			case i >= len(av):
				return &jsonPathDiff{p, absent, render(bv[i])}
			case i >= len(bv):
				return &jsonPathDiff{p, render(av[i]), absent}
			}
			if d := jsonDiff(av[i], bv[i], p, added); d != nil {
				return d
			}
		}
		return nil
	case json.Number:
		if bv, ok := b.(json.Number); ok && av == bv {
			return nil
		}
	case string:
		if bv, ok := b.(string); ok && av == bv {
			return nil
		}
	case bool:
		if bv, ok := b.(bool); ok && av == bv {
			return nil
		}
	case nil:
		if b == nil {
			return nil
		}
	}
	return &jsonPathDiff{path, render(a), render(b)}
}

func jsonKey(k string) string {
	if storeParamRe.MatchString(k) {
		return "." + k
	}
	b, _ := json.Marshal(k)
	return "[" + string(b) + "]"
}

func render(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return fmt.Sprintf("%v", v)
	}
	return clip(string(b))
}

func clip(s string) string {
	const n = 200
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}

// Failed returns the results that are different.
func (r *StoreDiffReport) Failed() []StoreCaseResult {
	var out []StoreCaseResult
	for _, c := range r.Cases {
		if c.Verdict == DiffDifferent {
			out = append(out, c)
		}
	}
	return out
}

// Skipped returns the results that did not run: a path var found no value. They prove nothing.
func (r *StoreDiffReport) Skipped() []StoreCaseResult {
	var out []StoreCaseResult
	for _, c := range r.Cases {
		if c.Verdict == DiffSkip {
			out = append(out, c)
		}
	}
	return out
}

// Line is a one-line summary of a result.
func (c StoreCaseResult) Line() string {
	mark := "ok"
	switch c.Verdict {
	case DiffDifferent:
		mark = "FAIL"
	case DiffSkip:
		mark = "SKIP"
	}
	s := fmt.Sprintf("%-4s %s limit %d: %s, %d pages", mark, c.ID, c.Limit, c.Verdict, c.Pages)
	if len(c.Classes) > 0 {
		s += " " + strings.Join(c.Classes, " ")
	}
	if c.Detail != "" {
		s += "; " + c.Detail
	}
	return s
}

// Markdown renders the report.
func (r *StoreDiffReport) Markdown() string {
	var b strings.Builder
	b.WriteString("# storediff report\n\n| case | limit | pages | verdict | classes | detail |\n|---|---|---|---|---|---|\n")
	esc := func(s string) string { return strings.ReplaceAll(strings.ReplaceAll(s, "|", `\|`), "\n", " ") }
	for _, c := range r.Cases {
		fmt.Fprintf(&b, "| %s | %d | %d | %s | %s | %s |\n", c.ID, c.Limit, c.Pages, c.Verdict, strings.Join(c.Classes, " "), esc(c.Detail))
	}
	fmt.Fprintf(&b, "\n%d results, %d different, %d skipped\n", len(r.Cases), len(r.Failed()), len(r.Skipped()))
	return b.String()
}
