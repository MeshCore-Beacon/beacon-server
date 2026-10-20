// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package planguard

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"unicode/utf8"
)

// RowSet is one statement result in the server's text format. A nil value is NULL.
type RowSet struct {
	Columns []string
	Rows    [][]*string
}

// caseHeader is a case file's first line. Rows is the row count, so a file cut at a line boundary is an error too.
type caseHeader struct {
	Columns []string `json:"columns"`
	Rows    *int     `json:"rows"`
}

// WriteCaseFile writes rs as JSON Lines: {"columns":[…],"rows":N}, then one JSON array per row (a string per value,
// null for NULL). Every value round-trips unchanged, newlines and quotes included. Text that is not UTF-8 cannot be
// written losslessly as JSON, so it is an error, as is a row whose arity differs from the columns.
func WriteCaseFile(path string, rs RowSet) error {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	n := len(rs.Rows)
	cols := rs.Columns
	if cols == nil {
		cols = []string{}
	}
	for _, c := range cols {
		if !utf8.ValidString(c) {
			return fmt.Errorf("%s: column name %q is not UTF-8", path, c)
		}
	}
	if err := enc.Encode(caseHeader{Columns: cols, Rows: &n}); err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}
	for i, row := range rs.Rows {
		if len(row) != len(cols) {
			return fmt.Errorf("%s: row %d has %d values for %d columns", path, i+1, len(row), len(cols))
		}
		for j, v := range row {
			if v != nil && !utf8.ValidString(*v) {
				return fmt.Errorf("%s: row %d column %s is not UTF-8", path, i+1, cols[j])
			}
		}
		if err := enc.Encode(row); err != nil {
			return fmt.Errorf("%s: row %d: %w", path, i+1, err)
		}
	}
	return os.WriteFile(path, b.Bytes(), 0o644)
}

// ReadCaseFile reads a file written by WriteCaseFile. Bad JSON, a missing or malformed header, a value that is
// neither a string nor null, a row whose arity differs, a line with no newline, or a row count that differs from
// the header's is an error naming the line: a damaged file never loses rows silently.
func ReadCaseFile(path string) (RowSet, error) {
	f, err := os.Open(path)
	if err != nil {
		return RowSet{}, err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var rs RowSet
	want := -1
	for line := 1; ; line++ {
		raw, err := r.ReadBytes('\n')
		if errors.Is(err, io.EOF) {
			if len(raw) > 0 {
				return RowSet{}, fmt.Errorf("%s: line %d: no newline at the end (truncated)", path, line)
			}
			if want < 0 {
				return RowSet{}, fmt.Errorf("%s: empty file, no header", path)
			}
			if len(rs.Rows) != want {
				return RowSet{}, fmt.Errorf("%s: %d rows, the header says %d (truncated)", path, len(rs.Rows), want)
			}
			return rs, nil
		}
		if err != nil {
			return RowSet{}, fmt.Errorf("%s: line %d: %w", path, line, err)
		}
		if line == 1 {
			if !utf8.Valid(raw) {
				return RowSet{}, fmt.Errorf("%s: line 1: not UTF-8", path)
			}
			var h caseHeader
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.DisallowUnknownFields()
			if err := dec.Decode(&h); err != nil || h.Columns == nil || h.Rows == nil || *h.Rows < 0 || dec.More() {
				return RowSet{}, fmt.Errorf("%s: line 1: want the header {\"columns\":[…],\"rows\":N} (%v)", path, err)
			}
			rs.Columns, want = h.Columns, *h.Rows
			continue
		}
		if !utf8.Valid(raw) {
			return RowSet{}, fmt.Errorf("%s: line %d: not UTF-8", path, line)
		}
		row, err := decodeRow(raw, len(rs.Columns))
		if err != nil {
			return RowSet{}, fmt.Errorf("%s: line %d: %w", path, line, err)
		}
		if len(rs.Rows) == want {
			return RowSet{}, fmt.Errorf("%s: line %d: more rows than the header's %d", path, line, want)
		}
		rs.Rows = append(rs.Rows, row)
	}
}

func decodeRow(raw []byte, arity int) ([]*string, error) {
	var cells []json.RawMessage
	dec := json.NewDecoder(bytes.NewReader(raw))
	if err := dec.Decode(&cells); err != nil {
		return nil, fmt.Errorf("bad JSON: %w", err)
	}
	if dec.More() {
		return nil, errors.New("bad JSON: more than one value on the line")
	}
	if cells == nil {
		return nil, errors.New("want a JSON array")
	}
	if len(cells) != arity {
		return nil, fmt.Errorf("%d values for %d columns", len(cells), arity)
	}
	row := make([]*string, len(cells))
	for i, c := range cells {
		if string(c) == "null" {
			continue
		}
		var v string
		if len(c) == 0 || c[0] != '"' {
			return nil, fmt.Errorf("value %d is %s, want a string or null", i+1, c)
		}
		if err := json.Unmarshal(c, &v); err != nil {
			return nil, fmt.Errorf("value %d: %w", i+1, err)
		}
		row[i] = &v
	}
	return row, nil
}
