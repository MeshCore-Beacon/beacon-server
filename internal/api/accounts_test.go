// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"errors"
	"strings"
	"testing"
)

func TestNormalizeAccountName(t *testing.T) {
	for _, tc := range []struct {
		input, want string
		invalid     bool
	}{
		{"  Montréal 🦀 \t", "Montréal 🦀", false}, {"A", "A", false},
		{strings.Repeat("é", 128), strings.Repeat("é", 128), false},
		{"", "", true}, {"\u00a0\t", "", true}, {strings.Repeat("é", 129), "", true},
		{"x\x00y", "", true}, {"x\ny", "", true}, {"\xff", "", true},
	} {
		got, err := NormalizeAccountName(tc.input)
		if got != tc.want || errors.Is(err, ErrAccountNameInvalid) != tc.invalid {
			t.Fatalf("name validation: got %q, error=%v", got, err)
		}
	}
}
