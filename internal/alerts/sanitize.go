// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"encoding/hex"
	"strings"
	"unicode"
	"unicode/utf8"
)

// MaxNameRunes caps a radio's name in an alert: names come from radio adverts and are untrusted.
const MaxNameRunes = 40

// SanitizeName makes a radio's name safe for a mail subject or body and a push title: control, format and line
// characters are dropped (no header injection, no bidi tricks), whitespace is collapsed and the length capped. An
// empty result becomes the first 8 hex characters of the key.
func SanitizeName(name string, key []byte) string {
	s := strings.Map(func(r rune) rune {
		switch {
		case unicode.IsSpace(r): // tabs, line breaks and line separators become spaces
			return ' '
		case r == utf8.RuneError, unicode.IsControl(r), unicode.Is(unicode.Cf, r):
			return -1
		}
		return r
	}, name)
	s = strings.Join(strings.Fields(s), " ")
	if utf8.RuneCountInString(s) > MaxNameRunes {
		s = strings.TrimSpace(string([]rune(s)[:MaxNameRunes]))
	}
	if s == "" {
		k := hex.EncodeToString(key)
		if len(k) > 8 {
			k = k[:8]
		}
		return k
	}
	return s
}
