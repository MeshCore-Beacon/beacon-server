// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package config

import (
	"strings"
	"unicode"
)

const (
	defaultInstanceName = "Beacon"
	maxInstanceName     = 60
)

// InstanceName turns the VITE_APP_NAME value into the name mail uses for this instance. Control characters and
// line breaks are dropped, whitespace is trimmed and collapsed, and the result is capped at 60 characters. Unset,
// blank, or the web's all-caps default "BEACON" (any case) give "Beacon".
func InstanceName(raw string) string {
	var b strings.Builder
	for _, r := range raw {
		switch {
		case r == '\t':
			b.WriteRune(' ')
		case unicode.IsControl(r) || unicode.Is(unicode.Cf, r) || r == ' ' || r == ' ':
			// dropped
		default:
			b.WriteRune(r)
		}
	}
	name := strings.Join(strings.Fields(b.String()), " ")
	if r := []rune(name); len(r) > maxInstanceName {
		name = strings.TrimSpace(string(r[:maxInstanceName]))
	}
	if name == "" || strings.EqualFold(name, defaultInstanceName) {
		return defaultInstanceName
	}
	return name
}
