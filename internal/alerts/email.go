// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package alerts

import (
	"fmt"
	"strings"
)

// EmailNotice is one alert in a mail.
type EmailNotice struct {
	Kind, Name, Detail string
	Value              int64
}

// ComposeEmail builds one message per user per evaluation pass: the subject counts the alerts, each is one plain
// line, and the last links to the Atlas. Names are sanitised again here. tempUnit is the instance unit ("f" or "c").
func ComposeEmail(name string, notices []EmailNotice, atlasURL, tempUnit string) (subject, text string) {
	n := len(notices)
	if n == 1 {
		subject = name + ": 1 alert"
	} else {
		subject = fmt.Sprintf("%s: %d alerts", name, n)
	}
	var b strings.Builder
	for _, x := range notices {
		b.WriteString(Sentence(x.Kind, SanitizeName(x.Name, nil), x.Value, x.Detail, tempUnit))
		b.WriteString("\n")
	}
	b.WriteString("\nOpen your Atlas:\n")
	b.WriteString(atlasURL)
	b.WriteString("\n")
	return subject, b.String()
}
