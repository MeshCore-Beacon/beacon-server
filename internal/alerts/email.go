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

// ComposeEmail builds one message for everything one evaluation pass produced for a user: the subject counts the
// alerts, each alert is one plain line, and the last line links to the Atlas. Names are sanitised again here, so a
// name can never add a header or a line.
func ComposeEmail(name string, notices []EmailNotice, atlasURL string) (subject, text string) {
	n := len(notices)
	if n == 1 {
		subject = name + ": 1 alert"
	} else {
		subject = fmt.Sprintf("%s: %d alerts", name, n)
	}
	var b strings.Builder
	for _, x := range notices {
		b.WriteString(Sentence(x.Kind, SanitizeName(x.Name, nil), x.Value, x.Detail))
		b.WriteString("\n")
	}
	b.WriteString("\nOpen your Atlas:\n")
	b.WriteString(atlasURL)
	b.WriteString("\n")
	return subject, b.String()
}
