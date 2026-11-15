// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import (
	"strings"
	"testing"
	"time"
)

func TestMailTextCarriesInstanceName(t *testing.T) {
	at := time.Date(2026, 1, 2, 3, 4, 0, 0, time.UTC)
	for _, name := range []string{"Beacon", "Beacon - Ottawa"} {
		type mt struct{ subject, text string }
		mk := func(s, x string) mt { return mt{s, x} }
		all := map[string]mt{
			"verify":  mk(verifyMailText(name, "h.example", "alice", "L")),
			"change":  mk(emailChangeMailText(name, "h.example", "L")),
			"reset":   mk(resetMailText(name, "h.example", "L")),
			"pwchg":   mk(passwordChangedMailText(name, "h.example")),
			"mailchg": mk(emailChangedMailText(name, "h.example")),
			"appsign": mk(appSignInMailText(name, "h.example", "Pixel", at)),
		}
		for k, m := range all {
			if !strings.Contains(m.subject, name) || !strings.Contains(m.text, name) {
				t.Errorf("%s/%s: name missing: %q / %q", name, k, m.subject, m.text)
			}
			if !strings.Contains(m.text, "h.example") {
				t.Errorf("%s/%s: host missing", name, k)
			}
			if strings.Contains(m.text, "Beacon at") {
				t.Errorf("%s/%s: old wording: %q", name, k, m.text)
			}
		}
	}
	s, x := verifyMailText("Beacon - Ottawa", "h.example", "alice", "https://h.example/v?t=1")
	if s != "Confirm your email address for Beacon - Ottawa" {
		t.Errorf("subject %q", s)
	}
	if !strings.HasPrefix(x, "Welcome to Beacon - Ottawa (h.example).\n") ||
		!strings.Contains(x, "\n\nhttps://h.example/v?t=1\n\n") ||
		!strings.Contains(x, "If you did not sign up for Beacon - Ottawa, ignore this mail and nothing will happen.") {
		t.Errorf("body %q", x)
	}
	s, x = resetMailText("Beacon", "h.example", "L")
	if s != "Reset your Beacon password" || !strings.Contains(x, "your Beacon account (h.example)") {
		t.Errorf("%q %q", s, x)
	}
}
