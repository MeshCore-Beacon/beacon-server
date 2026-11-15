// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import (
	"fmt"
	"time"
)

// The texts are plain English. The link sits on its own line so mail clients make it clickable.

func verifyMailText(host, link string) (subject, text string) {
	return "Confirm your email address for Beacon",
		fmt.Sprintf("Welcome to Beacon at %s.\n\nConfirm your email address by opening this link within 24 hours:\n\n%s\n\nIf you did not create this account, ignore this mail and nothing will happen.\n", host, link)
}

func emailChangeMailText(host, link string) (subject, text string) {
	return "Confirm your new email address for Beacon",
		fmt.Sprintf("Someone asked to use this address for a Beacon account at %s.\n\nConfirm it by opening this link within 24 hours:\n\n%s\n\nIf you did not ask for this, ignore this mail and nothing will change.\n", host, link)
}

func resetMailText(host, link string) (subject, text string) {
	return "Reset your Beacon password",
		fmt.Sprintf("Someone asked to reset the password of your Beacon account at %s.\n\nChoose a new password by opening this link within 1 hour:\n\n%s\n\nIf you did not ask for this, ignore this mail and your password stays as it is.\n", host, link)
}

func appSignInMailText(host, label string, at time.Time) (subject, text string) {
	return "A new app signed in to your Beacon account",
		fmt.Sprintf("A new app (%s) signed in to your Beacon account at %s on %s.\n\nIf this was you, there is nothing to do. If it was not, sign out that device under Profile, Devices, and change your password.\n",
			label, host, at.UTC().Format("2006-01-02 15:04 UTC"))
}
