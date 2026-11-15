// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import (
	"fmt"
	"time"
)

// The texts are plain English. The link sits on its own line so mail clients make it clickable.

func verifyMailText(host, username, link string) (subject, text string) {
	return "Confirm your email address for Beacon",
		fmt.Sprintf("Welcome to Beacon at %s.\n\nConfirm your email address and activate the account named %s by opening this link within 24 hours:\n\n%s\n\nIf you did not sign up for Beacon, ignore this mail and nothing will happen.\n", host, username, link)
}

func emailChangeMailText(host, link string) (subject, text string) {
	return "Confirm your new email address for Beacon",
		fmt.Sprintf("Someone asked to use this address for a Beacon account at %s.\n\nConfirm it by opening this link within 24 hours:\n\n%s\n\nIf you did not ask for this, ignore this mail and nothing will change.\n", host, link)
}

func resetMailText(host, link string) (subject, text string) {
	return "Reset your Beacon password",
		fmt.Sprintf("Someone asked to reset the password of your Beacon account at %s.\n\nChoose a new password by opening this link within 1 hour:\n\n%s\n\nIf you did not ask for this, ignore this mail and your password stays as it is.\n", host, link)
}

func passwordChangedMailText(host string) (subject, text string) {
	return "Your Beacon password was changed",
		fmt.Sprintf("The password of your Beacon account at %s was just changed, and every other device was signed out.\n\nIf this was you, there is nothing to do. If it was not, reset your password now from the sign-in page.\n", host)
}

func emailChangedMailText(host string) (subject, text string) {
	return "The email address of your Beacon account was changed",
		fmt.Sprintf("The email address of your Beacon account at %s was just changed. This address no longer receives mail for it.\n\nIf this was you, there is nothing to do. If it was not, contact the operator of this server.\n", host)
}

func appSignInMailText(host, label string, at time.Time) (subject, text string) {
	return "A new app signed in to your Beacon account",
		fmt.Sprintf("A new app (%s) signed in to your Beacon account at %s on %s.\n\nIf this was you, there is nothing to do. If it was not, sign out that device under Profile, Devices, and change your password.\n",
			label, host, at.UTC().Format("2006-01-02 15:04 UTC"))
}
