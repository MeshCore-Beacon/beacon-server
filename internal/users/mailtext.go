// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import (
	"fmt"
	"time"
)

// The texts are plain English. The link sits on its own line so mail clients make it clickable.
// name is the instance name (VITE_APP_NAME, default "Beacon"); it may hold several words, so the host follows
// it in brackets.

func verifyMailText(name, host, username, link string) (subject, text string) {
	return "Confirm your email address for " + name,
		fmt.Sprintf("Welcome to %s (%s).\n\nConfirm your email address and activate the account named %s by opening this link within 24 hours:\n\n%s\n\nIf you did not sign up for %s, ignore this mail and nothing will happen.\n", name, host, username, link, name)
}

func emailChangeMailText(name, host, link string) (subject, text string) {
	return "Confirm your new email address for " + name,
		fmt.Sprintf("Someone asked to use this address for a %s account (%s).\n\nConfirm it by opening this link within 24 hours:\n\n%s\n\nIf you did not ask for this, ignore this mail and nothing will change.\n", name, host, link)
}

func resetMailText(name, host, link string) (subject, text string) {
	return "Reset your " + name + " password",
		fmt.Sprintf("Someone asked to reset the password of your %s account (%s).\n\nChoose a new password by opening this link within 1 hour:\n\n%s\n\nIf you did not ask for this, ignore this mail and your password stays as it is.\n", name, host, link)
}

func passwordChangedMailText(name, host string) (subject, text string) {
	return "Your " + name + " password was changed",
		fmt.Sprintf("The password of your %s account (%s) was just changed, and every other device was signed out.\n\nIf this was you, there is nothing to do. If it was not, reset your password now from the sign-in page.\n", name, host)
}

func emailChangedMailText(name, host string) (subject, text string) {
	return "The email address of your " + name + " account was changed",
		fmt.Sprintf("The email address of your %s account (%s) was just changed. This address no longer receives mail for it.\n\nIf this was you, there is nothing to do. If it was not, contact the operator of this server.\n", name, host)
}

func appSignInMailText(name, host, label string, at time.Time) (subject, text string) {
	return "A new app signed in to your " + name + " account",
		fmt.Sprintf("A new app (%s) signed in to your %s account (%s) on %s.\n\nIf this was you, there is nothing to do. If it was not, sign out that device under Profile, Devices, and change your password.\n",
			label, name, host, at.UTC().Format("2006-01-02 15:04 UTC"))
}
