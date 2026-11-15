// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

// Package users holds the account types shared by the store and the account service.
package users

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

type User struct {
	ID              uuid.UUID
	Username        string
	DisplayName     string
	Email           string
	PendingEmail    *string
	PassHash        string
	EmailVerifiedAt *time.Time
	Provider        string
	ProviderSubject *string
	CreatedAt       time.Time
	LastLoginAt     *time.Time
	DisabledAt      *time.Time
}

type Session struct {
	ID, UserID                       uuid.UUID
	Kind, Label                      string
	CreatedAt, LastUsedAt, ExpiresAt time.Time
	HardExpiresAt                    *time.Time
	IP, UserAgent                    string
}

// TokenKind says what a one-time token may be used for: "verify", "reset", "email_change" or "app_code".
type TokenKind string

const (
	TokenVerify      TokenKind = "verify"
	TokenReset       TokenKind = "reset"
	TokenEmailChange TokenKind = "email_change"
	TokenAppCode     TokenKind = "app_code"
)

var (
	ErrNotFound      = errors.New("users: not found")
	ErrUsernameTaken = errors.New("users: username taken")
	ErrEmailTaken    = errors.New("users: email taken")
)
