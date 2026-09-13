// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"context"
	"errors"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/google/uuid"
)

var (
	ErrAccountNotFound     = errors.New("account not found")
	ErrAccountInactive     = errors.New("account is already inactive")
	ErrAccountNameConflict = errors.New("an active account already uses this name")
	ErrAccountNameInvalid  = errors.New("account name must be 1-128 characters without control characters")
)

// Account is an operator-defined record, not a login or authentication principal.
type Account struct {
	ID            uuid.UUID  `json:"id"`
	Name          string     `json:"name"`
	CreatedAt     time.Time  `json:"created_at"`
	DeactivatedAt *time.Time `json:"deactivated_at,omitempty"`
	Active        bool       `json:"active"`
}

type AccountList struct {
	Items []Account `json:"items"`
}

// AccountStore is separate from public Reader/cache and ingest write interfaces.
type AccountStore interface {
	CreateAccount(context.Context, string) (Account, error)
	ListAccounts(context.Context) ([]Account, error)
	GetAccount(context.Context, uuid.UUID) (Account, error)
	DeactivateAccount(context.Context, uuid.UUID) error
}

// NormalizeAccountName applies the same validation to every account writer.
func NormalizeAccountName(name string) (string, error) {
	if !utf8.ValidString(name) {
		return "", ErrAccountNameInvalid
	}
	name = strings.TrimSpace(name)
	if length := utf8.RuneCountInString(name); length < 1 || length > 128 {
		return "", ErrAccountNameInvalid
	}
	for _, r := range name {
		if unicode.IsControl(r) {
			return "", ErrAccountNameInvalid
		}
	}
	return name, nil
}
