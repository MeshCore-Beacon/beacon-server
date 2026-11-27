// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import (
	"fmt"
	"time"

	"github.com/MeshCore-Beacon/beacon-server/internal/users"
	"github.com/google/uuid"
)

// User is a signed-in person as clients see them. The password hash never appears in any response type.
type User struct {
	ID            string  `json:"id" example:"3f2b8c1e-6a52-4d0e-9b57-0d3d2b6a1c11"`
	Username      string  `json:"username" example:"ridgewalker"`
	DisplayName   string  `json:"displayName" example:"Ridge Walker"`
	Email         string  `json:"email" example:"a@example.org"`
	EmailVerified bool    `json:"emailVerified" example:"true"`
	PendingEmail  *string `json:"pendingEmail" example:"new@example.org"`
	Provider      string  `json:"provider" example:"local"`
	// CreatedAt is epoch milliseconds.
	CreatedAt int64 `json:"createdAt" example:"1794700000000"`
}

// NewUser converts a stored user for a response.
func NewUser(u users.User) User {
	return User{
		ID: u.ID.String(), Username: u.Username, DisplayName: u.DisplayName, Email: u.Email,
		EmailVerified: u.EmailVerifiedAt != nil, PendingEmail: u.PendingEmail, Provider: u.Provider,
		CreatedAt: u.CreatedAt.UnixMilli(),
	}
}

// AdminUser is a user as the operator sees them.
type AdminUser struct {
	User
	// Disabled users cannot sign in and have no live sessions.
	Disabled    bool   `json:"disabled" example:"false"`
	LastLoginAt *int64 `json:"lastLoginAt" example:"1794703600000"`
}

// NewAdminUser converts a stored user for the operator API.
func NewAdminUser(u users.User) AdminUser {
	a := AdminUser{User: NewUser(u), Disabled: u.DisabledAt != nil}
	if u.LastLoginAt != nil {
		ms := u.LastLoginAt.UnixMilli()
		a.LastLoginAt = &ms
	}
	return a
}

// UserSession is one sign-in of the current user. Times are epoch milliseconds.
type UserSession struct {
	ID         string `json:"id"`
	Kind       string `json:"kind" enums:"web,app"`
	Label      string `json:"label" example:"Firefox on macOS"`
	Current    bool   `json:"current"`
	CreatedAt  int64  `json:"createdAt"`
	LastUsedAt int64  `json:"lastUsedAt"`
	ExpiresAt  int64  `json:"expiresAt"`
}

// NewUserSession converts a stored session; current marks the one making the request.
func NewUserSession(s users.Session, current bool) UserSession {
	return UserSession{
		ID: s.ID.String(), Kind: s.Kind, Label: s.Label, Current: current,
		CreatedAt: s.CreatedAt.UnixMilli(), LastUsedAt: s.LastUsedAt.UnixMilli(), ExpiresAt: s.ExpiresAt.UnixMilli(),
	}
}

type UserResponse struct {
	User User `json:"user"`
}

type SessionList struct {
	Sessions []UserSession `json:"sessions"`
}

type StatusResponse struct {
	Status string `json:"status" example:"check_email"`
}

type RegisterRequest struct {
	Username string `json:"username"`
	Email    string `json:"email"`
	Password string `json:"password"`
}

type TokenRequest struct {
	Token string `json:"token"`
}

type EmailRequest struct {
	Email string `json:"email"`
}

type LoginRequest struct {
	// Login is a username or an email address.
	Login    string `json:"login"`
	Password string `json:"password"`
}

type ResetConfirmRequest struct {
	Token    string `json:"token"`
	Password string `json:"password"`
}

type AppCodeRequest struct {
	// CodeChallenge is the base64url SHA-256 of the code verifier (S256 only).
	CodeChallenge string `json:"codeChallenge"`
	State         string `json:"state"`
	DeviceLabel   string `json:"deviceLabel"`
}

type AppCodeResponse struct {
	Code        string `json:"code"`
	RedirectURI string `json:"redirectUri" example:"meshcorebeacon://auth"`
}

type AppTokenRequest struct {
	Code         string `json:"code"`
	CodeVerifier string `json:"codeVerifier"`
	// DeviceLabel names the app's device, e.g. "Beacon app on iPhone". Optional: when empty the session keeps the
	// label of the browser that issued the code.
	DeviceLabel string `json:"deviceLabel,omitempty" example:"Beacon app on iPhone"`
}

type AppTokenResponse struct {
	Token     string `json:"token"`
	ExpiresAt int64  `json:"expiresAt"`
	User      User   `json:"user"`
}

type UpdateMeRequest struct {
	DisplayName string `json:"displayName"`
}

type ChangePasswordRequest struct {
	CurrentPassword string `json:"currentPassword"`
	NewPassword     string `json:"newPassword"`
}

type ChangeEmailRequest struct {
	Email           string `json:"email"`
	CurrentPassword string `json:"currentPassword"`
}

// DeleteMeRequest confirms an account deletion: the password, or for a portal user (who has none) their username.
type DeleteMeRequest struct {
	CurrentPassword string `json:"currentPassword"`
	ConfirmUsername string `json:"confirmUsername"`
}

// UserCursor is a boundary in (created_at DESC, id DESC) order for the operator's user list.
type UserCursor struct {
	CreatedAt time.Time
	ID        uuid.UUID
}

func (c UserCursor) String() string { return fmt.Sprintf("v1:%d:%s", c.CreatedAt.UnixMicro(), c.ID) }

// ParseUserCursor parses the opaque cursor returned by the user list.
func ParseUserCursor(raw string) (*UserCursor, error) {
	at, id, err := parseUUIDCursor(raw)
	if err != nil {
		return nil, err
	}
	return &UserCursor{CreatedAt: at, ID: id}, nil
}

// AdminUserList is one page of users, newest first. NextCursor is empty on the last page.
type AdminUserList struct {
	Items      []AdminUser `json:"items"`
	NextCursor string      `json:"nextCursor,omitempty"`
}

type AdminUserResponse struct {
	User AdminUser `json:"user"`
}

// SetUserDisabledRequest disables (true) or re-enables (false) a user.
type SetUserDisabledRequest struct {
	Disabled bool `json:"disabled"`
}
