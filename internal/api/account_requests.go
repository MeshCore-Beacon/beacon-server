// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

type CreateAccountRequest struct {
	Name string `json:"name" validate:"required"`
}
