// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package users

import "golang.org/x/crypto/argon2"

func argon2IDKey(pw string, salt []byte, t, m uint32) []byte {
	return argon2.IDKey([]byte(pw), salt, t, m, 1, 32)
}
