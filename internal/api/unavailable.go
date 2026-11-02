// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import "errors"

// ErrPagesNotReady means the read model or data a page is served from is still being filled, repaired or
// verified, or the operator turned it off; the page answers once it is ready.
var ErrPagesNotReady = errors.New("this page is not available until the server finishes preparing its data")
