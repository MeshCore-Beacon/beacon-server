// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package api

import "errors"

// ErrSlowPoolBusy means every connection kept for the slow region and message statements is in use; the request can
// be retried shortly.
var ErrSlowPoolBusy = errors.New("every connection for slow region and message requests is busy")
