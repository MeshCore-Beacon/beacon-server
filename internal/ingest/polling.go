// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import "context"

// PollingSink receives the repeater polling topics. Implemented by
// polling.Service; kept here so ingest does not import that package.
type PollingSink interface {
	HandleClaim(ctx context.Context, topicPubkeyHex string, raw []byte)
	HandleReading(ctx context.Context, topicPubkeyHex string, raw []byte)
}

// SetPolling installs the sink. Called once at startup before Start, so no locking.
func (w *Worker) SetPolling(p PollingSink) { w.polling = p }
