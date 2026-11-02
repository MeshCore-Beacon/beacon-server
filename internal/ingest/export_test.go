// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

import "context"

// HandlePacketForTest runs one packet hearing through the worker, for tests in package ingest_test.
func (w *Worker) HandlePacketForTest(ctx context.Context, iata, pubkeyHex string, raw []byte) {
	w.handlePacket(ctx, iata, pubkeyHex, raw)
}
