// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later

package ingest

// Bounds apply to JSON envelopes, not LoRa payloads. An oversized message is refused before it
// is queued; a neighbor report keeps its first maxNeighbors entries.
const maxEnvelopeBytes = 64 << 10
const maxNeighbors = 256
