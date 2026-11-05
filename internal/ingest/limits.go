// Copyright 2026 Beacon Contributors
// SPDX-License-Identifier: AGPL-3.0-or-later
package ingest

// Bounds apply to JSON envelopes, not LoRa payloads. Oversized reports are
// rejected before queue admission; a neighbor snapshot is accepted whole or not at all.
const maxEnvelopeBytes = 64 << 10
const maxNeighbors = 256
