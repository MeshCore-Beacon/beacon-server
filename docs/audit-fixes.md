# Development audit fixes — November 2026

This candidate starts at upstream dev `647e17e` and carries fixes for existing
Beacon behavior. Topology, scope investigation, MeshMapper profile extensions,
and the independent companion collector are not part of this candidate.

The full cross-repository findings, measurements and release sequence are in
[the shared audit](https://github.com/MeshCore-Beacon/beacon-docs/blob/codex/audit-docs-20261116/docs/audit-20261116.md).

## Behavior

- Redis failures retain request coalescing. Cache operations have bounded
  timeouts, callers can cancel their wait, and a new leader checks whether an
  earlier request already filled the key. Responses remain separate copies.
- Boot backfills take smaller slices, serialize their heavy writes, honor a
  smaller configured statement timeout, and retry smaller slices after genuine
  statement timeouts. Exhausting the work-cycle budget does not permanently
  shrink the batch. The saved watermark moves only after a successful write.
- JSON encoding errors produce a structured 500 before headers are committed.
- MQTT envelopes, neighbor lists, WebSocket control messages, subscription counts,
  expanded scopes and write lifetimes are bounded. A missing requested region
  does not silently become an all-region subscription. Global stream drops tell
  clients to recover the gap; metrics distinguish global, repeat and client drops.
- Missing, invalid and non-finite SNR/RSSI remain missing. An explicitly reported
  zero remains present. Observer activity keeps separate denominators for the two
  signal fields. An omitted scope list does not erase the last explicit report.
- TRACE uses its payload hash width and original wire SNR bytes. Unsupported or
  ambiguous evidence is labeled; requested but unvisited hops do not establish
  routes or firmware capabilities. Ordinary path hash widths remain unchanged.
  Retained evidence is not deleted, and these checks do not authenticate a trace
  or prove an RF collision.
- Route evidence cursors pin the complete path representation as well as the time
  window. Older cursors remain readable; widths are never merged implicitly.
- Node activity includes late hearings of older retained packets. A requested
  rolled period still queued for rebuilding returns a retryable unavailable
  response rather than a misleading empty chart.
- The sequential clock-history update retains the newest 16 samples, including
  the inserted row, without dropping an extra sample on an insert conflict.
- `/info` optionally exposes `buildVersion` and `buildRevision`, distinct from
  the API compatibility version. Build with `-X main.version=...` and
  `-X main.revision=<source SHA>` to populate them.

## Compatibility and recovery

No migration is added, removed from upstream, or rewritten. Existing extra tables
in a former preview may remain as retained history, but no route in this candidate
serves the old collector or scope-extension APIs. Restore the previous binary and
configuration to reverse a deployment; do not roll back accumulated packet data.

New telemetry uses upstream's observer polling and request contracts. Configuration
remains opt-in. The retired collector does not share reservations with that system;
do not run both pollers for the same targets during a transition.

## Verification

The CI workflow includes PostgreSQL regression cases for backfill timeout/retry
behavior, signal validity, trace and route evidence, clock history and node
activity. The normal build, vet, unit, race, query-plan, backup and vulnerability
checks remain enabled. The shared audit records final check runs and deployment
identity; this file is a behavior contract, not a claim of production qualification.
