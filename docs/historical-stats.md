# Historical stats

## The rule

Historical stats never scan `packet_observations` per request or per refresh. They read
hourly rollup tables filled once per closed hour by one background task. Raw rows are read
only for per-entity drill-downs (a packet, a trace, a route), observer comparison and
sub-hour observer activity.

## Rollup contract

- **Hours.** `hour` is the UTC `date_trunc('hour', heard_at)` and leads every rollup table's
  primary key. Rollup tables have no foreign keys: history outlives deleted observers, nodes
  and IATAs.
- **Eligibility.** Hour `[H, H+1h)` can be rolled once `now() >= H + 95 min`. Ingest clamps
  `heard_at` to ±30 minutes of server time, so nothing can still land in it.
- **Registration.** Every eligible hour since the earliest retained observation gets a row
  in `analytics_rollup_hours`, including hours with no traffic. Status is `missing`,
  `complete` or `partial`.
- **Rolling.** One process holds the advisory lock (`analytics_rollup` task, every 5
  minutes, plus a catch-up at boot). Each hour is rolled in one REPEATABLE READ transaction:
  copy its observations (joined to packet dimensions) into a temp table, delete and reinsert
  every family, hash the result, mark the hour complete. A serialization failure skips the
  hour until the next pass.
- **Revision.** `analytics_state.revision` increases when an hour's content hash changes or
  an hour turns partial; `analytics_raw_state.coverage` increases when retention deletes
  rollup hours. Cache keys for rollup-backed endpoints include their sum (each counter only
  grows), read through a 10 s memo.
- **Raw deletion.** Cleanup deletes packets older than `packets.retention`, but holds the
  cutoff 35 minutes behind the oldest `missing` hour, for at most 24 hours (it logs a
  warning past 6). Each batch advances `analytics_raw_state.raw_deleted_before` to the
  newest deleted `last_heard_at`. Cleanup and the rollup never write the same row.
- **Partial hours.** A not-yet-complete hour starting at or before `raw_deleted_before + 30 min`
  lost raw rows before it was rolled. It is marked `partial` and never rolled; readers
  report it without values.
- **Re-rolls.** Hours are not re-rolled periodically. A message stored by channel backfill (at
  startup, and when keys are imported at runtime) queues every hour its packet was heard in
  `analytics_dirty_hours`, in the same statement as the message insert. Each pass re-rolls a
  few complete ones while their raw rows remain. A queued hour still waiting for its first roll
  keeps its entry unless that roll's snapshot already included it.
- **Retention.** Rollups are kept for `analytics.rollup_retention` (default 90 days), which
  is also the longest window `/stats/series` accepts.

## Combining hours

| Metric | Across hours and IATAs |
| --- | --- |
| Observations, receptions, payload counts | Sum |
| Unique packets, adverts, messages, scoped packets | Sum of hourly IATA-set counts: each is counted once per hour it was heard, so one heard across an hour boundary counts twice (measured +1.1%) |
| Active observers, IATAs, scopes | Distinct across the window |
| Active scope observers and nodes (per scope, per hour) | Distinct within the hour, across the requested IATAs |
| Max path entries | Max |
| SNR / RSSI averages | Σ sum / Σ samples, never an average of averages |

Windows snap to UTC hours on the server, and responses report the effective window.
`/stats/overview` spans the 24 newest eligible hours; its `windowHours` counts the complete
ones its totals cover.

## IATA sets

A packet heard in YVR and YYZ appears in both IATAs' per-IATA rows, so summing per-IATA
distinct counts overcounts (2.26× on production data). The `*_sets` tables instead group
each distinct packet, advert or message under the exact sorted set of IATAs that heard it
that hour (`iatas char(3)[]`, kept canonical by a CHECK). For any requested set S, the rows
where `iatas && S` count every item heard in S exactly once. This is exact for one IATA, a
region or all IATAs.

## Raw or current, by design

- Packet, trace and route detail, the packet list, observer comparison, and observer
  activity below one hour. Hourly observer activity reads raw rows for the hours after the
  newest complete one (at most the last 24 hours, the raw holdback), so the unrolled tail isn't
  shown as zero. `rolledUntil` and `rawFrom` in the response mark any hours still uncovered.
- Current memberships: scope observer and node counts, node types, clock drift, radio presets.
- `/traces` reads `trace_tags`, a per-tag summary kept at ingest, not a rollup. A new packet
  adds its count, type and payload in the statement that stores it; hearings set the times.

## Adding a metric

1. Add a family table in a new numbered migration (`001_baseline.sql` is frozen from 2.0.0): `hour`
   first in the primary key, no foreign keys, and a set table with
   `CHECK (iata_set_is_canonical(iatas))` plus a GIN index if it needs distinct counts.
2. Add a delete in `DeleteRollupHour`, a `Roll*` insert over `rollup_obs` in
   `db/queries/rollup.sql`, a line in `RollupContentHash` and `DeleteOldRollups`, and the
   call in `rollHour` (`db/rollup.go`).
3. Read it with an hour-bounded query, using the UNION-ALL IATA guard (see `series.sql`) for
   per-IATA tables or `iatas && @iatas` for set tables.
4. Cache it with `cachedRollup` so its key carries the revision.
5. Add a raw-SQL oracle case to `db/rollup_integration_test.go`.
6. Run `sqlc generate`, regenerate mocks and swagger.
