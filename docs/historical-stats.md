# Historical stats

## The rule

Historical stats read hourly rollup tables filled once per closed hour by one background
task. Raw rows are read for per-entity drill-downs (a packet, a trace, a route), observer
comparison, sub-hour observer activity, and bounded observer-directory slices that cannot
use a completed whole-hour rollup.

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
  report it without values, except the observer directory, which counts any retained raw
  observations in the requested slice.
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

Historical stats windows snap to UTC hours on the server, and responses report the
effective window. The observer directory preserves exact millisecond bounds instead.
`/stats/overview` spans the 24 newest eligible hours; its `windowHours` counts the complete
ones its totals cover.

## Observer directory

`GET /api/v1/observers/directory` defaults to `[request time - 7 days, request time)`.
Explicit `since` and `until` are epoch milliseconds and remain exact; windows must be
positive and at most 31 days. If only `until` is supplied, `since` defaults to seven days
before it. If only `since` is supplied, `until` defaults to request time.

Counts combine complete rollups for whole UTC hours fully inside the window with retained
raw observations for every other hour and the exact boundary slices. These sources never
overlap. Completed whole-hour history survives raw retention; a sliced boundary whose raw
rows have expired cannot be reconstructed from its whole-hour total.

Every observer receives a numeric `observationCount`, including zero. `maxObservationCount`
is the maximum across all matching observers, independent of pagination, and is zero when
none match. Incomplete history never suppresses these counts or changes `effectiveSort`:
traffic sorting uses available counts, while explicit name sorting remains supported.
Both sorts break name ties by observer ID.

`coverage` is conservative and informational. `expectedHours` counts intersecting UTC
hours, including boundary slices. `completeHours` counts the whole hours read from complete
rollups. Other hours are `partialHours` when there is a partial/boundary rollup or matching
retained raw traffic; otherwise they are `missingHours`. Raw-only traffic does not establish
complete history. Status is `complete` only when all intersecting hours are complete,
`partial` when any complete or partial hour exists, and `unavailable` otherwise. Zero counts
with incomplete coverage mean no available observations, not proof that no traffic occurred.

The response uses `Cache-Control: no-store`. For continuation requests, send `nextCursor`,
the same filters and sort, and the exact returned `windowStart`/`windowEnd` as `since`/`until`.
These bounds fix the time window, not a database snapshot; late arrivals and metadata changes
can still move results. Refresh without bounds to obtain a new rolling window and include
new traffic.

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
