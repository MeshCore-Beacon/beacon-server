# Production CPU profiling

Beacon can record bounded CPU profiles on Linux and macOS. It's off by default, writes
private files only (there's no HTTP endpoint or port), and a profiling failure stops the
recorder with a warning without affecting ingest.

## Enable a session

Create a dedicated `0700` directory writable by the container user, mount it, and set both
variables:

```yaml
services:
  app:
    environment:
      BEACON_CPU_PROFILE_DIR: /profiles
      BEACON_CPU_PROFILE_UNTIL: "2026-10-02T12:00:00Z" # fixed deadline
    volumes:
      - ./profiles:/profiles
```

- The deadline is RFC 3339, at most 72 hours ahead. It survives restarts and is inactive
  once past. Don't generate a fresh deadline on every start.
- Changing either setting needs a restart.
- Start with a deadline about a minute out, and compare process CPU, packet freshness, queue
  drops and database timeouts with an unprofiled period before running a longer session.
- One Beacon process per directory. Keep it out of web roots, public backups and source
  control, and don't run another CPU profiler in the process meanwhile.

## What gets captured

- A 30-second sample at start, another 35 minutes later, then every 30 minutes (the extra
  five minutes keeps a maintenance trigger from landing in the cooldown).
- An extra sample when route reconfirmation maintenance starts (including its retention
  step).
- A five-minute cooldown between capture starts. A periodic sample due during it runs when it
  ends; a triggered one is skipped, and any sample cancels a periodic one that's waiting.
- Background task stacks carry a `task` label.
- Shutdown or the deadline ends the active sample early and saves it.

Limits: 8 MiB per profile; 256 MiB and 512 files per directory, counting metadata, leftovers
and unrelated regular files (subdirectories are ignored). The recorder reserves room for a
full capture first and stops at a limit. Nothing is deleted automatically. Files are `0600`;
a `.partial` file is an interrupted capture.

## Reading the output

Each `.pprof` has a `.pprof.json` sidecar with UTC start and end, the trigger, Go version,
process CPU counters, goroutine count, and database-pool counters before and after (pool
values are cumulative, so subtract). No SQL text or credentials.

Copy completed files off the host privately, noting the image revision and digest. Then:

```sh
go tool pprof -top capture.pprof
go tool pprof -top -cum capture.pprof
go tool pprof -tags capture.pprof
```

Profiles show CPU inside Beacon, not PostgreSQL work or lock waits. Correlate each interval
with host/container CPU and I/O, PostgreSQL wait states, observation throughput and Beacon's
timeout/drop logs. Don't turn on SQL text logging or run full-table counts for this; the
recorder itself makes no database queries.

Review quiet, burst and maintenance captures separately: maintenance samples bias an
aggregate. Thirty-second windows can miss brief stalls, so a missing stack isn't proof it
never runs.

When the deadline passes, look for `CPU profiling stopped` in the log, export the files, and
remove the variables and mount at the next deployment.
