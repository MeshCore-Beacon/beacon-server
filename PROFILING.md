# Production CPU profiling

CPU profiling is supported on Linux and macOS and is disabled by default. It writes private files inside the container;
there is no HTTP endpoint or published profiling port. Profiling failures stop the
recorder and log a warning without stopping ingest.

## Enable a bounded session

Create a dedicated directory writable by the container user with mode `0700`.
Mount it into the app container and set both environment variables:

```yaml
services:
  app:
    environment:
      BEACON_CPU_PROFILE_DIR: /profiles
      BEACON_CPU_PROFILE_UNTIL: "2026-10-02T12:00:00Z" # Replace with your fixed deadline.
    volumes:
      - ./profiles:/profiles
```

Choose an RFC3339 deadline no more than 72 hours in the future. The same deadline
remains in effect after restarts; expired settings are inactive. Do not generate a
fresh deadline automatically on each startup. Restarting the app is required to
change these settings. Preserve the rest of the deployment configuration.

Start with a deadline about one minute away to assess the first capture's overhead.
Check process CPU, packet freshness, queue drops and database timeouts against a
comparable period with profiling disabled. After reviewing that capture, an operator
can enable a longer session. Profiling adds overhead while a capture is active.

## Capture behavior

- One 30-second sample immediately, then every 30 minutes, offset five minutes past
  the half hour so a maintenance trigger due on the hour is not lost to the cooldown.
- Route reconfirmation requests an additional sample when the maintenance task starts.
  This includes the retention step before route validation. A five-minute cooldown
  between capture starts prevents overlap and repeated triggers from increasing load;
  a periodic sample due during the cooldown runs when it ends. A triggered sample
  during the cooldown is skipped, and any sample cancels a periodic one waiting on
  the cooldown.
- Background task stacks carry a `task` label while profiling is enabled.
- Shutdown or expiry stops the active sample and saves the shorter profile.
- Each profile is limited to 8 MiB. The dedicated directory is limited to 256 MiB
  and 512 files, including metadata and files left by interrupted runs. The recorder
  reserves space for a full capture before starting and stops when a limit is reached.
  Files are never automatically deleted. Existing unrelated regular files consume the budget; subdirectories are ignored.
- Profiles and metadata are written with mode `0600`. A `.partial` file indicates
  an interrupted capture and is not a completed profile.

Only one Beacon process should write to a profiling directory. Keep the mount
private and out of web roots, backups intended for public download, and source control.
Do not run another CPU profiler in the same process during a session.

## Interpret the output

Each `.pprof` has a `.pprof.json` sidecar containing UTC start/end times, the trigger,
Go version, process CPU counters (Linux/macOS), goroutine count, and database-pool
counters before and after the capture. Pool acquire duration and counts are cumulative;
use differences between the two snapshots. They do not contain SQL text or credentials.

Copy completed files privately off the host. Record the container's image revision
and digest alongside them. On a workstation with Go installed:

```sh
go tool pprof -top capture.pprof
go tool pprof -top -cum capture.pprof
go tool pprof -tags capture.pprof
```

CPU samples show work inside Beacon, not CPU used by PostgreSQL or time waiting on
locks. Correlate each UTC interval with separately collected host/container CPU and
I/O, PostgreSQL wait states, observation throughput, and Beacon timeout/drop logs.
Do not enable SQL text logging or run full-table counts for this purpose. The recorder
makes no diagnostic database queries and does not change the ingest path.

Review captures from quiet periods, bursts, and maintenance separately before
combining them. Additional maintenance samples deliberately bias an aggregate profile.
Thirty-second windows can miss brief stalls; the absence of a stack is not proof that
it never consumes CPU.

At the deadline, check for `CPU profiling stopped` in the app log. Export the files,
then remove the environment variables and mount during the next planned deployment.
