# Backup and export

Beacon exports its PostgreSQL database and saved `config.yaml` as a private, versioned
`.tar.gz`, either with the standalone `beacon-backup` command or an opt-in admin download.
`beacon-backup -verify` checks an archive offline. There is no import, scheduling or remote
storage.

## Archive format 1

Exactly three regular files:

- `manifest.json`: format and exporter version, creation time, payload sizes and SHA-256
  hashes, and the exclusions below.
- `database.sql`: one consistent `pg_dump` snapshot (schema, data, migration journal),
  without ownership, ACLs or tablespaces so it restores under any role.
- `config.yaml`: the saved file byte-for-byte, read just before the snapshot. Avoid config
  edits during an export.

**It is sensitive and unencrypted** (keys, message data); keep it private. It is not a full
recovery package: `.env` and environment variables, `borderFile` and TLS files, runtime-only
changes, PostgreSQL roles and cluster settings, Redis and deployment files are not included.

## Requirements and limits

- `pg_dump` must be in the **same runtime** as the exporter (the CLI's environment, or the
  server's PATH for the download), with a major version matching the database. A client on
  the Docker host or in another container doesn't count.
- The Docker image ships the PostgreSQL 16 client. Its Alpine 3.19 base has no 17/18
  packages, so a newer database needs a different runtime, not just `POSTGRES_CLIENT_MAJOR`.
- The temp directory (`TMPDIR`) needs about twice the SQL limit free. A small tmpfs won't do.
- The output filesystem must support hard links (used to publish atomically). Existing
  destinations, symlinks included, are never replaced. Files are 0600 and staging dirs 0700;
  on Windows, restrict the directory ACLs yourself.

| Limit | Default |
| --- | --- |
| Timeout | 10 min (`-timeout`) |
| Uncompressed SQL | 1 GiB (`-max-bytes`, up to 1 TiB) |
| Saved YAML / manifest | 1 MiB / 64 KiB |
| Compressed archive (verify) | SQL limit + 1% + 2 MiB |
| Lock wait | 5 s |

Failures, timeouts and interrupts clean up and publish nothing. A SIGKILL or power loss can
leave a private staging dir: `.beacon-backup-*` next to the CLI output, or
`beacon-download-*` in the server's temp dir, which Beacon sweeps on its next start.

## CLI export

```sh
go build ./cmd/beacon-backup
beacon-backup -config /private/config.yaml -output /private/beacon-20260913.tar.gz
```

Connection comes only from the standard libpq environment (`PGHOST`, `PGPORT`,
`PGDATABASE`, `PGUSER`, TLS settings); `PGDATABASE` is required. Keep the password in a 0600
`PGPASSFILE` or a service file. The command ignores `.env` and `POSTGRES_DSN`, runs no
migrations, and never prints `pg_dump` stderr since it can leak private data. If it fails,
check the client version, privileges, connection settings and free space.

## Download API

`GET /api/v1/admin/backup` streams a fresh archive. Enable it with `backup.enabled: true` and
the admin bearer key, then restart. Send the key only in the `Authorization` header over
HTTPS; the endpoint takes no query or body. Leave it off on public previews.

At startup Beacon checks `pg_dump --version` against the server. If that or another
prerequisite fails, it logs why and disables only backup; fix it and restart.

**Connection.** The download uses `POSTGRES_DSN`, which must be a single-host `postgres://`
URL with an explicit user and database. TLS options, `passfile`, `connect_timeout`,
`application_name`, `target_session_attrs` and `options` are passed through; `pool_*`
options are dropped; supported `PG*` env defaults apply under the URL. Keyword DSNs,
services, multiple hosts, unknown or duplicate options, protocol-negotiation settings and
the `PGSERVICE`, `PGSERVICEFILE`, `PGSSLNEGOTIATION`, `PG{MIN,MAX}PROTOCOLVERSION` and `PGTZ`
env vars are rejected. Use the CLI for those setups. Prefer `sslmode=verify-full` for remote
databases. Credentials go to a 0600 service file in staging, never into process arguments,
logs or errors.

**Behaviour.** One export at a time. Disconnecting cancels it, and the transfer has a
10-minute write deadline. Responses are `no-store`. A failed export sends no partial archive
and logs a sanitized reason.

| Status | Meaning |
| --- | --- |
| 400 | Query string or body sent |
| 401 | Missing or invalid key |
| 409 | Another export is running |
| 500 | Export failed |
| 503 | Backup unavailable, no admin key, or shutting down |
| 504 | Timed out |
| 507 | SQL size limit exceeded |

## Verify

```sh
beacon-backup -verify /private/beacon-20260913.tar.gz
```

Reads the file without extracting, running SQL or connecting to anything, so it needs no
config, `pg_dump` or credentials. `-max-bytes` and `-timeout` apply. It requires exactly the
three members as 0600 regular files with USTAR headers (size-only PAX allowed for SQL over
8 GiB). It rejects duplicates, links, devices, extra metadata, truncated or concatenated
streams, and unknown or duplicate manifest keys. Sizes and SHA-256 hashes must match.

Exit 0 means structure and checksums passed. Errors don't include payload, manifest values
or the path. The manifest is unsigned, so a pass does **not** prove authenticity, safe SQL or
that the dump restores.

## Restore

Only restore trusted bundles, because a dump can run arbitrary SQL. Verify, extract
privately, and load into a **new, empty database** on a compatible PostgreSQL with the
needed extensions:

```sh
psql -X --set ON_ERROR_STOP=on --single-transaction --file database.sql
```

Check the migration journal and some records, review the config, and restore external
secrets and files before pointing a new Beacon at it. Never test an export by restoring over
live data. See [pg_dump](https://www.postgresql.org/docs/16/app-pgdump.html) and
[libpq environment](https://www.postgresql.org/docs/16/libpq-envars.html).

## Testing

CI runs export, restore round-trip, offline verify and truncated-archive rejection against
PostgreSQL 16. Locally, with `PG*` pointing at a throwaway server whose role can create
databases:

```sh
go build -o "$PWD/beacon-backup" ./cmd/beacon-backup
BEACON_BACKUP_TEST_POSTGRES=1 BEACON_BACKUP_TEST_BINARY="$PWD/beacon-backup" \
  go test ./internal/backup -run '^TestExportPostgres$' -v
```

The test creates, migrates, restores and drops two randomly named databases.
