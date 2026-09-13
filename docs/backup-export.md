# Database and saved-config export

`beacon-backup` is the export foundation for issue #72. It produces a private,
versioned `.tar.gz` using PostgreSQL's `pg_dump`. It does not yet provide a web
interface, account login, scheduled/remote storage or automatic import.

Build it with `go build ./cmd/beacon-backup`. Install `pg_dump` in the same runtime
as this command; an installation on the Docker host does not install it inside an
app container. Use a client of the same major version as the source PostgreSQL
server; an older client cannot dump a newer server.

Set libpq's standard `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER` and TLS settings.
`PGDATABASE` is required and must be a plain database name. Prefer a private
`PGPASSFILE` (0600 on Unix) or an existing libpq service configuration for secrets.
The command does not load `.env`, read `POSTGRES_DSN`, start Beacon, run migrations,
subscribe to MQTT or connect to Redis. Connection settings are not command-line
arguments and client stderr is not printed because it can contain private data.

For example, after configuring those connection settings:

```sh
beacon-backup -config /private/config.yaml -output /private/beacon-20260913.tar.gz
```

Use an output directory controlled by the operator. Staging directories are
0700 and files are 0600 on Unix; Windows operators must use a directory with
appropriately restricted ACLs. Existing destinations, including symlinks, are
never replaced. A hard link publishes the finished archive atomically, so the
destination filesystem must support hard links. Unsupported filesystems fail
without publishing an output. Normal failure, timeout and handled interruption
remove temporary files; a power loss or SIGKILL can leave a private
`.beacon-backup-*` staging directory for the operator to inspect.

## Format 1

The tar contains exactly three regular files with fixed names:

- `manifest.json`: format version, creation time, exporter version, dump format,
  uncompressed payload sizes/SHA-256 hashes and explicit exclusions.
- `database.sql`: one consistent `pg_dump` snapshot of schema and data, including
  Beacon's migration journal. Ownership, ACLs and tablespace placement are omitted
  so objects can be restored under the destination operator.
- `config.yaml`: the supplied saved file, byte-for-byte, including comments and
  any keys. It is captured before the database snapshot; avoid configuration edits
  during export if the files must describe the same deployment state.

This is sensitive, unencrypted data. Store and transfer it privately. The bundle
does not include deployment environment variables, `.env`, external files such as
`borderFile` inputs or TLS keys, runtime-only changes, PostgreSQL roles/cluster
settings, Redis or service/deployment files. Retain those separately. A config
export is not the sanitized admin-config response and is not a complete server
recovery package by itself.

Defaults are ten minutes and a 1 GiB uncompressed SQL limit. Saved YAML is capped
at 1 MiB. `-timeout` and `-max-bytes` set finite positive limits (SQL maximum 1 TiB).
Allow disk space for both the uncompressed SQL and compressed bundle, roughly
twice the chosen SQL limit plus overhead. A five-second lock-wait limit prevents
waiting indefinitely behind schema changes. Export failure publishes no backup;
check the client version, privileges, connection settings and available capacity
privately. The command deliberately does not expose raw client diagnostics.

## Restore verification

Use trusted bundles only: PostgreSQL dumps can contain executable SQL. Inspect
the fixed members, verify the gzip stream and the manifest's payload sizes and
hashes, and extract into private staging. For a **new, empty disposable database**,
with its own explicit `PGDATABASE` and target-role connection settings:

```sh
psql -X --set ON_ERROR_STOP=on --single-transaction --file database.sql
```

Restore with a compatible PostgreSQL version and the required extensions already
available. Reconcile the migration journal and representative records/relationships
before relying on the bundle. Review saved configuration and restore external
secrets/files separately before starting a new Beacon server. Never restore over
live data simply to test an export; overwrite/import requires a separate workflow.

PostgreSQL references: [pg_dump](https://www.postgresql.org/docs/16/app-pgdump.html),
[connection environment](https://www.postgresql.org/docs/16/libpq-envars.html),
[password files](https://www.postgresql.org/docs/16/libpq-pgpass.html).

CI runs the compiled command and its PostgreSQL round-trip test with a dedicated
PostgreSQL 16 service. To repeat it privately, build the command, set the `PG*`
connection variables for an isolated test server and set
`BEACON_BACKUP_TEST_POSTGRES=1` plus `BEACON_BACKUP_TEST_BINARY` to the command's
absolute path. Run `go test ./internal/backup -run '^TestExportPostgres$' -v`.
The test role needs permission to create/drop its two randomly named databases;
the test migrates and restores only those databases and removes them afterward.
