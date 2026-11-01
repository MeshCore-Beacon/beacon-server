# MeshCore Beacon

MeshCore Beacon is a MeshCore network observation backend. It connects to one or
more MeshCore MQTT brokers, ingests LoRa packet traffic in real time, stores it
in PostgreSQL, and streams live events to WebSocket clients.

[![CI](https://github.com/MeshCore-Beacon/beacon-server/actions/workflows/ci.yml/badge.svg)](https://github.com/MeshCore-Beacon/beacon-server/actions/workflows/ci.yml)
[![CodeQL](https://github.com/MeshCore-Beacon/beacon-server/actions/workflows/codeql.yml/badge.svg?branch=main)](https://github.com/MeshCore-Beacon/beacon-server/actions/workflows/codeql.yml)
![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/446564/3e707bdf3f06ecb4575166ce598051c3/raw/beacon-coverage.json)
[![Docker](https://github.com/MeshCore-Beacon/beacon-server/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/MeshCore-Beacon/beacon-server/actions/workflows/docker-publish.yml)

## What it does

- Subscribes to MeshCore MQTT brokers and decodes incoming LoRa packets using
  [meshcore-go](https://github.com/meshcore-go/meshcore-go)
- Stores packets, observations, nodes, observers, traces, routes and channel
  messages in PostgreSQL
- Deduplicates observations across multiple brokers (the same packet heard by two
  brokers is one observation per observer)
- Decrypts group text messages for known channel keys
- Detects firmware capability flags from path hash sizes
- Streams live events to WebSocket clients with subscription filtering by IATA,
  region, payload type, and event type
- Serves a REST API for querying stored data
- Seeds regions, IATA display names, and channel keys from a YAML config file on
  startup

This repo is the code. Deploying, configuring and operating Beacon is documented in
[beacon-docs](https://github.com/MeshCore-Beacon/beacon-docs); see
[Documentation](#documentation) below.

## Stack

| Component     | Technology                                                      |
| ------------- | --------------------------------------------------------------- |
| Language      | Go 1.26                                                         |
| Router        | [Chi v5](https://github.com/go-chi/chi)                         |
| Database      | PostgreSQL 16                                                   |
| Caching       | Redis 7                                                         |
| DB queries    | [sqlc](https://sqlc.dev) + pgx/v5                               |
| MQTT          | [paho.mqtt.golang](https://github.com/eclipse/paho.mqtt.golang) |
| WebSocket     | [coder/websocket](https://github.com/coder/websocket)           |
| Packet decode | [meshcore-go](https://github.com/meshcore-go/meshcore-go)       |
| Config        | YAML via gopkg.in/yaml.v3                                       |
| Env           | godotenv                                                        |

## Running it locally

You need Go 1.26+ and a PostgreSQL 16 database. Docker is the easy way to get the database.

```bash
git clone https://github.com/MeshCore-Beacon/beacon-server.git && cd beacon-server
cp env.example .env
cp config.yaml.example config.yaml
docker run -d --name beacon-postgres -p 5432:5432 \
  -e POSTGRES_USER=beacon -e POSTGRES_PASSWORD=beacon -e POSTGRES_DB=beacon postgres:16-alpine
go run ./cmd/beacon
```

Fill in `POSTGRES_DSN` and your MQTT broker credentials in `.env`. Every variable is described
in [Configuration](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/docs/configuration.md),
and the fully annotated `config.yaml` lives in beacon-docs as well; the copy here is a working
starter. Migrations run on startup. The API listens on `LISTEN_ADDR` (default `:8080`) and
Swagger is at `http://localhost:8080/swagger/index.html`.

### MeshMapper authentication

MeshMapper APIs require either a regional API key or a grouped-region API key
covering multiple regions. Your local MeshMapper regional or grouped-region admin
can generate these keys.

Set `MESHMAPPER_API_KEY` in each deployment's private environment or
gitignored `.env`; it overrides `meshmapper.api_key` in `config.yaml`, including
when empty. The examples leave the key empty. Never put it in browser settings
or use the mobile app's App key. Restart Beacon after changing the key.

A regional deployment needs access to its IATA; a group deployment needs access
to every member IATA, including members added by `import_groups`. API keys do not
remove rate limits or change the `get_zones.php?country=...` request shape.
IP exemptions are not authentication.

Beacon sends `X-API-Key` to `get_zones.php`, `get_geojson.php`, `get_scopes.php`,
`get_channels.php`, and `get_repeaters.php`.
Requests are restricted to those HTTPS MeshMapper endpoints and never follow
redirects. Coverage consumers keep their separate keys, scopes, quotas, and
supported `?key=` authentication; this client does not call `coverage.php`.

Missing keys are reported as unconfigured without stopping Beacon. Repeater
profiles are bound to the credential and selected import areas: changing either
discards the old profiles and ETags while preserving request cooldowns. A 401/403
also removes that source's profiles. Temporary failures retain the last successful
profiles with a failed-refresh indicator. Other imports retain their existing cache
policy. Inspect the `meshmapper.zones`, `meshmapper.scopes`,
`meshmapper.channels`, and `meshmapper.repeaters` logs for freshness and the next attempt. No anonymous
fallback occurs. Existing refresh limits still apply, including longer
`Retry-After` delays on 429/503.

Use the environment setting when backups are enabled. Backup exports preserve
saved YAML verbatim and refuse a nonempty `meshmapper.api_key` in that file so
the credential cannot enter a backup download. Clear that YAML value after
moving the key to the environment; environment secrets are excluded from backups.

Deployment coordination is required with MeshMapper Server before enforcement:
confirm `X-API-Key` support, obtain and verify the integration key's API and IATA
permissions, and confirm these requests do not consume Coverage quota. Header
support must not be assumed deployed. The first four endpoints can enforce
authentication before the app rollout; repeater enforcement waits for app 1.4.1
and its forced update. Key issuance, permissions, quotas, and enforcement switches
remain MeshMapper Server work, tracked in
[MeshMapper_Server#431](https://github.com/MeshMapper/MeshMapper_Server/issues/431).

### Repeater profiles in node details

Ask the regional or group MeshMapper administrator for a dedicated **integration
key** assigned to the IATAs your Beacon serves. In MeshMapper's admin panel,
**User Settings → API Access** creates a key with a project description; the
administrator should grant the region-directory and repeater read endpoints.
See the current [key instructions](https://wiki.meshmapper.net/api-keys/) and
[repeater API contract](https://wiki.meshmapper.net/repeaters-api/). Group exports
are still filtered by the integration key's authorized members. Beacon cannot
grant or widen those permissions.

Put the credential in the private `MESHMAPPER_API_KEY` environment setting, then
enable this optional import in the server configuration:

```yaml
meshmapper:
  repeaters:
    enabled: true
    refresh_interval: 6h
    iatas: [YKF] # optional: omit to discover all known, key-authorized IATAs
```

The IATA list limits both source discovery and imported profile rows, including
group responses. It is independent of packet ingestion geography. No browser
request calls MeshMapper, and no RF polling is added. Opening a node reads the
cached profile by its **full public key**; legacy prefixes are never guessed.
Changing access can leave profiles unavailable until existing cooldowns expire.
The initial import needs the directory and repeater fetches to complete.

`GET /nodes/{id}` includes optional `meshmapper` data for repeaters when available:
owner/admin-entered hardware, power, antenna, heights, power source and public
notes; administrator display names; separately sourced scope evidence; provider
status, backbone traffic share, advert width, radio presets and an uploaded
neighbour table. `fetchedAt` is when Beacon fetched the list, not when the owner
edited it. `refreshFailed` marks a failed subsequent refresh; absent fields remain
unknown. The provider link points back to the cached record's region/group map.
Some regions return `created_at` as a timezone-free SQL date string instead of
documented epoch seconds. Beacon keeps the rest of that profile and omits the
ambiguous registration instant. Incompatible optional metadata cannot discard
valid site details; a nonempty response with no valid identities is a failed
refresh, not an authoritative empty region.

These fields do not overwrite Beacon's name/location, radio/clock measurements,
scope repeat counters, topology, or telemetry. Uploaded neighbour entries are
displayed as evidence only, including unresolved prefixes; they never create
Beacon routes. Provider traffic share is not Beacon traffic share. Missing API
fields, including coverage/range plots, are not fabricated.

Schedule all enabled imports within the key's shared read budget (currently a
default 1,000 reads/day and 30/minute; administrators can assign different limits).
Conditional requests also count. Respect `Retry-After` on 429/503; repeater imports
pause all their sources when a shared-budget response specifies a retry time.
Keep credentials and their private cache-binding fingerprints out of public
diagnostics, screenshots, source and client bundles.

To run the web frontend against it, see
[Running the full stack locally](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/CONTRIBUTING.md#running-the-full-stack-locally).

The published image is `ghcr.io/meshcore-beacon/beacon-server`. `latest` tracks stable
releases, `dev` follows the development branch, and each release is also tagged `X.Y.Z` and
`X.Y`.

### What you see on an empty database

Path resolution, capability detection and known routes depend on nodes having advertised to a
local observer. On a fresh database every hop shows `"confidence": "none"` and
`supportsMultibytePaths` is `false` until adverts populate `node_short_ids`. That is expected
and fills in as the mesh is observed.

GRP_TXT packets whose channel key is not known yet are stored hash-only. After adding the key to
`config.yaml`, the next restart decrypts the matching history; look for
`config: backfilled N previously-undecrypted channel message(s)` in the log.

## Documentation

In [beacon-docs](https://github.com/MeshCore-Beacon/beacon-docs):

- [Deploy with Docker](https://github.com/MeshCore-Beacon/beacon-docs#deploy-the-all-in-one-stack)
- [Getting packets in](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/docs/getting-packets-in.md): brokers, the subscriber account, topics
- [Configuration](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/docs/configuration.md): environment variables and `config.yaml`
- [API contract](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/docs/api-contract.md): REST, WebSocket, admin endpoints
- [Reverse proxy and rate limits](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/docs/reverse-proxy.md)
- [Upgrading](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/docs/upgrading.md)
- [Operations](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/docs/operations.md), [backup and export](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/docs/backup-export.md) and [CPU profiling](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/docs/profiling.md)
- [High level design](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/docs/high-level-design.md)
- [Releases and versioning](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/docs/releases.md)

In this repo: [Historical stats](docs/historical-stats.md) explains the hourly rollups behind
the stats endpoints, for anyone changing them. `docs/swagger.yaml` is the generated OpenAPI
description.

## Contributing

[CONTRIBUTING.md](CONTRIBUTING.md) covers code style, tests, and database and API changes.
Branches, commits and the release flow are shared across the Beacon repos and live in
[beacon-docs/CONTRIBUTING.md](https://github.com/MeshCore-Beacon/beacon-docs/blob/main/CONTRIBUTING.md).
Please read the [Code of Conduct](CODE_OF_CONDUCT.md). Security reports go through
[SECURITY.md](SECURITY.md).

## Acknowledgements

See [CONTRIBUTORS.md](CONTRIBUTORS.md) for the people who have helped build Beacon, and
[SHOULDERS.md](SHOULDERS.md) for the open source projects it stands on.
