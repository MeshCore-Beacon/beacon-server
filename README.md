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
