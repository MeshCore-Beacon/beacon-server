# MeshCore Beacon

MeshCore Beacon is a MeshCore network observation backend. It connects to one or
more MeshCore MQTT brokers, ingests LoRa packet traffic in real time, stores it
in PostgreSQL, and streams live events to WebSocket clients.

[![CI](https://github.com/MeshCore-Beacon/beacon-server/actions/workflows/ci.yml/badge.svg)](https://github.com/MeshCore-Beacon/beacon-server/actions/workflows/ci.yml)
[![CodeQL](https://github.com/MeshCore-Beacon/beacon-server/actions/workflows/codeql.yml/badge.svg)](https://github.com/MeshCore-Beacon/beacon-server/actions/workflows/codeql.yml)
![Coverage](https://img.shields.io/endpoint?url=https://gist.githubusercontent.com/446564/3e707bdf3f06ecb4575166ce598051c3/raw/beacon-coverage.json)
[![Docker](https://github.com/MeshCore-Beacon/beacon-server/actions/workflows/docker-publish.yml/badge.svg)](https://github.com/MeshCore-Beacon/beacon-server/actions/workflows/docker-publish.yml)

## What it does

- Subscribes to MeshCore MQTT brokers and decodes incoming LoRa packets using
  [meshcore-go](https://github.com/meshcore-go/meshcore-go)
- Stores packets, observations, nodes, observers, traces, routes and channel
  messages in PostgreSQL (more backends to come)
- Deduplicates observations across multiple brokers (same packet heard by two
  brokers is one observation per observer)
- Decrypts group text messages for known channel keys
- Detects firmware capability flags from path hash sizes
- Streams live events to WebSocket clients with subscription filtering by IATA,
  region, payload type, and event type
- Serves a REST API for querying stored data
- Seeds regions, IATA display names, and channel keys from a YAML config file on
  startup

For deployment instructions including the frontend app, see the deployment docs.

---

## Stack

| Component     | Technology                                                      |
| ------------- | --------------------------------------------------------------- |
| Language      | Go 1.26                                                         |
| Router        | [Chi v5](https://github.com/go-chi/chi)                         |
| Database      | PostgreSQL 16                                                   |
| DB queries    | [sqlc](https://sqlc.dev) + pgx/v5                               |
| MQTT          | [paho.mqtt.golang](https://github.com/eclipse/paho.mqtt.golang) |
| WebSocket     | [coder/websocket](https://github.com/coder/websocket)           |
| Packet decode | [meshcore-go](https://github.com/meshcore-go/meshcore-go)       |
| Config        | YAML via gopkg.in/yaml.v3                                       |
| Env           | godotenv                                                        |

---

## Getting started

### Prerequisites

- Go 1.26+
- Docker and Docker Compose

### 1. Clone and configure

```bash
git clone https://github.com/MeshCore-Beacon/beacon-server.git
cd beacon-server
cp env.example .env
cp config.yaml.example config.yaml
```

Edit `.env` with your broker credentials and database DSN. Edit `config.yaml` to
define your regions, IATA display names, channel keys, and retention settings.

### 2. Start PostgreSQL

```bash
docker compose up postgres -d
```

The schema in `db/migrations/001_schema.sql` is applied automatically on first
start via `docker-entrypoint-initdb.d`.

### 3. Run

```bash
go run ./cmd/beacon
```

Or pull and run the Docker image:

```bash
docker pull ghcr.io/meshcore-beacon/beacon-server:latest
```

Beacon will:

- Load `.env` and `config.yaml`
- Connect to PostgreSQL and seed config data
- Connect to the configured MQTT brokers
- Start the HTTP server on `LISTEN_ADDR` (default `:8080`)

### Cold start and path resolution

Path resolution, firmware capability detection, and known route storage all
depend on nodes having advertised at least once to a local observer. On a fresh
deployment `resolvedPath` will show `"confidence": "none"` for all hops and
`supportsMultibytePaths` will be `false` for all nodes until advert traffic
arrives and populates `node_short_ids`. This is expected behaviour — resolution
improves automatically as the mesh is observed over time.

---

## Configuration

### Environment variables (`.env`)

| Variable                 | Default       | Description                                                 |
| ------------------------ | ------------- | ----------------------------------------------------------- |
| `LISTEN_ADDR`            | `:8080`       | HTTP listen address                                         |
| `POSTGRES_DSN`           | —             | PostgreSQL connection string                                |
| `CONFIG_PATH`            | `config.yaml` | Path to YAML config file                                    |
| `MQTT_BROKER_1_URL`      | —             | Broker 1 WebSocket URL (e.g. `wss://mqtt1.example.com:443`) |
| `MQTT_BROKER_1_USERNAME` | —             | Broker 1 username                                           |
| `MQTT_BROKER_1_PASSWORD` | —             | Broker 1 password                                           |
| `MQTT_BROKER_2_URL`      | —             | Broker 2 WebSocket URL                                      |
| `MQTT_BROKER_2_USERNAME` | —             | Broker 2 username                                           |
| `MQTT_BROKER_2_PASSWORD` | —             | Broker 2 password                                           |

### Config file (`config.yaml`)

```yaml
# Optional IATA overrides — auto-created on first packet arrival,
# only needed if you want to customise display name or coordinates.
iatas:
  YVR:
    name: Vancouver International
    lat: 49.1967
    lng: -123.1815

# Super-regions grouping multiple IATAs.
regions:
  - slug: western-canada
    name: Western Canada
    display_order: 1
    center_lat: 51.0
    center_lng: -114.0
    zoom_level: 5
    iatas: [YVR, YYJ, YYC, YEG]

# Channel keys for decrypting group messages.
channel_keys:
  # Hashtag channels: Beacon derives the PSK from the tag name automatically.
  # secret = SHA256("#tag")[:16], channel_hash = SHA256(secret)[0]
  # Tag names should be provided without the # prefix.
  hashtags:
    - meshcore

  # Explicit keys: channel hash (hex) and key (hex), with optional display name.
  # The public MeshCore channel key is included in config.yaml.example.
  keys:
    "11":
      key: "8b3387e9c5cdea6ac9e5edbaa115cd72"
      name: "Public"

# Regional transport scopes for matching TRANSPORT_FLOOD packets.
# Plain names have # prepended automatically (e.g. "bc" → "#bc").
scopes:
  - name: bc
  - name: "#west"

# Observer telemetry storage settings.
telemetry:
  retention: 672h # how long to keep telemetry snapshots (default: 4 weeks)
  resolution: 1h # snapshot frequency per observer; duplicates within window are dropped (default: 1h)

# Packet and observation retention.
packets:
  retention: 720h # how long to keep packets and observations (default: 30 days)

# WebSocket settings.
websocket:
  max_connections_per_ip: 5 # default: 5

# Geographic ingest filter (optional).
# Drop packets from observers outside the specified area.
# Country codes are ISO 3166-1 alpha-2. Continent codes: AF AN AS EU NA OC SA.
# If both are set an IATA passes if it matches either (OR semantics).
# Omit entirely to accept all IATAs (default).
ingest:
  allow_countries: [CA, US] # only store packets from these countries
  allow_continents: [NA] # or: accept all of North America
```

IATAs are auto-created on first packet arrival. The config file adds display
names and coordinates. Regions and channel keys must be defined here — they are
not auto-created.

---

## Authentication

API authentication is not yet implemented. Beacon is intended for trusted
internal network or reverse-proxy deployments. Do not expose it directly to the
public internet without an authentication layer in front of it.

---

## WebSocket API

Connect to `ws://host:8080/ws`.

On connect the server sends a `hello`:

```json
{ "v": 1, "type": "hello", "serverTime": 1234567890000, "connectionId": "uuid" }
```

The connection closes after 90 seconds of inactivity. Clients should send a
`ping` every 30 seconds.

### Client → Server messages

**Subscribe** — add a filter to this connection. Multiple subscriptions are
unioned (OR semantics): an event matches if it satisfies any active
subscription. The server replies with a `subscriptionId` to use for
unsubscribing.

```json
{
  "v": 1,
  "type": "subscribe",
  "id": "sub-1",
  "scope": {
    "iatas": ["YOW", "YYZ"],
    "regionIds": ["1"],
    "regionSlugs": ["western-canada"],
    "payloadTypes": [4, 5],
    "channelHashes": ["11"],
    "events": ["packetObservation", "channelMessage"]
  }
}
```

All scope fields are optional. Omitted means no filter on that dimension (match
everything). Empty array means match nothing on that dimension. `regionIds` and
`regionSlugs` are both expanded to their member IATAs server-side.

**Unsubscribe** — remove a specific subscription by ID.

```json
{
  "v": 1,
  "type": "unsubscribe",
  "id": "unsub-1",
  "subscriptionId": "<uuid from subscribed reply>"
}
```

**Ping**

```json
{ "v": 1, "type": "ping", "id": "ping-1" }
```

### Server → Client events

| Type                | Description                                         |
| ------------------- | --------------------------------------------------- |
| `packetObservation` | New observation written to DB                       |
| `observerStatus`    | Observer status update                              |
| `nodeUpdate`        | Node upserted from advert                           |
| `channelMessage`    | Decrypted channel message (scope must include hash) |

### Backpressure

The server write buffer per connection is bounded at 256 events. If a client
falls behind, the server drops the oldest queued events and sends a `lagged`
notice:

```json
{ "v": 1, "type": "lagged", "droppedCount": 12, "since": 1234567890000 }
```

Clients should respond by re-fetching the relevant REST endpoint using `afterId`
to backfill missed events, then resume streaming.

### Reconnection

Subscriptions are not persisted — they exist only for the lifetime of the
connection. On any disconnect the client should reconnect with backoff, re-issue
all subscriptions, and backfill via REST using
`afterId=<last seen observation id>`.

### Connection limits

By default a maximum of 5 concurrent WebSocket connections are allowed per IP
address. Connections beyond this limit receive `HTTP 429`. The limit is
configurable via `websocket.max_connections_per_ip` in `config.yaml`.

---

## REST API

Base path: `/api/v1`

All list endpoints support cursor-based pagination via `cursor` and `limit`
query params. See the Swagger UI at `http://localhost:8080/swagger/index.html`
for full parameter documentation.

### Authentication

Not yet implemented — see the Authentication section above.

### Endpoints

| Method | Path                                | Description                                                                                        |
| ------ | ----------------------------------- | -------------------------------------------------------------------------------------------------- |
| `GET`  | `/brokers`                          | List MQTT brokers and connection status                                                            |
| `GET`  | `/channels`                         | List channels (optional: `?hash=<hex>&iata=<code>&limit=50`)                                       |
| `GET`  | `/channels/{id}`                    | Get channel detail by integer ID                                                                   |
| `GET`  | `/channels/{id}/messages`           | List messages for a channel (optional: `?since=<ms>&iata=<code>&limit=50`)                         |
| `GET`  | `/iatas`                            | List all known IATA codes                                                                          |
| `GET`  | `/iatas/{iata}`                     | Get a single IATA code                                                                             |
| `GET`  | `/messages`                         | List all messages (optional: `?channelId=<int>&channelHash=<hex>&iata=<code>&since=<ms>&limit=50`) |
| `GET`  | `/messages/backfill`                | Backfill messages after a given message ID                                                         |
| `GET`  | `/nodes`                            | List nodes                                                                                         |
| `GET`  | `/nodes/{nodeId}`                   | Get node detail                                                                                    |
| `GET`  | `/nodes/{nodeId}/neighbors`         | List neighboring nodes observed in the mesh                                                        |
| `GET`  | `/nodes/{nodeId}/observations`      | List observations for a node                                                                       |
| `GET`  | `/observers`                        | List observers (optional: `?iata=<code>&type=<str>&broker=<name>&status=online\|offline`)          |
| `GET`  | `/observers/{observerId}`           | Get observer detail including broker last-seen timestamps                                          |
| `GET`  | `/observers/{observerId}/adverts`   | Adverts heard by observer                                                                          |
| `GET`  | `/observers/{observerId}/telemetry` | Observer telemetry history (optional: `?range=24h&interval=1h\|6h\|24h`)                           |
| `GET`  | `/packets`                          | List packets with filters                                                                          |
| `GET`  | `/packets/backfill`                 | Backfill packets after a given observation ID                                                      |
| `GET`  | `/packets/{packetHash}`             | Get packet with all observations                                                                   |
| `GET`  | `/regions`                          | List all regions (summary)                                                                         |
| `GET`  | `/regions/{id}`                     | Get a single region with IATA list                                                                 |
| `GET`  | `/routes`                           | List known routes (all hops high confidence)                                                       |
| `GET`  | `/routes/search`                    | Search routes by source and destination hash                                                       |
| `GET`  | `/routes/cross`                     | Search for routes crossing IATA boundaries                                                         |
| `GET`  | `/scopes`                           | List transport scopes                                                                              |
| `GET`  | `/scopes/{name}`                    | Get scope detail                                                                                   |
| `GET`  | `/stats/observations`               | Hourly observation time series (last 7 days by default)                                            |
| `GET`  | `/stats/overview`                   | Network overview stats                                                                             |
| `GET`  | `/stats/payload-breakdown`          | Observation counts by payload type (last 24h by default)                                           |
| `GET`  | `/stats/scopes`                     | Configured region scopes and breakdown of packets, nodes, observers                                |
| `GET`  | `/stats/top-nodes`                  | Top N nodes by observation count (from materialized view)                                          |
| `GET`  | `/stats/top-observers`              | Top N observers by observation count (last 24h by default)                                         |
| `GET`  | `/traces`                           | List trace tags with filters                                                                       |
| `GET`  | `/traces/{tag}`                     | Get full trace detail with resolved routes                                                         |

---

## Road Map

- [ ] Redis caching for stats endpoints
- [ ] Caddy reverse proxy config for production
- [ ] Admin authentication middleware
- [ ] Server management via API (currently config-file only)
- [ ] Log levels, debug and info

---

## Acknowledgements

See [CONTRIBUTORS.md](CONTRIBUTORS.md) for the people who have helped build
Beacon.

Beacon stands on the shoulders of giants. See [SHOULDERS.md](SHOULDERS.md) for
the full list of open source projects that make this possible.
