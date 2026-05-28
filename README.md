# MeshCore Tower

MeshCore Tower is a MeshCore network observation backend. It connects to one or
more MeshCore MQTT brokers, ingests LoRa packet traffic in real time, stores it
in PostgreSQL, and streams live events to WebSocket clients.

## What it does

- Subscribes to MeshCore MQTT brokers and decodes incoming LoRa packets using
  [meshcore-go](https://github.com/meshcore-go/meshcore-go)
- Stores packets, observations, nodes, observers, and channel messages in
  PostgreSQL
- Deduplicates observations across multiple brokers (same packet heard by two
  brokers is one observation per observer)
- Decrypts group text messages for known channel keys
- Detects firmware capability flags from path hash sizes
- Streams live events to WebSocket clients with subscription filtering by IATA,
  payload type, and event type
- Serves a REST API for querying stored data
- Seeds regions, IATA display names, and channel keys from a YAML config file on
  startup

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

## Project layout

```
tower-server/
├── cmd/tower/          entry point
├── db/
│   ├── migrations/     SQL schema (001_schema.sql)
│   ├── queries/        sqlc query definitions
│   ├── sqlc/           generated Go DB code (do not edit)
│   └── store.go        DB interface implementations
├── internal/
│   ├── api/
│   │   ├── handlers/   HTTP route handlers
│   │   ├── middleware/  Auth middleware stub
│   │   ├── router/     Chi router wiring
│   │   ├── helpers.go  Node type name helpers
│   │   └── reader.go   Read-only DB interface + response types
│   ├── config/         Config file loading and DB seeding
│   ├── hub/            WebSocket fan-out broker
│   ├── ingest/         MQTT ingest pipeline
│   ├── keystore/       Channel key store
│   └── ws/             WebSocket handler
├── config.yaml.example
├── env.example
├── docker-compose.yml
└── sqlc.yaml
```

---

## Getting started

### Prerequisites

- Go 1.26+
- Docker and Docker Compose
- [sqlc](https://sqlc.dev) (only needed if modifying queries)

### 1. Clone and configure

```bash
git clone https://github.com/MeshCore-Tower/tower-server.git
cd tower-server
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
go run ./cmd/tower
```

Tower will:

- Load `.env` and `config.yaml`
- Connect to PostgreSQL and seed config data
- Connect to the configured MQTT brokers
- Start the HTTP server on `LISTEN_ADDR` (default `:8080`)

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
iatas:
  YVR:
    name: Vancouver International
    lat: 49.1967
    lng: -123.1815

regions:
  - slug: western-canada
    name: Western Canada
    display_order: 1
    center_lat: 51.0
    center_lng: -114.0
    zoom_level: 5
    iatas: [YVR, YYJ, YYC, YEG]

channel_keys:
  # Hashtag channels: Tower derives the PSK from the tag name automatically.
  # secret = SHA256("#tag")[:16], channel_hash = SHA256(secret)[0]
  # Tag names should be provided without the # prefix.
  hashtags:
    - meshcore

  # Explicit keys: channel hash (hex) and key (hex), with optional display name.
  keys:
    "11":
      key: "8b3387e9c5cdea6ac9e5edbaa115cd72"
      name: "Public"

# Observer telemetry storage settings.
telemetry:
  retention: 672h # how long to keep telemetry snapshots (default: 4 weeks)
  resolution: 1h # snapshot frequency per observer; duplicates within window are dropped (default: 1h)

# Packet and observation retention.
packets:
  retention: 720h # how long to keep packets and observations (default: 30 days)
```

IATAs are auto-created on first packet arrival. The config file adds display
names and coordinates. Regions and channel keys must be defined here — they are
not auto-created.

The public MeshCore channel (hash `0x11`) key is included in
`config.yaml.example` and should be copied to your `config.yaml`.

---

## WebSocket API

Connect to `ws://host:8080/ws`.

On connect the server sends a hello message:

```json
{ "v": 1, "type": "hello", "serverTime": 1234567890000, "connectionId": "uuid" }
```

### Client → Server messages

- **Subscribe**

```json
{
  "v": 1,
  "type": "subscribe",
  "id": "sub-1",
  "scope": {
    "iatas": ["YOW", "YYZ"],
    "payloadTypes": [4, 5],
    "events": ["packetObservation"]
  }
}
```

- **Ping** (send every 30s; connection closes after 90s idle)

```json
{ "v": 1, "type": "ping", "id": "ping-1" }
```

### Server → Client events

| Type                | Description                   |
| ------------------- | ----------------------------- |
| `packetObservation` | New observation written to DB |
| `observerStatus`    | Observer status update        |
| `nodeUpdate`        | Node upserted from advert     |
| `channelMessage`    | Decrypted channel message     |

---

## REST API

Base path: `/api/v1`

### Implemented

| Method | Path                                | Description                                                                                        |
| ------ | ----------------------------------- | -------------------------------------------------------------------------------------------------- |
| `GET`  | `/brokers`                          | List MQTT brokers and connection status                                                            |
| `GET`  | `/iatas`                            | List all known IATA codes                                                                          |
| `GET`  | `/iatas/{iata}`                     | Get a single IATA code                                                                             |
| `GET`  | `/regions`                          | List all regions (summary)                                                                         |
| `GET`  | `/regions/{id}`                     | Get a single region with IATA list                                                                 |
| `GET`  | `/channels`                         | List channels (optional: `?hash=<hex>&iata=<code>&limit=50`)                                       |
| `GET`  | `/channels/{id}`                    | Get channel detail by integer ID                                                                   |
| `GET`  | `/channels/{id}/messages`           | List messages for a channel (optional: `?since=<ms>&iata=<code>&limit=50`)                         |
| `GET`  | `/messages`                         | List all messages (optional: `?channelId=<int>&channelHash=<hex>&iata=<code>&since=<ms>&limit=50`) |
| `GET`  | `/observers`                        | List observers (optional: `?iata=<code>&type=<str>&broker=<name>&status=online\|offline`)          |
| `GET`  | `/observers/{observerId}`           | Get observer detail including broker last-seen timestamps                                          |
| `GET`  | `/packets`                          | List packets with filters                                                                          |
| `GET`  | `/packets/{packetHash}`             | Get packet with all observations                                                                   |
| `GET`  | `/nodes`                            | List nodes                                                                                         |
| `GET`  | `/nodes/{nodeId}`                   | Get node detail                                                                                    |
| `GET`  | `/nodes/{nodeId}/observations`      | List observations for a node                                                                       |
| `GET`  | `/observers/{observerId}/telemetry` | Observer telemetry history                                                                         |
| `GET`  | `/observers/{observerId}/adverts`   | Adverts heard by observer                                                                          |
| `GET`  | `/stats/overview`                   | Network overview stats                                                                             |
| `GET`  | `/stats/observations`               | Hourly observation time series (last 7 days by default)                                            |
| `GET`  | `/stats/payload-breakdown`          | Observation counts by payload type (last 24h by default)                                           |
| `GET`  | `/stats/top-nodes`                  | Top N nodes by observation count (from materialized view)                                          |
| `GET`  | `/stats/top-observers`              | Top N observers by observation count (last 24h by default)                                         |

---

## Development

### Modifying DB queries

Edit `db/queries/queries.sql`, then regenerate:

```bash
sqlc generate
```

### Running with Docker

```bash
docker compose up
```

This starts PostgreSQL, Redis (reserved for future caching), and the MeshCore
Tower server.

---

## API Documentation (Swagger)

Tower uses [swaggo/swag](https://github.com/swaggo/swag) to generate OpenAPI
documentation from annotations in the handler comments.

### Viewing the docs

Start the server and open:

```
http://localhost:8080/swagger/index.html
```

### Regenerating after API changes

After adding or modifying any handler, regenerate the docs:

```bash
swag init -g cmd/tower/main.go -o docs
```

Commit the updated `docs/` directory alongside your handler changes.

### Install swag

```bash
go install github.com/swaggo/swag/cmd/swag@latest
```

### Annotation format

Each handler closure should have a godoc-style annotation block immediately
above the `r.Get()`/`r.Post()` call:

```go
// listThings godoc
//
//	@Summary	Short description shown in the UI
//	@Tags		TagName
//	@Produce	json
//	@Param		paramName	query		string	false	"Description"
//	@Param		id			path		string	true	"Resource ID"
//	@Success	200			{object}	api.MyResponseType
//	@Failure	400			{object}	handlers.APIError
//	@Failure	500			{object}	handlers.APIError
//	@Router		/things [get]
r.Get("/", func(w http.ResponseWriter, r *http.Request) {
```

**Param types:** `query`, `path`, `header`, `body`  
**Required:** use `true` or `false` as the fifth field  
**Pagination params** (`cursor`, `limit`) should always be `false`

For paginated responses use the generic page wrapper:

```go
//	@Success	200	{object}	api.Page[api.MyType]
```

---

## Road Map

### Done

- [x] MQTT ingest pipeline (two brokers, cross-broker dedup)
- [x] Packet decode via meshcore-go
- [x] Observer upsert and status processing
- [x] Node upsert from advert payloads
- [x] Channel message storage with key-based decryption
- [x] Firmware capability detection scaffolding
- [x] Hub-based WebSocket fan-out with subscription filtering
- [x] WebSocket server (hello, subscribe, ping/pong, events)
- [x] Config file loading (regions, IATA overrides, channel keys)
- [x] Observer radio settings on observations
- [x] DB seeding on startup
- [x] Observer telemetry storage with configurable resolution and retention
- [x] Packet retention cleanup goroutine
- [x] Hashtag channel PSK derivation (SHA256("#tag")[:16])
- [x] Channel hash collision handling via key fingerprint
- [x] REST API: IATAs, Regions
- [x] REST API: Channels (list + detail + messages) with IATA filter
- [x] REST API: Messages (cross-channel) with IATA filter
- [x] REST API: Observers (heard adverts, telemetry, list + detail with broker
      last-seen)
- [x] REST API: Brokers (list with connection status)
- [x] REST API: Pagination
- [x] REST API: Nodes (list + detail + observations)
- [x] REST API: Packets (list + detail)
- [x] REST API: Stats
- [x] Materialized view refresh (mv_hourly_iata_stats, mv_top_nodes_by_iata)
- [x] Swagger/OpenAPI documentation via swaggo/swag

### In progress / next

- [ ] Path resolution (node short ID lookup)
- [ ] Propagation time calculation
- [ ] Routes and traces endpoints
- [ ] WebSocket subscription unsubscribe (scaffolded, not implemented)

### Future

- [ ] Redis caching for stats endpoints
- [ ] Admin authentication middleware
- [ ] Channel key rotation / multi-key support (scaffolded)
- [ ] Caddy reverse proxy config for production
- [ ] Region management via API (currently config-file only)
- [ ] Observer owner tracking (schema exists, API excluded by design)
