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

- Go 1.23+
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
define your regions, IATA display names, and channel keys.

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
  "11": "8b3387e9c5cdea6ac9e5edbaa115cd72" # hash hex: key hex
```

IATAs are auto-created on first packet arrival. The config file adds display
names and coordinates. Regions and channel keys must be defined here — they are
not auto-created.

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

| Method | Path                      | Description                                                                            |
| ------ | ------------------------- | -------------------------------------------------------------------------------------- |
| `GET`  | `/iatas`                  | List all known IATA codes                                                              |
| `GET`  | `/iatas/{iata}`           | Get a single IATA code                                                                 |
| `GET`  | `/regions`                | List all regions (summary)                                                             |
| `GET`  | `/regions/{id}`           | Get a single region with IATA list                                                     |
| `GET`  | `/channels`               | List channels (optional: `?hash=<hex>&since=<ms>&limit=50`)                            |
| `GET`  | `/channels/{id}`          | Get channel detail by integer ID                                                       |
| `GET`  | `/channels/{id}/messages` | List messages for a channel                                                            |
| `GET`  | `/messages`               | List all messages (optional: `?channelId=<int>&channelHash=<hex>&since=<ms>&limit=50`) |

### Stubbed (501 Not Implemented)

| Method | Path                                | Description                        |
| ------ | ----------------------------------- | ---------------------------------- |
| `GET`  | `/packets`                          | List packets with filters          |
| `GET`  | `/packets/{packetHash}`             | Get packet with all observations   |
| `GET`  | `/nodes`                            | List nodes                         |
| `GET`  | `/nodes/{nodeId}`                   | Get node detail                    |
| `GET`  | `/nodes/{nodeId}/observations`      | List observations for a node       |
| `GET`  | `/observers`                        | List observers                     |
| `GET`  | `/observers/{observerId}`           | Get observer detail                |
| `GET`  | `/observers/{observerId}/telemetry` | Observer telemetry history         |
| `GET`  | `/observers/{observerId}/adverts`   | Adverts heard by observer          |
| `GET`  | `/stats/overview`                   | Network overview stats             |
| `GET`  | `/stats/observations`               | Observation time series            |
| `GET`  | `/stats/payloadBreakdown`           | Observations by payload type       |
| `GET`  | `/stats/topNodes`                   | Top nodes by observation count     |
| `GET`  | `/stats/topObservers`               | Top observers by observation count |

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
- [x] DB seeding on startup
- [x] REST API: IATAs, Regions

### In progress / next

- [ ] REST API: Packets, Nodes, Observers, Channels, Stats
- [ ] Path resolution (node short ID lookup)
- [ ] Propagation time calculation
- [x] Observer radio settings on observations
- [ ] Routes and traces endpoints
- [ ] Packet search endpoint (requirements TBD)

### Future

- [ ] Redis caching for stats endpoints
- [ ] Admin authentication middleware
- [ ] Channel key rotation / multi-key support (scaffolded)
- [ ] Caddy reverse proxy config for production
- [ ] Region management via API (currently config-file only)
- [ ] WebSocket subscription unsubscribe (scaffolded, not implemented)
- [ ] Observer owner tracking (schema exists, API excluded by design)
