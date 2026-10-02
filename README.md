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

More documentation:

- [beacon-docs](https://github.com/MeshCore-Beacon/beacon-docs): deployment with the
  frontend and reverse proxy examples
- [Backup and export](docs/backup-export.md): private database and config bundles
- [Historical stats](docs/historical-stats.md): how the hourly rollups work
- [Packet summaries](docs/packet-summaries.md): the `summary` field on packet lists
- [CPU profiling](PROFILING.md): bounded production captures

---

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

Any PostgreSQL 16 database works. For a local throwaway one:

```bash
docker run -d --name beacon-postgres -p 5432:5432 \
  -e POSTGRES_USER=beacon -e POSTGRES_PASSWORD=beacon -e POSTGRES_DB=beacon \
  postgres:16-alpine
```

Database migrations are applied automatically on startup.

### 3. Run

```bash
go run ./cmd/beacon
```

Or pull and run the Docker image:

```bash
docker pull ghcr.io/meshcore-beacon/beacon-server:latest
```

The image is public on GitHub Container Registry — no `docker login` required.
`latest` tracks stable releases; pin a version tag (e.g. `2.0.0`) for
predictable upgrades, and `dev` follows the development branch.

Beacon will:

- Load `.env` and `config.yaml`
- Connect to PostgreSQL and seed config data
- Connect to the configured MQTT brokers
- Start the HTTP server on `LISTEN_ADDR` (default `:8080`)

### Upgrading from 1.x

2.0.0 starts from a new schema baseline and **needs a fresh, empty database**; 1.x history
does not carry over. Pointed at a 1.x database it refuses to start with `database schema
predates Beacon 2.0.0; 2.0.0 needs a fresh database` (Docker: stop the stack and move or
remove the Postgres data directory).

Config changes to review:

- Every manually configured `scopes:` entry needs a `region`.
- `meshmapper.scopes.sources` is ignored; MeshMapper refresh intervals must be within
  1h–24h for scopes and 24h–168h for zones, or startup fails.
- REST rate limiting is on by default. Behind a reverse proxy, set
  `server.trusted_proxies` (see [Reverse proxies and rate limits](#reverse-proxies-and-rate-limits)).
- `packets.retention` now defaults to 7 days.

### Cold start

Path resolution, capability detection and known routes depend on nodes having advertised
to a local observer. On a fresh database every hop shows `"confidence": "none"` and
`supportsMultibytePaths` is `false` until adverts populate `node_short_ids`. That's
expected and fills in as the mesh is observed.

GRP_TXT packets whose channel key isn't known yet are stored hash-only. After adding the
key to `config.yaml`, the next restart decrypts the matching history; look for
`backfilled N previously-undecrypted channel message(s)` in the log.

---

## Configuration

### Environment variables (`.env`)

| Variable                 | Default       | Description                                                  |
| ------------------------ | ------------- | ------------------------------------------------------------ |
| `LISTEN_ADDR`            | `:8080`       | HTTP listen address                                          |
| `POSTGRES_DSN`           | —             | PostgreSQL connection string                                 |
| `REDIS_ADDR`             | —             | Redis address (`host:port`). Leave unset to disable caching. |
| `REDIS_PASSWORD`         | —             | Redis password (optional)                                    |
| `REDIS_DB`               | `0`           | Redis database index                                         |
| `CONFIG_PATH`            | `config.yaml` | Path to YAML config file                                     |
| `MQTT_BROKER_1_URL`      | —             | Broker 1 WebSocket URL (e.g. `wss://mqtt1.example.com:443`)  |
| `MQTT_BROKER_1_USERNAME` | —             | Broker 1 username                                            |
| `MQTT_BROKER_1_PASSWORD` | —             | Broker 1 password                                            |
| `MQTT_BROKER_2_URL`      | —             | Broker 2 WebSocket URL                                       |
| `MQTT_BROKER_2_USERNAME` | —             | Broker 2 username                                            |
| `MQTT_BROKER_2_PASSWORD` | —             | Broker 2 password                                            |
| `BEACON_API_KEY`         | —             | Admin bearer key; overrides `auth.api_key` when set           |
| `LOG_LEVEL`              | `info`        | `debug`, `info`, `warn` or `error`; overrides `log.level`     |
| `LOG_FORMAT`             | `text`        | `text` or `json`; overrides `log.format`                      |
| `BEACON_CPU_PROFILE_DIR` | —             | Private CPU capture directory (see [PROFILING.md](PROFILING.md)) |
| `BEACON_CPU_PROFILE_UNTIL` | —           | RFC 3339 deadline for CPU captures, at most 72h ahead         |

### Config file (`config.yaml`)

```yaml
# MeshMapper integrations (optional, no API key). Import transport scopes and
# IATA border outlines from MeshMapper instead of maintaining scopes: and
# iatas.*.borderFile by hand. Both cover every known IATA that MeshMapper lists;
# no sources to configure. See config.yaml.example for limits and behaviour.
#meshmapper:
#  scopes:
#    enabled: true
#  zones:
#    enabled: true
#    import_groups: true # MeshMapper zone groups become regions
#  channels:
#    enabled: true       # import each region's public hashtag channels

# Optional IATA overrides — auto-created on first packet arrival,
# only needed if you want to customise display name or coordinates.
# borderFile points to a GeoJSON Feature (Polygon or MultiPolygon) for the
# region border map feature; relative paths resolve against this config
# file's own directory. Validated at boot — invalid geometry fails startup.
iatas:
  YVR:
    name: Vancouver International
    lat: 49.1967
    lng: -123.1815
    borderFile: borders/yvr.geojson # optional; or use meshmapper.zones

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
  # Tag names should be provided without the # prefix. Plain names show under
  # every region; {name, region: <slug>} limits a channel to one region.
  hashtags:
    - meshcore
    - name: vancouver-mesh
      region: western-canada

  # Explicit keys: channel hash (hex) and key (hex), with optional display name.
  # The public MeshCore channel key is included in config.yaml.example.
  keys:
    "11":
      key: "8b3387e9c5cdea6ac9e5edbaa115cd72"
      name: "Public"

# Regional transport scopes for matching TRANSPORT_FLOOD packets.
# Plain names have # prepended automatically (e.g. "bc" → "#bc").
# region (required) is a configured region slug; region-filtered scope lists
# and scope stats show the scope under that region's IATAs. Matching stays global.
# Optional when meshmapper.scopes covers your regions.
scopes:
  - name: bc
    region: western-canada
  - name: "#west"
    region: western-canada

# Observer telemetry storage settings.
telemetry:
  retention: 744h # how long to keep telemetry snapshots (default: 31 days)
  resolution: 1h # snapshot frequency per observer; duplicates within window are dropped (default: 1h)

# Packet and observation retention.
packets:
  retention: 168h # packets, observations, channel messages (default: 7 days, minimum 24h)

# Hourly analytics rollups behind the stats endpoints; they outlive raw packets.
analytics:
  rollup_retention: 2160h # also the longest /stats/series window (default: 90 days, minimum 24h)

# Presence write coalescing.
# Observer last_seen and packet last_heard_at bumps are batched in memory and
# flushed on an interval instead of writing one row per observation. An
# unclean shutdown loses at most one interval of presence freshness.
presence:
  flush_interval: 30s # how often coalesced bumps are flushed (default: 30s)
  packet_ttl: 30s # how long a quiet packet stays coalesced before writing through again (default: 30s)

# Direct proxy peers allowed to set the client IP through X-Real-IP.
server:
  trusted_proxies: [172.30.0.0/24] # default: [] (direct access)

# Per-client REST limits for /api/v1/*.
ratelimit:
  enabled: true # default: true
  requests_per_minute: 300 # default: 300

# WebSocket settings.
websocket:
  max_connections_per_ip: 5 # default: 5
  max_connects_per_minute: 10 # upgrade attempts per IP (default: 10)
  allowed_origins: [https://beacon.example.com] # other sites allowed to open /ws (default: same host only)

# Node staleness, deletion, and clock-drift thresholds.
nodes:
  mark_foreign: false # optional indication for repeaters outside configured IATA borders
  stale_threshold: 24h # mark a node "stale" in the API after this long unseen (default: 24h)
  delete_after: 720h # delete a node entirely after this long unseen (default: 30 days)
  clock_drift_threshold: 5m # |device clock - server clock| above which clockOutOfSync=true for a repeater/room server (default: 5m)

# Optional observer age-out (disabled by default; set e.g. 720h to opt in).
# Enable only when one Beacon ingest process owns the database: presence-cache
# preparation is local to that process, including both of its MQTT workers.
# Deletes at most 1000 observers per background.cleanup interval, only when
# last_seen/last_status_at are old and no observations, telemetry or ownership remain.
# Broker/location/scope metadata cascades; a returning observer receives a new ID.
observers:
  delete_after: 0s # omitted or nonpositive disables deletion

# Redis caching layer (optional).
# Caches read-heavy, slow-changing responses to reduce PostgreSQL load.
# Connection details (address, password, database) are set via environment
# variables. Leave REDIS_ADDR unset to disable caching entirely.
# TTLs are duration strings e.g. "30m", "1h". Per-category TTLs override
# the global ttl. Any unset category inherits ttl. Default: 1h.
cache:
  ttl: "1h"
  ttls:
    stats: "1h" # stats endpoints (hourly rollups; keys change when a new hour is rolled)
    reference: "1h" # IATAs, regions, scopes
    nodes: "1h" # node detail (also explicitly invalidated on upsert)
    observers: "1h" # observer detail (also explicitly invalidated on upsert)

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
names, coordinates, and optional region borders. Regions and channel keys
must be defined here unless MeshMapper imports them (`meshmapper.zones.import_groups`,
`meshmapper.channels`).

### Admin API

The `/api/v1/admin` subtree requires `Authorization: Bearer <key>`. Everything else,
including the WebSocket and CORS preflights, is public.

- Set the key with `BEACON_API_KEY` or `auth.api_key`. A set environment variable wins; an
  explicitly empty one disables admin access. No key is generated for you.
- The key must be at least 16 characters with no inner whitespace (surrounding whitespace is
  trimmed). An unusable key fails startup. Changing it needs a restart.
- With no key, admin requests return 503. A missing, wrong or duplicated header returns 401
  with `WWW-Authenticate: Bearer`. After auth, unknown paths are 404 and unsupported methods
  405.
- Use a long random key, send it only in the header (never the URL or body), and keep it out
  of source control and logs. Terminate HTTPS at the proxy and keep Beacon's listener
  private to it.

Endpoints:

- **`GET /admin/config`** returns the CORS options with defaults applied, `auth.configured`
  and `ingest.broker_count` (configured broker workers, not connection status). No
  credentials, broker addresses, channel material or database settings.
- **`PUT /admin/config`** accepts only `{"cors":{"allowed_origins":[...]}}` and replaces the
  origin list immediately. It is **runtime-only**: nothing is written, and a restart reloads
  the file. The response carries `config`, `persisted: false` and `requires_restart: false`.
  Send 1–32 ASCII http(s) origins of at most 512 bytes each, with an optional single
  hostname wildcard; a lone `*` allows all. Empty or null lists, paths, queries, credentials,
  control characters, unknown fields and bodies over 16 KiB are rejected. Concurrent updates
  apply one at a time; requests already in flight may see the old list.
- **`GET/POST /admin/accounts`, `GET/DELETE /admin/accounts/{id}`** manage operator account
  records (no login, session or token). POST takes `{"name": "..."}` (body ≤ 4 KiB; name
  trimmed, case-sensitive, ≤ 128 characters, no control characters, unique among active
  accounts). DELETE deactivates: 204, or 404 if missing, 409 if already inactive. The name
  can then be reused. The list returns active and inactive accounts, newest first,
  unpaginated.
- **`GET /admin/backup`** is opt-in; see [Backup and export](docs/backup-export.md).

Browser admin clients need the matching methods in the saved `cors.allowed_methods`. The
default `GET, HEAD, OPTIONS` is read-only. For an admin UI, use
`[GET, HEAD, OPTIONS, POST, PUT, DELETE]`, restrict `cors.allowed_origins` to that UI and
allow the `Authorization` and `Content-Type` headers, or preflight blocks requests that work
from curl. CORS controls browser access, not authentication.

### Reverse proxies and rate limits

REST requests under `/api/v1` are rate limited per client IP (300/min by
default; IPv6 clients share a /64). Exhausted clients get `429` with
`rate_limited` and `Retry-After`. WebSocket upgrades are limited separately by
`websocket.max_connects_per_minute`.

Beacon never trusts `X-Forwarded-For` or `True-Client-IP`. Behind a reverse
proxy, list the proxy's address in `server.trusted_proxies` (CIDR, e.g.
`127.0.0.1/32` or the Docker network subnet) and have the proxy overwrite
`X-Real-IP` with the connecting client's address:

```nginx
proxy_set_header X-Real-IP $remote_addr;
```

```caddy
reverse_proxy app:8080 {
	header_up X-Real-IP {remote_host}
}
```

Without this every visitor shares the proxy's budget and the site gets 429s
under normal load. Beacon logs a warning at startup when limits are on and no
proxy is trusted, and once at runtime when a request carries forwarding headers
it is ignoring. Full nginx and Caddy examples are in
[beacon-docs](https://github.com/MeshCore-Beacon/beacon-docs).

### Analytics retention

Historical stats read hourly rollups kept for `analytics.rollup_retention` (default 90
days), independently of `packets.retention`. Each UTC hour is rolled about 95 minutes after
it closes, and packet cleanup holds back up to 24h of raw rows until their hours are rolled.
An hour that lost raw rows first is reported as partial, without values. Drill-downs,
sub-hour observer activity and observer comparison still read raw data. Details:
[docs/historical-stats.md](docs/historical-stats.md).

### Logging

Logs go to stderr; collect and rotate them with Docker or systemd. Set `log.level`
(`debug`, `info`, `warn`, `error`; default `info`) and `log.format` (`text` or `json`) in
`config.yaml`, or override with `LOG_LEVEL` / `LOG_FORMAT`. Invalid values fail startup.

Every record has a `component`. Ingest records add the broker name; HTTP completion records
add the validated client address, route, status and duration. Query strings and WebSocket
hello payloads are never logged. Expected ingest skips and routine WebSocket lifecycle
events are debug-level. Reverse-proxy access logs (and fail2ban rules on them) are
unaffected.

### Foreign repeater indication

Set `nodes.mark_foreign: true` to add `possiblyForeign` to repeater nodes. It is `true` when
the repeater's reported position is outside the union of all configured
`iatas.<code>.borderFile` polygons, with `meshmapper.zones` borders replacing them where
MeshMapper has one. IATAs without a border add no area, and airport coordinates aren't used.
Enabling it with no border source, a missing file or invalid geometry fails startup. Border
changes need a restart.

- Points on an edge (including hole edges) are inside; hole interiors are outside.
- Other roles and missing or invalid positions get no value. A 0/0 advert clears the stored
  position.
- It's a hint from the reported position, not proof of origin. Ingest, heard-in IATAs and
  route matching are unaffected.
- Computed at read time, so existing nodes need no backfill. The field is omitted when the
  feature is off (the default).
- In `nodeUpdate`, it's a boolean when the advert carries a position, `null` when the
  position is 0/0 or invalid or the node stops being a repeater, and omitted to keep the
  previous value. `lat`/`lng` follow the same rule.

GeoJSON uses longitude/latitude order. Split antimeridian-crossing borders into
MultiPolygons per [RFC 7946 §3.1.9](https://www.rfc-editor.org/rfc/rfc7946#section-3.1.9);
edges spanning more than 180° are rejected.

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
    "routeTypes": [1, 2],
    "channelHashes": ["11"],
    "observerIds": ["<observer uuid>"],
    "events": ["packetObservation", "channelMessage"]
  }
}
```

All scope fields are optional; omitted or empty means no filter on that
dimension. `routeTypes` and `observerIds` filter `packetObservation` events
(`observerIds` also `observerStatus`), and `channelHashes` only `channelMessage`. `regionIds` and
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

The server replies `{ "v": 1, "type": "unsubscribed", "id": "unsub-1", "subscriptionId": "..." }`.

**Configure** — connection-wide flags, all default `false`:

- `resolvePath` adds per-hop node resolution to `packetObservation` events (see below).
- `includeObserverKey` adds `observation.observerPublicKey`.
- `includeRepeats` also streams later hearings of an already-stored observation
  over a new path, as `packetObservation` events with `packet.isRepeat: true`
  and `observationCount: 0`.

Unlike `subscribe`, this is not additive: each `configure` sets all three flags
to exactly the values sent, so an omitted flag turns off.

```json
{ "v": 1, "type": "configure", "id": "cfg-1", "resolvePath": true, "includeRepeats": true }
```

The server replies:

```json
{ "v": 1, "type": "configured", "id": "cfg-1", "resolvePath": true, "includeObserverKey": false, "includeRepeats": true }
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

When `resolvePath` is enabled via `configure`, `packetObservation` events
include a `resolvedPath` array: one entry per hop in the packet's path, each
with a `confidence` (`"high"` for exactly one candidate node, `"ambiguous"`
for multiple, `"none"` for zero) and the matching node(s)' id, name,
lat/lng, and public key. Without `resolvePath` enabled, `resolvedPath` is
`null` — this keeps the default event payload small; only opt in if you're
actually rendering path connections (e.g. drawing hops on a map).

### Backpressure

The server write buffer per connection is bounded at 256 events. If a client
falls behind, the server drops the oldest queued events and sends a `lagged`
notice:

```json
{ "v": 1, "type": "lagged", "droppedCount": 12, "since": 1234567890000 }
```

Clients should respond by backfilling over REST, then resume streaming:
`/api/v1/packets/backfill?afterObservationId=<last observation id>` and
`/api/v1/messages/backfill?afterId=<last message id>`.

### Reconnection

Subscriptions are not persisted — they exist only for the lifetime of the
connection. On any disconnect the client should reconnect with backoff, re-issue
all subscriptions, and backfill via the same REST endpoints.

### Connection limits

- At most `websocket.max_connections_per_ip` (default 5) concurrent connections
  per IP. A connection over the cap is accepted and then closed with code
  `1013` (try again later) before `hello`.
- At most `websocket.max_connects_per_minute` (default 10) upgrade attempts per
  IP; beyond that the handshake gets `429` with `Retry-After: 60`.
- Browsers may only connect from the same host unless their origin is listed in
  `websocket.allowed_origins` (`https://*.example.com` covers every subdomain).

Behind a reverse proxy both limits need `server.trusted_proxies`, or every
visitor counts as the proxy's IP.

---

## REST API

Base path: `/api/v1`

All list endpoints support cursor-based pagination via `cursor` and `limit`
query params. See the Swagger UI at `http://localhost:8080/swagger/index.html`
for full parameter documentation.

### Authentication and limits

Only `/admin/*` needs `Authorization: Bearer <key>`. All `/api/v1` routes are
rate limited per client IP; see [Reverse proxies and rate limits](#reverse-proxies-and-rate-limits).
List `limit` values are clamped to 1–200.

### Endpoints

| Method   | Path                                    | Description                                                                                                           |
| -------- | --------------------------------------- | --------------------------------------------------------------------------------------------------------------------- |
| `GET`    | `/admin/accounts`                       | List operator accounts (bearer key)                                                                                   |
| `POST`   | `/admin/accounts`                       | Create an operator account (bearer key)                                                                               |
| `GET`    | `/admin/accounts/{id}`                  | Get an operator account (bearer key)                                                                                  |
| `DELETE` | `/admin/accounts/{id}`                  | Deactivate an operator account (bearer key)                                                                           |
| `GET`    | `/admin/backup`                         | Download a private database and saved-config backup (bearer key, opt-in)                                              |
| `GET`    | `/admin/config`                         | Inspect selected running configuration (bearer key)                                                                   |
| `PUT`    | `/admin/config`                         | Replace runtime CORS origins (bearer key)                                                                             |
| `GET`    | `/brokers`                              | List MQTT brokers and connection status                                                                               |
| `GET`    | `/channels`                             | List channels (`hash`, `iata`/`iatas`, `keyKnown`, `pageCursor`, `limit`)                                             |
| `GET`    | `/channels/{channelID}`                 | Get channel detail by integer ID                                                                                      |
| `GET`    | `/channels/{channelID}/messages`        | List messages for a channel (`since`, `iatas`, `region`/`regionId`, `scope`, `cursor`, `limit`)                       |
| `GET`    | `/iatas`                                | List all known IATA codes                                                                                             |
| `GET`    | `/iatas/{iata}`                         | Get a single IATA code                                                                                                |
| `GET`    | `/iatas/{iata}/border`                  | Get an IATA's GeoJSON border, if any (204 if not)                                                                     |
| `GET`    | `/messages`                             | List channel messages (`channelID`, `channelHash`, `since`, `iatas`, `region`/`regionId`, `scope`, `cursor`, `limit`) |
| `GET`    | `/messages/backfill`                    | Messages after `afterId`                                                                                              |
| `GET`    | `/nodes`                                | List nodes (type, IATA/region, name, scope, `pubkey`/`pubkeyPrefix`, capability flags, `neighbors`)                   |
| `GET`    | `/nodes/{nodeId}`                       | Get node detail                                                                                                       |
| `GET`    | `/nodes/{nodeId}/neighbors`             | List neighboring nodes observed in the mesh                                                                           |
| `GET`    | `/nodes/{nodeId}/observations`          | List observations of packets from a node                                                                              |
| `GET`    | `/observers`                            | List observers (IATA/region, `type`, `broker`, `status=online\|offline`, `name`, `scope`)                             |
| `GET`    | `/observers/{observerId}`               | Get observer detail including broker last-seen timestamps                                                             |
| `GET`    | `/observers/{observerId}/activity`      | Heard-activity history (`range`, `interval`, `until`)                                                                 |
| `GET`    | `/observers/{observerId}/adverts`       | Adverts heard by observer                                                                                             |
| `GET`    | `/observers/{observerId}/telemetry`     | Telemetry history (`range`, `interval=1h\|6h\|24h`, `afterId`)                                                        |
| `GET`    | `/packets`                              | List packets (payload/route type, IATA/region, scope, `since`/`until`; plural params take CSV)                        |
| `GET`    | `/packets/backfill`                     | Packets after `afterObservationId`                                                                                    |
| `GET`    | `/packets/{packetHash}`                 | Get packet with all observations                                                                                      |
| `GET`    | `/regions`                              | List all regions (summary)                                                                                            |
| `GET`    | `/regions/{regionId}`                   | Get a single region with IATA list                                                                                    |
| `GET`    | `/routes`                               | List known routes (`iata`, `hopCount`, `cursor`+`cursorId`)                                                           |
| `GET`    | `/routes/search`                        | Search routes by source and destination hash                                                                          |
| `GET`    | `/routes/cross`                         | Search for routes crossing IATA boundaries                                                                            |
| `GET`    | `/routes/{iata}/{pathKey}/observations` | Retained observations matching a saved route (see below)                                                              |
| `GET`    | `/scopes`                               | List transport scope names; IATA/region filters use configured regions and MeshMapper catalogues                      |
| `GET`    | `/scopes/{name}`                        | Get scope detail                                                                                                      |
| `GET`    | `/stats/clock-drift`                    | Repeaters and room servers whose clock drifted past the threshold, worst first                                        |
| `GET`    | `/stats/node-types`                     | Node type breakdown                                                                                                   |
| `GET`    | `/stats/observations`                   | Hourly observation counts per IATA (last 7 days by default)                                                           |
| `GET`    | `/stats/observer-comparison`            | Compare flood packets reported by two observers                                                                       |
| `GET`    | `/stats/overview`                       | Network overview over the last 24 rolled hours                                                                        |
| `GET`    | `/stats/paths`                          | Path-entry and hash-width distributions                                                                               |
| `GET`    | `/stats/payload-breakdown`              | Observation counts by payload type (last 24h by default)                                                              |
| `GET`    | `/stats/radio-presets`                  | Radio preset usage by IATA                                                                                            |
| `GET`    | `/stats/scopes`                         | Region scopes with hourly packet, node and observer counts (last 7 days by default)                                   |
| `GET`    | `/stats/series`                         | Hourly card metrics and window summary (`since`/`until` required)                                                     |
| `GET`    | `/stats/signal`                         | Reception signal distributions and hourly trends                                                                      |
| `GET`    | `/stats/top-advertisers`                | Top N nodes by distinct ADVERT packets, split flood/direct (last 24h by default)                                      |
| `GET`    | `/stats/top-nodes`                      | Top N nodes by advert hearings (last 7 days by default)                                                               |
| `GET`    | `/stats/top-observers`                  | Top N observers by observation count (last 24h by default)                                                            |
| `GET`    | `/stats/top-talkers`                    | Top N companion names by decrypted channel message count (last 24h by default)                                        |
| `GET`    | `/traces`                               | List trace tags (`type=TRACE\|PING`, IATA/region, scope, `since`/`until`, `cursor`+`cursorTag`)                       |
| `GET`    | `/traces/{tag}`                         | Get full trace detail with resolved routes                                                                            |

### Observer activity

`GET /observers/{observerId}/activity` returns buckets over `[windowStart, windowEnd)` (live
requests include the current partial bucket), plus `generatedAt`, `source` (`raw` or
`hourly`) and `summary`.

- `recordedPackets` counts stored observations in the window; repeated broker deliveries of
  the same packet count once. Unknown payload types show as `-1`.
- Freshness fields (`lastCompleteHour` with its start and end, `latestRecordedAt`) are
  measured at `generatedAt`, even for historical `until` requests.
- `until` (epoch ms, within the last 30 days) lines up two observers' charts.
- Hourly mode reads the rollups plus raw rows for hours not rolled yet (at most 24h).
  `rolledUntil` ends the rolled range and `rawFrom` starts the raw tail; hours between them
  are uncovered.
- Missing records don't prove downtime. The observer's `observationCount` is a legacy
  cumulative presence counter (including status and neighbour events), not a packet total.

### Saved-route evidence

`GET /routes/{iata}/{pathKey}/observations` returns retained observations matching a saved
route's full path bytes, hash size and hop count within its IATA. `range` defaults to `24h`
(max `720h`) and `limit` to 50 (max 200); page with `nextPageCursor` → `pageCursor`.
Evidence expires with raw packets.

---

## Acknowledgements

See [CONTRIBUTORS.md](CONTRIBUTORS.md) for the people who have helped build Beacon, and
[SHOULDERS.md](SHOULDERS.md) for the open source projects it stands on.
