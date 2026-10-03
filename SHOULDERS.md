# Shoulders

Beacon stands on the shoulders of giants. Thank you to the following open source
projects that make this possible:

## Core dependencies

- [meshcore-go](https://github.com/meshcore-go/meshcore-go): MeshCore packet
  decoding
- [paho.mqtt.golang](https://github.com/eclipse/paho.mqtt.golang): MQTT client
- [pgx](https://github.com/jackc/pgx): PostgreSQL driver and connection pool
- [sqlc](https://sqlc.dev): Type-safe SQL code generation
- [chi](https://github.com/go-chi/chi): HTTP router
- [go-chi/cors](https://github.com/go-chi/cors): CORS middleware for chi
- [go-chi/httprate](https://github.com/go-chi/httprate): Per-client HTTP
  sliding-window rate limits and retry headers
- [go-redis](https://github.com/redis/go-redis): Redis client
- [coder/websocket](https://github.com/coder/websocket): WebSocket
  implementation
- [swaggo/swag](https://github.com/swaggo/swag): OpenAPI documentation
  generation
- [godotenv](https://github.com/joho/godotenv): .env file loading
- [yaml.v3](https://github.com/go-yaml/yaml): YAML config parsing
- [google/uuid](https://github.com/google/uuid): UUID generation
- [golang.org/x/sync](https://pkg.go.dev/golang.org/x/sync): singleflight for
  collapsing concurrent cache misses
- [paulmach/orb](https://github.com/paulmach/orb): GeoJSON border geometry and
  point-in-polygon checks
- [swaggo/http-swagger](https://github.com/swaggo/http-swagger): Swagger UI
  handler

## Testing

- [go.uber.org/mock](https://github.com/uber-go/mock): gomock mocks for the
  generated queries
- [miniredis](https://github.com/alicebob/miniredis): In-memory Redis for cache
  tests

## Data

- [OurAirports](https://ourairports.com/data/): IATA airport geographic data
  (CC0 public domain)

## Infrastructure

- [PostgreSQL](https://www.postgresql.org): Primary data store
- [Redis](https://redis.io): Optional caching layer
- [Caddy](https://caddyserver.com): Reverse proxy

## Language

- [Go](https://go.dev): The language that makes this all possible
