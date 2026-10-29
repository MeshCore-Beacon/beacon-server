# Optional companion telemetry intake

Beacon Mobile and the alternate headless client can submit attributed community
readings through the selected Beacon instance. They remain separate from observer
telemetry, received packets and observed mesh routes. Beacon core does not poll RF.

Configure a compatible, separately deployed validating intake:

```yaml
node_telemetry:
  gateway_url: http://beacon-collector:8090/api/v1/node-telemetry
```

The URL is an operator-owned service address, never supplied by clients. Use HTTPS
when the connection crosses hosts or an untrusted network. Configure the intake's
canonical instance URL to the externally visible Beacon base URL, including any
deployment prefix. Incoming signed bytes are preserved; redirects are rejected.
This is installation configuration, not approval of each participant.

Empty configuration disables the routes. `/api/v1/info` exposes
`collectedNodeTelemetry: true` when configured (not a gateway-health assertion).
Only these routes are forwarded under `/api/v1/node-telemetry`:

| Method/path | Contract |
| --- | --- |
| POST `/challenge` | Radio and client full public keys; short-lived instance-bound signing challenge |
| POST `/enroll` | Radio and client signatures; returns scoped expiring authorization |
| POST `/reports` | Exact base64 JSON payload, Ed25519 signature and authorization; durable `accepted: true` receipt |
| GET `/{full-public-key}` | Up to 500 accepted samples in retained history |
| GET `/availability` | Full node keys and latest reading timestamps, `retentionDays`, and explicit `truncated` flag |

The intake must verify keys, both enrollment signatures, exact instance binding,
authorization expiry, signed payloads, replay/idempotence, numeric/sensor bounds,
timestamps and per-radio quotas before acknowledging persistence. Key possession
is not hardware attestation or proof of measurement truth. Nodes are matched by
their full key; availability consumers must reject a truncated/incomplete index
instead of presenting missing entries as nodes without telemetry.

The current compatible intake retains 30 days and bounds the availability index
at 5,000 nodes. Expired/future-dated samples do not qualify. An index entry means
there is retained data; it does not prove a node is currently online. Collection
is opt-in; passwords, coordinates, messages and radio private keys are not reports.

Beacon applies its existing trusted-proxy client-IP rate limiter. The gateway
connection has a 15-second total deadline, 32 connections per host, 12,000-byte
write envelopes and 8-MiB read responses. It forwards no caller credentials,
cookies or caller-supplied proxy headers; errors are sanitized. It sends a new
`X-Real-IP` derived only from the already validated request peer. Configure the
intake to trust that header only from Beacon's service address, so independent
clients do not share one gateway IP quota. Retry-After is preserved with a
one-day upper bound. No retry replays a write at this boundary. The validating
intake must remain inaccessible to untrusted administrative access.

The upstream collector worker is not required on the phone. The independent
intake remains a deployment prerequisite, and MQTT sharing needs separately
scoped credentials, ACLs and persistence receipts. No broker password is embedded
in the mobile app. This change introduces no core database migrations.
