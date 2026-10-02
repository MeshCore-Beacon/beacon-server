# Packet summaries

Packet lists, regional lists, reconnect backfill and live `packetObservation` events carry
an optional `summary` string. It comes from the packet's own stored metadata, never from a
node's current name or a fresh decode.

| Payload | Summary | Source |
|---|---|---|
| ADVERT | Advertised name | Saved `appData.name` / verified decoded advert |
| ACK | `ACK 01020304` | Four-byte acknowledgement checksum |
| TRACE | `TRACE efbeadde` | Trace tag |
| TRACE classified as PING | `PING efbeadde` | Trace tag |

- References are eight lowercase hex characters in the same byte order as packet detail's
  `checksum` / `traceTag`. Zero is valid. Extended ACK retry bytes don't change the checksum.
- They are short references, not unique packet IDs or proof of delivery. No message body,
  trace auth code or inferred sender/recipient is included.
- The text is projected from saved JSON in the existing list queries: no extra column,
  backfill or per-packet lookup. A stored value of the wrong type or not exactly eight hex
  characters omits the field.
- Other payload types have no summary yet. A missing summary doesn't mean the packet is
  invalid.
