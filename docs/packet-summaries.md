# Packet summaries

Packet lists, regional lists, reconnect backfill and live `packetObservation`
events share an optional `summary` string. It describes metadata carried by that
packet, rather than looking up a node's current name or decoding history again.

| Payload | Summary | Source |
|---|---|---|
| ADVERT | Advertised name | Saved `appData.name` / verified decoded advert |
| ACK | `ACK 01020304` | Four-byte acknowledgement checksum |
| TRACE | `TRACE efbeadde` | Trace tag |
| TRACE classified as PING | `PING efbeadde` | Trace tag, with the existing single-destination classification |

References use eight lowercase hexadecimal characters in the same byte order
as packet detail's `checksum` / `traceTag`. Extended ACK retry bytes do not change
the displayed checksum. Zero is a valid reference. These short references are
not globally unique packet identifiers or proof of delivery/authentication; no
message body, trace auth code or inferred sender/recipient is added to the summary.

Historical reads require the expected stored type and a string containing
exactly eight hex characters. Missing, malformed or unsupported references omit
the field. Advert behavior is unchanged. List/backfill queries project the text
from saved JSON in their existing query, without a schema change, backfill job
or per-packet database lookup. The existing web summary display needs no new API
field or client release to render these values.

Other packet types remain separate follow-ups under issue #99. A missing summary
does not mean the packet is invalid or unreadable in packet detail.
