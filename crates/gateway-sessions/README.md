# gateway-sessions

Gateway-owned broker session protocol primitives, independent of MQTT/AMQP
clients and JSON-RPC dispatch:

- `BrokerSessionPolicy` validates heartbeat cadence and idle lease bounds.
- `SessionIdentity`, `MessageKind`, `MessageMetadata`, and `BrokerEnvelope`
  define the broker session wire metadata (`rpc`, `open`, `close`, `heartbeat`,
  and `push`).
- `SessionRegistry` enforces identity binding, in-flight request retention,
  idle expiration, bounded capacity, and a tombstone period that prevents late
  traffic from recreating a closed session.

The default heartbeat is 10 seconds and the default idle lease is 45 seconds.
Broker route identifiers are routing metadata, not authentication; broker ACLs
must protect them.

The crate deliberately has no dependency on a broker implementation, the
JSON-RPC protocol, or the gateway binary. Adapters can use it as a shared
contract while transport-specific publish, subscribe, and acknowledgment
mechanics remain in their respective layers.
