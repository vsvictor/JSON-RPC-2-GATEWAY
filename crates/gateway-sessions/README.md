# gateway-sessions

Gateway-owned broker session protocol and lifecycle runtime, independent of
MQTT/AMQP clients and JSON-RPC message handling:

- `BrokerSessionPolicy` validates heartbeat cadence and idle lease bounds.
- `SessionIdentity`, `MessageKind`, `MessageMetadata`, and `BrokerEnvelope`
  define the broker session wire metadata (`rpc`, `open`, `close`, `heartbeat`,
  and `push`).
- `SessionRuntime<T>` interprets lifecycle metadata, admits identified RPCs,
  owns adapter-provided session resources, retires failed/expired sessions,
  and provides their associated contexts to the adapter.
- `SessionRegistry` enforces identity binding, in-flight request retention,
  idle expiration, bounded capacity, and a tombstone period that prevents late
  traffic from recreating a closed session.

The default heartbeat is 10 seconds and the default idle lease is 45 seconds.
Broker route identifiers are routing metadata, not authentication; broker ACLs
must protect them.

The crate deliberately has no dependency on a broker implementation, the
JSON-RPC protocol, or the gateway binary. Broker adapters supply a factory for
transport-specific session resources and continue to own publish, subscribe,
acknowledgment, and application-message handling.
