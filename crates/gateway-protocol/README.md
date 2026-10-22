# Gateway/backend integration protocol

This workspace-owned crate defines the opaque binary contract between the
gateway balancer and trusted internal backend listeners. JSON-RPC-2 provides
transport APIs; it does not implement or depend on this protocol.

Wrap the backend's ordinary RPC protocol before constructing its runtime:

```rust,no_run
let protocol = std::sync::Arc::new(gateway_protocol::GatewayBackendProtocol::new(rpc));
let server = json_rpc_2_runtime::MultiTransportServer::new(protocol);
```

The backend adapter unwraps only transport metadata and passes the original
payload unchanged to `JsonRpcProtocol::handle_message_with_context`. Business
JSON-RPC parsing remains the responsibility of the backend. The protocol is not
authentication; backend listeners must remain private/trusted.

## Version 3 binary envelope

Integers are unsigned, big-endian. Frames are self-delimiting and have no
newline, Base64 or payload copy:

| Offset | Size | Field |
| --- | --- | --- |
| 0 | 8 | Magic `JRPC-GW\x03` |
| 8 | 1 | Kind: request=1, response=2, none=3, error=4, ping=5, pong=6 |
| 9 | 2 | Connection ID length |
| 11 | 2 | Source transport length |
| 13 | 4 | Opaque payload length |
| 17 | variable | Metadata followed by the original payload |

Request metadata values contain 1..128 ASCII alphanumeric/hyphen bytes.
Responses contain only payload; notification acknowledgements and ping/pong
have empty payloads. Readers validate the fixed header and declared lengths
before consuming a frame. The payload limit is 16,776,943 bytes, reserving
header and metadata space within the transport's 16 MiB message limit.

The balancer uses `encode_request` and `decode_response`; backend startup health
checks require a matching framed PONG. Never replay an exchange after it was
sent: the backend may have executed it even when its acknowledgement was lost.
There is no legacy `JRPC-GW/1` or Base64 `JRPC-GW/2` compatibility mode.

TCP/TLS clients opt into the protocol's generic `StreamFraming` implementation;
HTTP clients carry it as `application/octet-stream`, and WebSocket clients mark
it explicitly binary. Ordinary application messages retain their normal
transport representation. These APIs preserve `Bytes` payload ownership
through gateway dispatch; WebSocket and other contiguous-frame boundaries may
still require copies. No universal kernel/network zero-copy guarantee is made.
