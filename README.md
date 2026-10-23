# JSON-RPC gateway workspace

This workspace separates ingress/session concerns from backend selection:

- `json-rpc-gateway` is the transport-facing adapter, message bus, configuration loader, and Control Plane.
- `json-rpc-balancer` owns backend endpoint validation, load-balancing strategy, and persistent backend connection pools.
- `gateway-iot` owns IoT server adapters, broker ingress configuration, and server-side broker I/O; `json-rpc-2-iot` retains client transports. `gateway-sessions` is a separately consumable crate for broker session identity, lifecycle metadata, heartbeat/lease policy, and runtime orchestration. Its `SessionRuntime` validates metadata, applies lifecycle transitions, admits RPC work, and owns transport session resources.
- JSON-RPC-2 is fetched directly from the upstream [`start` branch](https://github.com/vsvictor/JSON-RPC-2/tree/start), which provides reusable accepted-I/O, session, push and transport framing APIs. The workspace-owned `gateway-protocol` crate defines the private gateway/backend envelope and backend adapter. `Cargo.lock` pins the resolved commit; no local upstream checkout is required.

To adopt new JSON-RPC-2 changes, run `cargo update -p json-rpc-2-runtime -p json-rpc-2-server`, then rebuild and test the workspace. Keep the updated `Cargo.lock` with the project so normal builds use the same library revision.

## Backend contract

Backends are built with `json-rpc-2-server::RpcServer` and `json-rpc-2-runtime::MultiTransportServer` from JSON-RPC-2. Wrap trusted internal backend protocols with `gateway_protocol::GatewayBackendProtocol` before starting them. The workspace-owned binary version-3 envelope (`JRPC-GW\x03`) carries the client connection ID, source transport, and original opaque payload, without Base64. The backend adapter unwraps it and passes the original bytes and `TransportContext` to the RPC handler. Only the backend parses JSON-RPC; the gateway never inspects business payloads or IDs. See [`gateway-protocol`](crates/gateway-protocol/README.md).

Every framed exchange is acknowledged, including notifications, so pooled connections cannot hang waiting for a nonexistent notification response. Ordinary direct JSON-RPC clients continue to work. Backend push must use the Control Plane, never the pooled connection shared by multiple clients. Gateway metadata is not authentication: do not enable this backend mode on untrusted public listeners.

Framing adds a fixed 17-byte header plus request metadata, not 33% payload expansion. Owned ingress dispatch and the message bus retain `Bytes` ownership; the balancer keeps header/payload separate, and response decoding uses a shared slice. TCP/TLS, HTTP/1, QUIC, and HTTP/3 send separate segments without concatenating the payload. WebSocket, datagram, and broker adapters can still copy at contiguous-frame/library boundaries, as can encryption and receive reassembly: this is **not a kernel/network-wide zero-copy guarantee**. The codec accepts up to 16,776,943 payload bytes; transport-specific limits can be smaller and errors are propagated.

Upgrade gateways and internal backends together: the former Base64 `JRPC-GW/2` and legacy TCP/TLS `JRPC-GW/1` profiles are not supported. Startup health checks reject incompatible backends; requests are not replayed through another profile.

Example backend setup:

```rust,no_run
use std::sync::Arc;
use json_rpc_2_runtime::MultiTransportServer;
use json_rpc_2_server::RpcServer;

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let rpc = Arc::new(RpcServer::new());
    rpc.register("echo", |params| async move { Ok(params) }).unwrap();
        let protocol = Arc::new(gateway_protocol::GatewayBackendProtocol::new(rpc));
        let running = MultiTransportServer::new(protocol)
        .set_bind(["0.0.0.0".parse().unwrap()])
        .set_tcp_port(8081)
        .start()
        .await?;
    tokio::signal::ctrl_c().await?;
    running.close().await
}
```

## Gateway configuration

Required process environment (a `.env` file is also loaded):

```dotenv
GATEWAY_BIND=0.0.0.0
GATEWAY_PORT=8443
CONTROL_PORT=8500
BACKEND_CONFIG_PATH=./backends.json
LOG_LEVEL=info
```

`GATEWAY_PORT` is the fallback for TCP and UDP. Other enabled transports are selected with `GATEWAY_<TRANSPORT>_PORT` (hyphens become underscores); set these to the same value to share ingress. For WebSocket over TLS, use `GATEWAY_WS_TLS_PORT` (WSS endpoint `/ws`). Gateway-owned `GatewayIngress` groups listeners by IP-protocol/port: one physical listener owns each group, while transport contexts retain their original identifiers. Classification, ALPN policy, association selection, and the SRT relay live in `crates/json-rpc-gateway/src/ingress`, not in JSON-RPC-2. The library supplies reusable accepted-stream, accepted-connection, HTTP-router and datagram-lane APIs; standalone adapters and broker consumers reuse its ordinary runtime. MQTT and AMQP remain external broker consumers, requiring `MQTT_BROKER_URL` and `AMQP_BROKER_URL`; TLS/WSS/QUIC/HTTP3 require `TLS_CERT_PATH` and `TLS_KEY_PATH`. Unconfigured optional transports are not implicitly enabled.

TCP/TLS/HTTP/WS are dispatched directly after bounded prefix detection and, where needed, TLS termination. Ordinary UDP profiles share one reader and bounded owned-packet lanes; QUIC retains ECN, destination-IP and segmentation metadata, with reserved-prefix server connection IDs for short-header dispatch. Shared QUIC disables fixed-bit greasing negotiation to keep long headers distinguishable from RTP/SRT. HTTP/3 and WS-HTTP3 share an endpoint and dispatch POST versus extended CONNECT. SCTP, DCCP and UDP-Lite have independent IP protocol namespaces and can use the same numeric port; raw and WS variants share their native listener. Classification reserves technical HTTP method prefixes, the `WSU2` application envelope and QUIC CID prefix; no business JSON is parsed.

Native SRT is the explicit exception to direct shared I/O: a bounded per-peer internal UDP relay forwards unchanged encrypted libsrt packets to one private loopback listener. This adds socket hops/copies and is not zero-copy. Stream IDs distinguish raw SRT from WS-SRT. A peer's UDP endpoint is dedicated to SRT once admitted, until relay expiry/close; native listener logs see its loopback relay address. Shared listeners drain and release their physical sockets once, not once per alias.

The gateway admits new SRT peers only from structural native induction handshakes, not a shared control bit or arbitrary data. Each peer has one loopback socket/task and a 64-packet queue, bounded by `TransportRuntime` connection limits. Full queues return/log `WouldBlock`; native transport retransmission is not application replay. Mappings expire after 30 seconds without traffic in either direction; native keepalives retain live sessions, while failed handshakes can occupy a slot until expiry. SRT endpoint migration is unsupported. `external_peer` provides diagnostic lookup while a mapping exists; it does not alter backend metadata. Shutdown keeps forwarding while native sessions drain, then cancels the public reader and relay and joins tasks within the close timeout.

Shared-port regression tests belong to the gateway: `cargo test -p json-rpc-gateway ingress::` checks wire-profile dispatch, opaque bytes, lifecycle and socket release. Enable `srt` to include encrypted raw/WS-SRT and relay bounds/expiry tests; run the ignored `shared_port_kernel` test on a kernel with SCTP and UDP-Lite support.

```json
{
  "backends": [
    {
      "id": "backend-1",
      "host": "127.0.0.1",
      "ports": { "tcp": 8081, "http1": 8082, "ws-tcp": 8083 },
      "pool_size": 4
    }
  ]
}
```

The balancer first selects a backend using round robin, then prefers its port matching the ingress transport. If unavailable, it uses TCP, or otherwise the lexicographically first configured transport. Each configured port has `pool_size` persistent connections. All connections are opened and checked before ingress listeners start; unavailable backends cause startup to fail explicitly. Existing connections are checked periodically; failed or timed-out connections are discarded and reconnected for later exchanges. Requests that may have executed are never replayed.

QUIC and WS-QUIC choose a backend once per logical ingress session using round robin. That affinity is keyed by the session UUID, never the peer IP or port, so path migration does not change the backend. Closing the session releases the affinity entry; a failed exchange is not retried on another backend. Other ingress transports retain per-message round robin.

Backend connectors cover all 30 transport identifiers, including `ws-tls`; SRT requires the `srt` feature, and kernel transports require platform support. Backend forwarding is verified for 28 identifiers: TCP, HTTP/1, UDP, RTP, RUDP, CoAP, UDP-Lite, SCTP, their WS carriers, TLS, WS-TLS, QUIC, HTTP/3, WS-QUIC, WS-HTTP3, encrypted SRT/WS-SRT, and real MQTT/AMQP with their WS carriers. Broker, UDP-Lite, and SCTP compatibility tests also preserve arbitrary binary bytes, metadata, and notification acknowledgements.

SCTP uses one-to-one associations and reassembles partial deliveries until `MSG_EOR`; the upstream regression covers record isolation and EOF. DCCP and WS-DCCP cannot be exercised on this kernel (`Socket type not supported`). These two identifiers remain unverified; registry coverage alone does not imply runtime support. Upstream runtime matrices explicitly exclude unavailable DCCP carriers, without suppressing other socket errors; `JSON_RPC_REQUIRE_DCCP=1` makes their absence a failure on DCCP-capable CI hosts. Production startup still fails if any explicitly requested transport is unavailable.

Optional compatibility tests are ignored by default and fail explicitly if their prerequisites are absent. Run `cargo test -p json-rpc-balancer broker_backends -- --ignored` with `MQTT_BROKER_URL` and `AMQP_BROKER_URL` pointing to isolated test brokers. Run the `udplite_backends`, `sctp_backends`, or `dccp_backends` test selectors with `-- --ignored` on a suitable kernel. The `sctp_backends` selector includes WS-SCTP. Keep broker tests isolated from production services.

Optional per-backend `options` configure `operation_timeout_ms` and `health_check_interval_ms` (both default to 30000, must be positive). Broker sessions accept `broker_heartbeat_interval_ms` (default 10000) and `broker_session_lease_ms` (default 45000, must exceed the heartbeat). Secure TLS/WS-TLS/QUIC/HTTP3 backends require `tls_ca_path`; `tls_server_name` defaults to `host`, with certificate verification enabled. WS-TLS connects to the resolved backend address while verifying this server name in its WSS URL. SRT backends require `srt_passphrase_env`, naming an environment variable containing the passphrase. Brokers accept `mqtt_url_env` / `amqp_url_env` for URL environment variables and independent `mqtt_request_topic`, `ws_mqtt_request_topic`, `amqp_request_queue`, and `ws_amqp_request_queue` routing overrides. Broker URLs and passphrases should not be stored in committed config files.

Check and test the gateway with `cargo check -p json-rpc-gateway` and `cargo test -p json-rpc-gateway -p json-rpc-balancer`. Persistent session context and push are wired for TCP/TLS, SCTP, encrypted SRT, QUIC, MQTT/AMQP and their connection-oriented WS adapters, including WS-HTTP1, WS-TLS and WS-HTTP3. Raw QUIC push uses separate server-initiated unidirectional streams; SRT uses a distinct push frame and brokers use per-client push routes. Stateless HTTP/datagram requests register only for the request lifetime and have no push queue; Control Plane push is rejected with `PushNotSupportedForTransport`. DCCP has a stable non-push session. Closing a session also removes its client/group registrations; concurrent registration and disconnect cannot leave stale entries.

QUIC acceptance tests rebind the client endpoint from `127.0.0.1` to `127.0.0.2`, verify that the same session continues on the same one of two backends, and exercise unicast, multicast and broadcast through an actual TCP Control Plane connection. HTTP acceptance tests verify rejection during an active request and cleanup after its response.

Broker clients have independent push routes, explicit Close and configurable heartbeats/leases. Defaults are 10-second heartbeats and a 45-second lease; under normal broker progress, an abandoned idle session expires after 45..55 seconds. An admitted request may retain its session while in flight. Broker/network operation delays can extend these bounds. Gateway ingress uses `GATEWAY_BROKER_HEARTBEAT_INTERVAL_MS` and `GATEWAY_BROKER_SESSION_LEASE_MS`; broker backend pool entries use the corresponding `broker_heartbeat_interval_ms` and `broker_session_lease_ms` options. An ordinary MQTT/AMQP consumer cannot immediately observe another client's socket loss. Lifecycle controls remain live under saturated dispatch; excess application requests are rejected without replay, and their callers time out rather than receiving synthetic business responses. Push is not a durable mailbox.

To build with SRT without installing a system `libsrt`, use `scripts/build-with-srt.sh check -p json-rpc-gateway --features srt` (or replace `check` with `build`/`test`). The script fetches the official [`Haivision/srt`](https://github.com/Haivision/srt) source and builds the pinned `v1.5.7` commit with CMake and OpenSSL encryption. It also supplies the C++ runtime and OpenSSL linker flags required by the static library, without forcing unrelated transitive dependencies such as zstd to be linked statically. A C/C++ compiler, CMake, OpenSSL development files, pkg-config, and libclang (required by the Rust bindings generator) must be available. To update SRT, change `srt_tag` and `srt_commit` together in `scripts/build-with-srt.sh`; the immutable commit pin keeps builds reproducible.

For SRT/WS-SRT ingress, enable the `srt` feature, configure the listener port and set `GATEWAY_SRT_PASSPHRASE` to a shared secret of 10..79 bytes. Startup validates it before opening listeners. This is independent of per-backend `srt_passphrase_env` settings.

Linux/Android UDP-Lite ingress accepts `GATEWAY_UDPLITE_SEND_CHECKSUM_COVERAGE` and `GATEWAY_UDPLITE_RECV_CHECKSUM_COVERAGE` in bytes (default `8`). Coverage `0` protects the entire packet; partial coverage must be at least `8`. Invalid values fail startup.

## Architecture and current upstream boundary

The workspace uses an Adapter (`GatewayProtocol`), an asynchronous Message Bus (`IncomingEnvelope`), a Strategy (`LoadBalancingStrategy`, default `RoundRobin`), a Pool (`BackendPool`), and a Registry (`SessionRegistry`).

The merged upstream extensions in `start` provide `TransportContext`, session lifecycle callbacks, context-aware JSON-RPC handlers, and transport-specific push channels, plus binary backend metadata, notification acknowledgements, owned ingress dispatch, and segmented outbound messages. Sessionless/request-only carriers intentionally reject unsolicited push. Universal zero-copy across carrier libraries/kernel boundaries is not claimed; DCCP runtime verification still requires a kernel with DCCP support.

Shared-port ingress is implemented for configured local carriers, including the agreed native-SRT relay. Complete acceptance of the original specification is still not claimed: broker disconnect detection uses a lease, DCCP requires a supporting kernel, and the custom `WSU2` profile has Close but does not yet expose WebSocket-level Ping/Pong independently of carrier liveness controls.
