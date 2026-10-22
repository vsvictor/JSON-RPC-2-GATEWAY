#![cfg(feature = "srt")]

// Exercise the runtime's bounded helper without the runtime's unrelated native
// transport dev-dependencies, and cover both application profiles together.
use crate::ingress::shared_srt;

use std::{io, net::SocketAddr, sync::Arc, time::Duration};

use bytes::Bytes;
use json_rpc_2_transport::{
    CancellationToken, JsonRpcProtocol, ProtocolFuture, RpcClientTransport, Transport,
    TransportRuntime,
};
use shared_srt::SrtUdpRelay;
use tokio::net::UdpSocket;

fn induction() -> Bytes {
    let mut packet = vec![0; 64];
    packet[0] = 0x80;
    for (offset, value) in [
        (16, 4u32),
        (20, 2),
        (24, 42),
        (28, 1500),
        (32, 8192),
        (36, 1),
        (40, 1234),
    ] {
        packet[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
    }
    Bytes::from(packet)
}

async fn receive(socket: &UdpSocket) -> (Bytes, SocketAddr) {
    let mut buffer = vec![0; 65_535];
    let (length, source) =
        tokio::time::timeout(Duration::from_secs(3), socket.recv_from(&mut buffer))
            .await
            .unwrap()
            .unwrap();
    buffer.truncate(length);
    (Bytes::from(buffer), source)
}

#[test]
fn initial_induction_is_structurally_distinct_from_rtp_quic_and_legacy_jsrt() {
    let packet = induction();
    assert!(SrtUdpRelay::is_handshake(&packet));
    assert!(!SrtUdpRelay::is_handshake(b"JSRT"));
    assert!(!SrtUdpRelay::is_handshake(&packet[..63]));
    for offset in [0, 1, 2, 4, 12, 16, 20, 36, 44] {
        let mut malformed = packet.to_vec();
        malformed[offset] ^= 1;
        assert!(!SrtUdpRelay::is_handshake(&malformed), "offset {offset}");
    }
    for (offset, value) in [(24, 0x8000_0000u32), (28, 0), (32, 0), (40, 0)] {
        let mut malformed = packet.to_vec();
        malformed[offset..offset + 4].copy_from_slice(&value.to_be_bytes());
        assert!(!SrtUdpRelay::is_handshake(&malformed), "offset {offset}");
    }
    assert!(!SrtUdpRelay::is_handshake(&[0x80; 64])); // RTP first-bit overlap.
    assert!(!SrtUdpRelay::is_handshake(&[0xc0; 64])); // QUIC long-header overlap.
}

#[tokio::test]
async fn relay_preserves_packets_uses_one_socket_per_peer_and_enforces_limits() {
    let public = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let private = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let external = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let second = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let runtime = TransportRuntime {
        limits: json_rpc_2_transport::ServerLimits {
            max_connections: 1,
            ..Default::default()
        },
        ..Default::default()
    };
    let relay = SrtUdpRelay::new(
        public.clone(),
        private.local_addr().unwrap(),
        Arc::new(runtime),
        CancellationToken::new(),
    )
    .unwrap();
    let source = external.local_addr().unwrap();
    assert!(!relay.owns_peer(source));
    assert_eq!(
        relay
            .forward(Bytes::from_static(b"not an induction"), source)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    relay.forward(induction(), source).await.unwrap();
    assert!(relay.owns_peer(source));
    let (packet, relayed_source) = receive(&private).await;
    assert_eq!(packet, induction());
    assert!(relayed_source.ip().is_loopback());
    assert_ne!(relayed_source.port(), 0);
    assert_ne!(relayed_source, public.local_addr().unwrap());
    assert_eq!(
        relay
            .forward(induction(), second.local_addr().unwrap())
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::ConnectionRefused
    );
    for payload in [
        Bytes::from_static(b"opaque native data"),
        Bytes::from(vec![0x55; 48_000]),
    ] {
        relay.forward(payload.clone(), source).await.unwrap();
        let (packet, next_source) = receive(&private).await;
        assert_eq!(next_source, relayed_source);
        assert_eq!(packet, payload);
        private.send_to(&packet, next_source).await.unwrap();
        let (packet, public_source) = receive(&external).await;
        assert_eq!(public_source, public.local_addr().unwrap());
        assert_eq!(packet, payload);
    }
    // Current-thread execution cannot drain the queue until this test yields.
    let mut full = false;
    for _ in 0..65 {
        if let Err(error) = relay.forward(Bytes::from_static(b"bounded"), source).await {
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock);
            full = true;
            break;
        }
    }
    assert!(full);
    relay.close().await.unwrap();
    relay.close().await.unwrap();
    assert!(!relay.owns_peer(source));
    assert_eq!(
        relay.forward(induction(), source).await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
}

#[tokio::test]
async fn relay_ipv6_private_loopback_and_parent_cancellation() {
    let public = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let private = UdpSocket::bind("[::1]:0").await.unwrap();
    let external = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let stop = CancellationToken::new();
    let relay = SrtUdpRelay::new(
        public,
        private.local_addr().unwrap(),
        Arc::new(TransportRuntime::default()),
        stop.clone(),
    )
    .unwrap();
    relay
        .forward(induction(), external.local_addr().unwrap())
        .await
        .unwrap();
    let (packet, source) = receive(&private).await;
    assert!(source.is_ipv6());
    assert_eq!(packet, induction());
    private.send_to(b"native reply", source).await.unwrap();
    assert_eq!(
        receive(&external).await.0,
        Bytes::from_static(b"native reply")
    );
    stop.cancel();
    relay.close().await.unwrap();
    assert!(!relay.owns_peer(external.local_addr().unwrap()));
}

struct Echo;
impl JsonRpcProtocol for Echo {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move { Some(Bytes::copy_from_slice(input)) })
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn native_encrypted_rpc_fragments_reconnect_and_drain_through_one_public_udp_reader() {
    use json_rpc_2_core::SrtConfig;
    use json_rpc_2_datagram::{
        client::transports::srt::SrtClientTransport, server::transports::srt::SrtTransport,
    };

    let (selected, mut selected_ids) = tokio::sync::mpsc::unbounded_channel();
    let listener = SrtTransport::bind(
        "127.0.0.1:0".parse().unwrap(),
        SrtConfig {
            passphrase: "shared-srt-relay-secret".into(),
        },
    )
    .await
    .unwrap();

    let listener = {
        use json_rpc_2_datagram::client::transports::srt::{RAW_SRT_STREAM_ID, WS_SRT_STREAM_ID};
        struct WsEcho;
        impl JsonRpcProtocol for WsEcho {
            fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
                Box::pin(async move {
                    let message = json_rpc_2_core::decode_websocket_message(input).unwrap()?;
                    Some(Bytes::from(
                        json_rpc_2_core::encode_websocket_message(&message).unwrap(),
                    ))
                })
            }
        }
        let ws: Arc<dyn JsonRpcProtocol> = Arc::new(WsEcho);
        listener.with_protocol_selector(move |id| {
            selected.send(id.to_owned()).unwrap();
            match id {
                "" | RAW_SRT_STREAM_ID => Ok(Arc::new(Echo)),
                WS_SRT_STREAM_ID => Ok(ws.clone()),
                _ => Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unknown shared SRT stream ID",
                )),
            }
        })
    };
    let private = listener.local_addr().unwrap();
    let running = listener.start(Arc::new(Echo));
    let public = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let address = public.local_addr().unwrap();
    let stop = CancellationToken::new();
    let relay = Arc::new(
        SrtUdpRelay::new(
            public.clone(),
            private,
            Arc::new(TransportRuntime::default()),
            stop.clone(),
        )
        .unwrap(),
    );
    let ingress_relay = relay.clone();
    let ingress_stop = stop.clone();
    let ingress = tokio::spawn(async move {
        let mut buffer = vec![0; 65_535];
        loop {
            tokio::select! {
                _ = ingress_stop.cancelled() => break,
                packet = public.recv_from(&mut buffer) => {
                    let (length, source) = packet.unwrap();
                    assert!(ingress_relay.owns_peer(source) || SrtUdpRelay::is_handshake(&buffer[..length]));
                    ingress_relay.forward(Bytes::copy_from_slice(&buffer[..length]), source).await.unwrap();
                }
            }
        }
    });
    let client = SrtClientTransport::connect(address, "shared-srt-relay-secret")
        .await
        .unwrap();
    let payload = Bytes::from(format!(
        r#"{{"method":"echo","id":1,"params":"{}"}}"#,
        "x".repeat(80_000)
    ));
    assert_eq!(
        client.send(payload.clone()).await.unwrap(),
        Some(payload.clone())
    );
    client.reconnect().await.unwrap();
    assert_eq!(client.send(payload.clone()).await.unwrap(), Some(payload));
    client.transport_ping().await.unwrap();

    let ws = {
        use json_rpc_2_websockets::client::transports::srt::SrtWebSocketClientTransport;
        let ws = SrtWebSocketClientTransport::connect(address, "shared-srt-relay-secret")
            .await
            .unwrap();
        let payload = Bytes::from_static(br#"{"method":"ws","id":2}"#);
        assert_eq!(
            ws.send(payload.clone()).await.unwrap(),
            Some(payload.clone())
        );
        ws.reconnect().await.unwrap();
        assert_eq!(ws.send(payload.clone()).await.unwrap(), Some(payload));
        use json_rpc_2_datagram::client::transports::srt::WS_SRT_STREAM_ID;
        for expected in ["", "", WS_SRT_STREAM_ID, WS_SRT_STREAM_ID] {
            assert_eq!(selected_ids.recv().await.unwrap(), expected);
        }
        ws
    };
    // Keep public relay alive while native shutdown exchanges its drain barrier.
    running.close().await.unwrap();
    client.close().await.unwrap();

    ws.close().await.unwrap();
    stop.cancel();
    ingress.await.unwrap();
    relay.close().await.unwrap();
}

#[tokio::test]
async fn idle_peer_expires_but_incoming_native_keepalives_retain_the_mapping() {
    let public = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
    let private = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let relay = SrtUdpRelay::new(
        public,
        private.local_addr().unwrap(),
        Arc::new(TransportRuntime::default()),
        CancellationToken::new(),
    )
    .unwrap();
    let idle_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let alive_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let idle = idle_socket.local_addr().unwrap();
    let alive = alive_socket.local_addr().unwrap();
    relay.forward(induction(), idle).await.unwrap();
    relay.forward(induction(), alive).await.unwrap();
    let mut relay_sockets = std::collections::HashMap::new();
    let mut buffer = vec![0; 2048];
    for _ in 0..2 {
        let (_, relay_socket) = private.recv_from(&mut buffer).await.unwrap();
        let external = relay.external_peer(relay_socket).unwrap();
        relay_sockets.insert(external, relay_socket);
    }
    assert_eq!(relay.external_peer(idle), None);
    // Drain private traffic so OS buffers do not affect the eviction test.
    let drain = tokio::spawn(async move {
        let mut buffer = vec![0; 2048];
        loop {
            private.recv_from(&mut buffer).await.unwrap();
        }
    });
    for _ in 0..32 {
        tokio::time::sleep(Duration::from_secs(1)).await;
        relay
            .forward(Bytes::from_static(b"native keepalive packet"), alive)
            .await
            .unwrap();
    }
    assert!(!relay.owns_peer(idle));
    assert!(relay.owns_peer(alive));
    assert_eq!(relay.external_peer(relay_sockets[&idle]), None);
    assert_eq!(relay.external_peer(relay_sockets[&alive]), Some(alive));
    relay.close().await.unwrap();
    assert_eq!(relay.external_peer(relay_sockets[&alive]), None);
    drain.abort();
    assert!(drain.await.unwrap_err().is_cancelled());
}
