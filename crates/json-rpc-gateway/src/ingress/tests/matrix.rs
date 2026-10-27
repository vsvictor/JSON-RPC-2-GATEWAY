use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use json_rpc_2_client::{RpcClient, RpcClientTransport, TransportError};
#[cfg(feature = "srt")]
use json_rpc_2_core::SrtConfig;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_datagram::client::transports::dccp::DccpClientTransport;

use json_rpc_2_datagram::client::transports::rtp::RtpClientTransport;

use json_rpc_2_datagram::client::transports::rudp::RudpClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_datagram::client::transports::sctp::SctpClientTransport;
#[cfg(feature = "srt")]
use json_rpc_2_datagram::client::transports::srt::SrtClientTransport;
use json_rpc_2_datagram::client::transports::udp::UdpClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_datagram::client::transports::udplite::UdpLiteClientTransport;

use json_rpc_2_iot::client::transports::amqp::AmqpClientTransport;

use json_rpc_2_iot::client::transports::coap::CoapClientTransport;

use json_rpc_2_iot::client::transports::mqtt::MqttClientTransport;
use json_rpc_2_quic::client::transports::{http3::Http3ClientTransport, quic::QuicClientTransport};
use json_rpc_2_runtime::{H3_ALPN, QUIC_ALPN, TransportKind, WS_QUIC_ALPN};
use json_rpc_2_server::{JsonRpcProtocol, RpcServer};

use json_rpc_2_tcp::client::transports::tls::TlsClientTransport;
use json_rpc_2_tcp::client::transports::{http1::Http1ClientTransport, tcp::TcpClientTransport};

use json_rpc_2_websockets::client::transports::amqp::AmqpWebSocketClientTransport;

use json_rpc_2_websockets::client::transports::coap::CoapWebSocketClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_websockets::client::transports::dccp::DccpWebSocketClientTransport;

use json_rpc_2_websockets::client::transports::mqtt::MqttWebSocketClientTransport;

use json_rpc_2_websockets::client::transports::rtp::RtpWebSocketClientTransport;

use json_rpc_2_websockets::client::transports::rudp::RudpWebSocketClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_websockets::client::transports::sctp::SctpWebSocketClientTransport;
#[cfg(feature = "srt")]
use json_rpc_2_websockets::client::transports::srt::SrtWebSocketClientTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_websockets::client::transports::udplite::UdpLiteWebSocketClientTransport;
use json_rpc_2_websockets::client::transports::{
    http1::Http1WebSocketClientTransport, http3::Http3WebSocketClientTransport,
    quic::QuicWebSocketClientTransport, tcp::TcpWebSocketClientTransport,
    udp::UdpWebSocketClientTransport,
};
use quinn::crypto::rustls::QuicClientConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use serde_json::{Value, json};

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

trait DynTransport: RpcClientTransport {}
impl<T: RpcClientTransport> DynTransport for T {}

type DynClient = RpcClient<Arc<dyn DynTransport>>;
type DynTransportConnect =
    std::pin::Pin<Box<dyn Future<Output = Result<Arc<dyn DynTransport>, TransportError>> + Send>>;

#[allow(dead_code)]
fn make_server() -> Arc<RpcServer> {
    let server = Arc::new(RpcServer::new());
    server
        .register("echo", |params| async move { Ok(params) })
        .unwrap();
    server
        .register("delay_echo", |params| async move {
            let delay_ms = params.get("delay_ms").and_then(Value::as_u64).unwrap_or(25);
            tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            Ok(params.get("value").cloned().unwrap_or(Value::Null))
        })
        .unwrap();
    server
}

fn identity() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let generated =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
    let key = PrivateKeyDer::Pkcs8(generated.key_pair.serialize_der().into());
    (generated.cert.into(), key)
}

fn client_endpoint(
    ip: IpAddr,
    certificate: &CertificateDer<'static>,
    alpn: &[u8],
) -> quinn::Endpoint {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate.clone()).unwrap();
    let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_protocol_versions(&[&rustls::version::TLS13])
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    tls.alpn_protocols = vec![alpn.to_vec()];
    let mut endpoint = quinn::Endpoint::client(SocketAddr::new(ip, 0)).unwrap();
    endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
        QuicClientConfig::try_from(tls).unwrap(),
    )));
    endpoint
}

fn boxed<T: RpcClientTransport>(
    connect: impl Future<Output = Result<T, TransportError>> + Send + 'static,
) -> DynTransportConnect {
    Box::pin(async move { Ok(Arc::new(connect.await?) as Arc<dyn DynTransport>) })
}

async fn connect(
    kind: TransportKind,
    to: SocketAddr,
    certificate: &CertificateDer<'static>,
) -> Arc<dyn DynTransport> {
    let server_name = to.ip().to_string();
    let connect = match kind {
        TransportKind::Tcp => boxed(TcpClientTransport::connect(to)),

        TransportKind::Tls => {
            let certificate = certificate.clone();
            let server_name = server_name.clone();
            boxed(async move {
                TlsClientTransport::connect_with_certificates(
                    to,
                    &server_name,
                    &[certificate],
                    json_rpc_2_transport::TcpTuningConfig::default(),
                )
                .await
            })
        }
        TransportKind::Udp => boxed(UdpClientTransport::connect(to)),

        TransportKind::Rtp => boxed(RtpClientTransport::connect(to)),

        TransportKind::Rudp => boxed(RudpClientTransport::connect(to)),

        TransportKind::Mqtt => boxed(MqttClientTransport::connect(to)),

        TransportKind::Coap => boxed(CoapClientTransport::connect(to)),

        TransportKind::Amqp => boxed(AmqpClientTransport::connect(to)),
        #[cfg(feature = "srt")]
        TransportKind::Srt => boxed(SrtClientTransport::connect(to, "test-passphrase")),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::UdpLite => boxed(UdpLiteClientTransport::connect(to)),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::Dccp => boxed(DccpClientTransport::connect(to)),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::Sctp => boxed(SctpClientTransport::connect(to)),
        TransportKind::Quic => {
            let endpoint = client_endpoint(to.ip(), certificate, QUIC_ALPN);
            boxed(async move { QuicClientTransport::connect(endpoint, to, &server_name).await })
        }
        TransportKind::Http1 => boxed(Http1ClientTransport::connect(to)),
        TransportKind::Http3 => {
            let endpoint = client_endpoint(to.ip(), certificate, H3_ALPN);
            boxed(async move { Http3ClientTransport::connect(endpoint, to, &server_name).await })
        }
        TransportKind::WebSocketTcp => {
            boxed(async move { TcpWebSocketClientTransport::connect(&format!("ws://{to}")).await })
        }
        TransportKind::WebSocketUdp => boxed(UdpWebSocketClientTransport::connect(to)),

        TransportKind::WebSocketRtp => boxed(RtpWebSocketClientTransport::connect(to)),

        TransportKind::WebSocketRudp => boxed(RudpWebSocketClientTransport::connect(to)),

        TransportKind::WebSocketMqtt => boxed(MqttWebSocketClientTransport::connect(to)),

        TransportKind::WebSocketCoap => boxed(CoapWebSocketClientTransport::connect(to)),

        TransportKind::WebSocketAmqp => boxed(AmqpWebSocketClientTransport::connect(to)),
        #[cfg(feature = "srt")]
        TransportKind::WebSocketSrt => {
            boxed(SrtWebSocketClientTransport::connect(to, "test-passphrase"))
        }
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::WebSocketUdpLite => boxed(UdpLiteWebSocketClientTransport::connect(to)),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::WebSocketDccp => boxed(DccpWebSocketClientTransport::connect(to)),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        TransportKind::WebSocketSctp => boxed(SctpWebSocketClientTransport::connect(to)),
        TransportKind::WebSocketQuic => {
            let endpoint = client_endpoint(to.ip(), certificate, WS_QUIC_ALPN);
            boxed(
                async move { QuicWebSocketClientTransport::connect(endpoint, to, &server_name).await },
            )
        }
        TransportKind::WebSocketHttp1 => {
            boxed(
                async move { Http1WebSocketClientTransport::connect(&format!("ws://{to}/ws")).await },
            )
        }

        TransportKind::WebSocketTls => {
            let certificate = certificate.clone();
            boxed(async move {
                Http1WebSocketClientTransport::connect_with_certificates_to_addr(
                    &format!("wss://{to}/ws"),
                    to,
                    &[certificate],
                    json_rpc_2_transport::TcpTuningConfig::default(),
                )
                .await
            })
        }
        TransportKind::WebSocketHttp3 => {
            let endpoint = client_endpoint(to.ip(), certificate, H3_ALPN);
            boxed(async move {
                Http3WebSocketClientTransport::connect(endpoint, to, &server_name, "/ws").await
            })
        }
    };
    connect.await.unwrap()
}

async fn shared_port_matrix(kernel: bool) {
    let protocol = Arc::new(RpcServer::new());
    protocol
        .register_with_context("context", |_, context| async move {
            Ok(json!(context.unwrap().transport))
        })
        .unwrap();
    let (certificate, key) = identity();
    let probe = tokio::net::TcpListener::bind((LOCALHOST, 0)).await.unwrap();
    let port = probe.local_addr().unwrap().port();
    let udp_probe = tokio::net::UdpSocket::bind((LOCALHOST, port))
        .await
        .unwrap();
    drop(probe);
    drop(udp_probe);
    let mut server = crate::ingress::GatewayIngress::new(protocol);
    server
        .set_bind([LOCALHOST])
        .set_tls(vec![certificate.clone()], key);
    #[cfg(feature = "srt")]
    server.set_srt_config(SrtConfig {
        passphrase: "test-passphrase".into(),
    });
    for &kind in TransportKind::ALL {
        if kind.is_broker() || kind.id().contains("dccp") {
            continue;
        }
        let needs_kernel = kind.id().contains("sctp") || kind.id().contains("udplite");
        if needs_kernel != kernel {
            continue;
        }
        server.set_port(kind, port);
    }
    let running = server.start().await.unwrap();
    assert!(running.listeners().count() > 1);
    assert!(
        running
            .listeners()
            .all(|(_, address)| address.port() == port)
    );
    let calls = running.listeners().map(|(kind, address)| {
        let certificate = certificate.clone();
        async move {
            let transport = tokio::time::timeout(
                Duration::from_secs(15),
                connect(kind, address, &certificate),
            )
            .await
            .unwrap_or_else(|_| panic!("{} shared-port connect timeout", kind.id()));
            transport
                .transport_ping()
                .await
                .unwrap_or_else(|error| panic!("{} ping: {error}", kind.id()));
            let client = DynClient::new(transport);
            let value: String = tokio::time::timeout(
                Duration::from_secs(15),
                client.request("context", json!({})),
            )
            .await
            .unwrap_or_else(|_| panic!("{} shared-port RPC timeout", kind.id()))
            .unwrap_or_else(|error| panic!("{} shared-port RPC: {error}", kind.id()));
            assert_eq!(value, kind.id());
            client.close().await.unwrap();
        }
    });
    futures_util::future::join_all(calls).await;
    tokio::time::timeout(Duration::from_secs(15), running.close())
        .await
        .unwrap()
        .unwrap();
    // Every physical listener is released, not merely its logical aliases.
    let tcp = tokio::net::TcpListener::bind((LOCALHOST, port))
        .await
        .unwrap();
    let udp = tokio::net::UdpSocket::bind((LOCALHOST, port))
        .await
        .unwrap();
    drop((tcp, udp));
}

#[tokio::test]
async fn shared_port_parallel_wire_profiles_preserve_context_and_rebind() {
    shared_port_matrix(false).await;
}

async fn hot_policy_matrix(kernel: bool) {
    use crate::{
        gateway::{DispatcherLimits, Gateway},
        transport_policy::{TransportPolicy, tests::StateFile},
    };
    use bytes::Bytes;
    use json_rpc_2_runtime::MultiTransportServer;
    use json_rpc_2_transport::TransportCapabilities;
    use json_rpc_balancer::{BackendConfig, BackendPool};
    let rpc = make_server();
    rpc.register_with_context("session_id", |_, context| async move {
        Ok(json!(context.unwrap().connection_id))
    })
    .unwrap();
    let backend =
        MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(rpc)))
            .set_bind([LOCALHOST])
            .set_tcp_port(0)
            .start()
            .await
            .unwrap();
    let pool = BackendPool::new(vec![BackendConfig {
        id: "backend".into(),
        host: "127.0.0.1".into(),
        ports: [("tcp".into(), backend.listeners().next().unwrap().1.port())]
            .into_iter()
            .collect(),
        pool_size: 1,
        options: Default::default(),
    }])
    .unwrap();
    let kinds = TransportKind::ALL
        .iter()
        .copied()
        .filter(|kind| {
            !kind.is_broker()
                && !kind.id().contains("dccp")
                && (kind.id().contains("sctp") || kind.id().contains("udplite")) == kernel
        })
        .collect::<Vec<_>>();
    let file = StateFile::new();
    let policy = TransportPolicy::load(kinds.iter().copied(), file.0.clone()).unwrap();
    let gateway = Gateway::with_policy(pool, DispatcherLimits::default(), policy.clone()).unwrap();
    let (certificate, key) = identity();
    let mut ingress = crate::ingress::GatewayIngress::new(gateway.multiplexed_protocol());
    ingress
        .set_bind([LOCALHOST])
        .set_tls(vec![certificate.clone()], key);
    #[cfg(feature = "srt")]
    ingress.set_srt_config(SrtConfig {
        passphrase: "test-passphrase".into(),
    });
    for kind in &kinds {
        ingress.set_port(*kind, 0);
    }
    let running = ingress.start().await.unwrap();
    let listeners = running.listeners().collect::<Vec<_>>();
    if !kernel {
        let address = listeners
            .iter()
            .find(|(kind, _)| *kind == TransportKind::Http3)
            .unwrap()
            .1;
        mixed_http3_hot_policy(&policy, address, &certificate).await;
    }
    let request = Bytes::from_static(br#"{"jsonrpc":"2.0","method":"echo","params":[17],"id":1}"#);
    for &(kind, address) in &listeners {
        let stateless = matches!(
            kind.id().strip_prefix("ws-").unwrap_or(kind.id()),
            "udp" | "rtp" | "rudp" | "coap" | "udplite"
        );
        let client = connect(kind, address, &certificate).await;
        let value = tokio::time::timeout(Duration::from_secs(3), client.send(request.clone()))
            .await
            .unwrap_or_else(|_| panic!("{} initial RPC timed out", kind.id()))
            .unwrap()
            .unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&value).unwrap()["result"],
            json!([17]),
            "{}",
            kind.id()
        );
        let connection_id = if !stateless && !matches!(kind.id(), "http1" | "http3") {
            let response = client
                .send(Bytes::from_static(
                    br#"{"jsonrpc":"2.0","method":"session_id","id":9}"#,
                ))
                .await
                .unwrap()
                .unwrap();
            let id = serde_json::from_slice::<Value>(&response).unwrap()["result"]
                .as_str()
                .unwrap()
                .to_owned();
            gateway
                .sessions()
                .register_client(&id, "hot-probe".into(), Vec::new())
                .unwrap();
            Some(id)
        } else {
            None
        };
        policy.set_enabled(kind.id(), false).await.unwrap();
        if let Some(id) = connection_id {
            tokio::time::timeout(Duration::from_millis(500), async {
                loop {
                    if gateway
                        .sessions()
                        .register_client(&id, "hot-probe".into(), Vec::new())
                        .is_err()
                    {
                        break;
                    }
                    tokio::task::yield_now().await;
                }
            })
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{} did not remove the closed session registry entry",
                    kind.id()
                )
            });
        }
        let denied =
            tokio::time::timeout(Duration::from_millis(500), client.send(request.clone())).await;
        match denied {
            Ok(Ok(Some(value))) => panic!(
                "{} returned a response while disabled: {value:?}",
                kind.id()
            ),
            Err(_) if !stateless => panic!(
                "{} did not close its active session within 500 ms",
                kind.id()
            ),
            _ => {}
        }
        let &(other, other_address) = listeners
            .iter()
            .find(|(other, _)| other.socket() == kind.socket() && *other != kind)
            .unwrap();
        let survivor = connect(other, other_address, &certificate).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(3), survivor.send(request.clone()))
                .await
                .unwrap_or_else(|_| panic!("{} affected unrelated {}", kind.id(), other.id()))
                .unwrap()
                .is_some()
        );
        survivor.close().await.unwrap();
        policy.set_enabled(kind.id(), true).await.unwrap();
        let reopened = connect(kind, address, &certificate).await;
        assert!(
            tokio::time::timeout(Duration::from_secs(3), reopened.send(request.clone()))
                .await
                .unwrap_or_else(|_| panic!("{} did not reopen", kind.id()))
                .unwrap()
                .is_some()
        );
        reopened.close().await.unwrap();
        let _ = tokio::time::timeout(Duration::from_secs(2), client.close()).await;
    }
    tokio::time::timeout(Duration::from_secs(5), running.close())
        .await
        .unwrap()
        .unwrap();
    backend.close().await.unwrap();
}

async fn mixed_http3_hot_policy(
    policy: &crate::transport_policy::TransportPolicy,
    address: SocketAddr,
    certificate: &CertificateDer<'static>,
) {
    use bytes::{Buf, Bytes};
    use http::{Method, Request, StatusCode};
    use json_rpc_2_quic::client::transports::http3::{H3Sender, close_h3, connect_h3};
    async fn rpc(sender: &mut H3Sender) -> StatusCode {
        let mut stream = sender
            .send_request(
                Request::builder()
                    .method(Method::POST)
                    .uri("https://localhost/")
                    .body(())
                    .unwrap(),
            )
            .await
            .unwrap();
        stream
            .send_data(Bytes::from_static(
                br#"{"jsonrpc":"2.0","method":"echo","params":[17],"id":1}"#,
            ))
            .await
            .unwrap();
        stream.finish().await.unwrap();
        let status = stream.recv_response().await.unwrap().status();
        while stream.recv_data().await.unwrap().is_some() {}
        status
    }
    let endpoint = client_endpoint(LOCALHOST, certificate, H3_ALPN);
    let (mut sender, driver) = connect_h3(&endpoint, address, "localhost", true)
        .await
        .unwrap();
    assert_eq!(rpc(&mut sender).await, StatusCode::OK);
    let mut request = Request::builder()
        .method(Method::CONNECT)
        .uri("https://localhost/ws")
        .header("sec-websocket-version", "13")
        .header("sec-websocket-key", "dGhlIHNhbXBsZSBub25jZQ==")
        .body(())
        .unwrap();
    request
        .extensions_mut()
        .insert(h3::ext::Protocol::WEBSOCKET);
    let mut websocket = sender.send_request(request).await.unwrap();
    assert_eq!(
        websocket.recv_response().await.unwrap().status(),
        StatusCode::OK
    );
    policy.set_enabled("http3", false).await.unwrap();
    let payload = br#"{"jsonrpc":"2.0","method":"echo","params":[17],"id":2}"#;
    let mut frame = vec![0x81, 0x80 | payload.len() as u8, 1, 2, 3, 4];
    frame.extend(
        payload
            .iter()
            .enumerate()
            .map(|(index, byte)| byte ^ [1, 2, 3, 4][index % 4]),
    );
    websocket.send_data(Bytes::from(frame)).await.unwrap();
    let mut response = Vec::new();
    loop {
        let mut data = websocket.recv_data().await.unwrap().unwrap();
        response.extend_from_slice(&data.copy_to_bytes(data.remaining()));
        if response.len() < 2 {
            continue;
        }
        let (header, length) = match response[1] & 0x7f {
            126 if response.len() >= 4 => {
                (4, u16::from_be_bytes([response[2], response[3]]) as usize)
            }
            length @ 0..=125 => (2, usize::from(length)),
            _ => continue,
        };
        if response.len() >= header + length {
            assert_eq!(
                serde_json::from_slice::<Value>(&response[header..header + length]).unwrap()["result"],
                json!([17])
            );
            break;
        }
    }
    assert_eq!(rpc(&mut sender).await, StatusCode::SERVICE_UNAVAILABLE);
    policy.set_enabled("http3", true).await.unwrap();
    policy.set_enabled("ws-http3", false).await.unwrap();
    let closed = tokio::time::timeout(Duration::from_secs(1), websocket.recv_data())
        .await
        .expect("disabled HTTP/3 websocket stream remained open");
    assert!(closed.is_err() || closed.unwrap().is_none());
    assert_eq!(
        rpc(&mut sender).await,
        StatusCode::OK,
        "raw HTTP/3 must survive on the same connection"
    );
    policy.set_enabled("ws-http3", true).await.unwrap();
    drop(websocket);
    close_h3(&endpoint, sender, driver).await.unwrap();
}

#[tokio::test]
async fn hot_policy_matrix_covers_every_userspace_wire_profile() {
    tokio::time::timeout(Duration::from_secs(90), hot_policy_matrix(false))
        .await
        .unwrap();
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
#[ignore = "requires SCTP and UDP-Lite kernel support"]
async fn hot_policy_matrix_covers_kernel_wire_profiles() {
    tokio::time::timeout(Duration::from_secs(30), hot_policy_matrix(true))
        .await
        .unwrap();
}

#[cfg(any(target_os = "linux", target_os = "android"))]
#[tokio::test]
#[ignore = "requires SCTP and UDP-Lite kernel support"]
async fn shared_port_kernel_wire_profiles_preserve_context_and_rebind() {
    shared_port_matrix(true).await;
}

#[tokio::test]
async fn shared_port_datagrams_keep_non_json_payloads_opaque() {
    struct Echo;
    impl JsonRpcProtocol for Echo {
        fn handle_message<'a>(&'a self, input: &'a [u8]) -> json_rpc_2_server::ProtocolFuture<'a> {
            Box::pin(async move { Some(bytes::Bytes::copy_from_slice(input)) })
        }
    }
    let mut server = crate::ingress::GatewayIngress::new(Arc::new(Echo));
    server.set_bind([LOCALHOST]);
    for &kind in TransportKind::ALL {
        if matches!(
            kind.id(),
            "udp" | "ws-udp" | "rtp" | "ws-rtp" | "rudp" | "ws-rudp" | "coap" | "ws-coap"
        ) {
            server.set_port(kind, 0);
        }
    }
    let running = server.start().await.unwrap();
    let (certificate, _) = identity();
    let address = running.listeners().next().unwrap().1;
    assert!(running.listeners().all(|(_, bound)| bound == address));
    for (kind, _) in running.listeners() {
        let transport = connect(kind, address, &certificate).await;
        let input = bytes::Bytes::from_static(b"\0\xffopaque-without-json");
        let output = transport
            .send_with_expectation(input.clone(), true)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(output, input, "{}", kind.id());
        transport.close().await.unwrap();
    }
    running.close().await.unwrap();
}

#[tokio::test]
async fn failed_start_releases_already_started_shared_listeners() {
    let occupied = tokio::net::UdpSocket::bind((LOCALHOST, 0)).await.unwrap();
    let port = occupied.local_addr().unwrap().port();
    for shared_datagram in [true, false] {
        let mut ingress = crate::ingress::GatewayIngress::new(Arc::new(RpcServer::new()));
        ingress.set_bind([LOCALHOST]);
        for kind in [TransportKind::Tcp, TransportKind::Http1, TransportKind::Udp] {
            ingress.set_port(kind, port);
        }
        if shared_datagram {
            ingress.set_port(TransportKind::Quic, port);
        }

        let error = match ingress.start().await {
            Ok(running) => {
                running.close().await.unwrap();
                panic!("startup unexpectedly succeeded");
            }
            Err(error) => error,
        };
        assert_eq!(error.kind(), std::io::ErrorKind::AddrInUse);
        let tcp = tokio::net::TcpListener::bind((LOCALHOST, port))
            .await
            .unwrap();
        drop(tcp);
    }
}

#[tokio::test]
async fn missing_identity_releases_preceding_shared_tcp_listener() {
    let probe = tokio::net::TcpListener::bind((LOCALHOST, 0)).await.unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);
    let mut ingress = crate::ingress::GatewayIngress::new(Arc::new(RpcServer::new()));
    ingress.set_bind([LOCALHOST]);
    for kind in [
        TransportKind::Tcp,
        TransportKind::Http1,
        TransportKind::Udp,
        TransportKind::Quic,
    ] {
        ingress.set_port(kind, port);
    }
    let error = match ingress.start().await {
        Ok(running) => {
            running.close().await.unwrap();
            panic!("startup without TLS identity unexpectedly succeeded");
        }
        Err(error) => error,
    };
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
    let tcp = tokio::net::TcpListener::bind((LOCALHOST, port))
        .await
        .unwrap();
    let udp = tokio::net::UdpSocket::bind((LOCALHOST, port))
        .await
        .unwrap();
    drop((tcp, udp));
}
