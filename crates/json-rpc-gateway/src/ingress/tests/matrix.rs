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
