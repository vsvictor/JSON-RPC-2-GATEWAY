//! Gateway-owned listener grouping, protocol policy and demultiplexing.
mod shared_association;
mod shared_quic;
#[cfg(feature = "srt")]
mod shared_srt;
mod shared_tcp;
mod shared_udp;
#[cfg(test)]
mod tests;
use std::{
    io,
    net::{IpAddr, SocketAddr},
    sync::Arc,
};

use quinn::crypto::rustls::QuicServerConfig;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};

use tokio::net::TcpListener;

#[cfg(feature = "srt")]
use json_rpc_2_core::SrtConfig;
use json_rpc_2_core::UdpLiteConfig;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_datagram::server::transports::dccp::DccpTransport;
use json_rpc_2_runtime::{
    H3_ALPN, QUIC_ALPN, TransportCapabilities, TransportSocket, WS_QUIC_ALPN,
};

use json_rpc_2_datagram::server::transports::rtp::RtpTransport;

use json_rpc_2_datagram::server::transports::rudp::RudpTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_datagram::server::transports::sctp::SctpTransport;
#[cfg(feature = "srt")]
use json_rpc_2_datagram::server::transports::srt::SrtTransport;

use json_rpc_2_datagram::server::transports::udp::UdpTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_datagram::server::transports::udplite::UdpLiteTransport;

pub use json_rpc_2_iot::AmqpConfig;

pub use json_rpc_2_iot::MqttConfig;

use json_rpc_2_iot::server::transports::coap::CoapTransport;

use json_rpc_2_server::JsonRpcProtocol;

use json_rpc_2_transport::TLS_ALPN;
use json_rpc_2_transport::TcpTuningConfig;
use json_rpc_2_transport::{RunningTransport, Transport, TransportRuntime};

use json_rpc_2_websockets::server::transports::coap::CoapWebSocketTransport;

use json_rpc_2_websockets::server::transports::rtp::RtpWebSocketTransport;

use json_rpc_2_websockets::server::transports::rudp::RudpWebSocketTransport;

use json_rpc_2_websockets::server::transports::udp::UdpWebSocketTransport;
#[cfg(any(target_os = "linux", target_os = "android"))]
use json_rpc_2_websockets::server::transports::udplite::UdpLiteWebSocketTransport;

use json_rpc_2_websockets::server::transports::{
    JsonRpcWebSocket, WebSocketProtocol, WebSocketTransport,
};

use json_rpc_2_runtime::{MultiTransportServer, RunningServer, TransportKind};

struct Identity {
    certificates: Vec<CertificateDer<'static>>,
    key: PrivateKeyDer<'static>,
}

pub struct GatewayIngress {
    protocol: Arc<dyn JsonRpcProtocol>,
    bind: Vec<IpAddr>,
    ports: std::collections::HashMap<TransportKind, u16>,
    identity: Option<Identity>,
    udp_lite_config: UdpLiteConfig,
    #[cfg(feature = "srt")]
    srt_config: Option<SrtConfig>,
    standalone: MultiTransportServer,
}

impl GatewayIngress {
    pub fn new(protocol: Arc<dyn JsonRpcProtocol>) -> Self {
        Self {
            standalone: MultiTransportServer::new(protocol.clone()),
            protocol,
            bind: Vec::new(),
            ports: Default::default(),
            identity: None,
            udp_lite_config: Default::default(),
            #[cfg(feature = "srt")]
            srt_config: None,
        }
    }

    pub fn set_bind(&mut self, bind: impl IntoIterator<Item = IpAddr>) -> &mut Self {
        self.bind = bind.into_iter().collect();
        self.standalone.set_bind(self.bind.clone());
        self
    }

    pub fn set_port(&mut self, kind: TransportKind, port: u16) -> &mut Self {
        self.ports.insert(kind, port);
        self
    }

    pub fn transports(&self) -> impl Iterator<Item = TransportKind> + '_ {
        TransportKind::ALL
            .iter()
            .copied()
            .filter(|kind| self.ports.contains_key(kind))
    }

    pub fn set_tls(
        &mut self,
        certificates: Vec<CertificateDer<'static>>,
        key: PrivateKeyDer<'static>,
    ) -> &mut Self {
        self.standalone
            .set_tls(certificates.clone(), key.clone_key());
        self.identity = Some(Identity { certificates, key });
        self
    }

    pub fn set_udplite_config(&mut self, config: UdpLiteConfig) -> &mut Self {
        self.standalone.set_udplite_config(config.clone());
        self.udp_lite_config = config;
        self
    }

    #[cfg(feature = "srt")]
    pub fn set_srt_config(&mut self, config: SrtConfig) -> &mut Self {
        self.standalone.set_srt_config(config.clone());
        self.srt_config = Some(config);
        self
    }

    pub fn set_mqtt_config(&mut self, config: MqttConfig) -> &mut Self {
        self.standalone.set_mqtt_config(config);
        self
    }
    #[cfg(feature = "websocket")]
    pub fn set_ws_mqtt_config(&mut self, config: MqttConfig) -> &mut Self {
        self.standalone.set_ws_mqtt_config(config);
        self
    }
    pub fn set_amqp_config(&mut self, config: AmqpConfig) -> &mut Self {
        self.standalone.set_amqp_config(config);
        self
    }
    #[cfg(feature = "websocket")]
    pub fn set_ws_amqp_config(&mut self, config: AmqpConfig) -> &mut Self {
        self.standalone.set_ws_amqp_config(config);
        self
    }

    pub async fn start(mut self) -> io::Result<RunningIngress> {
        if self.ports.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no ingress transports configured",
            ));
        }
        if self.bind.is_empty() && self.ports.keys().any(|kind| !kind.is_broker()) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "no ingress bind address",
            ));
        }
        self.udp_lite_config
            .validate()
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        #[cfg(feature = "srt")]
        if let Some(config) = &self.srt_config {
            config
                .validate()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        }
        let runtime = Arc::new(TransportRuntime::default());
        let mut groups: Vec<Vec<TransportKind>> = Vec::new();
        for kind in self.transports() {
            if kind.is_broker() {
                continue;
            }
            if let Some(group) = groups.iter_mut().find(|group| {
                group[0].socket() == kind.socket() && self.ports[&group[0]] == self.ports[&kind]
            }) {
                group.push(kind);
            } else {
                groups.push(vec![kind]);
            }
        }
        let mut running = RunningIngress {
            listeners: Vec::new(),
            standalone: None,
        };
        for kinds in groups {
            if kinds.len() == 1 {
                self.standalone.set_port(kinds[0], self.ports[&kinds[0]]);
                continue;
            }
            for &ip in &self.bind {
                let address = SocketAddr::new(ip, self.ports[&kinds[0]]);
                match self.listen_shared(kinds.clone(), address, &runtime).await {
                    Ok(listener) => running.listeners.push(listener),
                    Err(error) => {
                        if let Err(close) = running.close().await {
                            tracing::error!(%close, "failed to close partially started ingress");
                        }
                        return Err(io::Error::new(
                            error.kind(),
                            format!("shared ingress on {address}: {error}"),
                        ));
                    }
                }
            }
        }
        for (&kind, &port) in &self.ports {
            // Explicit broker configurations already set their upstream kind.
            if kind.is_broker() && !self.standalone.transports().any(|enabled| enabled == kind) {
                self.standalone.set_port(kind, port);
            }
        }
        if self.standalone.transports().next().is_some() {
            match self.standalone.start().await {
                Ok(server) => running.standalone = Some(server),
                Err(error) => {
                    if let Err(close) = running.close().await {
                        tracing::error!(%close, "failed to close partially started ingress");
                    }
                    return Err(error);
                }
            }
        }
        Ok(running)
    }
    #[allow(unused_variables)]
    async fn listen_shared(
        &self,
        kinds: Vec<TransportKind>,
        address: SocketAddr,
        runtime: &Arc<TransportRuntime>,
    ) -> io::Result<Listener> {
        let socket = kinds[0].socket();
        match socket {
            TransportSocket::Tcp => {
                let listener = bind_tcp(address, &runtime.tcp_tuning)?;
                let address = listener.local_addr()?;
                let tls = if kinds.iter().any(|kind| kind.security().is_tls()) {
                    let mut config = self.tls_server_config(TLS_ALPN)?;
                    Arc::make_mut(&mut config)
                        .alpn_protocols
                        .push(b"http/1.1".to_vec());
                    Some(config)
                } else {
                    None
                };
                let protocol = self.protocol.clone();
                let runtime = runtime.clone();
                let aliases = kinds.clone();
                Ok(Listener {
                    kinds: aliases,
                    address,
                    transport: RunningTransport::spawn(move |shutdown| {
                        Box::pin(crate::ingress::shared_tcp::serve(
                            listener, kinds, protocol, tls, runtime, shutdown,
                        ))
                    }),
                })
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            TransportSocket::Sctp => {
                let websocket = Arc::new(JsonRpcWebSocket::new(self.protocol.clone()));
                let protocol = crate::ingress::shared_association::protocol_adapter(
                    Some(self.protocol.clone()),
                    Some(
                        json_rpc_2_websockets::server::transports::protocol_adapter_with_kind(
                            websocket, "ws-sctp",
                        ),
                    ),
                );
                let transport = SctpTransport::bind(address)?.with_runtime(runtime.clone());
                Ok(Listener {
                    kinds,
                    address: transport.local_addr(),
                    transport: transport.start(protocol),
                })
            }
            #[cfg(any(target_os = "linux", target_os = "android"))]
            TransportSocket::Dccp => {
                let websocket = Arc::new(JsonRpcWebSocket::new(self.protocol.clone()));
                let protocol = crate::ingress::shared_association::protocol_adapter(
                    Some(self.protocol.clone()),
                    Some(
                        json_rpc_2_websockets::server::transports::protocol_adapter_with_kind(
                            websocket, "ws-dccp",
                        ),
                    ),
                );
                let transport = DccpTransport::bind(address)?.with_runtime(runtime.clone());
                Ok(Listener {
                    kinds,
                    address: transport.local_addr(),
                    transport: transport.start(protocol),
                })
            }
            TransportSocket::Udp | TransportSocket::UdpLite => {
                self.listen_shared_udp(kinds, address, runtime).await
            }
            #[cfg(not(any(target_os = "linux", target_os = "android")))]
            _ => Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "unsupported shared socket namespace",
            )),
        }
    }

    async fn listen_shared_udp(
        &self,
        kinds: Vec<TransportKind>,
        address: SocketAddr,
        runtime: &Arc<TransportRuntime>,
    ) -> io::Result<Listener> {
        use json_rpc_2_transport::DatagramReceiver;
        let udplite = kinds[0].socket() == TransportSocket::UdpLite;
        let socket = if udplite {
            #[cfg(any(target_os = "linux", target_os = "android"))]
            {
                bind_udplite(address, &self.udp_lite_config)?
            }
            #[cfg(not(any(target_os = "linux", target_os = "android")))]
            {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "UDP-Lite unavailable",
                ));
            }
        } else {
            bind_udp(address)?
        };
        let address = socket.local_addr()?;
        let capacity = runtime.limits.max_concurrent_requests.clamp(1, 256);

        let (quic, endpoint) = if kinds
            .iter()
            .any(|kind| matches!(kind.id(), "quic" | "ws-quic" | "http3" | "ws-http3"))
        {
            let runtime =
                quinn::default_runtime().ok_or_else(|| io::Error::other("no QUIC runtime"))?;
            let native = runtime.wrap_udp_socket(socket.try_clone()?)?;
            let (ingress, lane) = crate::ingress::shared_udp::quic::lane(native, capacity);
            let mut config = quinn::EndpointConfig::default();
            // Keep the fixed bit mandatory for disjoint long-header/RTP/SRT
            // classification. Otherwise negotiated greasing clears it at random.
            config.grease_quic_bit(false);
            config.cid_generator(|| Box::new(crate::ingress::shared_udp::quic::CidGenerator));
            let mut alpn = Vec::new();
            if kinds.iter().any(|kind| kind.id() == "quic") {
                alpn.push(QUIC_ALPN.to_vec());
            }
            if kinds.iter().any(|kind| kind.id() == "ws-quic") {
                alpn.push(WS_QUIC_ALPN.to_vec());
            }
            if kinds
                .iter()
                .any(|kind| matches!(kind.id(), "http3" | "ws-http3"))
            {
                alpn.push(H3_ALPN.to_vec());
            }
            let endpoint = quinn::Endpoint::new_with_abstract_socket(
                config,
                Some(self.quic_server_config(alpn)?),
                lane,
                runtime,
            )?;
            (Some(ingress), Some(endpoint))
        } else {
            (None, None)
        };
        let socket = Arc::new(tokio::net::UdpSocket::from_std(socket)?);
        #[cfg(feature = "srt")]
        let srt = if kinds
            .iter()
            .any(|kind| matches!(kind.id(), "srt" | "ws-srt"))
        {
            let ip = if address.is_ipv4() {
                IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
            } else {
                IpAddr::V6(std::net::Ipv6Addr::LOCALHOST)
            };
            Some(
                SrtTransport::bind(
                    SocketAddr::new(ip, 0),
                    self.srt_config.clone().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::InvalidInput, "missing SRT configuration")
                    })?,
                )
                .await?
                .with_runtime(runtime.clone()),
            )
        } else {
            None
        };
        let mut lanes = Vec::new();
        let mut receivers = Vec::new();
        for kind in &kinds {
            if matches!(
                kind.id().strip_prefix("ws-").unwrap_or(kind.id()),
                "udp" | "rtp" | "rudp" | "coap" | "udplite"
            ) {
                let (sender, receiver) = tokio::sync::mpsc::channel(capacity);
                lanes.push((*kind, sender));
                receivers.push((*kind, DatagramReceiver::shared(socket.clone(), receiver)));
            }
        }
        let reader_stop = json_rpc_2_transport::CancellationToken::new();
        #[cfg(feature = "srt")]
        let relay = srt
            .as_ref()
            .map(|srt| {
                crate::ingress::shared_srt::SrtUdpRelay::new(
                    socket.clone(),
                    srt.local_addr()?,
                    runtime.clone(),
                    reader_stop.clone(),
                )
                .map(Arc::new)
            })
            .transpose()?;
        let protocol = self.protocol.clone();
        #[allow(unused_variables)]
        let runtime = runtime.clone();
        let aliases = kinds.clone();
        Ok(Listener {
            kinds: aliases,
            address,
            transport: RunningTransport::spawn(move |shutdown| {
                Box::pin(async move {
                    let router = crate::ingress::shared_udp::Router {
                        socket,
                        lanes,

                        quic,
                        #[cfg(feature = "srt")]
                        srt: relay.clone(),
                        udplite,
                    };
                    let mut children = Vec::new();

                    let websocket: Arc<dyn WebSocketProtocol> =
                        Arc::new(JsonRpcWebSocket::new(protocol.clone()));
                    #[allow(unreachable_code, unreachable_patterns, unused_variables)]
                    for (kind, source) in receivers {
                        let child = match kind {
                            TransportKind::Udp => UdpTransport::new(source)
                                .with_runtime(runtime.clone())
                                .start(protocol.clone()),

                            TransportKind::Rtp => RtpTransport::new(source)
                                .with_runtime(runtime.clone())
                                .start(protocol.clone()),

                            TransportKind::Rudp => RudpTransport::new(source)
                                .with_runtime(runtime.clone())
                                .start(protocol.clone()),

                            TransportKind::Coap => CoapTransport::new(source)
                                .with_runtime(runtime.clone())
                                .start(protocol.clone()),
                            #[cfg(any(target_os = "linux", target_os = "android"))]
                            TransportKind::UdpLite => UdpLiteTransport::new(source)
                                .with_runtime(runtime.clone())
                                .start(protocol.clone()),

                            TransportKind::WebSocketUdp => UdpWebSocketTransport::new(source)
                                .with_runtime(runtime.clone())
                                .start(websocket.clone()),

                            TransportKind::WebSocketRtp => RtpWebSocketTransport::new(source)
                                .with_runtime(runtime.clone())
                                .start(websocket.clone()),

                            TransportKind::WebSocketRudp => RudpWebSocketTransport::new(source)
                                .with_runtime(runtime.clone())
                                .start(websocket.clone()),

                            TransportKind::WebSocketCoap => CoapWebSocketTransport::new(source)
                                .with_runtime(runtime.clone())
                                .start(websocket.clone()),
                            #[cfg(any(target_os = "linux", target_os = "android"))]
                            TransportKind::WebSocketUdpLite => {
                                UdpLiteWebSocketTransport::new(source)
                                    .with_runtime(runtime.clone())
                                    .start(websocket.clone())
                            }
                            _ => unreachable!("only datagram lane kinds create receivers"),
                        };
                        children.push(child);
                    }

                    if let Some(endpoint) = endpoint {
                        let protocol = protocol.clone();
                        let runtime = runtime.clone();
                        let kinds = kinds.clone();
                        children.push(RunningTransport::spawn(move |stop| {
                            Box::pin(crate::ingress::shared_quic::serve(
                                endpoint, kinds, protocol, runtime, stop,
                            ))
                        }));
                    }
                    #[cfg(feature = "srt")]
                    if let Some(srt) = srt {
                        let raw = kinds.iter().any(|kind| kind.id() == "srt");
                        let raw_protocol = protocol.clone();

                        let ws_protocol =
                            json_rpc_2_websockets::server::transports::protocol_adapter_with_kind(
                                websocket, "ws-srt",
                            );

                        let ws = kinds.iter().any(|kind| kind.id() == "ws-srt");
                        let srt = srt.with_protocol_selector(move |id| {
                            if raw && matches!(id, "" | "json-rpc-2/srt/1") {
                                return Ok(raw_protocol.clone());
                            }

                            if ws && id == "json-rpc-2/ws-srt/1" {
                                return Ok(ws_protocol.clone());
                            }
                            Err(io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "disabled SRT stream ID",
                            ))
                        });
                        children.push(srt.start(protocol));
                    }
                    let reader_token = reader_stop.clone();
                    let mut reader = tokio::spawn(async move { router.run(reader_token).await });
                    let (mut result, reader_consumed) = tokio::select! {
                        _ = shutdown.cancelled() => (Ok(()), false),
                        result = &mut reader => (result.map_err(io::Error::other).and_then(|result| result), true),
                    };
                    // Keep public I/O alive while native sessions perform close.
                    for child in children {
                        if let Err(error) = child.close().await {
                            Self::log_shared_close_error(&mut result, error);
                        }
                    }
                    #[cfg(feature = "srt")]
                    if let Some(relay) = relay
                        && let Err(error) = relay.close().await
                    {
                        Self::log_shared_close_error(&mut result, error);
                    }
                    reader_stop.cancel();
                    if !reader_consumed
                        && let Err(error) = reader
                            .await
                            .map_err(io::Error::other)
                            .and_then(|result| result)
                    {
                        Self::log_shared_close_error(&mut result, error);
                    }
                    result
                })
            }),
        })
    }

    fn tls_server_config(&self, alpn: &[u8]) -> io::Result<Arc<rustls::ServerConfig>> {
        let identity = self
            .identity
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no TLS certificate"))?;
        let mut config = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .map_err(io::Error::other)?
        .with_no_client_auth()
        .with_single_cert(identity.certificates.clone(), identity.key.clone_key())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        if !alpn.is_empty() {
            config.alpn_protocols = vec![alpn.to_vec()];
        }
        Ok(Arc::new(config))
    }

    fn quic_server_config(&self, alpn: Vec<Vec<u8>>) -> io::Result<quinn::ServerConfig> {
        let identity = self
            .identity
            .as_ref()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no TLS certificate"))?;
        let mut tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(io::Error::other)?
        .with_no_client_auth()
        .with_single_cert(identity.certificates.clone(), identity.key.clone_key())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        tls.alpn_protocols = alpn;
        let crypto = QuicServerConfig::try_from(tls).map_err(io::Error::other)?;
        Ok(quinn::ServerConfig::with_crypto(Arc::new(crypto)))
    }

    fn log_shared_close_error(result: &mut io::Result<()>, error: io::Error) {
        tracing::error!(%error, "shared listener shutdown failed");
        if result.is_ok() {
            *result = Err(error);
        }
    }
}

struct Listener {
    kinds: Vec<TransportKind>,
    address: SocketAddr,
    transport: RunningTransport,
}

pub struct RunningIngress {
    listeners: Vec<Listener>,
    standalone: Option<RunningServer>,
}

impl RunningIngress {
    pub fn listeners(&self) -> impl Iterator<Item = (TransportKind, SocketAddr)> + '_ {
        self.listeners
            .iter()
            .flat_map(|listener| listener.kinds.iter().map(|kind| (*kind, listener.address)))
            .chain(self.standalone.iter().flat_map(|server| server.listeners()))
    }

    pub async fn close(self) -> io::Result<()> {
        let mut result = Ok(());
        for listener in self.listeners {
            if let Err(error) = listener.transport.close().await {
                GatewayIngress::log_shared_close_error(&mut result, error);
            }
        }
        if let Some(server) = self.standalone
            && let Err(error) = server.close().await
        {
            GatewayIngress::log_shared_close_error(&mut result, error);
        }
        result
    }
}
/// A socket bound to `address`. An IPv6 socket accepts IPv6 only
/// (`IPV6_V6ONLY`), so `0.0.0.0` and `::` can be bound side by side on every OS.
fn bind_socket(
    address: SocketAddr,
    kind: socket2::Type,
    _tcp_tuning: &TcpTuningConfig,
) -> io::Result<socket2::Socket> {
    let socket = socket2::Socket::new(socket2::Domain::for_address(address), kind, None)?;
    if address.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    #[cfg(unix)]
    if kind == socket2::Type::STREAM {
        // Like `TcpListener::bind`: restart without waiting for TIME_WAIT.
        socket.set_reuse_address(true)?;
    }

    if kind == socket2::Type::STREAM {
        json_rpc_2_transport::apply_tcp_listener_tuning(&socket, _tcp_tuning)?;
    }
    socket.bind(&address.into())?;
    Ok(socket)
}

fn bind_tcp(address: SocketAddr, tcp_tuning: &TcpTuningConfig) -> io::Result<TcpListener> {
    let socket = bind_socket(address, socket2::Type::STREAM, tcp_tuning)?;
    socket.listen(1024)?;
    socket.set_nonblocking(true)?;
    TcpListener::from_std(socket.into())
}

fn bind_udp(address: SocketAddr) -> io::Result<std::net::UdpSocket> {
    let socket = bind_socket(address, socket2::Type::DGRAM, &TcpTuningConfig::default())?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}

#[cfg(any(target_os = "linux", target_os = "android"))]
fn bind_udplite(address: SocketAddr, config: &UdpLiteConfig) -> io::Result<std::net::UdpSocket> {
    let socket = socket2::Socket::new(
        socket2::Domain::for_address(address),
        socket2::Type::DGRAM,
        Some(socket2::Protocol::from(libc::IPPROTO_UDPLITE)),
    )?;
    if address.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    json_rpc_2_transport::apply_udp_lite_coverage(&socket, config)?;
    socket.bind(&address.into())?;
    socket.set_nonblocking(true)?;
    Ok(socket.into())
}
