use gateway_protocol::{decode_response, encode_request, GATEWAY_PING, GATEWAY_PONG};
mod connector;

use std::{
    collections::{BTreeMap, HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Context;
use bytes::Bytes;
use json_rpc_2_transport::RpcClientTransport;
use serde::Deserialize;
use tokio::{
    sync::Mutex,
    time::{timeout, Instant},
};

#[derive(Clone, Debug, Deserialize)]
pub struct BackendConfig {
    pub id: String,
    pub host: String,
    pub ports: HashMap<String, u16>,
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    #[serde(default)]
    pub options: BackendOptions,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct BackendOptions {
    pub tls_ca_path: Option<String>,
    pub tls_server_name: Option<String>,
    pub srt_passphrase_env: Option<String>,
    pub mqtt_url_env: Option<String>,
    pub mqtt_request_topic: Option<String>,
    pub ws_mqtt_request_topic: Option<String>,
    pub amqp_url_env: Option<String>,
    pub amqp_request_queue: Option<String>,
    pub ws_amqp_request_queue: Option<String>,
    pub operation_timeout_ms: u64,
    pub health_check_interval_ms: u64,
    pub broker_heartbeat_interval_ms: u64,
    pub broker_session_lease_ms: u64,
}

impl Default for BackendOptions {
    fn default() -> Self {
        Self {
            tls_ca_path: None,
            tls_server_name: None,
            srt_passphrase_env: None,
            mqtt_url_env: None,
            mqtt_request_topic: None,
            ws_mqtt_request_topic: None,
            amqp_url_env: None,
            amqp_request_queue: None,
            ws_amqp_request_queue: None,
            operation_timeout_ms: 30_000,
            health_check_interval_ms: 30_000,
            broker_heartbeat_interval_ms: 10_000,
            broker_session_lease_ms: 45_000,
        }
    }
}

fn default_pool_size() -> usize {
    4
}

pub trait LoadBalancingStrategy: Send + Sync {
    fn select(&self, candidates: usize) -> usize;
}

#[derive(Default)]
pub struct RoundRobin {
    cursor: AtomicUsize,
}

impl LoadBalancingStrategy for RoundRobin {
    fn select(&self, candidates: usize) -> usize {
        assert!(candidates > 0, "cannot select from an empty candidate set");
        self.cursor.fetch_add(1, Ordering::Relaxed) % candidates
    }
}

pub struct BackendPool {
    backends: Vec<BackendPoolEntry>,
    strategy: Arc<dyn LoadBalancingStrategy>,
    affinity: std::sync::Mutex<HashMap<String, usize>>,
}

struct BackendPoolEntry {
    config: BackendConfig,
    transports: BTreeMap<String, TransportPool>,
}

struct TransportPool {
    port: u16,
    slots: Vec<Mutex<ConnectionSlot>>,
    cursor: RoundRobin,
}

#[derive(Default)]
struct ConnectionSlot {
    connection: Option<ManagedConnection>,
    checked_at: Option<Instant>,
}

struct ManagedConnection {
    transport: Arc<dyn RpcClientTransport>,
    close_timeout: Duration,
}

impl Drop for ManagedConnection {
    fn drop(&mut self) {
        let transport = self.transport.clone();
        let deadline = self.close_timeout;
        if let Ok(runtime) = tokio::runtime::Handle::try_current() {
            runtime.spawn(async move {
                match timeout(deadline, transport.close()).await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => tracing::warn!(%error, "backend transport close failed"),
                    Err(_) => tracing::warn!("backend transport close timed out"),
                }
            });
        }
    }
}

impl BackendPool {
    pub fn new(configs: Vec<BackendConfig>) -> anyhow::Result<Self> {
        Self::with_strategy(configs, Arc::new(RoundRobin::default()))
    }

    pub fn with_strategy(
        configs: Vec<BackendConfig>,
        strategy: Arc<dyn LoadBalancingStrategy>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            !configs.is_empty(),
            "backend pool must contain at least one backend"
        );
        let mut ids = HashSet::new();
        let mut backends = Vec::new();
        for config in configs {
            anyhow::ensure!(!config.id.trim().is_empty(), "backend id must not be empty");
            anyhow::ensure!(
                ids.insert(config.id.clone()),
                "duplicate backend id: {}",
                config.id
            );
            anyhow::ensure!(
                !config.host.trim().is_empty(),
                "backend {} host must not be empty",
                config.id
            );
            anyhow::ensure!(
                config.pool_size > 0,
                "backend {} pool_size must be greater than zero",
                config.id
            );
            anyhow::ensure!(
                !config.ports.is_empty(),
                "backend {} must define at least one transport port",
                config.id
            );
            anyhow::ensure!(
                config.options.operation_timeout_ms > 0
                    && config.options.health_check_interval_ms > 0,
                "backend {} timeouts must be greater than zero",
                config.id
            );
            let sessions = gateway_sessions::BrokerSessionPolicy {
                heartbeat_interval: Duration::from_millis(
                    config.options.broker_heartbeat_interval_ms,
                ),
                session_lease: Duration::from_millis(config.options.broker_session_lease_ms),
            };
            sessions.validate()?;
            let mut transports = BTreeMap::new();
            for (kind, &port) in &config.ports {
                anyhow::ensure!(
                    connector::supported(kind),
                    "backend {} transport {kind} is unsupported or disabled",
                    config.id
                );
                anyhow::ensure!(
                    port > 0,
                    "backend {} transport {kind} port must be nonzero",
                    config.id
                );
                if matches!(
                    kind.as_str(),
                    "tls" | "quic" | "http3" | "ws-tls" | "ws-quic" | "ws-http3"
                ) {
                    anyhow::ensure!(
                        config.options.tls_ca_path.is_some(),
                        "backend {} transport {kind} requires tls_ca_path",
                        config.id
                    );
                }
                if matches!(kind.as_str(), "srt" | "ws-srt") {
                    anyhow::ensure!(
                        config.options.srt_passphrase_env.is_some(),
                        "backend {} transport {kind} requires srt_passphrase_env",
                        config.id
                    );
                }
                transports.insert(
                    kind.clone(),
                    TransportPool {
                        port,
                        slots: (0..config.pool_size)
                            .map(|_| Mutex::new(ConnectionSlot::default()))
                            .collect(),
                        cursor: RoundRobin::default(),
                    },
                );
            }
            backends.push(BackendPoolEntry { config, transports });
        }
        Ok(Self {
            backends,
            strategy,
            affinity: Default::default(),
        })
    }

    /// Pin a logical QUIC session independently of its current network path.
    pub fn open_session(&self, connection_id: &str, transport: &str) -> anyhow::Result<()> {
        if !matches!(transport, "quic" | "ws-quic") {
            return Ok(());
        }
        let mut affinity = self.affinity.lock().unwrap();
        if let std::collections::hash_map::Entry::Vacant(entry) =
            affinity.entry(connection_id.to_owned())
        {
            let index = self.strategy.select(self.backends.len());
            anyhow::ensure!(
                index < self.backends.len(),
                "load balancing strategy selected an invalid backend index"
            );
            entry.insert(index);
        }
        Ok(())
    }

    pub fn close_session(&self, connection_id: &str) {
        self.affinity.lock().unwrap().remove(connection_id);
    }

    /// Establish and validate all configured connections before accepting traffic.
    pub async fn warm_up(&self) -> anyhow::Result<()> {
        for backend in &self.backends {
            for (kind, pool) in &backend.transports {
                for slot in &pool.slots {
                    let mut slot = slot.lock().await;
                    self.check_slot(backend, kind, pool.port, &mut slot).await?;
                }
            }
        }
        Ok(())
    }

    /// Prefer the ingress transport; fall back to TCP, then a deterministic configured transport.
    fn route<'a>(backend: &'a BackendPoolEntry, source: &str) -> (&'a str, &'a TransportPool) {
        backend
            .transports
            .get_key_value(source)
            .or_else(|| backend.transports.get_key_value("tcp"))
            .or_else(|| backend.transports.first_key_value())
            .map(|(kind, pool)| (kind.as_str(), pool))
            .expect("backend transport map is validated as nonempty")
    }

    /// Forward once, without replay. QUIC sources require `open_session` first;
    /// their lifecycle owner must call `close_session` when ingress disconnects.
    pub async fn forward(
        &self,
        payload: &Bytes,
        source_transport: &str,
        connection_id: &str,
    ) -> anyhow::Result<Option<Bytes>> {
        let request = encode_request(payload, connection_id, source_transport)?;
        let index = if matches!(source_transport, "quic" | "ws-quic") {
            *self
                .affinity
                .lock()
                .unwrap()
                .get(connection_id)
                .context("QUIC session is not registered or has already closed")?
        } else {
            self.strategy.select(self.backends.len())
        };
        let backend = self
            .backends
            .get(index)
            .context("load balancing strategy selected an invalid backend index")?;
        let (kind, pool) = Self::route(backend, source_transport);
        let mut slot = pool.slots[pool.cursor.select(pool.slots.len())]
            .lock()
            .await;
        if slot.connection.is_none()
            || slot
                .checked_at
                .is_none_or(|at| at.elapsed() >= health_interval(backend))
        {
            self.check_slot(backend, kind, pool.port, &mut slot).await?;
        }
        // Taking ownership invalidates the slot on error, timeout, or future cancellation.
        // Never replay an exchange: the backend may already have executed it.
        let connection = slot
            .connection
            .take()
            .context("backend slot has no connection after validation")?;
        tracing::debug!(backend = %backend.config.id, transport = kind, source_transport, connection_id, "forwarding opaque gateway message");
        let result = timeout(
            operation_timeout(backend),
            connection.transport.send_message(request, true),
        )
        .await;
        let response = match result {
            Ok(Ok(Some(frame))) => decode_response(frame)?,
            Ok(Ok(None)) => anyhow::bail!(
                "backend {} returned no gateway acknowledgement",
                backend.config.id
            ),
            Ok(Err(error)) => {
                return Err(error).with_context(|| {
                    format!(
                        "backend {} transport {kind} exchange failed",
                        backend.config.id
                    )
                })
            }
            Err(_) => anyhow::bail!(
                "backend {} transport {kind} exchange timed out; request was not replayed",
                backend.config.id
            ),
        };
        slot.connection = Some(connection);
        Ok(response)
    }

    async fn check_slot(
        &self,
        backend: &BackendPoolEntry,
        kind: &str,
        port: u16,
        slot: &mut ConnectionSlot,
    ) -> anyhow::Result<()> {
        let previous = slot.connection.take();
        let connection = timeout(operation_timeout(backend), async {
            let connection = match previous {
                Some(connection) => connection,
                None => ManagedConnection {
                    transport: connector::connect(&backend.config, kind, port).await?,
                    close_timeout: operation_timeout(backend),
                },
            };
            connection.transport.transport_ping().await?;
            let acknowledgement = connection
                .transport
                .send_message(
                    json_rpc_2_transport::TransportMessage::binary(Bytes::from_static(
                        GATEWAY_PING,
                    )),
                    true,
                )
                .await?;
            anyhow::ensure!(
                acknowledgement.as_deref() == Some(GATEWAY_PONG),
                "backend must enable gateway framing"
            );
            Ok::<_, anyhow::Error>(connection)
        })
        .await
        .with_context(|| {
            format!(
                "backend {} transport {kind} health check timed out",
                backend.config.id
            )
        })?
        .with_context(|| {
            format!(
                "backend {} transport {kind} health check failed",
                backend.config.id
            )
        })?;
        slot.connection = Some(connection);
        slot.checked_at = Some(Instant::now());
        Ok(())
    }

    pub fn start_maintenance(self: &Arc<Self>) {
        let weak = Arc::downgrade(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let Some(pool) = weak.upgrade() else { break };
                for backend in &pool.backends {
                    for (kind, transport) in &backend.transports {
                        for slot in &transport.slots {
                            let Ok(mut slot) = slot.try_lock() else {
                                continue;
                            };
                            if slot
                                .checked_at
                                .is_none_or(|at| at.elapsed() >= health_interval(backend))
                            {
                                if let Err(error) = pool
                                    .check_slot(backend, kind, transport.port, &mut slot)
                                    .await
                                {
                                    slot.checked_at = Some(Instant::now());
                                    tracing::warn!(backend = %backend.config.id, transport = kind, %error, "backend connection removed after failed validation");
                                }
                            }
                        }
                    }
                }
            }
        });
    }
}

fn operation_timeout(backend: &BackendPoolEntry) -> Duration {
    Duration::from_millis(backend.config.options.operation_timeout_ms)
}

fn health_interval(backend: &BackendPoolEntry) -> Duration {
    Duration::from_millis(backend.config.options.health_check_interval_ms)
}

#[cfg(test)]
mod tests {
    use super::*;
    use json_rpc_2_runtime::{MultiTransportServer, TransportKind};
    use json_rpc_2_server::RpcServer;
    use tokio::net::TcpListener;

    fn config(port: u16, kind: &str) -> BackendConfig {
        BackendConfig {
            id: "backend".into(),
            host: "127.0.0.1".into(),
            ports: [(kind.to_owned(), port)].into_iter().collect(),
            pool_size: 1,
            options: BackendOptions::default(),
        }
    }

    #[test]
    fn round_robin_and_validation() {
        let strategy = RoundRobin::default();
        assert_eq!(
            (strategy.select(2), strategy.select(2), strategy.select(2)),
            (0, 1, 0)
        );
        assert!(BackendPool::new(vec![]).is_err());
        assert!(BackendPool::new(vec![config(1, "unknown")]).is_err());
        assert!(BackendPool::new(vec![config(0, "http1")]).is_err());
        assert!(BackendPool::new(vec![config(1, "http1")]).is_ok());
    }

    #[test]
    fn native_route_precedes_tcp_and_fallback_is_deterministic() {
        let mut config = config(80, "http1");
        config.ports.insert("tcp".into(), 81);
        let pool = BackendPool::new(vec![config]).unwrap();
        assert_eq!(BackendPool::route(&pool.backends[0], "http1").0, "http1");
        assert_eq!(BackendPool::route(&pool.backends[0], "ws-tcp").0, "tcp");
    }

    #[test]
    fn connector_registry_covers_every_enabled_upstream_transport() {
        for &kind in TransportKind::ALL {
            assert!(
                connector::supported(kind.id()),
                "missing connector for {}",
                kind.id()
            );
        }
    }

    async fn assert_opaque_backend(
        kind: TransportKind,
        mut settings: BackendConfig,
        mut server: MultiTransportServer,
    ) {
        let broker = matches!(
            kind,
            TransportKind::Mqtt
                | TransportKind::WebSocketMqtt
                | TransportKind::Amqp
                | TransportKind::WebSocketAmqp
        );
        server.set_bind(["127.0.0.1".parse().unwrap()]);
        if !broker {
            server.set_port(kind, 0);
        }
        let running = server.start().await.unwrap();
        if !broker {
            let (_, address) = running
                .listeners()
                .find(|(candidate, _)| *candidate == kind)
                .unwrap();
            settings.ports = [(kind.id().to_owned(), address.port())]
                .into_iter()
                .collect();
        }
        let pool = BackendPool::new(vec![settings]).unwrap();
        pool.warm_up().await.unwrap();
        for _ in 0..2 {
            assert!(pool
                .forward(&Bytes::new(), "ws-tls", "opaque-client")
                .await
                .unwrap()
                .is_none());
            let payload = Bytes::from_static(b"\0\xff\r\nopaque-payload");
            assert_eq!(
                pool.forward(&payload, "ws-tls", "opaque-client")
                    .await
                    .unwrap()
                    .unwrap(),
                payload
            );
        }
        drop(pool);
        running.close().await.unwrap();
    }

    struct OpaqueEcho;

    impl json_rpc_2_transport::JsonRpcProtocol for OpaqueEcho {
        fn handle_message<'a>(&'a self, _: &'a [u8]) -> json_rpc_2_transport::ProtocolFuture<'a> {
            panic!("gateway context must be provided");
        }

        fn handle_message_with_context<'a>(
            &'a self,
            input: &'a [u8],
            context: &'a json_rpc_2_transport::TransportContext,
        ) -> json_rpc_2_transport::ProtocolFuture<'a> {
            assert_eq!(context.connection_id, "opaque-client");
            assert_eq!(context.transport, "ws-tls");
            Box::pin(async move { (!input.is_empty()).then(|| Bytes::copy_from_slice(input)) })
        }
    }

    #[tokio::test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[ignore = "requires UDP-Lite kernel support"]
    async fn udplite_backends_preserve_opaque_binary_and_notification_ack() {
        for kind in [TransportKind::UdpLite, TransportKind::WebSocketUdpLite] {
            assert_opaque_backend(
                kind,
                config(1, kind.id()),
                MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(
                    Arc::new(OpaqueEcho),
                ))),
            )
            .await;
        }
    }

    #[tokio::test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[ignore = "requires SCTP kernel support"]
    async fn sctp_backends_preserve_opaque_binary_and_notification_ack() {
        let kind = TransportKind::Sctp;
        assert_opaque_backend(
            kind,
            config(1, kind.id()),
            MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(
                Arc::new(OpaqueEcho),
            ))),
        )
        .await;
    }

    #[tokio::test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[ignore = "requires SCTP kernel support"]
    async fn ws_sctp_backends_preserve_opaque_binary_and_notification_ack() {
        let kind = TransportKind::WebSocketSctp;
        assert_opaque_backend(
            kind,
            config(1, kind.id()),
            MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(
                Arc::new(OpaqueEcho),
            ))),
        )
        .await;
    }

    #[tokio::test]
    #[cfg(any(target_os = "linux", target_os = "android"))]
    #[ignore = "requires DCCP kernel support"]
    async fn dccp_backends_preserve_opaque_binary_and_notification_ack() {
        for kind in [TransportKind::Dccp, TransportKind::WebSocketDccp] {
            assert_opaque_backend(
                kind,
                config(1, kind.id()),
                MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(
                    Arc::new(OpaqueEcho),
                ))),
            )
            .await;
        }
    }

    #[tokio::test]
    #[ignore = "requires MQTT_BROKER_URL and AMQP_BROKER_URL test brokers"]
    async fn broker_backends_preserve_opaque_binary_and_notification_ack() {
        for kind in [
            TransportKind::Mqtt,
            TransportKind::WebSocketMqtt,
            TransportKind::Amqp,
            TransportKind::WebSocketAmqp,
        ] {
            let mut settings = config(1, kind.id());
            let mut server = MultiTransportServer::new(Arc::new(
                gateway_protocol::GatewayBackendProtocol::new(Arc::new(OpaqueEcho)),
            ));
            let namespace = format!(
                "compat-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            );
            match kind {
                TransportKind::Mqtt | TransportKind::WebSocketMqtt => {
                    let broker = json_rpc_2_iot::MqttConfig {
                        broker_url: std::env::var("MQTT_BROKER_URL")
                            .expect("MQTT_BROKER_URL required"),
                        request_topic: format!("json-rpc-2/{namespace}/requests"),
                        ..Default::default()
                    };
                    settings.options.mqtt_url_env = Some("MQTT_BROKER_URL".into());
                    if kind == TransportKind::Mqtt {
                        settings.options.mqtt_request_topic = Some(broker.request_topic.clone());
                        server.set_mqtt_config(broker);
                    } else {
                        settings.options.ws_mqtt_request_topic = Some(broker.request_topic.clone());
                        server.set_ws_mqtt_config(broker);
                    }
                }
                _ => {
                    let broker = json_rpc_2_iot::AmqpConfig {
                        broker_url: std::env::var("AMQP_BROKER_URL")
                            .expect("AMQP_BROKER_URL required"),
                        request_queue: format!("json-rpc-2.{namespace}.requests"),
                        ..Default::default()
                    };
                    settings.options.amqp_url_env = Some("AMQP_BROKER_URL".into());
                    if kind == TransportKind::Amqp {
                        settings.options.amqp_request_queue = Some(broker.request_queue.clone());
                        server.set_amqp_config(broker);
                    } else {
                        settings.options.ws_amqp_request_queue = Some(broker.request_queue.clone());
                        server.set_ws_amqp_config(broker);
                    }
                }
            }
            assert_opaque_backend(kind, settings, server).await;
        }
    }

    #[tokio::test]
    async fn all_available_local_carriers_preserve_metadata_notifications_and_reuse() {
        for kind in [
            TransportKind::Tcp,
            TransportKind::Http1,
            TransportKind::Udp,
            TransportKind::Rudp,
            TransportKind::Rtp,
            TransportKind::Coap,
            TransportKind::WebSocketTcp,
            TransportKind::WebSocketHttp1,
            TransportKind::WebSocketUdp,
            TransportKind::WebSocketRtp,
            TransportKind::WebSocketRudp,
            TransportKind::WebSocketCoap,
        ] {
            let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = probe.local_addr().unwrap().port();
            drop(probe);
            let rpc = Arc::new(RpcServer::new());
            rpc.register_with_context("echo", |params, context| async move {
                let context = context.unwrap();
                assert_eq!(context.connection_id, "client-1");
                assert_eq!(context.transport, "ws-tcp");
                Ok(params)
            })
            .unwrap();
            let mut server = MultiTransportServer::new(Arc::new(
                gateway_protocol::GatewayBackendProtocol::new(rpc),
            ));
            server
                .set_bind(["127.0.0.1".parse().unwrap()])
                .set_port(kind, port);
            let running = server.start().await.unwrap();
            let pool = BackendPool::new(vec![config(port, kind.id())]).unwrap();
            pool.warm_up().await.unwrap();
            let slot = pool.backends[0].transports[kind.id()].slots[0].lock().await;
            let original = Arc::downgrade(&slot.connection.as_ref().unwrap().transport);
            drop(slot);
            for _ in 0..2 {
                let notification =
                    Bytes::from_static(b"{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"params\":[1]}");
                assert!(
                    pool.forward(&notification, "ws-tcp", "client-1")
                        .await
                        .unwrap()
                        .is_none(),
                    "{}",
                    kind.id()
                );
                let request = Bytes::from_static(b"{\n\"jsonrpc\":\"2.0\",\n\"method\":\"echo\",\"params\":{\"value\":1},\"id\":7\n}");
                let response = pool
                    .forward(&request, "ws-tcp", "client-1")
                    .await
                    .unwrap()
                    .unwrap();
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&response).unwrap()["result"]
                        ["value"],
                    1
                );
            }
            let slot = pool.backends[0].transports[kind.id()].slots[0].lock().await;
            assert!(
                original.ptr_eq(&Arc::downgrade(
                    &slot.connection.as_ref().unwrap().transport
                )),
                "connection was replaced on {}",
                kind.id()
            );
            drop(slot);
            drop(pool);
            running.close().await.unwrap();
        }
    }

    #[tokio::test]
    async fn secure_carriers_validate_ca_and_forward_context() {
        let certified = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let path = std::env::temp_dir().join(format!(
            "json-rpc-balancer-ca-{}-{}.pem",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        tokio::fs::write(&path, certified.cert.pem()).await.unwrap();
        for kind in [
            TransportKind::Tls,
            TransportKind::WebSocketTls,
            TransportKind::Quic,
            TransportKind::Http3,
            TransportKind::WebSocketQuic,
            TransportKind::WebSocketHttp3,
        ] {
            let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = probe.local_addr().unwrap().port();
            drop(probe);
            let rpc = Arc::new(RpcServer::new());
            rpc.register_with_context("echo", |params, context| async move {
                assert_eq!(context.unwrap().connection_id, "secure-client");
                Ok(params)
            })
            .unwrap();
            let mut server = MultiTransportServer::new(Arc::new(
                gateway_protocol::GatewayBackendProtocol::new(rpc),
            ));
            server
                .set_bind(["127.0.0.1".parse().unwrap()])
                .set_port(kind, port)
                .set_tls(
                    vec![certified.cert.der().clone()],
                    rustls_pki_types::PrivatePkcs8KeyDer::from(certified.key_pair.serialize_der())
                        .into(),
                );
            let running = server.start().await.unwrap();
            let mut settings = config(port, kind.id());
            settings.options.tls_ca_path = Some(path.to_str().unwrap().to_owned());
            settings.options.tls_server_name = Some("localhost".into());
            if kind == TransportKind::WebSocketTls {
                let mut invalid = settings.clone();
                invalid.options.tls_server_name = Some("wrong-host.invalid".into());
                assert!(BackendPool::new(vec![invalid])
                    .unwrap()
                    .warm_up()
                    .await
                    .is_err());
            }
            let pool = BackendPool::new(vec![settings]).unwrap();
            pool.warm_up().await.unwrap();
            let request = Bytes::from_static(
                b"{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"params\":[1],\"id\":1}",
            );
            let notification =
                Bytes::from_static(b"{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"params\":[1]}");
            pool.open_session("secure-client", kind.id()).unwrap();
            assert!(pool
                .forward(&notification, kind.id(), "secure-client")
                .await
                .unwrap()
                .is_none());
            assert!(
                pool.forward(&request, "tcp", "secure-client")
                    .await
                    .unwrap()
                    .is_some(),
                "{}",
                kind.id()
            );
            drop(pool);
            running.close().await.unwrap();
        }
        tokio::fs::remove_file(path).await.unwrap();
    }

    #[tokio::test]
    async fn timeout_discards_socket_without_replaying_and_next_request_recovers() {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        let calls = Arc::new(AtomicUsize::new(0));
        let handler_calls = calls.clone();
        let rpc = Arc::new(RpcServer::new());
        rpc.register("slow", move |_| {
            let calls = handler_calls.clone();
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                tokio::time::sleep(Duration::from_millis(200)).await;
                Ok(serde_json::Value::Null)
            }
        })
        .unwrap();
        rpc.register("echo", |params| async move { Ok(params) })
            .unwrap();
        let mut server =
            MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(rpc)));
        server
            .set_bind(["127.0.0.1".parse().unwrap()])
            .set_tcp_port(port);
        let running = server.start().await.unwrap();
        let mut settings = config(port, "tcp");
        settings.options.operation_timeout_ms = 50;
        let pool = BackendPool::new(vec![settings]).unwrap();
        pool.warm_up().await.unwrap();
        let slow = Bytes::from_static(b"{\"jsonrpc\":\"2.0\",\"method\":\"slow\",\"id\":1}");
        assert!(pool.forward(&slow, "tcp", "client").await.is_err());
        assert!(pool.backends[0].transports["tcp"].slots[0]
            .lock()
            .await
            .connection
            .is_none());
        let echo = Bytes::from_static(
            b"{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"params\":[2],\"id\":2}",
        );
        let response = pool.forward(&echo, "tcp", "client").await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response).unwrap()["id"],
            2
        );
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        drop(pool);
        running.close().await.unwrap();
    }

    #[tokio::test]
    async fn maintenance_evicts_closed_connection_and_warmup_rejects_plain_backends() {
        let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = probe.local_addr().unwrap().port();
        drop(probe);
        let rpc = Arc::new(RpcServer::new());
        let mut server = MultiTransportServer::new(rpc);
        server
            .set_bind(["127.0.0.1".parse().unwrap()])
            .set_tcp_port(port);
        let running = server.start().await.unwrap();
        let mut settings = config(port, "tcp");
        settings.options.operation_timeout_ms = 100;
        let pool = BackendPool::new(vec![settings]).unwrap();
        assert!(pool.warm_up().await.is_err());
        drop(pool);
        running.close().await.unwrap();

        let rpc = Arc::new(RpcServer::new());
        let mut server =
            MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(rpc)));
        server
            .set_bind(["127.0.0.1".parse().unwrap()])
            .set_tcp_port(port);
        let running = server.start().await.unwrap();
        let mut settings = config(port, "tcp");
        settings.options.health_check_interval_ms = 10;
        settings.options.operation_timeout_ms = 100;
        let pool = Arc::new(BackendPool::new(vec![settings]).unwrap());
        pool.warm_up().await.unwrap();
        running.close().await.unwrap();
        pool.start_maintenance();
        timeout(Duration::from_secs(2), async {
            loop {
                if pool.backends[0].transports["tcp"].slots[0]
                    .lock()
                    .await
                    .connection
                    .is_none()
                {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }

    #[cfg(feature = "srt")]
    #[tokio::test]
    #[ignore = "requires encrypted libsrt and BALANCER_TEST_SRT_PASSPHRASE; run with scripts/build-with-srt.sh"]
    async fn srt_pools_forward_notifications_and_responses() {
        let passphrase = std::env::var("BALANCER_TEST_SRT_PASSPHRASE").unwrap();
        for kind in [TransportKind::Srt, TransportKind::WebSocketSrt] {
            let probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = probe.local_addr().unwrap().port();
            drop(probe);
            let rpc = Arc::new(RpcServer::new());
            rpc.register_with_context("echo", |params, context| async move {
                assert_eq!(context.unwrap().connection_id, "srt-client");
                Ok(params)
            })
            .unwrap();
            let mut server = MultiTransportServer::new(Arc::new(
                gateway_protocol::GatewayBackendProtocol::new(rpc),
            ));
            server
                .set_bind(["127.0.0.1".parse().unwrap()])
                .set_port(kind, port)
                .set_srt_config(json_rpc_2_datagram::SrtConfig {
                    passphrase: passphrase.clone(),
                });
            let running = server.start().await.unwrap();
            let mut settings = config(port, kind.id());
            settings.options.srt_passphrase_env = Some("BALANCER_TEST_SRT_PASSPHRASE".into());
            let pool = BackendPool::new(vec![settings]).unwrap();
            pool.warm_up().await.unwrap();
            let notification = Bytes::from_static(b"{\"jsonrpc\":\"2.0\",\"method\":\"echo\"}");
            assert!(pool
                .forward(&notification, "tcp", "srt-client")
                .await
                .unwrap()
                .is_none());
            let request = Bytes::from_static(
                b"{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"params\":[3],\"id\":3}",
            );
            assert!(pool
                .forward(&request, "tcp", "srt-client")
                .await
                .unwrap()
                .is_some());
            drop(pool);
            running.close().await.unwrap();
        }
    }
}
