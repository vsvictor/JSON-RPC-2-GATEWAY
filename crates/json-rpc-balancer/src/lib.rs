use gateway_protocol::{decode_response, encode_request, GATEWAY_PING, GATEWAY_PONG};
mod connector;

pub const VERSION: &str = env!("CARGO_PKG_VERSION");

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use anyhow::Context;
use bytes::Bytes;
use json_rpc_2_transport::RpcClientTransport;
use serde::{Deserialize, Serialize};
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
    pub circuit_breaker_failure_threshold: usize,
    pub circuit_breaker_reset_timeout_ms: u64,
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
            circuit_breaker_failure_threshold: 5,
            circuit_breaker_reset_timeout_ms: 10_000,
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

#[derive(Debug, Serialize)]
pub struct BackendSnapshot {
    pub id: String,
    pub host: String,
    pub transports: Vec<TransportSnapshot>,
}

#[derive(Debug, Serialize)]
pub struct MethodInventory {
    pub methods: Vec<String>,
    pub method_details: Vec<json_rpc_2_server::MethodDescription>,
    pub complete: bool,
    pub errors: Vec<MethodDiscoveryError>,
}

#[derive(Debug, Serialize)]
pub struct MethodDiscoveryError {
    pub backend_id: String,
    pub error: &'static str,
}

#[derive(Debug, Serialize)]
pub struct TransportSnapshot {
    pub transport: String,
    pub port: u16,
    pub pool_size: usize,
    pub connected_idle_slots: usize,
    pub empty_idle_slots: usize,
    pub busy_slots: usize,
    pub circuit: CircuitSnapshot,
}

#[derive(Debug, Serialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum CircuitSnapshot {
    Closed { consecutive_failures: usize },
    Open { retry_after_ms: u64 },
    HalfOpen { probe_in_flight: bool },
}

struct BackendPoolEntry {
    config: BackendConfig,
    transports: BTreeMap<String, TransportPool>,
}

struct TransportPool {
    port: u16,
    slots: Vec<Mutex<ConnectionSlot>>,
    cursor: RoundRobin,
    circuit_breaker: CircuitBreaker,
}

struct CircuitBreaker {
    failure_threshold: usize,
    reset_timeout: Duration,
    backend_id: String,
    transport: String,
    endpoint: String,
    state: std::sync::Mutex<CircuitState>,
}

enum CircuitState {
    Closed { consecutive_failures: usize },
    Open { opened_at: Instant },
    HalfOpen { probe_in_flight: bool },
}

struct CircuitPermit<'a> {
    breaker: &'a CircuitBreaker,
    half_open: bool,
    completed: bool,
}

impl CircuitBreaker {
    fn new(backend_id: &str, transport: &str, options: &BackendOptions) -> Self {
        let breaker = Self {
            failure_threshold: options.circuit_breaker_failure_threshold,
            reset_timeout: Duration::from_millis(options.circuit_breaker_reset_timeout_ms),
            backend_id: backend_id.to_owned(),
            transport: transport.to_owned(),
            endpoint: format!("{backend_id}/{transport}"),
            state: std::sync::Mutex::new(CircuitState::Closed {
                consecutive_failures: 0,
            }),
        };
        breaker.set_open_metric(false);
        breaker
    }

    fn set_open_metric(&self, open: bool) {
        metrics::gauge!(
            "gateway_backend_circuit_open",
            "backend" => self.backend_id.clone(),
            "transport" => self.transport.clone()
        )
        .set(if open { 1.0 } else { 0.0 });
    }

    fn record_probe(&self, result: &'static str) {
        metrics::counter!(
            "gateway_backend_circuit_probe_total",
            "backend" => self.backend_id.clone(),
            "transport" => self.transport.clone(),
            "result" => result
        )
        .increment(1);
    }

    fn acquire(&self) -> anyhow::Result<CircuitPermit<'_>> {
        let mut state = self.state.lock().unwrap();
        let half_open = match &mut *state {
            CircuitState::Closed { .. } => false,
            CircuitState::Open { opened_at } if opened_at.elapsed() >= self.reset_timeout => {
                *state = CircuitState::HalfOpen {
                    probe_in_flight: true,
                };
                true
            }
            CircuitState::Open { .. } => {
                anyhow::bail!("backend endpoint {} circuit breaker is open", self.endpoint)
            }
            CircuitState::HalfOpen {
                probe_in_flight: false,
            } => {
                *state = CircuitState::HalfOpen {
                    probe_in_flight: true,
                };
                true
            }
            CircuitState::HalfOpen {
                probe_in_flight: true,
            } => anyhow::bail!(
                "backend endpoint {} circuit breaker half-open probe is in progress",
                self.endpoint
            ),
        };
        Ok(CircuitPermit {
            breaker: self,
            half_open,
            completed: false,
        })
    }

    fn is_closed(&self) -> bool {
        matches!(&*self.state.lock().unwrap(), CircuitState::Closed { .. })
    }

    fn record_maintenance_result(&self, succeeded: bool) {
        let mut state = self.state.lock().unwrap();
        let CircuitState::Closed {
            consecutive_failures,
        } = &mut *state
        else {
            return;
        };

        if succeeded {
            *consecutive_failures = 0;
        } else {
            *consecutive_failures += 1;
            if *consecutive_failures >= self.failure_threshold {
                *state = CircuitState::Open {
                    opened_at: Instant::now(),
                };
                self.set_open_metric(true);
                tracing::warn!(
                    endpoint = %self.endpoint,
                    failures = self.failure_threshold,
                    reset_timeout_ms = self.reset_timeout.as_millis(),
                    "backend circuit breaker opened after maintenance health check failures"
                );
            }
        }
    }
}

impl CircuitPermit<'_> {
    fn is_half_open(&self) -> bool {
        self.half_open
    }

    fn complete(mut self, succeeded: bool) {
        self.record(succeeded);
        self.completed = true;
    }

    fn record(&self, succeeded: bool) {
        let mut state = self.breaker.state.lock().unwrap();
        match (&mut *state, self.half_open, succeeded) {
            (
                CircuitState::Closed {
                    consecutive_failures,
                },
                false,
                true,
            ) => {
                *consecutive_failures = 0;
            }
            (
                CircuitState::Closed {
                    consecutive_failures,
                },
                false,
                false,
            ) => {
                *consecutive_failures += 1;
                if *consecutive_failures >= self.breaker.failure_threshold {
                    *state = CircuitState::Open {
                        opened_at: Instant::now(),
                    };
                    self.breaker.set_open_metric(true);
                    tracing::warn!(
                        endpoint = %self.breaker.endpoint,
                        failures = self.breaker.failure_threshold,
                        reset_timeout_ms = self.breaker.reset_timeout.as_millis(),
                        "backend circuit breaker opened"
                    );
                }
            }
            (CircuitState::HalfOpen { .. }, true, true) => {
                *state = CircuitState::Closed {
                    consecutive_failures: 0,
                };
                self.breaker.set_open_metric(false);
                self.breaker.record_probe("success");
                tracing::info!(
                    endpoint = %self.breaker.endpoint,
                    "backend circuit breaker closed after successful probe"
                );
            }
            (CircuitState::HalfOpen { .. }, true, false) => {
                *state = CircuitState::Open {
                    opened_at: Instant::now(),
                };
                self.breaker.set_open_metric(true);
                self.breaker.record_probe("failure");
                tracing::warn!(
                    endpoint = %self.breaker.endpoint,
                    reset_timeout_ms = self.breaker.reset_timeout.as_millis(),
                    "backend circuit breaker reopened after failed probe"
                );
            }
            _ => {}
        }
    }
}

impl Drop for CircuitPermit<'_> {
    fn drop(&mut self) {
        if !self.completed && self.half_open {
            let mut state = self.breaker.state.lock().unwrap();
            if matches!(
                &*state,
                CircuitState::HalfOpen {
                    probe_in_flight: true
                }
            ) {
                self.breaker.record_probe("cancelled");
                *state = CircuitState::HalfOpen {
                    probe_in_flight: false,
                };
            }
        }
    }
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
    /// Query each backend independently without consuming round-robin or session state.
    pub async fn list_methods(&self) -> MethodInventory {
        let results =
            futures_util::future::join_all(self.backends.iter().map(|backend| async move {
                let deadline = operation_timeout(backend).min(Duration::from_secs(5));
                let result = timeout(deadline, async {
                    let (kind, pool) = Self::route(backend, "tcp");
                    let connection = ManagedConnection {
                        transport: connector::connect(&backend.config, kind, pool.port).await?,
                        close_timeout: deadline,
                    };
                    let payload = Bytes::from_static(
                    br#"{"jsonrpc":"2.0","id":"gateway-discovery","method":"rpc.list_methods","params":{"describe":true}}"#,
                );
                    let request = encode_request(&payload, "gateway-discovery", "tcp")?;
                    let frame = connection
                        .transport
                        .send_message(request, true)
                        .await?
                        .context("discovery response missing")?;
                    let payload = decode_response(frame)?
                        .context("discovery acknowledgement has no payload")?;
                    let response: serde_json::Value = serde_json::from_slice(&payload)?;
                    anyhow::ensure!(
                        response["jsonrpc"] == "2.0" && response["id"] == "gateway-discovery",
                        "invalid discovery response envelope"
                    );
                    anyhow::ensure!(
                        response.get("error").is_none(),
                        "backend rejected discovery"
                    );
                    let descriptions: Vec<json_rpc_2_server::MethodDescription> =
                        serde_json::from_value(response["result"].clone())?;
                    anyhow::ensure!(
                        descriptions.iter().all(|description| !description.name.is_empty()),
                        "empty method name"
                    );
                    for description in &descriptions {
                        if let Some(example) = &description.example_request {
                            anyhow::ensure!(
                                example["jsonrpc"] == "2.0" && example["method"] == description.name
                                    && example.get("id").is_some()
                                    && matches!(example.get("params"), Some(serde_json::Value::Object(_) | serde_json::Value::Array(_))),
                                "invalid discovery example"
                            );
                        }
                    }
                    Ok::<_, anyhow::Error>(descriptions)
                })
                .await;
                (backend, result)
            }))
            .await;
        let mut methods = BTreeSet::new();
        let mut method_details = Vec::new();
        let mut errors = Vec::new();
        for (backend, result) in results {
            match result {
                Ok(Ok(descriptions)) => {
                    for description in descriptions {
                        methods.insert(description.name.clone());
                        if !method_details.contains(&description) {
                            method_details.push(description);
                        }
                    }
                }
                failure => {
                    tracing::warn!(backend = %backend.config.id, ?failure, "backend method discovery failed");
                    errors.push(MethodDiscoveryError {
                        backend_id: backend.config.id.clone(),
                        error: if failure.is_err() {
                            "DiscoveryTimeout"
                        } else {
                            "DiscoveryFailed"
                        },
                    });
                }
            }
        }
        MethodInventory {
            methods: methods.into_iter().collect(),
            method_details: {
                method_details.sort_by(|a, b| a.name.cmp(&b.name));
                method_details
            },
            complete: errors.is_empty(),
            errors,
        }
    }

    /// A local, non-atomic snapshot; busy slots are not inspected or waited on.
    /// Existing connections and a closed circuit do not prove backend liveness.
    pub fn snapshot(&self) -> Vec<BackendSnapshot> {
        self.backends
            .iter()
            .map(|backend| BackendSnapshot {
                id: backend.config.id.clone(),
                host: backend.config.host.clone(),
                transports: backend
                    .transports
                    .iter()
                    .map(|(kind, pool)| {
                        let mut connected_idle_slots = 0;
                        let mut empty_idle_slots = 0;
                        let mut busy_slots = 0;
                        for slot in &pool.slots {
                            match slot.try_lock() {
                                Ok(slot) if slot.connection.is_some() => connected_idle_slots += 1,
                                Ok(_) => empty_idle_slots += 1,
                                Err(_) => busy_slots += 1,
                            }
                        }
                        let breaker = &pool.circuit_breaker;
                        let circuit = match &*breaker.state.lock().unwrap() {
                            CircuitState::Closed {
                                consecutive_failures,
                            } => CircuitSnapshot::Closed {
                                consecutive_failures: *consecutive_failures,
                            },
                            CircuitState::Open { opened_at } => CircuitSnapshot::Open {
                                retry_after_ms: breaker
                                    .reset_timeout
                                    .saturating_sub(opened_at.elapsed())
                                    .as_millis()
                                    as u64,
                            },
                            CircuitState::HalfOpen { probe_in_flight } => {
                                CircuitSnapshot::HalfOpen {
                                    probe_in_flight: *probe_in_flight,
                                }
                            }
                        };
                        TransportSnapshot {
                            transport: kind.clone(),
                            port: pool.port,
                            pool_size: pool.slots.len(),
                            connected_idle_slots,
                            empty_idle_slots,
                            busy_slots,
                            circuit,
                        }
                    })
                    .collect(),
            })
            .collect()
    }

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
            anyhow::ensure!(
                config.options.circuit_breaker_failure_threshold > 0
                    && config.options.circuit_breaker_reset_timeout_ms > 0,
                "backend {} circuit breaker settings must be greater than zero",
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
                        circuit_breaker: CircuitBreaker::new(&config.id, kind, &config.options),
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
        self.forward_traced(payload, source_transport, connection_id, 0)
            .await
    }

    pub async fn forward_traced(
        &self,
        payload: &Bytes,
        source_transport: &str,
        connection_id: &str,
        trace_id: u64,
    ) -> anyhow::Result<Option<Bytes>> {
        let request = encode_request(payload, connection_id, source_transport)?;
        let affinity_index = if matches!(source_transport, "quic" | "ws-quic") {
            Some(
                *self
                    .affinity
                    .lock()
                    .unwrap()
                    .get(connection_id)
                    .context("QUIC session is not registered or has already closed")?,
            )
        } else {
            None
        };
        let (backend, kind, pool, permit) = if let Some(index) = affinity_index {
            let backend = self
                .backends
                .get(index)
                .context("QUIC affinity selected an invalid backend index")?;
            let (kind, pool) = Self::route(backend, source_transport);
            let permit = pool.circuit_breaker.acquire().inspect_err(|error| {
                let reason = if error.to_string().contains("half-open") {
                    "half_open_probe_in_progress"
                } else {
                    "circuit_open"
                };
                metrics::counter!(
                    "gateway_backend_requests_rejected_total",
                    "backend" => backend.config.id.clone(),
                    "transport" => kind.to_owned(),
                    "reason" => reason
                )
                .increment(1);
            })?;
            (backend, kind, pool, permit)
        } else {
            let first = self.strategy.select(self.backends.len());
            anyhow::ensure!(
                first < self.backends.len(),
                "load balancing strategy selected an invalid backend index"
            );
            let mut selected = None;
            let mut last_error = None;
            for offset in 0..self.backends.len() {
                let index = (first + offset) % self.backends.len();
                let backend = &self.backends[index];
                let (kind, pool) = Self::route(backend, source_transport);
                match pool.circuit_breaker.acquire() {
                    Ok(permit) => {
                        selected = Some((backend, kind, pool, permit));
                        break;
                    }
                    Err(error) => last_error = Some(error),
                }
            }
            selected.ok_or_else(|| {
                metrics::counter!(
                    "gateway_backend_requests_rejected_total",
                    "backend" => "multiple",
                    "transport" => source_transport.to_owned(),
                    "reason" => "all_endpoints_unavailable"
                )
                .increment(1);
                last_error.unwrap_or_else(|| anyhow::anyhow!("no backend endpoint is available"))
            })?
        };
        let result = async {
            let mut slot = pool.slots[pool.cursor.select(pool.slots.len())]
                .lock()
                .await;
            if slot.connection.is_none()
                || permit.is_half_open()
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
            tracing::debug!(
                trace_id,
                backend = %backend.config.id,
                endpoint = %format!("{}:{}", backend.config.host, pool.port),
                transport = kind,
                source_transport,
                connection_id,
                direction = "gateway_to_backend",
                payload_bytes = payload.len(),
                payload = %String::from_utf8_lossy(payload),
                "sending gateway request to backend"
            );
            let exchange = timeout(
                operation_timeout(backend),
                connection.transport.send_message(request, true),
            )
            .await;
            let response = match exchange {
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
                    });
                }
                Err(_) => anyhow::bail!(
                    "backend {} transport {kind} exchange timed out; request was not replayed",
                    backend.config.id
                ),
            };
            tracing::debug!(
                trace_id,
                backend = %backend.config.id,
                endpoint = %format!("{}:{}", backend.config.host, pool.port),
                transport = kind,
                source_transport,
                connection_id,
                direction = "backend_to_gateway",
                response_bytes = response.as_ref().map_or(0, Bytes::len),
                response_payload = %response.as_ref().map(|bytes| String::from_utf8_lossy(bytes)).unwrap_or_default(),
                "received backend response"
            );
            slot.connection = Some(connection);
            Ok(response)
        }
        .await;
        permit.complete(result.is_ok());
        result
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
                                if !transport.circuit_breaker.is_closed() {
                                    continue;
                                }
                                let result = pool
                                    .check_slot(backend, kind, transport.port, &mut slot)
                                    .await;
                                transport
                                    .circuit_breaker
                                    .record_maintenance_result(result.is_ok());
                                if let Err(error) = result {
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

    #[tokio::test]
    async fn discovery_rejects_unsupported_malformed_and_timed_out_responses() {
        struct Reply(&'static [u8], bool);
        impl json_rpc_2_transport::JsonRpcProtocol for Reply {
            fn handle_message<'a>(
                &'a self,
                _: &'a [u8],
            ) -> json_rpc_2_transport::ProtocolFuture<'a> {
                Box::pin(async move {
                    if self.1 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                    Some(Bytes::from_static(self.0))
                })
            }
        }
        for (reply, delayed) in [
            (
                br#"{"jsonrpc":"2.0","id":"gateway-discovery","error":{"code":-32601}}"#.as_slice(),
                false,
            ),
            (b"not json".as_slice(), false),
            (
                br#"{"jsonrpc":"2.0","id":"wrong","result":["echo"]}"#.as_slice(),
                false,
            ),
            (
                br#"{"jsonrpc":"2.0","id":"gateway-discovery","result":[1]}"#.as_slice(),
                false,
            ),
            (
                br#"{"jsonrpc":"2.0","id":"gateway-discovery","result":[""]}"#.as_slice(),
                false,
            ),
            (
                br#"{"jsonrpc":"2.0","id":"gateway-discovery","result":[]}"#.as_slice(),
                true,
            ),
        ] {
            let mut server = MultiTransportServer::new(Arc::new(
                gateway_protocol::GatewayBackendProtocol::new(Arc::new(Reply(reply, delayed))),
            ));
            server
                .set_bind(["127.0.0.1".parse().unwrap()])
                .set_tcp_port(0);
            let running = server.start().await.unwrap();
            let mut settings = config(running.listeners().next().unwrap().1.port(), "tcp");
            settings.options.operation_timeout_ms = if delayed { 20 } else { 1000 };
            let pool = BackendPool::new(vec![settings]).unwrap();
            let result = pool.list_methods().await;
            assert!(!result.complete);
            assert!(result.methods.is_empty());
            assert_eq!(result.errors.len(), 1);
            assert_eq!(
                result.errors[0].error,
                if delayed {
                    "DiscoveryTimeout"
                } else {
                    "DiscoveryFailed"
                }
            );
            running.close().await.unwrap();
        }
    }
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

        let mut invalid_breaker = config(1, "http1");
        invalid_breaker.options.circuit_breaker_failure_threshold = 0;
        assert!(BackendPool::new(vec![invalid_breaker]).is_err());
    }

    #[tokio::test]
    async fn snapshot_reports_busy_slots_and_circuit_transitions_without_probing() {
        let pool = BackendPool::new(vec![config(8081, "tcp")]).unwrap();
        let transport = &pool.backends[0].transports["tcp"];
        let locked = transport.slots[0].lock().await;
        let snapshot = pool.snapshot();
        assert_eq!(snapshot[0].transports[0].busy_slots, 1);
        assert_eq!(snapshot[0].transports[0].empty_idle_slots, 0);
        drop(locked);
        let breaker = &transport.circuit_breaker;
        *breaker.state.lock().unwrap() = CircuitState::Open {
            opened_at: Instant::now(),
        };
        let snapshot = pool.snapshot();
        assert_eq!(snapshot[0].transports[0].empty_idle_slots, 1);
        assert!(matches!(snapshot[0].transports[0].circuit,
            CircuitSnapshot::Open { retry_after_ms } if retry_after_ms > 0));
        *breaker.state.lock().unwrap() = CircuitState::HalfOpen {
            probe_in_flight: true,
        };
        assert!(matches!(
            pool.snapshot()[0].transports[0].circuit,
            CircuitSnapshot::HalfOpen {
                probe_in_flight: true
            }
        ));
    }

    #[tokio::test]
    async fn circuit_breaker_allows_only_one_half_open_probe_and_recovers() {
        let options = BackendOptions {
            circuit_breaker_failure_threshold: 1,
            circuit_breaker_reset_timeout_ms: 1,
            ..BackendOptions::default()
        };
        let breaker = CircuitBreaker::new("backend", "tcp", &options);

        breaker.acquire().unwrap().complete(false);
        assert!(breaker.acquire().is_err());
        tokio::time::sleep(Duration::from_millis(2)).await;

        let probe = breaker.acquire().unwrap();
        assert!(probe.is_half_open());
        assert!(breaker.acquire().is_err());
        drop(probe);

        let probe = breaker.acquire().unwrap();
        assert!(probe.is_half_open());
        probe.complete(true);
        assert!(!breaker.acquire().unwrap().is_half_open());
    }

    #[tokio::test]
    async fn round_robin_skips_open_endpoints_but_quic_affinity_does_not_fail_over() {
        let mut running_backends = Vec::new();
        let mut configs = Vec::new();
        let calls = Arc::new(AtomicUsize::new(0));

        for backend_index in 0..2 {
            let rpc = Arc::new(RpcServer::new());
            let backend_calls = Arc::clone(&calls);
            rpc.register("identify", move |_| {
                let backend_calls = Arc::clone(&backend_calls);
                async move {
                    backend_calls.fetch_add(1, Ordering::Relaxed);
                    Ok(serde_json::json!({"backend": backend_index}))
                }
            })
            .unwrap();
            let running = MultiTransportServer::new(Arc::new(
                gateway_protocol::GatewayBackendProtocol::new(rpc),
            ))
            .set_bind(["127.0.0.1".parse().unwrap()])
            .set_tcp_port(0)
            .start()
            .await
            .unwrap();
            let port = running.listeners().next().unwrap().1.port();
            let mut backend_config = config(port, "tcp");
            backend_config.id = format!("backend-{backend_index}");
            backend_config.options.circuit_breaker_failure_threshold = 1;
            configs.push(backend_config);
            running_backends.push(running);
        }

        let pool = BackendPool::new(configs).unwrap();
        pool.warm_up().await.unwrap();
        pool.backends[0].transports["tcp"]
            .circuit_breaker
            .acquire()
            .unwrap()
            .complete(false);

        let request = Bytes::from_static(br#"{"jsonrpc":"2.0","method":"identify","id":1}"#);
        let response = pool
            .forward(&request, "tcp", "ordinary")
            .await
            .unwrap()
            .unwrap();
        let response: serde_json::Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(response["result"]["backend"], 1);
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        pool.affinity
            .lock()
            .unwrap()
            .insert("sticky-session".to_owned(), 0);
        let affinity_error = pool
            .forward(&request, "quic", "sticky-session")
            .await
            .unwrap_err();
        assert!(affinity_error
            .to_string()
            .contains("circuit breaker is open"));
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        drop(pool);
        for backend in running_backends {
            backend.close().await.unwrap();
        }
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
        server.set_bind(["127.0.0.1".parse().unwrap()]);
        server.set_port(kind, 0);
        let running = server.start().await.unwrap();
        let (_, address) = running
            .listeners()
            .find(|(candidate, _)| *candidate == kind)
            .unwrap();
        settings.ports = [(kind.id().to_owned(), address.port())]
            .into_iter()
            .collect();
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

    async fn assert_opaque_broker_backend(
        settings: BackendConfig,
        ingress: gateway_iot::server::GatewayIotIngress,
    ) {
        use json_rpc_2_transport::TransportRuntime;

        let running = ingress
            .start(Arc::new(OpaqueEcho), Arc::new(TransportRuntime::default()))
            .await
            .unwrap();
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
            let mut ingress = gateway_iot::server::GatewayIotIngress::default();
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
                    let ingress_config = gateway_iot::MqttConfig {
                        broker_url: broker.broker_url.clone(),
                        request_topic: broker.request_topic.clone(),
                        operation_timeout: broker.operation_timeout,
                        response_timeout: broker.response_timeout,
                        max_payload_bytes: broker.max_payload_bytes,
                        ca_certificate: broker.ca_certificate.clone(),
                        ..Default::default()
                    };
                    settings.options.mqtt_url_env = Some("MQTT_BROKER_URL".into());
                    if kind == TransportKind::Mqtt {
                        settings.options.mqtt_request_topic = Some(broker.request_topic.clone());
                        ingress.set_mqtt_config(ingress_config);
                    } else {
                        settings.options.ws_mqtt_request_topic = Some(broker.request_topic.clone());
                        ingress.set_ws_mqtt_config(ingress_config);
                    }
                }
                _ => {
                    let broker = json_rpc_2_iot::AmqpConfig {
                        broker_url: std::env::var("AMQP_BROKER_URL")
                            .expect("AMQP_BROKER_URL required"),
                        request_queue: format!("json-rpc-2.{namespace}.requests"),
                        ..Default::default()
                    };
                    let ingress_config = gateway_iot::AmqpConfig {
                        broker_url: broker.broker_url.clone(),
                        request_queue: broker.request_queue.clone(),
                        operation_timeout: broker.operation_timeout,
                        response_timeout: broker.response_timeout,
                        max_payload_bytes: broker.max_payload_bytes,
                        prefetch: broker.prefetch,
                        ..Default::default()
                    };
                    settings.options.amqp_url_env = Some("AMQP_BROKER_URL".into());
                    if kind == TransportKind::Amqp {
                        settings.options.amqp_request_queue = Some(broker.request_queue.clone());
                        ingress.set_amqp_config(ingress_config);
                    } else {
                        settings.options.ws_amqp_request_queue = Some(broker.request_queue.clone());
                        ingress.set_ws_amqp_config(ingress_config);
                    }
                }
            }
            assert_opaque_broker_backend(settings, ingress).await;
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
            TransportKind::WebSocketTcp,
            TransportKind::WebSocketHttp1,
            TransportKind::WebSocketUdp,
            TransportKind::WebSocketRtp,
            TransportKind::WebSocketRudp,
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
    async fn coap_carriers_preserve_metadata_notifications_and_reuse() {
        use json_rpc_2_transport::{Transport, TransportRuntime};

        for kind in [TransportKind::Coap, TransportKind::WebSocketCoap] {
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let address = socket.local_addr().unwrap();
            let rpc = Arc::new(RpcServer::new());
            rpc.register_with_context("echo", |params, context| async move {
                let context = context.unwrap();
                assert_eq!(context.connection_id, "client-1");
                assert_eq!(context.transport, "ws-tcp");
                Ok(params)
            })
            .unwrap();
            let protocol: Arc<dyn json_rpc_2_transport::JsonRpcProtocol> =
                Arc::new(gateway_protocol::GatewayBackendProtocol::new(rpc));
            let runtime = Arc::new(TransportRuntime::default());
            let running = match kind {
                TransportKind::Coap => {
                    gateway_iot::server::transports::coap::CoapTransport::new(socket)
                        .with_runtime(runtime)
                        .start(protocol)
                }
                TransportKind::WebSocketCoap => {
                    gateway_iot::server::transports::coap_websocket::CoapWebSocketTransport::new(
                        socket,
                    )
                    .with_runtime(runtime)
                    .start(protocol)
                }
                _ => unreachable!(),
            };
            let pool = BackendPool::new(vec![config(address.port(), kind.id())]).unwrap();
            pool.warm_up().await.unwrap();
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
                let request = Bytes::from_static(
                    b"{\n\"jsonrpc\":\"2.0\",\n\"method\":\"echo\",\"params\":{\"value\":1},\"id\":7\n}",
                );
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
        let echo_calls = calls.clone();
        rpc.register("echo", move |params| {
            let calls = echo_calls.clone();
            async move {
                calls.fetch_add(1, Ordering::Relaxed);
                Ok(params)
            }
        })
        .unwrap();
        let mut server =
            MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(rpc)));
        server
            .set_bind(["127.0.0.1".parse().unwrap()])
            .set_tcp_port(port);
        let running = server.start().await.unwrap();
        let mut settings = config(port, "tcp");
        settings.options.operation_timeout_ms = 50;
        settings.options.circuit_breaker_failure_threshold = 1;
        settings.options.circuit_breaker_reset_timeout_ms = 50;
        let pool = BackendPool::new(vec![settings]).unwrap();
        pool.warm_up().await.unwrap();
        let slow = Bytes::from_static(b"{\"jsonrpc\":\"2.0\",\"method\":\"slow\",\"id\":1}");
        assert!(pool.forward(&slow, "tcp", "client").await.is_err());
        assert!(pool.backends[0].transports["tcp"].slots[0]
            .lock()
            .await
            .connection
            .is_none());
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let echo = Bytes::from_static(
            b"{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"params\":[2],\"id\":2}",
        );
        let open_error = pool.forward(&echo, "tcp", "client").await.unwrap_err();
        assert!(open_error.to_string().contains("circuit breaker is open"));
        assert_eq!(calls.load(Ordering::Relaxed), 1);

        tokio::time::sleep(Duration::from_millis(60)).await;
        let response = pool.forward(&echo, "tcp", "client").await.unwrap().unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&response).unwrap()["id"],
            2
        );
        assert_eq!(calls.load(Ordering::Relaxed), 2);
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
        settings.options.circuit_breaker_failure_threshold = 1;
        settings.options.circuit_breaker_reset_timeout_ms = 60_000;
        let pool = Arc::new(BackendPool::new(vec![settings]).unwrap());
        pool.warm_up().await.unwrap();
        running.close().await.unwrap();
        pool.start_maintenance();
        timeout(Duration::from_secs(2), async {
            loop {
                let slot = pool.backends[0].transports["tcp"].slots[0].lock().await;
                if slot.connection.is_none()
                    && slot.checked_at.is_some()
                    && !pool.backends[0].transports["tcp"]
                        .circuit_breaker
                        .is_closed()
                {
                    break;
                }
                drop(slot);
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
        let checked_at = pool.backends[0].transports["tcp"].slots[0]
            .lock()
            .await
            .checked_at
            .unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        assert_eq!(
            pool.backends[0].transports["tcp"].slots[0]
                .lock()
                .await
                .checked_at,
            Some(checked_at),
            "maintenance must stop reconnecting while the endpoint circuit is open"
        );
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
