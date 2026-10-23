use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, bail};
use bytes::Bytes;
use json_rpc_2_runtime::{TransportContext, TransportKind, TransportSession};
use json_rpc_2_server::{JsonRpcProtocol, ProtocolFuture};
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinSet,
};

use crate::session::SessionRegistry;
use json_rpc_balancer::BackendPool;

#[derive(Debug)]
#[allow(dead_code)]
pub enum MessageFrame {
    Raw(Bytes),
    WebSocketText(String),
    WebSocketBinary(Vec<u8>),
}

pub struct IncomingEnvelope {
    pub trace_id: u64,
    pub payload: MessageFrame,
    pub source_transport: TransportKind,
    pub connection_id: String,
    pub responder: oneshot::Sender<Result<Option<MessageFrame>, String>>,
}

#[derive(Clone, Copy, Debug)]
pub struct DispatcherLimits {
    pub queue_capacity: usize,
    pub max_concurrent: usize,
}

impl Default for DispatcherLimits {
    fn default() -> Self {
        Self {
            queue_capacity: 1024,
            max_concurrent: 256,
        }
    }
}

impl DispatcherLimits {
    pub fn validate(self) -> anyhow::Result<()> {
        if self.queue_capacity == 0 {
            bail!("gateway dispatcher queue capacity must be greater than zero");
        }
        if self.queue_capacity > tokio::sync::Semaphore::MAX_PERMITS {
            bail!(
                "gateway dispatcher queue capacity must not exceed {}",
                tokio::sync::Semaphore::MAX_PERMITS
            );
        }
        if self.max_concurrent == 0 {
            bail!("gateway dispatcher max concurrency must be greater than zero");
        }
        Ok(())
    }
}

pub struct Gateway {
    incoming: mpsc::Sender<IncomingEnvelope>,
    next_trace_id: AtomicU64,
    backends: Arc<BackendPool>,
    sessions: Arc<SessionRegistry>,
}

pub struct GatewayProtocol {
    gateway: Arc<Gateway>,
    source_transport: TransportKind,
}

struct MultiplexedGatewayProtocol {
    protocols: std::collections::HashMap<&'static str, Arc<dyn JsonRpcProtocol>>,
}

impl MultiplexedGatewayProtocol {
    fn resolve(&self, context: &TransportContext) -> Option<&Arc<dyn JsonRpcProtocol>> {
        let protocol = self.protocols.get(context.transport.as_str());
        if protocol.is_none() {
            tracing::error!(transport = %context.transport, "unknown multiplexed ingress transport");
        }
        protocol
    }
}

impl JsonRpcProtocol for MultiplexedGatewayProtocol {
    fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
        tracing::error!("multiplexed ingress requires a transport context");
        Box::pin(async { None })
    }

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        match self.resolve(context) {
            Some(protocol) => protocol.handle_message_with_context(input, context),
            None => Box::pin(async { None }),
        }
    }

    fn handle_owned_message<'a>(
        &'a self,
        input: Bytes,
        context: &'a TransportContext,
    ) -> json_rpc_2_transport::ProtocolMessageFuture<'a> {
        match self.resolve(context) {
            Some(protocol) => protocol.handle_owned_message(input, context),
            None => Box::pin(async { None }),
        }
    }

    fn session_opened(&self, context: &TransportContext) {
        if let Some(protocol) = self.resolve(context) {
            protocol.session_opened(context);
        }
    }

    fn session_closed(&self, context: &TransportContext) {
        if let Some(protocol) = self.resolve(context) {
            protocol.session_closed(context);
        }
    }
}

struct TemporarySession {
    sessions: Arc<SessionRegistry>,
    connection_id: String,
}

impl Drop for TemporarySession {
    fn drop(&mut self) {
        self.sessions.remove_session(&self.connection_id);
    }
}

impl Gateway {
    pub fn new(backends: BackendPool, limits: DispatcherLimits) -> anyhow::Result<Arc<Self>> {
        limits.validate()?;

        let (incoming, mut receiver) = mpsc::channel::<IncomingEnvelope>(limits.queue_capacity);
        let gateway = Arc::new(Self {
            incoming,
            next_trace_id: AtomicU64::new(1),
            backends: Arc::new(backends),
            sessions: SessionRegistry::new(),
        });
        let backends = Arc::clone(&gateway.backends);
        backends.start_maintenance();
        tokio::spawn(async move {
            let mut workers = JoinSet::new();
            loop {
                if workers.len() >= limits.max_concurrent {
                    if let Some(Err(error)) = workers.join_next().await {
                        tracing::error!(%error, "gateway dispatcher worker failed");
                    }
                    continue;
                }

                tokio::select! {
                    envelope = receiver.recv() => match envelope {
                        Some(envelope) => {
                            let backends = Arc::clone(&backends);
                            workers.spawn(async move {
                                dispatch_envelope(backends, envelope).await;
                            });
                        }
                        None => break,
                    },
                    result = workers.join_next(), if !workers.is_empty() => {
                        if let Some(Err(error)) = result {
                            tracing::error!(%error, "gateway dispatcher worker failed");
                        }
                    }
                }
            }

            while let Some(result) = workers.join_next().await {
                if let Err(error) = result {
                    tracing::error!(%error, "gateway dispatcher worker failed");
                }
            }
        });
        Ok(gateway)
    }

    pub fn protocol(self: &Arc<Self>, source_transport: TransportKind) -> Arc<dyn JsonRpcProtocol> {
        Arc::new(GatewayProtocol {
            gateway: Arc::clone(self),
            source_transport,
        })
    }

    pub fn sessions(&self) -> Arc<SessionRegistry> {
        Arc::clone(&self.sessions)
    }

    pub fn multiplexed_protocol(self: &Arc<Self>) -> Arc<dyn JsonRpcProtocol> {
        Arc::new(MultiplexedGatewayProtocol {
            protocols: TransportKind::ALL
                .iter()
                .map(|&kind| (kind.id(), self.protocol(kind)))
                .collect(),
        })
    }

    async fn submit(
        &self,
        source_transport: TransportKind,
        connection_id: String,
        payload: Bytes,
    ) -> Option<Bytes> {
        let trace_id = self.next_trace_id.fetch_add(1, Ordering::Relaxed);
        tracing::debug!(
            trace_id,
            transport = source_transport.id(),
            connection_id = %connection_id,
            direction = "client_to_gateway",
            payload_bytes = payload.len(),
            payload = %String::from_utf8_lossy(&payload),
            "gateway received request"
        );
        let (responder, response) = oneshot::channel();
        let log_connection_id = connection_id.clone();
        let envelope = IncomingEnvelope {
            trace_id,
            payload: MessageFrame::Raw(payload),
            source_transport,
            connection_id,
            responder,
        };
        match self.incoming.try_send(envelope) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                tracing::warn!(
                    trace_id,
                    transport = source_transport.id(),
                    connection_id = %log_connection_id,
                    queue_capacity = self.incoming.max_capacity(),
                    "gateway dispatcher saturated; request rejected without replay"
                );
                metrics::counter!(
                    "gateway_dispatch_requests_rejected_total",
                    "reason" => "queue_full"
                )
                .increment(1);
                return None;
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                tracing::error!(
                    trace_id,
                    transport = source_transport.id(),
                    connection_id = %log_connection_id,
                    "gateway message bus is unavailable"
                );
                metrics::counter!(
                    "gateway_dispatch_requests_rejected_total",
                    "reason" => "dispatcher_closed"
                )
                .increment(1);
                return None;
            }
        }
        match response.await {
            Ok(Ok(Some(MessageFrame::Raw(response)))) => {
                tracing::debug!(
                    trace_id,
                    connection_id = %log_connection_id,
                    transport = source_transport.id(),
                    direction = "gateway_to_client",
                    payload_bytes = response.len(),
                    payload = %String::from_utf8_lossy(&response),
                    "gateway sent response"
                );
                Some(response)
            }
            Ok(Ok(Some(MessageFrame::WebSocketText(text)))) => {
                tracing::debug!(
                    trace_id,
                    connection_id = %log_connection_id,
                    transport = source_transport.id(),
                    direction = "gateway_to_client",
                    payload_bytes = text.len(),
                    payload = %text,
                    "gateway sent response"
                );
                Some(Bytes::from(text))
            }
            Ok(Ok(Some(MessageFrame::WebSocketBinary(bytes)))) => {
                tracing::debug!(
                    trace_id,
                    connection_id = %log_connection_id,
                    transport = source_transport.id(),
                    direction = "gateway_to_client",
                    payload_bytes = bytes.len(),
                    payload = %String::from_utf8_lossy(&bytes),
                    "gateway sent response"
                );
                Some(Bytes::from(bytes))
            }
            Ok(Ok(None)) => None,
            Ok(Err(error)) => {
                tracing::warn!(trace_id, %error, "gateway request was not completed");
                None
            }
            Err(error) => {
                tracing::debug!(trace_id, %error, "gateway response channel closed");
                None
            }
        }
    }
}

async fn dispatch_envelope(backends: Arc<BackendPool>, envelope: IncomingEnvelope) {
    let trace_id = envelope.trace_id;
    let transport = envelope.source_transport.id().to_owned();
    let connection_id = envelope.connection_id;
    let payload = match envelope.payload {
        MessageFrame::Raw(bytes) => bytes,
        MessageFrame::WebSocketText(text) => Bytes::from(text),
        MessageFrame::WebSocketBinary(bytes) => Bytes::from(bytes),
    };
    let result = backends
        .forward_traced(&payload, &transport, &connection_id, trace_id)
        .await
        .with_context(|| format!("backend request from {} on {}", connection_id, transport));
    match result {
        Ok(response) => {
            tracing::debug!(
                trace_id,
                connection_id = %connection_id,
                source_transport = %transport,
                direction = "backend_to_gateway",
                response_bytes = response.as_ref().map_or(0, Bytes::len),
                response_payload = %response.as_ref().map(|bytes| String::from_utf8_lossy(bytes)).unwrap_or_default(),
                "gateway received backend response"
            );
            if envelope
                .responder
                .send(Ok(response.map(MessageFrame::Raw)))
                .is_err()
            {
                tracing::debug!(
                    trace_id,
                    connection_id = %connection_id,
                    "client closed before backend response was delivered"
                );
            }
        }
        Err(error) => {
            tracing::warn!(
                trace_id,
                connection_id = %connection_id,
                transport,
                %error,
                "gateway backend request was rejected or failed"
            );
            if envelope.responder.send(Err(error.to_string())).is_err() {
                tracing::debug!(
                    trace_id,
                    connection_id = %connection_id,
                    "client closed before backend error was delivered"
                );
            }
        }
    }
}

impl JsonRpcProtocol for GatewayProtocol {
    fn handle_owned_message<'a>(
        &'a self,
        input: Bytes,
        context: &'a TransportContext,
    ) -> json_rpc_2_transport::ProtocolMessageFuture<'a> {
        let gateway = Arc::clone(&self.gateway);
        let transport = self.source_transport;
        let connection_id = context.connection_id.clone();
        let temporary = if context.push_sender.is_none()
            && gateway.sessions.register_temporary_session(context)
        {
            Some(TemporarySession {
                sessions: Arc::clone(&gateway.sessions),
                connection_id: connection_id.clone(),
            })
        } else {
            None
        };
        Box::pin(async move {
            let _session = temporary;
            gateway
                .submit(transport, connection_id, input)
                .await
                .map(json_rpc_2_transport::TransportMessage::from)
        })
    }

    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        let gateway = Arc::clone(&self.gateway);
        let transport = self.source_transport;
        let payload = Bytes::copy_from_slice(input);
        Box::pin(async move {
            let context = TransportSession::stateless(transport.id());
            gateway.sessions.register_session(&context);
            let connection_id = context.connection_id.clone();
            let _session = TemporarySession {
                sessions: Arc::clone(&gateway.sessions),
                connection_id: connection_id.clone(),
            };
            gateway.submit(transport, connection_id, payload).await
        })
    }

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        let gateway = Arc::clone(&self.gateway);
        let transport = self.source_transport;
        let connection_id = context.connection_id.clone();
        let payload = Bytes::copy_from_slice(input);
        Box::pin(async move { gateway.submit(transport, connection_id, payload).await })
    }

    fn session_opened(&self, context: &TransportContext) {
        if let Err(error) = self
            .gateway
            .backends
            .open_session(&context.connection_id, self.source_transport.id())
        {
            tracing::error!(%error, connection_id = %context.connection_id, "cannot bind gateway session to a backend");
            return;
        }
        self.gateway.sessions.register_session(context);
    }

    fn session_closed(&self, context: &TransportContext) {
        self.gateway.sessions.remove_session(&context.connection_id);
        self.gateway.backends.close_session(&context.connection_id);
    }
}

#[cfg(test)]
#[path = "gateway_acceptance.rs"]
mod acceptance;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bytes::Bytes;
    use json_rpc_2_runtime::{MultiTransportServer, TransportKind};
    use json_rpc_2_server::RpcServer;
    use json_rpc_balancer::{BackendConfig, BackendPool};
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::{TcpListener, TcpStream},
        sync::{Notify, mpsc},
        time::{Duration, timeout},
    };

    use super::{DispatcherLimits, Gateway};

    #[tokio::test]
    async fn tcp_gateway_forwards_session_metadata_and_push_to_json_rpc_2_server() {
        let backend_rpc = Arc::new(RpcServer::new());
        backend_rpc
            .register_with_context("echo", |params, context| async move {
                let context = context.expect("gateway metadata should reach the backend handler");
                Ok(serde_json::json!({
                    "params": params,
                    "connection_id": context.connection_id,
                    "transport": context.transport,
                }))
            })
            .unwrap();

        let port_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = port_probe.local_addr().unwrap().port();
        drop(port_probe);
        let mut backend_server = MultiTransportServer::new(Arc::new(
            gateway_protocol::GatewayBackendProtocol::new(backend_rpc),
        ));
        backend_server
            .set_bind(["127.0.0.1".parse().unwrap()])
            .set_tcp_port(port);
        let backend = backend_server.start().await.unwrap();

        let pool = BackendPool::new(vec![BackendConfig {
            id: "json-rpc-2-server".to_owned(),
            host: "127.0.0.1".to_owned(),
            ports: [("tcp".to_owned(), port)].into_iter().collect(),
            pool_size: 1,
            options: Default::default(),
        }])
        .unwrap();
        let gateway = Gateway::new(pool, super::DispatcherLimits::default()).unwrap();
        let frontend_probe = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let frontend_port = frontend_probe.local_addr().unwrap().port();
        drop(frontend_probe);
        let mut frontend_server = MultiTransportServer::new(gateway.protocol(TransportKind::Tcp));
        frontend_server
            .set_bind(["127.0.0.1".parse().unwrap()])
            .set_tcp_port(frontend_port);
        let frontend = frontend_server.start().await.unwrap();

        let mut client = BufReader::new(
            TcpStream::connect(("127.0.0.1", frontend_port))
                .await
                .unwrap(),
        );
        client
            .get_mut()
            .write_all(
                b"{\"jsonrpc\":\"2.0\",\"method\":\"echo\",\"params\":{\"value\":1},\"id\":7}\n",
            )
            .await
            .unwrap();
        let mut response_line = String::new();
        client.read_line(&mut response_line).await.unwrap();
        let response: serde_json::Value = serde_json::from_str(&response_line).unwrap();
        let connection_id = response["result"]["connection_id"]
            .as_str()
            .unwrap()
            .to_owned();

        assert_eq!(response["id"], 7);
        assert_eq!(response["result"]["params"]["value"], 1);
        assert_eq!(response["result"]["transport"], "tcp");
        assert!(uuid::Uuid::parse_str(&connection_id).is_ok());

        let sessions = gateway.sessions();
        sessions
            .register_client(&connection_id, "test-client".to_owned(), vec![])
            .unwrap();
        sessions
            .push_unicast("test-client", Bytes::from_static(b"{\"method\":\"push\"}"))
            .unwrap();
        let mut push_line = String::new();
        client.read_line(&mut push_line).await.unwrap();
        assert_eq!(push_line, "{\"method\":\"push\"}\n");

        drop(client);
        frontend.close().await.unwrap();
        assert_eq!(
            sessions.register_client(&connection_id, "after-close".to_owned(), vec![]),
            Err("UnknownConnection".to_owned())
        );
        backend.close().await.unwrap();
    }

    #[tokio::test]
    async fn dispatcher_rejects_when_running_and_queue_limits_are_reached() {
        let (started, mut requests_started) = mpsc::unbounded_channel();
        let release = Arc::new(Notify::new());
        let rpc = Arc::new(RpcServer::new());
        let handler_release = Arc::clone(&release);
        rpc.register("hold", move |params| {
            let started = started.clone();
            let release = Arc::clone(&handler_release);
            async move {
                started.send(()).unwrap();
                release.notified().await;
                Ok(params)
            }
        })
        .unwrap();
        let backend =
            MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(rpc)))
                .set_bind(["127.0.0.1".parse().unwrap()])
                .set_tcp_port(0)
                .start()
                .await
                .unwrap();
        let backend_port = backend.listeners().next().unwrap().1.port();
        let pool = BackendPool::new(vec![BackendConfig {
            id: "bounded-dispatcher-backend".to_owned(),
            host: "127.0.0.1".to_owned(),
            ports: [("tcp".to_owned(), backend_port)].into_iter().collect(),
            pool_size: 1,
            options: Default::default(),
        }])
        .unwrap();
        let gateway = Gateway::new(
            pool,
            DispatcherLimits {
                queue_capacity: 1,
                max_concurrent: 1,
            },
        )
        .unwrap();

        let first_gateway = Arc::clone(&gateway);
        let first = tokio::spawn(async move {
            first_gateway
                .submit(
                    TransportKind::Tcp,
                    "first".to_owned(),
                    Bytes::from_static(br#"{"jsonrpc":"2.0","method":"hold","id":1}"#),
                )
                .await
        });
        timeout(Duration::from_secs(2), requests_started.recv())
            .await
            .unwrap()
            .unwrap();

        let second_gateway = Arc::clone(&gateway);
        let second = tokio::spawn(async move {
            second_gateway
                .submit(
                    TransportKind::Tcp,
                    "second".to_owned(),
                    Bytes::from_static(br#"{"jsonrpc":"2.0","method":"hold","id":2}"#),
                )
                .await
        });
        timeout(Duration::from_secs(2), async {
            while gateway.incoming.capacity() != 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("second request should occupy the bounded queue");

        let rejected = gateway
            .submit(
                TransportKind::Tcp,
                "rejected".to_owned(),
                Bytes::from_static(br#"{"jsonrpc":"2.0","method":"hold","id":3}"#),
            )
            .await;
        assert!(rejected.is_none());
        assert!(requests_started.try_recv().is_err());

        release.notify_one();
        timeout(Duration::from_secs(2), requests_started.recv())
            .await
            .unwrap()
            .unwrap();
        release.notify_one();

        assert!(first.await.unwrap().is_some());
        assert!(second.await.unwrap().is_some());
        backend.close().await.unwrap();
    }
}
