use std::sync::Arc;

use anyhow::Context;
use bytes::Bytes;
use json_rpc_2_runtime::{TransportContext, TransportKind, TransportSession};
use json_rpc_2_server::{JsonRpcProtocol, ProtocolFuture};
use tokio::sync::{mpsc, oneshot};

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
    pub payload: MessageFrame,
    pub source_transport: TransportKind,
    pub connection_id: String,
    pub responder: oneshot::Sender<Result<Option<MessageFrame>, String>>,
}

pub struct Gateway {
    incoming: mpsc::UnboundedSender<IncomingEnvelope>,
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
    pub fn new(backends: BackendPool) -> Arc<Self> {
        let (incoming, mut receiver) = mpsc::unbounded_channel::<IncomingEnvelope>();
        let gateway = Arc::new(Self {
            incoming,
            backends: Arc::new(backends),
            sessions: SessionRegistry::new(),
        });
        let backends = Arc::clone(&gateway.backends);
        backends.start_maintenance();
        tokio::spawn(async move {
            while let Some(envelope) = receiver.recv().await {
                let backends = Arc::clone(&backends);
                tokio::spawn(async move {
                    let transport = envelope.source_transport.id().to_owned();
                    let connection_id = envelope.connection_id;
                    let payload = match envelope.payload {
                        MessageFrame::Raw(bytes) => bytes,
                        MessageFrame::WebSocketText(text) => Bytes::from(text),
                        MessageFrame::WebSocketBinary(bytes) => Bytes::from(bytes),
                    };
                    let result = backends
                        .forward(&payload, &transport, &connection_id)
                        .await
                        .with_context(|| {
                            format!("backend request from {} on {}", connection_id, transport)
                        });
                    match result {
                        Ok(response) => {
                            if envelope
                                .responder
                                .send(Ok(response.map(MessageFrame::Raw)))
                                .is_err()
                            {
                                tracing::debug!(
                                    connection_id = %connection_id,
                                    "client closed before backend response was delivered"
                                );
                            }
                        }
                        Err(error) => {
                            tracing::error!(
                                connection_id = %connection_id,
                                transport,
                                %error,
                                "gateway backend request failed"
                            );
                            if envelope.responder.send(Err(error.to_string())).is_err() {
                                tracing::debug!(
                                    connection_id = %connection_id,
                                    "client closed before backend error was delivered"
                                );
                            }
                        }
                    }
                });
            }
        });
        gateway
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
        tracing::debug!(
            transport = source_transport.id(),
            connection_id,
            payload_bytes = payload.len(),
            "ingress message framed"
        );
        let (responder, response) = oneshot::channel();
        let envelope = IncomingEnvelope {
            payload: MessageFrame::Raw(payload),
            source_transport,
            connection_id,
            responder,
        };
        if let Err(error) = self.incoming.send(envelope) {
            tracing::error!(%error, "gateway message bus is unavailable");
            return None;
        }
        match response.await {
            Ok(Ok(Some(MessageFrame::Raw(response)))) => Some(response),
            Ok(Ok(Some(MessageFrame::WebSocketText(text)))) => Some(Bytes::from(text)),
            Ok(Ok(Some(MessageFrame::WebSocketBinary(bytes)))) => Some(Bytes::from(bytes)),
            Ok(Ok(None)) => None,
            Ok(Err(error)) => {
                tracing::error!(%error, "gateway request was not completed");
                None
            }
            Err(error) => {
                tracing::error!(%error, "gateway response channel closed");
                None
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
    };

    use super::Gateway;

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
        let gateway = Gateway::new(pool);
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
}
