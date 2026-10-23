use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
};

use bytes::Bytes;
use json_rpc_2_core::{
    WebSocketMessage, decode_websocket_message, encode_websocket_message, websocket_message_bytes,
};
use json_rpc_2_transport::{
    JsonRpcProtocol, ProtocolFuture, ProtocolSessionGuard, TransportContext, TransportSession,
    log_transport_error,
};

pub(super) struct WebSocketJsonRpcAdapter {
    protocol: Arc<dyn JsonRpcProtocol>,
    kind: &'static str,
    sessions: Mutex<HashMap<String, WebSocketSession>>,
}

struct WebSocketSession {
    context: TransportContext,
    _guard: ProtocolSessionGuard,
    relay: Option<tokio::task::JoinHandle<()>>,
    closing: Arc<AtomicBool>,
}

impl Drop for WebSocketSession {
    fn drop(&mut self) {
        if let Some(relay) = &self.relay {
            relay.abort();
        }
    }
}

impl WebSocketJsonRpcAdapter {
    pub(super) fn new(protocol: Arc<dyn JsonRpcProtocol>, kind: &'static str) -> Self {
        Self {
            protocol,
            kind,
            sessions: Mutex::new(HashMap::new()),
        }
    }

    fn context(&self, context: &TransportContext) -> TransportContext {
        self.sessions
            .lock()
            .unwrap()
            .get(&context.connection_id)
            .map(|session| session.context.clone())
            .unwrap_or_else(|| {
                let mut translated = context.clone();
                translated.transport = self.kind.to_owned();
                translated
            })
    }
}

impl JsonRpcProtocol for WebSocketJsonRpcAdapter {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        let fallback = TransportSession::stateless(self.kind);
        self.process_message(input, self.context(&fallback))
    }

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        let context = self.context(context);
        self.process_message(input, context)
    }

    fn session_opened(&self, context: &TransportContext) {
        self.open_session(context);
    }

    fn session_closed(&self, context: &TransportContext) {
        self.close_session(context);
    }

    fn session_close_requested(&self, context: &TransportContext) -> bool {
        self.close_requested(context)
    }
}

impl WebSocketJsonRpcAdapter {
    fn process_message<'a>(
        &'a self,
        input: &'a [u8],
        context: TransportContext,
    ) -> ProtocolFuture<'a> {
        Box::pin(async move {
            let inbound = match decode_websocket_message(input) {
                Ok(Some(inbound)) => inbound,
                Ok(None) => {
                    if let Some(session) = self.sessions.lock().unwrap().get(&context.connection_id)
                    {
                        session.closing.store(true, Ordering::Release);
                    }
                    return None;
                }
                Err(error) => {
                    log_transport_error(self.kind, &context.connection_id, "decode frame", &error);
                    if let Some(session) = self.sessions.lock().unwrap().get(&context.connection_id)
                    {
                        session.closing.store(true, Ordering::Release);
                    }
                    return None;
                }
            };
            let inbound = match inbound {
                WebSocketMessage::Ping(payload) => {
                    return match encode_websocket_message(&WebSocketMessage::Pong(payload)) {
                        Ok(frame) => Some(Bytes::from(frame)),
                        Err(error) => {
                            log_transport_error(
                                self.kind,
                                &context.connection_id,
                                "encode pong",
                                &error,
                            );
                            None
                        }
                    };
                }
                WebSocketMessage::Pong(_) => return None,
                message @ (WebSocketMessage::Text(_) | WebSocketMessage::Binary(_)) => message,
            };
            let is_text = matches!(inbound, WebSocketMessage::Text(_));
            let request = websocket_message_bytes(&inbound);
            let response = self
                .protocol
                .handle_message_with_context(request, &context)
                .await?;
            let outbound = match (is_text, std::str::from_utf8(&response)) {
                (true, Ok(text)) => WebSocketMessage::Text(text.to_owned()),
                _ => WebSocketMessage::Binary(response),
            };
            match encode_websocket_message(&outbound) {
                Ok(frame) => Some(Bytes::from(frame)),
                Err(error) => {
                    log_transport_error(
                        self.kind,
                        &context.connection_id,
                        "encode response",
                        &error,
                    );
                    None
                }
            }
        })
    }
}

impl WebSocketJsonRpcAdapter {
    fn open_session(&self, context: &TransportContext) {
        let mut translated = context.clone();
        translated.transport = self.kind.to_owned();
        let relay = context.push_sender.clone().map(|carrier| {
            let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel::<Bytes>();
            translated.push_sender = Some(sender);
            tokio::spawn(async move {
                while let Some(payload) = receiver.recv().await {
                    let message = match std::str::from_utf8(&payload) {
                        Ok(text) => WebSocketMessage::Text(text.to_owned()),
                        Err(_) => WebSocketMessage::Binary(payload),
                    };
                    let frame = match encode_websocket_message(&message) {
                        Ok(frame) => frame,
                        Err(error) => {
                            log_transport_error("WebSocket", "session", "encode push", &error);
                            break;
                        }
                    };
                    if carrier.send(Bytes::from(frame)).is_err() {
                        break;
                    }
                }
            })
        });
        let guard = ProtocolSessionGuard::new(self.protocol.clone(), translated.clone());
        self.sessions.lock().unwrap().insert(
            context.connection_id.clone(),
            WebSocketSession {
                context: translated,
                _guard: guard,
                relay,
                closing: Arc::new(AtomicBool::new(false)),
            },
        );
    }

    fn close_session(&self, context: &TransportContext) {
        self.sessions.lock().unwrap().remove(&context.connection_id);
    }

    fn close_requested(&self, context: &TransportContext) -> bool {
        self.sessions
            .lock()
            .unwrap()
            .get(&context.connection_id)
            .is_some_and(|session| session.closing.load(Ordering::Acquire))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RejectRpc;

    impl JsonRpcProtocol for RejectRpc {
        fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
            panic!("WebSocket control frames must not reach the JSON-RPC protocol")
        }
    }

    #[tokio::test]
    async fn control_ping_returns_same_payload_pong_without_rpc_dispatch() {
        let adapter = WebSocketJsonRpcAdapter::new(Arc::new(RejectRpc), "ws-mqtt");
        let payload = Bytes::from_static(b"broker-ping");
        let ping = encode_websocket_message(&WebSocketMessage::Ping(payload.clone())).unwrap();
        let response = adapter.handle_message(&ping).await.unwrap();
        assert_eq!(
            decode_websocket_message(&response).unwrap(),
            Some(WebSocketMessage::Pong(payload))
        );
    }

    #[tokio::test]
    async fn close_control_frame_requests_session_cleanup() {
        let adapter = WebSocketJsonRpcAdapter::new(Arc::new(RejectRpc), "ws-mqtt");
        let session = TransportSession::new("ws-mqtt", true);
        adapter.session_opened(&session.context);
        let close = json_rpc_2_core::encode_websocket_close_frame().unwrap();
        assert!(
            adapter
                .handle_message_with_context(&close, &session.context)
                .await
                .is_none()
        );
        assert!(adapter.session_close_requested(&session.context));
        adapter.session_closed(&session.context);
    }
}
