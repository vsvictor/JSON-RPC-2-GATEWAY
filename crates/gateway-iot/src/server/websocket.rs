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
                Ok(None) => return None,
                Err(error) => {
                    log_transport_error(self.kind, &context.connection_id, "decode frame", &error);
                    if let Some(session) = self.sessions.lock().unwrap().get(&context.connection_id)
                    {
                        session.closing.store(true, Ordering::Release);
                    }
                    return None;
                }
            };
            let is_text = matches!(inbound, WebSocketMessage::Text(_));
            let request = websocket_message_bytes(&inbound);
            let response = self
                .protocol
                .handle_message_with_context(&request, &context)
                .await?;
            let outbound = match (is_text, std::str::from_utf8(&response)) {
                (true, Ok(text)) => WebSocketMessage::Text(text.to_owned()),
                _ => WebSocketMessage::Binary(response),
            };
            encode_websocket_message(&outbound).ok().map(Bytes::from)
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
