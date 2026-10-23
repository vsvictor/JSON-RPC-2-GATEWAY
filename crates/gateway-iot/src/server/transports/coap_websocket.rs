//! WebSocket-style framing over CoAP-style confirmable UDP messages.

use std::{collections::HashMap, io, net::SocketAddr, sync::Arc, time::Instant};

use tokio::{sync::Mutex, task::JoinSet};

use super::coap::{PeerSession, RequestAction, cache_request_state};
use bytes::Bytes;
use json_rpc_2_core::{
    CoapMessage, CoapMessageType, WebSocketMessage, decode_coap_message, decode_websocket_message,
    encode_coap_message, encode_websocket_message, websocket_message_bytes,
};
use json_rpc_2_transport::{
    CancellationToken, DatagramReceiver, JsonRpcProtocol, Transport, TransportFuture,
    TransportRuntime, dispatch_stateless, log_control, log_json_rpc, log_transport_error,
};

pub struct CoapWebSocketTransport {
    socket: DatagramReceiver,
    runtime: Arc<TransportRuntime>,
}

impl CoapWebSocketTransport {
    pub fn new(socket: impl Into<DatagramReceiver>) -> Self {
        Self {
            socket: socket.into(),
            runtime: Arc::new(TransportRuntime::default()),
        }
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.runtime = runtime;
        self
    }
}

impl Transport for CoapWebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(serve(self.socket, protocol, shutdown, self.runtime))
    }
}

pub async fn serve(
    socket: impl Into<DatagramReceiver>,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    let mut source = socket.into();
    let socket = source.socket();
    let sessions = Arc::new(Mutex::new(HashMap::<SocketAddr, PeerSession>::new()));
    let mut tasks = JoinSet::new();
    let mut buffer = vec![0u8; runtime.udp_reliability.max_packet_size];
    loop {
        let received = tokio::select! {
            received = source.receive(&mut buffer) => Some(received),
            _ = shutdown.cancelled() => None,
        };
        let Some(received) = received else { break };
        let received = received?;
        let peer = received.peer();
        let message = match decode_coap_message(received.packet()) {
            Ok(message) => message,
            Err(error) => {
                log_transport_error("WebSocket/CoAP", peer, "packet", &error);
                continue;
            }
        };
        match (message.ty, message.code) {
            (CoapMessageType::Confirmable, 0) => {
                log_control("WebSocket/CoAP", peer, "ping", "pong");
                let pong = encode_coap_message(&CoapMessage::pong(message.message_id))
                    .map_err(io::Error::other)?;
                socket.send_to(&pong, peer).await?;
            }
            (CoapMessageType::Confirmable, _) => {
                match cache_request_state(
                    &sessions,
                    &runtime,
                    peer,
                    message.message_id,
                    None,
                    false,
                )
                .await
                {
                    RequestAction::Process => {
                        let inbound = match decode_websocket_message(&message.payload) {
                            Ok(Some(message)) => message,
                            Ok(None) => continue,
                            Err(error) => {
                                log_transport_error("WebSocket/CoAP", peer, "decode frame", &error);
                                continue;
                            }
                        };
                        let request = Bytes::copy_from_slice(websocket_message_bytes(&inbound));
                        let is_text = matches!(inbound, WebSocketMessage::Text(_));
                        let socket = Arc::clone(&socket);
                        let protocol = Arc::clone(&protocol);
                        let sessions = Arc::clone(&sessions);
                        let runtime = Arc::clone(&runtime);
                        tasks.spawn(async move {
                            let started = Instant::now();
                            let response =
                                dispatch_stateless(protocol.as_ref(), request.clone(), "ws-coap")
                                    .await;
                            log_json_rpc(
                                "WebSocket/CoAP",
                                peer,
                                &request,
                                response.as_deref(),
                                started.elapsed(),
                            );
                            let outbound = match response {
                                Some(response) => {
                                    let frame = match (is_text, std::str::from_utf8(&response)) {
                                        (true, Ok(text)) => WebSocketMessage::Text(text.to_owned()),
                                        _ => WebSocketMessage::Binary(response),
                                    };
                                    match encode_websocket_message(&frame) {
                                        Ok(payload) => CoapMessage::response(
                                            message.message_id,
                                            message.token.clone(),
                                            payload,
                                        ),
                                        Err(error) => {
                                            log_transport_error(
                                                "WebSocket/CoAP",
                                                peer,
                                                "encode frame",
                                                &error,
                                            );
                                            return;
                                        }
                                    }
                                }
                                None => CoapMessage::ack(message.message_id, message.token.clone()),
                            };
                            match encode_coap_message(&outbound) {
                                Ok(encoded) => {
                                    if let Err(error) = socket.send_to(&encoded, peer).await {
                                        log_transport_error(
                                            "WebSocket/CoAP",
                                            peer,
                                            "response",
                                            &error,
                                        );
                                    } else {
                                        let _ = cache_request_state(
                                            &sessions,
                                            &runtime,
                                            peer,
                                            message.message_id,
                                            Some(encoded),
                                            true,
                                        )
                                        .await;
                                    }
                                }
                                Err(error) => {
                                    log_transport_error("WebSocket/CoAP", peer, "response", &error)
                                }
                            }
                        });
                    }
                    RequestAction::Replay(response) => {
                        let _ = socket.send_to(&response, peer).await;
                    }
                    RequestAction::Drop => {}
                }
            }
            _ => {}
        }
    }
    while tasks.join_next().await.is_some() {}
    Ok(())
}
