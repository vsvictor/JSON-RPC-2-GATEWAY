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
                            Ok(None) => {
                                log_control("WebSocket/CoAP", peer, "close", "ack");
                                let encoded = encode_coap_message(&CoapMessage::ack(
                                    message.message_id,
                                    message.token.clone(),
                                ))
                                .map_err(io::Error::other)?;
                                socket.send_to(&encoded, peer).await?;
                                let _ = cache_request_state(
                                    &sessions,
                                    &runtime,
                                    peer,
                                    message.message_id,
                                    Some(encoded),
                                    true,
                                )
                                .await;
                                continue;
                            }
                            Err(error) => {
                                log_transport_error("WebSocket/CoAP", peer, "decode frame", &error);
                                continue;
                            }
                        };
                        let socket = Arc::clone(&socket);
                        let protocol = Arc::clone(&protocol);
                        let sessions = Arc::clone(&sessions);
                        let runtime = Arc::clone(&runtime);
                        tasks.spawn(async move {
                            let started = Instant::now();
                            let response = match inbound {
                                WebSocketMessage::Ping(payload) => {
                                    log_control("WebSocket/CoAP", peer, "ping", "pong");
                                    match encode_websocket_message(&WebSocketMessage::Pong(payload))
                                    {
                                        Ok(frame) => Some(frame),
                                        Err(error) => {
                                            log_transport_error(
                                                "WebSocket/CoAP",
                                                peer,
                                                "encode pong",
                                                &error,
                                            );
                                            None
                                        }
                                    }
                                }
                                WebSocketMessage::Pong(_) => {
                                    log_control("WebSocket/CoAP", peer, "pong", "ignored");
                                    None
                                }
                                message @ (WebSocketMessage::Text(_)
                                | WebSocketMessage::Binary(_)) => {
                                    let is_text = matches!(message, WebSocketMessage::Text(_));
                                    let request =
                                        Bytes::copy_from_slice(websocket_message_bytes(&message));
                                    let response = dispatch_stateless(
                                        protocol.as_ref(),
                                        request.clone(),
                                        "ws-coap",
                                    )
                                    .await;
                                    log_json_rpc(
                                        "WebSocket/CoAP",
                                        peer,
                                        &request,
                                        response.as_deref(),
                                        started.elapsed(),
                                    );
                                    let response = response.map(|response| {
                                        match (is_text, std::str::from_utf8(&response)) {
                                            (true, Ok(text)) => {
                                                WebSocketMessage::Text(text.to_owned())
                                            }
                                            _ => WebSocketMessage::Binary(response),
                                        }
                                    });
                                    response.and_then(|message| {
                                        match encode_websocket_message(&message) {
                                            Ok(frame) => Some(frame),
                                            Err(error) => {
                                                log_transport_error(
                                                    "WebSocket/CoAP",
                                                    peer,
                                                    "encode response",
                                                    &error,
                                                );
                                                None
                                            }
                                        }
                                    })
                                }
                            };
                            let outbound = match response {
                                Some(payload) => CoapMessage::response(
                                    message.message_id,
                                    message.token.clone(),
                                    payload,
                                ),
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

#[cfg(test)]
mod tests {
    use super::*;
    use json_rpc_2_core::{decode_coap_message, encode_coap_message};
    use json_rpc_2_transport::ProtocolFuture;
    use tokio::net::UdpSocket;
    use tokio::time::{Duration, timeout};

    struct RejectRpc;

    impl JsonRpcProtocol for RejectRpc {
        fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
            panic!("WebSocket control frames must not reach the JSON-RPC protocol")
        }
    }

    #[tokio::test]
    async fn coap_websocket_control_frames_reply_without_rpc_dispatch() {
        let server_socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let address = server_socket.local_addr().unwrap();
        let shutdown = CancellationToken::new();
        let server = tokio::spawn(serve(
            server_socket,
            Arc::new(RejectRpc),
            shutdown.clone(),
            Arc::new(TransportRuntime::default()),
        ));
        let client = UdpSocket::bind("127.0.0.1:0").await.unwrap();

        let ping_payload = Bytes::from_static(b"coap-control-ping");
        let ping_frame =
            encode_websocket_message(&WebSocketMessage::Ping(ping_payload.clone())).unwrap();
        let request = CoapMessage::request(10, b"ping".to_vec(), ping_frame);
        client
            .send_to(&encode_coap_message(&request).unwrap(), address)
            .await
            .unwrap();
        let mut buffer = [0; 512];
        let received = timeout(Duration::from_secs(2), client.recv_from(&mut buffer))
            .await
            .unwrap()
            .unwrap()
            .0;
        let response = decode_coap_message(&buffer[..received]).unwrap();
        assert_eq!(response.message_id, request.message_id);
        assert_eq!(
            decode_websocket_message(&response.payload).unwrap(),
            Some(WebSocketMessage::Pong(ping_payload))
        );

        let close = CoapMessage::request(
            11,
            b"close".to_vec(),
            json_rpc_2_core::encode_websocket_close_frame().unwrap(),
        );
        client
            .send_to(&encode_coap_message(&close).unwrap(), address)
            .await
            .unwrap();
        let received = timeout(Duration::from_secs(2), client.recv_from(&mut buffer))
            .await
            .unwrap()
            .unwrap()
            .0;
        let response = decode_coap_message(&buffer[..received]).unwrap();
        assert_eq!(response.ty, CoapMessageType::Acknowledgement);
        assert_eq!(response.code, 0);
        assert_eq!(response.message_id, close.message_id);
        assert_eq!(response.token, close.token);
        assert!(response.payload.is_empty());

        shutdown.cancel();
        timeout(Duration::from_secs(2), server)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
    }
}
