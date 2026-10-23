//! Gateway JSON-RPC server over CoAP-style confirmable UDP messages.

use std::{
    collections::{HashMap, VecDeque},
    io,
    net::SocketAddr,
    sync::Arc,
    time::Instant,
};

use tokio::{sync::Mutex, task::JoinSet};

use json_rpc_2_core::{CoapMessage, CoapMessageType, decode_coap_message, encode_coap_message};
use json_rpc_2_transport::{
    CancellationToken, Transport, TransportFuture, TransportRuntime, log_control, log_json_rpc,
    log_transport_error,
};
use json_rpc_2_transport::{DatagramReceiver, JsonRpcProtocol};

/// CoAP-inspired transport over UDP.
pub struct CoapTransport {
    socket: DatagramReceiver,
    runtime: Arc<TransportRuntime>,
}

impl CoapTransport {
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

impl Transport for CoapTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(serve(self.socket, protocol, shutdown, self.runtime))
    }
}

#[derive(Default)]
pub(super) struct PeerSession {
    entries: HashMap<u16, CachedRequest>,
    order: VecDeque<u16>,
    last_seen: Option<Instant>,
}

#[derive(Clone)]
pub(super) enum CachedRequest {
    InFlight { seen_at: Instant },
    Complete { seen_at: Instant, response: Vec<u8> },
}

pub(super) enum RequestAction {
    Process,
    Replay(Vec<u8>),
    Drop,
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
                log_transport_error("CoAP", peer, "packet", &error);
                continue;
            }
        };
        match (message.ty, message.code) {
            (CoapMessageType::Confirmable, 0) => {
                log_control("CoAP", peer, "ping", "pong");
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
                        let socket = Arc::clone(&socket);
                        let protocol = Arc::clone(&protocol);
                        let sessions = Arc::clone(&sessions);
                        let runtime = Arc::clone(&runtime);
                        tasks.spawn(async move {
                            let started = Instant::now();
                            let payload = bytes::Bytes::from(message.payload);
                            let response = json_rpc_2_transport::dispatch_stateless(
                                protocol.as_ref(),
                                payload.clone(),
                                "coap",
                            )
                            .await;
                            log_json_rpc(
                                "CoAP",
                                peer,
                                &payload,
                                response.as_deref(),
                                started.elapsed(),
                            );
                            let outbound = match response {
                                Some(response) => CoapMessage::response(
                                    message.message_id,
                                    message.token.clone(),
                                    response.to_vec(),
                                ),
                                None => CoapMessage::ack(message.message_id, message.token.clone()),
                            };
                            match encode_coap_message(&outbound) {
                                Ok(encoded) => {
                                    if let Err(error) = socket.send_to(&encoded, peer).await {
                                        log_transport_error("CoAP", peer, "response", &error);
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
                                Err(error) => log_transport_error("CoAP", peer, "response", &error),
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

pub(super) async fn cache_request_state(
    sessions: &Arc<Mutex<HashMap<SocketAddr, PeerSession>>>,
    runtime: &Arc<TransportRuntime>,
    peer: SocketAddr,
    message_id: u16,
    response: Option<Vec<u8>>,
    complete: bool,
) -> RequestAction {
    let now = Instant::now();
    let mut sessions = sessions.lock().await;
    let session = sessions.entry(peer).or_default();
    session.last_seen = Some(now);
    prune_session(session, runtime, now);
    match session.entries.get(&message_id) {
        Some(CachedRequest::InFlight { .. }) if !complete => return RequestAction::Drop,
        Some(CachedRequest::Complete { response, .. }) if !complete => {
            return RequestAction::Replay(response.clone());
        }
        _ => {}
    }
    if complete {
        let response = response.unwrap_or_default();
        session.entries.insert(
            message_id,
            CachedRequest::Complete {
                seen_at: now,
                response,
            },
        );
        session.order.push_back(message_id);
        RequestAction::Drop
    } else {
        session
            .entries
            .insert(message_id, CachedRequest::InFlight { seen_at: now });
        session.order.push_back(message_id);
        RequestAction::Process
    }
}

fn prune_session(session: &mut PeerSession, runtime: &TransportRuntime, now: Instant) {
    while let Some(message_id) = session.order.front().copied() {
        let remove = match session.entries.get(&message_id) {
            Some(CachedRequest::InFlight { seen_at })
            | Some(CachedRequest::Complete { seen_at, .. }) => {
                now.duration_since(*seen_at) >= runtime.udp_reliability.dedup_cache_ttl
            }
            None => true,
        };
        if session.entries.len() > runtime.udp_reliability.dedup_cache_capacity || remove {
            session.order.pop_front();
            session.entries.remove(&message_id);
        } else {
            break;
        }
    }
}
