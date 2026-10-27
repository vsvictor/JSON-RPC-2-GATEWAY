use std::{io, sync::Arc};

use json_rpc_2_transport::H3_ALPN;

use json_rpc_2_transport::QUIC_ALPN;

use json_rpc_2_transport::WS_QUIC_ALPN;
use json_rpc_2_transport::{
    CancellationToken, JsonRpcProtocol, QUIC_NO_ERROR, TransportRuntime, log_transport_error,
    multiplex::StreamMultiplexor,
};
use quinn::{Connection, Endpoint};

use json_rpc_2_runtime::TransportKind;

pub(crate) async fn serve(
    endpoint: Endpoint,
    kinds: Vec<TransportKind>,
    protocol: Arc<dyn JsonRpcProtocol>,
    runtime: Arc<TransportRuntime>,
    shutdown: CancellationToken,
) -> io::Result<()> {
    let multiplexor =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    let connections = multiplexor.handle();
    let kinds: Arc<[TransportKind]> = kinds.into();
    loop {
        let incoming = tokio::select! {
            _ = shutdown.cancelled() => break,
            incoming = endpoint.accept() => match incoming {
                Some(incoming) => incoming,
                None => break,
            },
        };
        let protocol = protocol.clone();
        let runtime = runtime.clone();
        let shutdown = shutdown.clone();
        let kinds = kinds.clone();
        connections
            .spawn(async move {
                let peer = incoming.remote_address();
                let connecting = tokio::select! {
                    _ = shutdown.cancelled() => return,
                    result = incoming => result,
                };
                match connecting {
                    Ok(connection) => {
                        dispatch(connection, kinds, protocol, runtime, shutdown).await;
                    }
                    Err(error) => {
                        log_transport_error("shared QUIC", peer, "handshake", &error);
                    }
                }
            })
            .await?;
    }
    endpoint.set_server_config(None);
    shutdown.cancel();
    multiplexor.close().await;
    endpoint.close(QUIC_NO_ERROR.into(), b"shared listener shutdown");
    endpoint.wait_idle().await;
    Ok(())
}

async fn dispatch(
    connection: Connection,
    kinds: Arc<[TransportKind]>,
    protocol: Arc<dyn JsonRpcProtocol>,
    runtime: Arc<TransportRuntime>,
    shutdown: CancellationToken,
) {
    let peer = connection.remote_address();
    let handshake = connection
        .handshake_data()
        .and_then(|data| data.downcast::<quinn::crypto::rustls::HandshakeData>().ok());
    let alpn = handshake.as_ref().and_then(|data| data.protocol.as_deref());
    let enabled = |id| kinds.iter().any(|kind| kind.id() == id);
    match alpn {
        Some(alpn) if alpn == QUIC_ALPN && enabled("quic") => {
            json_rpc_2_quic::server::transports::quic::serve_connection(
                connection, peer, protocol, shutdown, runtime,
            )
            .await;
        }

        Some(alpn) if alpn == WS_QUIC_ALPN && enabled("ws-quic") => {
            let websocket = Arc::new(
                json_rpc_2_websockets::server::transports::JsonRpcWebSocket::new(protocol),
            );
            json_rpc_2_websockets::server::transports::quic::serve_connection(
                connection, peer, websocket, shutdown, runtime,
            )
            .await;
        }

        Some(alpn) if alpn == H3_ALPN && (enabled("http3") || enabled("ws-http3")) => {
            serve_http3(connection, kinds, protocol, runtime, shutdown).await;
        }
        _ => {
            log_transport_error(
                "shared QUIC",
                peer,
                "ALPN dispatch",
                &"missing or disabled protocol",
            );
            connection.close(1u32.into(), b"disabled application protocol");
        }
    }
}

async fn serve_http3(
    connection: Connection,
    kinds: Arc<[TransportKind]>,
    protocol: Arc<dyn JsonRpcProtocol>,
    runtime: Arc<TransportRuntime>,
    shutdown: CancellationToken,
) {
    let peer = connection.remote_address();
    let raw = kinds.iter().any(|kind| kind.id() == "http3");
    let websocket = kinds.iter().any(|kind| kind.id() == "ws-http3");
    let context =
        json_rpc_2_transport::TransportSession::stateless(if raw { "http3" } else { "ws-http3" });
    let disabled = protocol.session_cancelled(&context);
    tokio::pin!(disabled);
    let mut builder = h3::server::builder();
    builder.enable_extended_connect(websocket);
    let setup = tokio::select! {
        _ = shutdown.cancelled() => {
            connection.close(QUIC_NO_ERROR.into(), b"HTTP/3 setup cancelled");
            return;
        }
        setup = builder.build(h3_quinn::Connection::new(connection.clone())) => setup,
    };
    let mut h3 = match setup {
        Ok(h3) => h3,
        Err(error) => {
            log_transport_error("shared HTTP/3", peer, "setup", &error);
            connection.close(1u32.into(), b"HTTP/3 setup failed");
            return;
        }
    };
    let multiplexor = StreamMultiplexor::with_limit(
        runtime.limits.max_concurrent_requests,
        runtime.metrics.clone(),
    );
    let requests = multiplexor.handle();
    loop {
        let resolver = tokio::select! {
            biased;
            _ = &mut disabled, if raw != websocket => break,
            _ = shutdown.cancelled() => {
                if let Err(error) = h3.shutdown(0).await {
                    log_transport_error("shared HTTP/3", peer, "GOAWAY", &error);
                }
                break;
            },
            request = h3.accept() => match request {
                Ok(Some(resolver)) => resolver,
                Ok(None) => break,
                Err(error) => {
                    log_transport_error("shared HTTP/3", peer, "request headers", &error);
                    break;
                }
            },
        };
        let protocol = protocol.clone();

        let shutdown = shutdown.clone();

        let runtime = runtime.clone();
        if requests
            .spawn(async move {
                let result = async {
                    use json_rpc_2_websockets::server::transports::http3::handle_resolved_request;
                    let (request, mut stream) = resolver.resolve_request().await?;
                    let kind = if request.method() == http::Method::CONNECT {
                        "ws-http3"
                    } else {
                        "http3"
                    };
                    let context = json_rpc_2_transport::TransportSession::stateless(kind);
                    if protocol.session_close_requested(&context) {
                        stream
                            .send_response(
                                http::Response::builder()
                                    .status(http::StatusCode::SERVICE_UNAVAILABLE)
                                    .body(())?,
                            )
                            .await?;
                        stream.finish().await?;
                        return Ok(());
                    }
                    let cancellation_protocol = protocol.clone();
                    tokio::select! {
                        biased;
                        _ = cancellation_protocol.session_cancelled(&context) => Ok(()),
                        result = async move {
                    if websocket && request.method() == http::Method::CONNECT {
                        let websocket = Arc::new(
                            json_rpc_2_websockets::server::transports::JsonRpcWebSocket::new(
                                protocol,
                            ),
                        );
                        return handle_resolved_request(
                            request, stream, peer, websocket, shutdown, runtime,
                        )
                        .await;
                    }
                    if raw || request.method() != http::Method::POST {
                        json_rpc_2_quic::server::transports::http3::handle_resolved_request(
                            request, stream, peer, protocol,
                        )
                        .await
                    } else {
                        stream
                            .send_response(
                                http::Response::builder()
                                    .status(http::StatusCode::METHOD_NOT_ALLOWED)
                                    .body(())?,
                            )
                            .await?;
                        stream.finish().await?;
                        Ok(())
                    }
                        } => result,
                    }
                }
                .await;
                if let Err(error) = result {
                    log_transport_error("shared HTTP/3", peer, "request", error.as_ref());
                }
            })
            .await
            .is_err()
        {
            break;
        }
    }
    multiplexor.close().await;
    drop(h3);
    connection.close(QUIC_NO_ERROR.into(), b"HTTP/3 shutdown");
}
