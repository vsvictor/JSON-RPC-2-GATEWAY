//! Direct protocol dispatch for a listener shared by TCP, HTTP/1.1 and TLS.
//!
//! Dispatch is based only on the TLS record prefix, HTTP method prefix, and
//! masked WebSocket frame header. A plaintext request beginning `GET ` or
//! `POST ` is reserved for HTTP; clients using raw TCP framing must avoid
//! those prefixes. No JSON-RPC payload parsing is used for classification.

use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use bytes::Bytes;
use hyper::Request;
use hyper_util::{rt::TokioIo, service::TowerToHyperService};
use json_rpc_2_runtime::TransportKind;
use json_rpc_2_transport::{
    CancellationToken, JsonRpcProtocol, TransportRuntime, apply_tcp_stream_tuning,
    log_transport_error, multiplex::StreamMultiplexor,
};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf},
    net::{TcpListener, TcpStream},
    task::JoinSet,
    time::timeout,
};
use tokio_rustls::TlsAcceptor;
use tower::{ServiceExt, service_fn};

const PREFIX_TIMEOUT: Duration = Duration::from_secs(5);
const TLS_HANDSHAKE_TIMEOUT: Duration = Duration::from_secs(10);

/// Serve supported TCP-family protocols directly from one accepted listener.
///
/// TLS is terminated before application-protocol classification. The caller
/// retains ownership of the listener and supplies the combined TLS config.
/// This function stops accepting on cancellation, closes active connections,
/// waits for in-flight tasks, and then closes upgraded WebSocket sessions.
pub async fn serve(
    listener: TcpListener,
    kinds: Vec<TransportKind>,
    protocol: Arc<dyn JsonRpcProtocol>,
    tls: Option<Arc<rustls::ServerConfig>>,
    runtime: Arc<TransportRuntime>,
    shutdown: CancellationToken,
) -> io::Result<()> {
    if kinds.is_empty() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "shared TCP ingress requires at least one transport kind",
        ));
    }
    if kinds.iter().any(|kind| {
        !matches!(
            kind,
            TransportKind::Tcp
                | TransportKind::Tls
                | TransportKind::Http1
                | TransportKind::WebSocketTcp
                | TransportKind::WebSocketHttp1
                | TransportKind::WebSocketTls
        )
    }) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "shared TCP ingress only accepts TCP, TLS, HTTP/1.1 and WebSocket/TCP kinds",
        ));
    }
    let kinds = Arc::new(kinds);
    let tls_enabled = has_tls_kind(&kinds);
    if tls_enabled && tls.is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "shared TCP TLS transports require a TLS server config",
        ));
    }

    let sessions =
        StreamMultiplexor::with_limit(runtime.limits.max_connections, runtime.metrics.clone());
    let http_router = build_http_router(
        Arc::clone(&protocol),
        Arc::clone(&runtime),
        shutdown.clone(),
        sessions.handle(),
        &kinds,
    );
    let semaphore = Arc::new(tokio::sync::Semaphore::new(runtime.limits.max_connections));
    let mut connections = JoinSet::new();
    let mut result = Ok(());

    loop {
        let permit = tokio::select! {
            _ = shutdown.cancelled() => break,
            permit = Arc::clone(&semaphore).acquire_owned() => match permit {
                Ok(permit) => permit,
                Err(_) => {
                    result = Err(io::Error::other("shared TCP connection limit closed"));
                    break;
                }
            },
            Some(joined) = connections.join_next(), if !connections.is_empty() => {
                if let Err(error) = joined {
                    log_transport_error("shared TCP", "server", "connection task", &error);
                }
                continue;
            }
        };
        let accepted = tokio::select! {
            _ = shutdown.cancelled() => {
                drop(permit);
                break;
            }
            accepted = listener.accept() => accepted,
        };
        let (stream, peer) = match accepted {
            Ok(accepted) => accepted,
            Err(error) => {
                result = Err(error);
                break;
            }
        };
        let protocol = Arc::clone(&protocol);
        let runtime = Arc::clone(&runtime);
        let kinds = Arc::clone(&kinds);
        let tls = tls.clone();
        let http_router = http_router.clone();
        let shutdown = shutdown.clone();
        connections.spawn(async move {
            let _permit = permit;
            if let Err(error) = handle_connection(
                stream,
                peer,
                kinds,
                protocol,
                tls,
                runtime,
                http_router,
                shutdown,
            )
            .await
                && error.kind() != io::ErrorKind::NotConnected
                && error.kind() != io::ErrorKind::UnexpectedEof
            {
                log_transport_error("shared TCP", peer, "connection", &error);
            }
        });
    }

    drop(listener);
    shutdown.cancel();
    while let Some(joined) = connections.join_next().await {
        if let Err(error) = joined {
            log_transport_error("shared TCP", "server", "connection task", &error);
        }
    }
    sessions.close().await;
    result
}

type EmbeddedRouter = axum::Router;

fn build_http_router(
    protocol: Arc<dyn JsonRpcProtocol>,
    runtime: Arc<TransportRuntime>,
    shutdown: CancellationToken,
    sessions: json_rpc_2_transport::multiplex::StreamMultiplexorHandle,
    kinds: &[TransportKind],
) -> Option<EmbeddedRouter> {
    let http_enabled = kinds.iter().any(|kind| {
        matches!(
            kind,
            TransportKind::Http1
                | TransportKind::Tls
                | TransportKind::WebSocketHttp1
                | TransportKind::WebSocketTls
        )
    });
    if !http_enabled {
        return None;
    }

    let mut router = json_rpc_2_tcp::server::transports::http1::router_with_runtime(
        Arc::clone(&protocol),
        Arc::clone(&runtime),
    );
    let plain_ws = kinds.contains(&TransportKind::WebSocketHttp1);
    let tls_ws = kinds.contains(&TransportKind::WebSocketTls);
    if plain_ws || tls_ws {
        let websocket_protocol: Arc<
            dyn json_rpc_2_websockets::server::transports::WebSocketProtocol,
        > = Arc::new(json_rpc_2_websockets::server::transports::JsonRpcWebSocket::new(protocol));
        let ws_router =
            json_rpc_2_websockets::server::transports::http1::router_with_session_handle(
                websocket_protocol,
                sessions,
                shutdown,
                runtime,
                plain_ws,
                tls_ws,
            );
        router = router.merge(ws_router);
    }
    Some(router)
}

#[allow(clippy::too_many_arguments)]
async fn handle_connection(
    mut stream: TcpStream,
    peer: std::net::SocketAddr,
    kinds: Arc<Vec<TransportKind>>,
    protocol: Arc<dyn JsonRpcProtocol>,
    tls: Option<Arc<rustls::ServerConfig>>,
    runtime: Arc<TransportRuntime>,
    http_router: Option<EmbeddedRouter>,
    shutdown: CancellationToken,
) -> io::Result<()> {
    apply_tcp_stream_tuning(&stream, &runtime.tcp_tuning)?;
    let first = tokio::select! {
        _ = shutdown.cancelled() => return Ok(()),
        first = timeout(PREFIX_TIMEOUT, stream.read_u8()) => match first {
            Ok(Ok(first)) => first,
            Ok(Err(error)) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Ok(Err(error)) => return Err(error),
            Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "initial protocol byte timed out")),
        },
    };
    let mut prefix = vec![first];

    if is_tls_kind(&kinds) && first == 0x16 {
        let second = tokio::select! {
            _ = shutdown.cancelled() => return Ok(()),
            value = timeout(PREFIX_TIMEOUT, stream.read_u8()) => match value {
            Ok(Ok(value)) => value,
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TLS prefix timed out",
                ));
            }
            },
        };
        prefix.push(second);
        if second == 0x03 {
            let third = tokio::select! {
                _ = shutdown.cancelled() => return Ok(()),
                value = timeout(PREFIX_TIMEOUT, stream.read_u8()) => match value {
                Ok(Ok(value)) => value,
                Ok(Err(error)) => return Err(error),
                Err(_) => {
                    return Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "TLS record prefix timed out",
                    ));
                }
                },
            };
            prefix.push(third);
            if (1..=4).contains(&third) {
                let tls_config = tls.ok_or_else(|| {
                    io::Error::new(io::ErrorKind::InvalidInput, "TLS config is missing")
                })?;
                let replay = ReplayIo::new(prefix, stream);
                let acceptor = TlsAcceptor::from(tls_config);
                let tls_stream = tokio::select! {
                    _ = shutdown.cancelled() => return Ok(()),
                    accepted = timeout(TLS_HANDSHAKE_TIMEOUT, acceptor.accept(replay)) => match accepted {
                        Ok(Ok(stream)) => stream,
                        Ok(Err(error)) => return Err(io::Error::new(io::ErrorKind::InvalidData, error)),
                        Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "TLS handshake timed out")),
                    },
                };
                return dispatch(
                    tls_stream,
                    Vec::new(),
                    peer,
                    true,
                    kinds,
                    protocol,
                    runtime,
                    http_router,
                    shutdown,
                )
                .await;
            }
        }
    }

    dispatch(
        stream,
        prefix,
        peer,
        false,
        kinds,
        protocol,
        runtime,
        http_router,
        shutdown,
    )
    .await
}

#[allow(clippy::too_many_arguments)]
async fn dispatch<S>(
    mut stream: S,
    mut prefix: Vec<u8>,
    peer: std::net::SocketAddr,
    secure: bool,
    kinds: Arc<Vec<TransportKind>>,
    protocol: Arc<dyn JsonRpcProtocol>,
    runtime: Arc<TransportRuntime>,
    http_router: Option<EmbeddedRouter>,
    shutdown: CancellationToken,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let classification = tokio::select! {
        _ = shutdown.cancelled() => return Ok(()),
        result = timeout(PREFIX_TIMEOUT, classify(&mut stream, &mut prefix)) => match result {
            Ok(result) => result?,
            Err(_) => return Err(io::Error::new(io::ErrorKind::TimedOut, "protocol prefix timed out")),
        },
    };
    let replay = ReplayIo::new(prefix, stream);

    match classification {
        Classification::RawTcp if !secure && kinds.contains(&TransportKind::Tcp) => {
            let (reader, writer) = tokio::io::split(replay);
            json_rpc_2_tcp::server::transports::tcp::serve_stream(
                reader, writer, protocol, shutdown, peer, "tcp",
            )
            .await
        }
        Classification::RawTcp if secure && kinds.contains(&TransportKind::Tls) => {
            let (reader, writer) = tokio::io::split(replay);
            json_rpc_2_tcp::server::transports::tcp::serve_stream(
                reader, writer, protocol, shutdown, peer, "tls",
            )
            .await
        }
        Classification::RawWebSocket if !secure && kinds.contains(&TransportKind::WebSocketTcp) => {
            let websocket_protocol: Arc<
                dyn json_rpc_2_websockets::server::transports::WebSocketProtocol,
            > = Arc::new(
                json_rpc_2_websockets::server::transports::JsonRpcWebSocket::new(protocol),
            );
            json_rpc_2_websockets::server::transports::tcp::serve_established_stream(
                replay,
                peer,
                websocket_protocol,
                shutdown,
                runtime,
            )
            .await;
            Ok(())
        }
        Classification::RawWebSocket if secure && kinds.contains(&TransportKind::WebSocketTls) => {
            let websocket_protocol: Arc<
                dyn json_rpc_2_websockets::server::transports::WebSocketProtocol,
            > = Arc::new(
                json_rpc_2_websockets::server::transports::JsonRpcWebSocket::new(protocol),
            );
            json_rpc_2_websockets::server::transports::tcp::serve_established_stream_with_kind(
                replay,
                peer,
                websocket_protocol,
                shutdown,
                runtime,
                "ws-tls",
            )
            .await;
            Ok(())
        }
        Classification::HttpGet if http_allowed(&kinds, secure, true) => {
            let Some(router) = http_router else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP is not enabled on this shared listener",
                ));
            };
            serve_http(replay, peer, secure, router, protocol, shutdown).await
        }
        Classification::HttpPost if http_allowed(&kinds, secure, false) => {
            let Some(router) = http_router else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "HTTP is not enabled on this shared listener",
                ));
            };
            serve_http(replay, peer, secure, router, protocol, shutdown).await
        }
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "protocol prefix is not enabled on this shared listener",
        )),
    }
}

async fn serve_http<S>(
    stream: S,
    peer: std::net::SocketAddr,
    secure: bool,
    router: EmbeddedRouter,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
) -> io::Result<()>
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (profile, selected) = tokio::sync::watch::channel(None);
    let cancellation_protocol = protocol.clone();
    let service = service_fn(move |mut request: Request<hyper::body::Incoming>| {
        let router = router.clone();
        let protocol = protocol.clone();
        let websocket = request
            .headers()
            .get(hyper::header::UPGRADE)
            .is_some_and(|value| value.as_bytes().eq_ignore_ascii_case(b"websocket"));
        let kind = match (secure, websocket) {
            (true, true) => "ws-tls",
            (true, false) => "tls",
            (false, true) => "ws-http1",
            (false, false) => "http1",
        };
        let context = json_rpc_2_transport::TransportSession::stateless(kind);
        profile.send_replace(Some(context.clone()));
        request
            .extensions_mut()
            .insert(axum::extract::ConnectInfo(peer));
        if secure {
            request
                .extensions_mut()
                .insert(json_rpc_2_tcp::server::transports::http1::TlsIngress);
        }
        async move {
            if protocol.session_close_requested(&context) {
                return Ok::<_, std::convert::Infallible>(
                    axum::response::Response::builder()
                        .status(hyper::StatusCode::SERVICE_UNAVAILABLE)
                        .body(axum::body::Body::empty())
                        .unwrap(),
                );
            }
            router.oneshot(request.map(axum::body::Body::new)).await
        }
    });
    let service = TowerToHyperService::new(service);
    let connection = hyper::server::conn::http1::Builder::new()
        .serve_connection(TokioIo::new(stream), service)
        .with_upgrades();
    tokio::pin!(connection);
    tokio::select! {
        biased;
        _ = http_profile_cancelled(cancellation_protocol, selected) => Ok(()),
        _ = shutdown.cancelled() => {
            connection.as_mut().graceful_shutdown();
            connection.await.map_err(|error| io::Error::other(error.to_string()))
        }
        result = &mut connection => result.map_err(|error| io::Error::other(error.to_string())),
    }
}

async fn http_profile_cancelled(
    protocol: Arc<dyn JsonRpcProtocol>,
    mut selected: tokio::sync::watch::Receiver<Option<json_rpc_2_transport::TransportContext>>,
) {
    loop {
        let context = selected.borrow_and_update().clone();
        match context {
            Some(context) => tokio::select! {
                biased;
                _ = protocol.session_cancelled(&context) => return,
                result = selected.changed() => if result.is_err() { return },
            },
            None => {
                if selected.changed().await.is_err() {
                    return;
                }
            }
        }
    }
}

fn http_allowed(kinds: &[TransportKind], secure: bool, get: bool) -> bool {
    if secure {
        if get {
            kinds.contains(&TransportKind::WebSocketTls)
        } else {
            kinds.contains(&TransportKind::Tls)
        }
    } else {
        if get {
            kinds.contains(&TransportKind::Http1) || kinds.contains(&TransportKind::WebSocketHttp1)
        } else {
            kinds.contains(&TransportKind::Http1)
        }
    }
}

fn has_tls_kind(kinds: &[TransportKind]) -> bool {
    kinds.contains(&TransportKind::Tls) || kinds.contains(&TransportKind::WebSocketTls)
}

fn is_tls_kind(kinds: &[TransportKind]) -> bool {
    has_tls_kind(kinds)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Classification {
    RawTcp,
    RawWebSocket,
    HttpGet,
    HttpPost,
}

async fn classify<S>(stream: &mut S, prefix: &mut Vec<u8>) -> io::Result<Classification>
where
    S: AsyncRead + Unpin,
{
    if prefix.is_empty() {
        prefix.push(stream.read_u8().await?);
    }
    let first = prefix[0];

    if first & 0x70 == 0 && matches!(first & 0x0f, 0 | 1 | 2 | 8 | 9 | 10) {
        if prefix.len() < 2 {
            prefix.push(stream.read_u8().await?);
        }
        if prefix[1] & 0x80 != 0 {
            return Ok(Classification::RawWebSocket);
        }
    }

    let (candidate, classification): (&[u8], Classification) = match first {
        b'G' => (b"GET ", Classification::HttpGet),
        b'P' => (b"POST ", Classification::HttpPost),
        _ => return Ok(Classification::RawTcp),
    };
    while prefix.len() < candidate.len() {
        prefix.push(stream.read_u8().await?);
        let index = prefix.len() - 1;
        if prefix[index] != candidate[index] {
            return Ok(Classification::RawTcp);
        }
    }
    if prefix.as_slice() == candidate {
        Ok(classification)
    } else {
        Ok(Classification::RawTcp)
    }
}

struct ReplayIo<S> {
    prefix: Bytes,
    offset: usize,
    inner: S,
}

impl<S> ReplayIo<S> {
    fn new(prefix: Vec<u8>, inner: S) -> Self {
        Self {
            prefix: Bytes::from(prefix),
            offset: 0,
            inner,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for ReplayIo<S> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let available = &self.prefix[self.offset..];
        if !available.is_empty() && buffer.remaining() > 0 {
            let copied = available.len().min(buffer.remaining());
            buffer.put_slice(&available[..copied]);
            self.offset += copied;
            return Poll::Ready(Ok(()));
        }
        Pin::new(&mut self.inner).poll_read(cx, buffer)
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for ReplayIo<S> {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
