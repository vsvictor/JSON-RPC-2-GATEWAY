use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use bytes::Bytes;
use futures_util::{SinkExt, StreamExt};
use json_rpc_2_runtime::{CancellationToken, TransportRuntime};
use json_rpc_2_transport::{
    JsonRpcProtocol, ProtocolFuture, ProtocolMessageFuture, TransportContext, TransportMessage,
};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tokio_rustls::TlsConnector;
use tokio_tungstenite::{
    WebSocketStream, client_async,
    tungstenite::{Message, protocol::Role},
};

use crate::ingress::shared_tcp;
pub use json_rpc_2_runtime::TransportKind;

const LOCALHOST: IpAddr = IpAddr::V4(Ipv4Addr::LOCALHOST);

struct Echo {
    events: mpsc::UnboundedSender<String>,
}

impl JsonRpcProtocol for Echo {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move { Some(Bytes::copy_from_slice(input)) })
    }

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        Box::pin(async move {
            let _ = self.events.send(format!(
                "message:{}:{}",
                context.transport, context.connection_id
            ));
            Some(Bytes::from(format!("{}:", context.transport)).chain(input))
        })
    }

    fn handle_owned_message<'a>(
        &'a self,
        input: Bytes,
        context: &'a TransportContext,
    ) -> ProtocolMessageFuture<'a> {
        Box::pin(async move {
            let _ = self.events.send(format!(
                "message:{}:{}",
                context.transport, context.connection_id
            ));
            Some(TransportMessage::from(
                Bytes::from(format!("{}:", context.transport)).chain(&input),
            ))
        })
    }

    fn session_opened(&self, context: &TransportContext) {
        let _ = self.events.send(format!(
            "open:{}:{}",
            context.transport, context.connection_id
        ));
        if let Some(push) = &context.push_sender {
            let _ = push.send(Bytes::from_static(b"server-push"));
        }
    }

    fn session_closed(&self, context: &TransportContext) {
        let _ = self.events.send(format!(
            "closed:{}:{}",
            context.transport, context.connection_id
        ));
    }
}

trait BytesChain {
    fn chain(self, other: &[u8]) -> Bytes;
}

impl BytesChain for Bytes {
    fn chain(self, other: &[u8]) -> Bytes {
        let mut combined = self.to_vec();
        combined.extend_from_slice(other);
        Bytes::from(combined)
    }
}

fn tls_identity() -> (CertificateDer<'static>, PrivateKeyDer<'static>) {
    let generated =
        rcgen::generate_simple_self_signed(vec!["127.0.0.1".into(), "localhost".into()]).unwrap();
    (
        generated.cert.into(),
        PrivateKeyDer::Pkcs8(generated.key_pair.serialize_der().into()),
    )
}

fn tls_configs() -> (Arc<rustls::ServerConfig>, Arc<rustls::ClientConfig>) {
    let (certificate, key) = tls_identity();
    let server = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate.clone()], key)
        .unwrap();
    let mut roots = rustls::RootCertStore::empty();
    roots.add(certificate).unwrap();
    let client = rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    (Arc::new(server), Arc::new(client))
}

async fn tls_stream(
    address: SocketAddr,
    client: Arc<rustls::ClientConfig>,
) -> tokio_rustls::client::TlsStream<TcpStream> {
    TlsConnector::from(client)
        .connect(
            ServerName::try_from("127.0.0.1").unwrap(),
            TcpStream::connect(address).await.unwrap(),
        )
        .await
        .unwrap()
}

async fn read_http_response(stream: &mut TcpStream) -> Vec<u8> {
    let mut response = Vec::new();
    stream.read_to_end(&mut response).await.unwrap();
    response
}

async fn read_line<R: AsyncRead + Unpin>(stream: &mut R) -> Vec<u8> {
    let mut line = Vec::new();
    let mut byte = [0];
    loop {
        match stream.read_exact(&mut byte).await {
            Ok(_) => {
                line.push(byte[0]);
                if byte[0] == b'\n' {
                    return line;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return line,
            Err(error) => panic!("failed to read line: {error}"),
        }
    }
}

async fn receive_event(events: &mut mpsc::UnboundedReceiver<String>, expected: &str) -> String {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let event = events.recv().await.unwrap();
            if event.starts_with(expected) {
                break event;
            }
        }
    })
    .await
    .unwrap_or_else(|_| panic!("timed out waiting for event prefix {expected}"))
}

#[tokio::test]
async fn one_listener_dispatches_tcp_websocket_http_and_tls_directly() {
    let listener = TcpListener::bind(SocketAddr::new(LOCALHOST, 0))
        .await
        .unwrap();
    let address = listener.local_addr().unwrap();
    let (server_tls, client_tls) = tls_configs();
    let (event_sender, mut events) = mpsc::unbounded_channel();
    let protocol: Arc<dyn JsonRpcProtocol> = Arc::new(Echo {
        events: event_sender,
    });
    let mut runtime = TransportRuntime::default();
    runtime.limits.max_connections = 1;
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(shared_tcp::serve(
        listener,
        vec![
            TransportKind::Tcp,
            TransportKind::Tls,
            TransportKind::Http1,
            TransportKind::WebSocketTcp,
            TransportKind::WebSocketHttp1,
            TransportKind::WebSocketTls,
        ],
        protocol,
        Some(server_tls),
        Arc::new(runtime),
        shutdown.clone(),
    ));

    let mut raw = TcpStream::connect(address).await.unwrap();
    raw.write_all(b"raw-tcp\n").await.unwrap();
    let opened_raw = receive_event(&mut events, "open:tcp:").await;
    let mut line = read_line(&mut raw).await;
    if line == b"server-push\n" {
        line = read_line(&mut raw).await;
    }
    assert_eq!(line, b"tcp:raw-tcp\n");
    let raw_event = receive_event(&mut events, "message:tcp:").await;
    assert_eq!(raw_event.split(':').nth(2), opened_raw.split(':').nth(2));
    drop(raw);
    let _ = receive_event(&mut events, "closed:tcp:").await;

    let ws_stream = TcpStream::connect(address).await.unwrap();
    let mut ws_tcp: WebSocketStream<TcpStream> =
        WebSocketStream::from_raw_socket(ws_stream, Role::Client, None).await;
    let opaque = Bytes::from_static(b"\xff\0\nopaque");
    ws_tcp.send(Message::Binary(opaque.clone())).await.unwrap();
    let opened_ws_tcp = receive_event(&mut events, "open:ws-tcp:").await;
    let expected_response = Message::Binary(Bytes::from(
        [b"ws-tcp:".as_slice(), opaque.as_ref()].concat(),
    ));
    let first = ws_tcp.next().await.unwrap().unwrap();
    let second = ws_tcp.next().await.unwrap().unwrap();
    assert!(
        (first == Message::Text("server-push".into()) && second == expected_response)
            || (first == expected_response && second == Message::Text("server-push".into()))
    );
    let ws_tcp_event = receive_event(&mut events, "message:ws-tcp:").await;
    assert_eq!(
        ws_tcp_event.split(':').nth(2),
        opened_ws_tcp.split(':').nth(2)
    );
    drop(ws_tcp);
    let _ = receive_event(&mut events, "closed:ws-tcp:").await;

    let mut http = TcpStream::connect(address).await.unwrap();
    http.write_all(b"P").await.unwrap();
    tokio::time::sleep(Duration::from_millis(5)).await;
    http.write_all(
        b"OST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: 7\r\nConnection: close\r\n\r\nhttp-ok",
    )
    .await
    .unwrap();
    let response = read_http_response(&mut http).await;
    assert!(
        response
            .windows(b"http1:http-ok".len())
            .any(|window| window == b"http1:http-ok")
    );
    let _ = receive_event(&mut events, "message:http1:").await;

    let mut control = TcpStream::connect(address).await.unwrap();
    control
        .write_all(
            b"GET /.well-known/json-rpc-2/ping HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
        )
        .await
        .unwrap();
    let control_response = read_http_response(&mut control).await;
    assert!(
        control_response
            .windows(b"pong".len())
            .any(|window| window == b"pong")
    );

    let ws_plain = TcpStream::connect(address).await.unwrap();
    let (mut ws_http, _) = client_async("ws://127.0.0.1/ws", ws_plain).await.unwrap();
    let opened_ws_http = receive_event(&mut events, "open:ws-http1:").await;
    assert_eq!(
        ws_http.next().await.unwrap().unwrap(),
        Message::Text("server-push".into())
    );
    ws_http
        .send(Message::Text("http-websocket".into()))
        .await
        .unwrap();
    assert_eq!(
        ws_http.next().await.unwrap().unwrap(),
        Message::Text("ws-http1:http-websocket".into())
    );
    let ws_http_event = receive_event(&mut events, "message:ws-http1:").await;
    assert_eq!(
        ws_http_event.split(':').nth(2),
        opened_ws_http.split(':').nth(2)
    );
    drop(ws_http);
    let _ = receive_event(&mut events, "closed:ws-http1:").await;

    let mut tls = tls_stream(address, Arc::clone(&client_tls)).await;
    tls.write_all(b"raw-tls\n").await.unwrap();
    let opened_tls = receive_event(&mut events, "open:tls:").await;
    let mut tls_line = read_line(&mut tls).await;
    if tls_line == b"server-push\n" {
        tls_line = read_line(&mut tls).await;
    }
    assert_eq!(tls_line, b"tls:raw-tls\n");
    let tls_event = receive_event(&mut events, "message:tls:").await;
    assert_eq!(tls_event.split(':').nth(2), opened_tls.split(':').nth(2));
    drop(tls);
    let _ = receive_event(&mut events, "closed:tls:").await;

    let tls_ws_stream = tls_stream(address, client_tls).await;
    let (mut wss, _) = client_async("wss://127.0.0.1/ws", tls_ws_stream)
        .await
        .unwrap();
    let opened_wss = receive_event(&mut events, "open:ws-tls:").await;
    assert_eq!(
        wss.next().await.unwrap().unwrap(),
        Message::Text("server-push".into())
    );
    wss.send(Message::Binary(opaque.clone())).await.unwrap();
    assert_eq!(
        wss.next().await.unwrap().unwrap(),
        Message::Binary(Bytes::from(
            [b"ws-tls:".as_slice(), opaque.as_ref()].concat()
        ))
    );
    let wss_event = receive_event(&mut events, "message:ws-tls:").await;
    assert_eq!(wss_event.split(':').nth(2), opened_wss.split(':').nth(2));
    drop(wss);
    let _ = receive_event(&mut events, "closed:ws-tls:").await;
    let _ = raw_event;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
}
