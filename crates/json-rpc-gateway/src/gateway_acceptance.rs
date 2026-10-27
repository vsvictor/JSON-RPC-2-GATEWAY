use std::{sync::Arc, time::Duration};

use bytes::Bytes;
use json_rpc_2_quic::client::transports::quic::QuicClientTransport;
use json_rpc_2_runtime::{MultiTransportServer, QUIC_ALPN, TransportKind};
use json_rpc_2_server::RpcServer;
use json_rpc_2_transport::RpcClientTransport;
use json_rpc_balancer::{BackendConfig, BackendPool};
use quinn::crypto::rustls::QuicClientConfig;
use serde_json::{Value, json};
use tokio::{
    io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
    net::{TcpListener, TcpStream},
};

use super::{DispatcherLimits, Gateway};

fn inventory(
    gateway: &Gateway,
    control: std::net::SocketAddr,
) -> Arc<crate::components::ComponentInventory> {
    Arc::new(crate::components::ComponentInventory::new(
        std::time::Instant::now(),
        Vec::new(),
        control,
        "127.0.0.1:9090".parse().unwrap(),
        gateway.backends(),
    ))
}

async fn control(stream: &mut BufReader<TcpStream>, command: Value) -> Value {
    stream
        .get_mut()
        .write_all(format!("{command}\n").as_bytes())
        .await
        .unwrap();
    let mut line = String::new();
    stream.read_line(&mut line).await.unwrap();
    serde_json::from_str(&line).unwrap()
}

#[tokio::test]
async fn http_push_is_rejected_during_request_and_registration_is_removed_after_response() {
    use tokio::io::AsyncReadExt;
    tokio::time::timeout(Duration::from_secs(5), async {
        let (sender, mut received) = tokio::sync::mpsc::unbounded_channel();
        let release = Arc::new(tokio::sync::Notify::new());
        let rpc = Arc::new(RpcServer::new());
        let gate = release.clone();
        rpc.register_with_context("hold", move |params, context| {
            let sender = sender.clone();
            let gate = gate.clone();
            async move {
                sender.send(context.unwrap().connection_id).unwrap();
                gate.notified().await;
                Ok(params)
            }
        }).unwrap();
        let backend = MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(rpc)))
            .set_bind(["127.0.0.1".parse().unwrap()]).set_tcp_port(0).start().await.unwrap();
        let port = backend.listeners().next().unwrap().1.port();
        let pool = BackendPool::new(vec![BackendConfig {
            id: "backend".into(), host: "127.0.0.1".into(),
            ports: [("tcp".into(), port)].into_iter().collect(), pool_size: 1,
            options: Default::default(),
        }]).unwrap();
        let gateway = Gateway::new(pool, DispatcherLimits::default()).unwrap();
        let frontend = MultiTransportServer::new(gateway.protocol(TransportKind::Http1))
            .set_bind(["127.0.0.1".parse().unwrap()]).set_http1_port(0).start().await.unwrap();
        let address = frontend.listeners().next().unwrap().1;
        let request = tokio::spawn(async move {
            let mut client = TcpStream::connect(address).await.unwrap();
            let payload = br#"{"jsonrpc":"2.0","method":"hold","params":[1],"id":1}"#;
            client.write_all(format!("POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", payload.len()).as_bytes()).await.unwrap();
            client.write_all(payload).await.unwrap();
            let mut response = Vec::new();
            client.read_to_end(&mut response).await.unwrap();
            assert!(response.starts_with(b"HTTP/1.1 200"));
        });
        let id = received.recv().await.unwrap();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(crate::control_plane::serve(listener, gateway.sessions(), inventory(&gateway, address)));
        let mut plane = BufReader::new(TcpStream::connect(address).await.unwrap());
        assert_eq!(control(&mut plane, json!({"action":"register_client", "connection_id":id, "client_id":"http"})).await["success"], true);
        assert_eq!(control(&mut plane, json!({"action":"push_unicast", "client_id":"http", "payload":"forbidden"})).await,
            json!({"success":false,"error":"PushNotSupportedForTransport"}));
        release.notify_one();
        request.await.unwrap();
        assert_eq!(control(&mut plane, json!({"action":"register_client", "connection_id":id, "client_id":"stale"})).await["error"], "UnknownConnection");
        assert_eq!(control(&mut plane, json!({"action":"push_unicast", "client_id":"http", "payload":"forbidden"})).await["error"], "ClientNotFound");
        task.abort();
        frontend.close().await.unwrap();
        backend.close().await.unwrap();
    }).await.expect("HTTP request lifecycle acceptance test timed out");
}

#[tokio::test]
async fn quic_migration_keeps_backend_affinity_and_control_plane_push() {
    tokio::time::timeout(Duration::from_secs(15), async {
        let mut backends = Vec::new();
        let mut configs = Vec::new();
        for backend_id in 0..2 {
            let rpc = Arc::new(RpcServer::new());
            rpc.register_with_context("echo", move |params, context| async move {
                let context = context.unwrap();
                Ok(json!({"backend": backend_id, "connection_id": context.connection_id, "params": params}))
            }).unwrap();
            let running = MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(rpc)))
                .set_bind(["127.0.0.1".parse().unwrap()]).set_tcp_port(0).start().await.unwrap();
            let port = running.listeners().next().unwrap().1.port();
            configs.push(BackendConfig {
                id: format!("backend-{backend_id}"), host: "127.0.0.1".into(),
                ports: [("tcp".into(), port)].into_iter().collect(), pool_size: 1,
                options: Default::default(),
            });
            backends.push(running);
        }
        let pool = BackendPool::new(configs).unwrap();
        pool.warm_up().await.unwrap();
        let gateway = Gateway::new(pool, DispatcherLimits::default()).unwrap();
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert: rustls::pki_types::CertificateDer<'static> = identity.cert.into();
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(identity.key_pair.serialize_der().into());
        let mut ingress = crate::ingress::GatewayIngress::new(gateway.multiplexed_protocol());
        ingress.set_bind(["127.0.0.1".parse().unwrap()])
            .set_port(TransportKind::Quic, 0).set_port(TransportKind::Udp, 0)
            .set_tls(vec![cert.clone()], key);
        let frontend = ingress.start().await.unwrap();
        let address = frontend.listeners().next().unwrap().1;
        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert).unwrap();
        let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
            .with_protocol_versions(&[&rustls::version::TLS13]).unwrap()
            .with_root_certificates(roots).with_no_client_auth();
        tls.alpn_protocols = vec![QUIC_ALPN.to_vec()];
        let config = quinn::ClientConfig::new(Arc::new(QuicClientConfig::try_from(tls).unwrap()));
        let mut clients = Vec::new();
        let mut ids = Vec::new();
        let mut endpoints = Vec::new();
        let request = Bytes::from_static(br#"{"jsonrpc":"2.0","method":"echo","params":[1],"id":7}"#);
        for index in 0..2 {
            let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
            endpoint.set_default_client_config(config.clone());
            let client = QuicClientTransport::connect(endpoint.clone(), address, "localhost").await.unwrap();
            let response: Value = serde_json::from_slice(&client.send(request.clone()).await.unwrap().unwrap()).unwrap();
            assert_eq!(response["result"]["backend"], index);
            ids.push(response["result"]["connection_id"].as_str().unwrap().to_owned());
            endpoints.push(endpoint);
            clients.push(client);
        }

        assert_ne!(ids[0], ids[1]);
        endpoints[0].rebind(std::net::UdpSocket::bind("127.0.0.2:0").unwrap()).unwrap();
        for _ in 0..8 {
            let (first, second) = tokio::join!(clients[0].send(request.clone()), clients[1].send(request.clone()));
            for (index, response) in [first, second].into_iter().enumerate() {
                let response: Value = serde_json::from_slice(&response.unwrap().unwrap()).unwrap();
                assert_eq!(response["result"]["backend"], index);
                assert_eq!(response["result"]["connection_id"], ids[index]);
            }
        }
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control_address = listener.local_addr().unwrap();
        let task = tokio::spawn(crate::control_plane::serve(listener, gateway.sessions(), inventory(&gateway, control_address)));
        let mut plane = BufReader::new(TcpStream::connect(control_address).await.unwrap());
        for (index, id) in ids.iter().enumerate() {
            assert_eq!(control(&mut plane, json!({"action":"register_client", "connection_id": id,
                "client_id": format!("user-{index}"), "groups":["all"]})).await["success"], true);
        }
        for action in ["push_unicast", "push_multicast", "push_broadcast"] {
            let response = control(&mut plane, json!({"action":action, "client_id":"user-0", "group_id":"all", "payload":action})).await;
            assert_eq!(response["success"], true);
            assert_eq!(clients[0].recv_push().await.unwrap(), action.as_bytes());
            if action != "push_unicast" {
                assert_eq!(response["delivered"], 2);
                assert_eq!(clients[1].recv_push().await.unwrap(), action.as_bytes());
            }
        }
        for client in clients { client.close().await.unwrap(); }
        loop {
            let response = control(&mut plane, json!({"action":"push_unicast", "client_id":"user-0", "payload":"closed"})).await;
            if response["error"] == "ClientNotFound" { break; }
            tokio::task::yield_now().await;
        }
        assert_eq!(control(&mut plane, json!({"action":"register_client", "connection_id":ids[0],
            "client_id":"stale"})).await["error"], "UnknownConnection");
        frontend.close().await.unwrap();
        assert!(gateway.backends.forward(&request, "quic", &ids[0]).await.is_err());
        task.abort();
        for backend in backends { backend.close().await.unwrap(); }
    }).await.expect("QUIC gateway acceptance test timed out");
}

#[tokio::test]
async fn shared_tcp_http_ingress_preserves_backend_metadata_and_notifications() {
    use json_rpc_2_tcp::client::transports::{
        http1::Http1ClientTransport, tcp::TcpClientTransport,
    };
    let rpc = Arc::new(RpcServer::new());
    rpc.register_with_context("context", |_, context| async move {
        let context = context.unwrap();
        Ok(json!({"transport":context.transport, "id":context.connection_id}))
    })
    .unwrap();
    let backend =
        MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(rpc)))
            .set_bind(["127.0.0.1".parse().unwrap()])
            .set_tcp_port(0)
            .start()
            .await
            .unwrap();
    let pool = BackendPool::new(vec![BackendConfig {
        id: "backend".into(),
        host: "127.0.0.1".into(),
        ports: [("tcp".into(), backend.listeners().next().unwrap().1.port())]
            .into_iter()
            .collect(),
        pool_size: 2,
        options: Default::default(),
    }])
    .unwrap();
    let gateway = Gateway::new(pool, DispatcherLimits::default()).unwrap();
    let mut ingress = crate::ingress::GatewayIngress::new(gateway.multiplexed_protocol());
    ingress
        .set_bind(["127.0.0.1".parse().unwrap()])
        .set_port(TransportKind::Tcp, 0)
        .set_port(TransportKind::Http1, 0);
    let frontend = ingress.start().await.unwrap();
    let address = frontend.listeners().next().unwrap().1;
    assert!(frontend.listeners().all(|(_, alias)| alias == address));
    let tcp = TcpClientTransport::connect(address).await.unwrap();
    let http = Http1ClientTransport::connect(address).await.unwrap();
    let request = Bytes::from_static(br#"{"jsonrpc":"2.0","method":"context","id":1}"#);
    let (raw, web) = tokio::join!(
        tcp.send_with_expectation(request.clone(), true),
        http.send_with_expectation(request, true)
    );
    let raw: Value = serde_json::from_slice(&raw.unwrap().unwrap()).unwrap();
    let web: Value = serde_json::from_slice(&web.unwrap().unwrap()).unwrap();
    assert_eq!(raw["result"]["transport"], "tcp");
    assert_eq!(web["result"]["transport"], "http1");
    assert_ne!(raw["result"]["id"], web["result"]["id"]);
    let notification = Bytes::from_static(br#"{"jsonrpc":"2.0","method":"context"}"#);
    assert!(
        tcp.send_with_expectation(notification.clone(), false)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        http.send_with_expectation(notification, false)
            .await
            .unwrap()
            .is_none()
    );
    tcp.close().await.unwrap();
    http.close().await.unwrap();
    frontend.close().await.unwrap();
    backend.close().await.unwrap();
}
