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

#[tokio::test]
async fn hot_transport_policy_closes_idle_tcp_and_keeps_shared_websockets_alive() {
    use crate::transport_policy::{TransportPolicy, tests::StateFile};
    use futures_util::{SinkExt, StreamExt};
    use tokio_tungstenite::{
        WebSocketStream,
        tungstenite::{Message, protocol::Role},
    };

    tokio::time::timeout(Duration::from_secs(10), async {
        let rpc = Arc::new(RpcServer::new());
        rpc.register_with_context("echo", |params, context| async move {
            Ok(json!({"params":params,"connection_id":context.unwrap().connection_id}))
        }).unwrap();
        let backend = MultiTransportServer::new(Arc::new(gateway_protocol::GatewayBackendProtocol::new(rpc)))
            .set_bind(["127.0.0.1".parse().unwrap()]).set_tcp_port(0).start().await.unwrap();
        let port = backend.listeners().next().unwrap().1.port();
        let pool = BackendPool::new(vec![BackendConfig {
            id:"backend".into(), host:"127.0.0.1".into(),
            ports:[("tcp".into(),port)].into_iter().collect(), pool_size:1, options:Default::default(),
        }]).unwrap();
        let file = StateFile::new();
        let kinds = [TransportKind::Tcp, TransportKind::Http1, TransportKind::WebSocketTcp, TransportKind::WebSocketHttp1];
        let policy = TransportPolicy::load(kinds, file.0.clone()).unwrap();
        let gateway = Gateway::with_policy(pool, DispatcherLimits::default(), policy.clone()).unwrap();
        let mut ingress = crate::ingress::GatewayIngress::new(gateway.multiplexed_protocol());
        ingress.set_bind(["127.0.0.1".parse().unwrap()]);
        for kind in kinds { ingress.set_port(kind,0); }
        let frontend = ingress.start().await.unwrap();
        let address = frontend.listeners().next().unwrap().1;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let control_address = listener.local_addr().unwrap();
        let inventory = crate::components::ComponentInventory::new(
            std::time::Instant::now(), Vec::new(), control_address,
            "127.0.0.1:9090".parse().unwrap(), gateway.backends(),
        ).with_transport_policy(policy.clone());
        let task = tokio::spawn(crate::control_plane::serve(listener, gateway.sessions(), Arc::new(inventory)));
        let mut plane = BufReader::new(TcpStream::connect(control_address).await.unwrap());
        let status = control(&mut plane, json!({"action":"list_transports"})).await;
        assert_eq!(status["success"], true);
        assert_eq!(status["transports"].as_array().unwrap().len(), kinds.len());
        assert_eq!(control(&mut plane, json!({"action":"set_transport_enabled","transport":"invalid","enabled":false})).await["error"],"TransportNotConfigured");
        let request = r#"{"jsonrpc":"2.0","method":"echo","params":[1],"id":1}"#;
        let mut tcp = BufReader::new(TcpStream::connect(address).await.unwrap());
        tcp.get_mut().write_all(format!("{request}\n").as_bytes()).await.unwrap();
        let mut response = String::new();
        tcp.read_line(&mut response).await.unwrap();
        let connection_id = serde_json::from_str::<Value>(&response).unwrap()["result"]["connection_id"].clone();
        assert_eq!(control(&mut plane, json!({"action":"register_client","connection_id":connection_id,"client_id":"tcp-user"})).await["success"],true);
        let mut ws = WebSocketStream::from_raw_socket(TcpStream::connect(address).await.unwrap(),Role::Client,None).await;
        ws.send(Message::Text(request.into())).await.unwrap();
        assert!(ws.next().await.unwrap().is_ok());
        let (mut upgrade, _) = tokio_tungstenite::connect_async(format!("ws://{address}/ws")).await.unwrap();
        upgrade.send(Message::Text(request.into())).await.unwrap();
        assert!(upgrade.next().await.unwrap().is_ok());
        assert_eq!(control(&mut plane, json!({"action":"set_transport_enabled","transport":"tcp","enabled":false})).await["success"],true);
        response.clear();
        assert_eq!(tcp.read_line(&mut response).await.unwrap(),0, "idle TCP must be physically closed");
        assert_eq!(control(&mut plane, json!({"action":"push_unicast","client_id":"tcp-user","payload":"stale"})).await["error"],"ClientNotFound");
        let mut refused = BufReader::new(TcpStream::connect(address).await.unwrap());
        refused.get_mut().write_all(format!("{request}\n").as_bytes()).await.unwrap();
        assert!(match refused.read_line(&mut response).await {
            Ok(length) => length == 0,
            Err(error) => error.kind() == std::io::ErrorKind::ConnectionReset,
        });
        ws.send(Message::Text(request.into())).await.unwrap();
        assert!(ws.next().await.unwrap().is_ok());
        assert_eq!(control(&mut plane, json!({"action":"set_transport_enabled","transport":"tcp","enabled":true})).await["success"],true);
        let mut reopened = BufReader::new(TcpStream::connect(address).await.unwrap());
        reopened.get_mut().write_all(format!("{request}\n").as_bytes()).await.unwrap();
        assert!(reopened.read_line(&mut response).await.unwrap() > 0);
        assert_eq!(control(&mut plane, json!({"action":"set_transport_enabled","transport":"ws-tcp","enabled":false})).await["success"],true);
        assert!(ws.next().await.is_none_or(|result| result.is_err() || result.is_ok_and(|message| message.is_close())));
        upgrade.send(Message::Text(request.into())).await.unwrap();
        assert!(upgrade.next().await.unwrap().is_ok());
        let mut http = BufReader::new(TcpStream::connect(address).await.unwrap());
        http.get_mut().write_all(format!("POST / HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n{request}",request.len()).as_bytes()).await.unwrap();
        response.clear();
        http.read_line(&mut response).await.unwrap();
        assert!(response.starts_with("HTTP/1.1 200"));
        let mut length = 0;
        loop {
            let mut line = String::new();
            http.read_line(&mut line).await.unwrap();
            if line == "\r\n" { break }
            if let Some(value) = line.to_ascii_lowercase().strip_prefix("content-length:") {
                length = value.trim().parse::<usize>().unwrap();
            }
        }
        let mut body = vec![0;length];
        tokio::io::AsyncReadExt::read_exact(&mut http,&mut body).await.unwrap();
        assert_eq!(control(&mut plane,json!({"action":"set_transport_enabled","transport":"http1","enabled":false})).await["success"],true);
        response.clear();
        assert_eq!(http.read_line(&mut response).await.unwrap(),0, "idle HTTP keep-alive must close");
        upgrade.send(Message::Text(request.into())).await.unwrap();
        assert!(upgrade.next().await.unwrap().is_ok());
        assert_eq!(control(&mut plane,json!({"action":"set_transport_enabled","transport":"ws-http1","enabled":false})).await["success"],true);
        assert!(upgrade.next().await.is_none_or(|result| result.is_err() || result.is_ok_and(|message| message.is_close())));
        let restored = TransportPolicy::load(kinds,file.0.clone()).unwrap();
        assert!(restored.enabled("tcp"));
        assert!(!restored.enabled("ws-http1"));
        task.abort();
        drop(reopened);
        frontend.close().await.unwrap();
        backend.close().await.unwrap();
    }).await.expect("hot transport policy acceptance timed out");
}

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

#[tokio::test]
async fn hot_transport_policy_isolates_quic_alpns_and_resets_datagram_sessions() {
    use crate::transport_policy::{TransportPolicy, tests::StateFile};
    use json_rpc_2_core::{ReliableUdpPacket, decode_udp_packet, encode_udp_packet};
    use json_rpc_2_transport::WS_QUIC_ALPN;
    use json_rpc_2_websockets::client::transports::quic::QuicWebSocketClientTransport;
    use tokio::net::UdpSocket;

    async fn raw_request(connection: &quinn::Connection, request: &[u8]) -> Value {
        let (mut send, mut receive) = connection.open_bi().await.unwrap();
        send.write_all(request).await.unwrap();
        send.finish().unwrap();
        serde_json::from_slice(&receive.read_to_end(65536).await.unwrap()).unwrap()
    }

    async fn datagram_request(socket: &UdpSocket, packet: &[u8]) -> Value {
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                socket.send(packet).await.unwrap();
                let mut buffer = [0; 65536];
                while let Ok(Ok(size)) =
                    tokio::time::timeout(Duration::from_millis(50), socket.recv(&mut buffer)).await
                {
                    let response = decode_udp_packet(&buffer[..size]).unwrap();
                    if !response.payload.is_empty() {
                        socket
                            .send(
                                &encode_udp_packet(&ReliableUdpPacket::ack(response.packet_id))
                                    .unwrap(),
                            )
                            .await
                            .unwrap();
                        return serde_json::from_slice(&response.payload).unwrap();
                    }
                }
            }
        })
        .await
        .unwrap()
    }

    tokio::time::timeout(Duration::from_secs(15), async {
        let rpc = Arc::new(RpcServer::new());
        rpc.register_with_context("echo", |params, context| async move {
            Ok(json!({"params":params,"connection_id":context.unwrap().connection_id}))
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
            pool_size: 1,
            options: Default::default(),
        }])
        .unwrap();
        let file = StateFile::new();
        let kinds = [
            TransportKind::Udp,
            TransportKind::Quic,
            TransportKind::WebSocketQuic,
        ];
        let policy = TransportPolicy::load(kinds, file.0.clone()).unwrap();
        let gateway =
            Gateway::with_policy(pool, DispatcherLimits::default(), policy.clone()).unwrap();
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
        let cert: rustls::pki_types::CertificateDer<'static> = identity.cert.into();
        let key = rustls::pki_types::PrivateKeyDer::Pkcs8(identity.key_pair.serialize_der().into());
        let mut ingress = crate::ingress::GatewayIngress::new(gateway.multiplexed_protocol());
        ingress
            .set_bind(["127.0.0.1".parse().unwrap()])
            .set_tls(vec![cert.clone()], key);
        for kind in kinds {
            ingress.set_port(kind, 0);
        }
        let frontend = ingress.start().await.unwrap();
        let address = frontend.listeners().next().unwrap().1;
        let endpoint = |alpn: &[u8]| {
            let mut roots = rustls::RootCertStore::empty();
            roots.add(cert.clone()).unwrap();
            let mut tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_protocol_versions(&[&rustls::version::TLS13])
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();
            tls.alpn_protocols = vec![alpn.to_vec()];
            let mut endpoint = quinn::Endpoint::client("127.0.0.1:0".parse().unwrap()).unwrap();
            endpoint.set_default_client_config(quinn::ClientConfig::new(Arc::new(
                QuicClientConfig::try_from(tls).unwrap(),
            )));
            endpoint
        };
        let raw_endpoint = endpoint(QUIC_ALPN);
        let raw = raw_endpoint
            .connect(address, "localhost")
            .unwrap()
            .await
            .unwrap();
        let ws =
            QuicWebSocketClientTransport::connect(endpoint(WS_QUIC_ALPN), address, "localhost")
                .await
                .unwrap();
        let request = br#"{"jsonrpc":"2.0","method":"echo","params":[1],"id":1}"#;
        let response = raw_request(&raw, request).await;
        let id = response["result"]["connection_id"]
            .as_str()
            .unwrap()
            .to_owned();
        gateway
            .sessions()
            .register_client(&id, "quic-user".into(), Vec::new())
            .unwrap();
        assert!(
            ws.send(Bytes::from_static(request))
                .await
                .unwrap()
                .is_some()
        );
        let udp = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp.connect(address).await.unwrap();
        let packet = encode_udp_packet(&ReliableUdpPacket::request(41, request.to_vec())).unwrap();
        assert_eq!(
            datagram_request(&udp, &packet).await["result"]["params"],
            json!([1])
        );
        policy.set_enabled("udp", false).await.unwrap();
        udp.send(&packet).await.unwrap();
        let mut buffer = [0; 65536];
        assert!(
            tokio::time::timeout(Duration::from_millis(100), udp.recv(&mut buffer))
                .await
                .is_err()
        );
        assert_eq!(
            raw_request(&raw, request).await["result"]["params"],
            json!([1])
        );
        policy.set_enabled("udp", true).await.unwrap();
        let changed = br#"{"jsonrpc":"2.0","method":"echo","params":[2],"id":1}"#;
        let packet = encode_udp_packet(&ReliableUdpPacket::request(41, changed.to_vec())).unwrap();
        assert_eq!(
            datagram_request(&udp, &packet).await["result"]["params"],
            json!([2]),
            "old reliability cache must not survive disable"
        );
        policy.set_enabled("quic", false).await.unwrap();
        tokio::time::timeout(Duration::from_secs(2), raw.closed())
            .await
            .expect("idle raw QUIC did not close");
        assert!(
            raw.close_reason().is_some(),
            "idle QUIC connection must close"
        );
        assert!(
            gateway
                .sessions()
                .push_unicast("quic-user", Bytes::new())
                .is_err()
        );
        assert!(
            gateway
                .backends()
                .forward(&Bytes::from_static(request), "quic", &id)
                .await
                .is_err(),
            "QUIC affinity must be removed"
        );
        assert!(
            ws.send(Bytes::from_static(request))
                .await
                .unwrap()
                .is_some()
        );
        let denied = raw_endpoint.connect(address, "localhost").unwrap().await;
        if let Ok(denied) = denied {
            tokio::time::timeout(Duration::from_secs(2), denied.closed())
                .await
                .expect("disabled QUIC accepted new session");
        }
        policy.set_enabled("quic", true).await.unwrap();
        let reopened = raw_endpoint
            .connect(address, "localhost")
            .unwrap()
            .await
            .unwrap();
        assert_eq!(
            raw_request(&reopened, request).await["result"]["params"],
            json!([1])
        );
        policy.set_enabled("ws-quic", false).await.unwrap();
        assert!(
            tokio::time::timeout(Duration::from_secs(2), ws.send(Bytes::from_static(request)))
                .await
                .expect("disabled ws-quic did not close")
                .is_err()
        );
        assert_eq!(
            raw_request(&reopened, request).await["result"]["params"],
            json!([1])
        );
        reopened.close(0u32.into(), b"test done");
        raw_endpoint.close(0u32.into(), b"test done");
        tokio::time::timeout(Duration::from_secs(3), frontend.close())
            .await
            .expect("ingress failed to close after policy toggles")
            .unwrap();
        backend.close().await.unwrap();
    })
    .await
    .expect("QUIC/datagram policy acceptance timed out");
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
