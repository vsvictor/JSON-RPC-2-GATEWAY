use super::*;
use std::{net::Ipv4Addr, sync::Arc};

use gateway_protocol::GatewayBackendProtocol;
use json_rpc_2_runtime::MultiTransportServer;
use json_rpc_2_server::RpcServer;
use json_rpc_balancer::BackendConfig;
use tokio::net::TcpListener;

fn pool(port: u16) -> BackendPool {
    BackendPool::new(vec![BackendConfig {
        id: "readiness-test".into(),
        host: "127.0.0.1".into(),
        ports: [("tcp".into(), port)].into_iter().collect(),
        pool_size: 1,
        options: Default::default(),
    }])
    .unwrap()
}

#[tokio::test]
async fn waits_for_backend_protocol_before_accepting_startup() {
    let reservation = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let port = reservation.local_addr().unwrap().port();
    let pool = pool(port);
    drop(reservation);
    let backend = async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        MultiTransportServer::new(Arc::new(GatewayBackendProtocol::new(Arc::new(
            RpcServer::new(),
        ))))
        .set_bind([Ipv4Addr::LOCALHOST.into()])
        .set_tcp_port(port)
        .start()
        .await
        .unwrap()
    };
    let (result, backend) = tokio::join!(wait_for_backends(&pool, Duration::from_secs(3)), backend);
    result.unwrap();
    backend.close().await.unwrap();
}

#[tokio::test]
async fn listening_socket_without_backend_protocol_exceeds_deadline() {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let pool = pool(listener.local_addr().unwrap().port());
    let started = Instant::now();
    let error = wait_for_backends(&pool, Duration::from_millis(100))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("readiness deadline"));
    assert!(started.elapsed() < Duration::from_secs(1));
}

#[tokio::test]
async fn unavailable_backend_fails_explicitly() {
    let reservation = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
    let pool = pool(reservation.local_addr().unwrap().port());
    drop(reservation);
    let error = wait_for_backends(&pool, Duration::from_millis(100))
        .await
        .unwrap_err();
    assert!(error.to_string().contains("readiness deadline"));
    assert!(format!("{error:#}").contains("health check failed"));
}
