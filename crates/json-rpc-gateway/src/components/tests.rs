use super::*;

fn backend_pool() -> Arc<BackendPool> {
    Arc::new(
        BackendPool::new(vec![json_rpc_balancer::BackendConfig {
            id: "backend-a".into(),
            host: "127.0.0.1".into(),
            ports: [("tcp".into(), 8081)].into_iter().collect(),
            pool_size: 2,
            options: Default::default(),
        }])
        .unwrap(),
    )
}

#[test]
fn snapshot_exposes_gateway_traffic_counters() {
    let inbound = json_rpc_gateway::inbound_traffic_bytes();
    let outbound = json_rpc_gateway::outbound_traffic_bytes();
    json_rpc_gateway::record_inbound_traffic_bytes(13);
    json_rpc_gateway::record_outbound_traffic_bytes(29);

    let inventory = ComponentInventory::new(
        Instant::now(),
        Vec::new(),
        "127.0.0.1:8500".parse().unwrap(),
        "127.0.0.1:9090".parse().unwrap(),
        backend_pool(),
    );
    let snapshot = serde_json::to_value(inventory.snapshot()).unwrap();
    assert!(
        snapshot["gateway"]["inbound_traffic_bytes"]
            .as_u64()
            .unwrap()
            >= inbound + 13
    );
    assert!(
        snapshot["gateway"]["outbound_traffic_bytes"]
            .as_u64()
            .unwrap()
            >= outbound + 29
    );
}

#[tokio::test]
async fn control_plane_returns_traffic_counters() {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::{TcpListener, TcpStream};

    let inbound = json_rpc_gateway::inbound_traffic_bytes();
    let outbound = json_rpc_gateway::outbound_traffic_bytes();
    json_rpc_gateway::record_inbound_traffic_bytes(13);
    json_rpc_gateway::record_outbound_traffic_bytes(29);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let inventory = Arc::new(ComponentInventory::new(
        Instant::now(),
        Vec::new(),
        address,
        "127.0.0.1:9090".parse().unwrap(),
        backend_pool(),
    ));
    let server = tokio::spawn(crate::control_plane::serve(
        listener,
        crate::session::SessionRegistry::new(),
        inventory,
    ));
    let stream = TcpStream::connect(address).await.unwrap();
    let mut reader = BufReader::new(stream);
    reader
        .get_mut()
        .write_all(b"{\"action\":\"list_components\"}\n")
        .await
        .unwrap();
    let mut response = String::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        reader.read_line(&mut response),
    )
    .await
    .unwrap()
    .unwrap();
    server.abort();
    let response: serde_json::Value = serde_json::from_str(&response).unwrap();
    assert_eq!(response["success"], true);
    let gateway = &response["components"]["gateway"];
    assert!(gateway["inbound_traffic_bytes"].as_u64().unwrap() >= inbound + 13);
    assert!(gateway["outbound_traffic_bytes"].as_u64().unwrap() >= outbound + 29);
}
