use std::{env, net::IpAddr, sync::Arc};

use anyhow::{Context, bail};
use gateway_protocol::GatewayBackendProtocol;
use json_rpc_2_runtime::MultiTransportServer;
use json_rpc_2_server::RpcServer;
use serde_json::json;

#[path = "../logging.rs"]
mod logging;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let level = env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_owned());
    let (_log_guard, log_errors) = logging::init(&level).map_err(anyhow::Error::from_boxed)?;
    let _log_metrics = logging::report_dropped_logs(log_errors);
    let instance_id = env::var("BACKEND_ID").unwrap_or_else(|_| "demo-backend".to_owned());
    let bind = env::var("BACKEND_BIND")
        .unwrap_or_else(|_| "127.0.0.1".to_owned())
        .parse::<IpAddr>()
        .context("BACKEND_BIND must be an IP address")?;
    let port = env::var("BACKEND_TCP_PORT")
        .unwrap_or_else(|_| "8081".to_owned())
        .parse::<u16>()
        .context("BACKEND_TCP_PORT must be a port in 1..=65535")?;
    if port == 0 {
        bail!("BACKEND_TCP_PORT must be a port in 1..=65535");
    }

    let protocol = Arc::new(GatewayBackendProtocol::new(Arc::new(build_rpc(
        instance_id,
    )?)));
    let mut server = MultiTransportServer::new(protocol);
    server
        .set_bind([bind])
        .set_tcp_port(port)
        .set_udp_port(port);
    let running = server
        .start()
        .await
        .with_context(|| format!("starting demo backend on {bind}:{port}"))?;
    tracing::info!(%bind, port, "demo backend listening on TCP and UDP");

    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result?,
            _ = terminate.recv() => {},
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await?;
    running.close().await?;
    Ok(())
}

fn build_rpc(instance_id: String) -> anyhow::Result<RpcServer> {
    let rpc = RpcServer::new();
    rpc.register("echo", |params| async move { Ok(params) })?;
    rpc.register("identify", move |_| {
        let instance_id = instance_id.clone();
        async move { Ok(serde_json::Value::String(instance_id)) }
    })?;
    rpc.document_method(
        "echo",
        json!({"type":["object","array"],"description":"Returns supplied params unchanged."}),
        json!({"message":"example"}),
    )?;
    rpc.document_method(
        "identify",
        json!({"type":["object","array"],"description":"No required params; supplied params are ignored."}),
        json!({}),
    )?;
    Ok(rpc)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use json_rpc_2_transport::JsonRpcProtocol;
    use serde_json::Value;

    async fn request(protocol: &GatewayBackendProtocol, method: &str, params: Value) -> Value {
        let payload = Bytes::from(
            serde_json::to_vec(&json!({
                "jsonrpc": "2.0", "id": 1, "method": method, "params": params
            }))
            .unwrap(),
        );
        let frame = gateway_protocol::encode_request(&payload, "base-client", "tcp")
            .unwrap()
            .into_bytes();
        let response = protocol.handle_message(&frame).await.unwrap();
        let response = gateway_protocol::decode_response(response)
            .unwrap()
            .unwrap();
        serde_json::from_slice(&response).unwrap()
    }

    #[tokio::test]
    async fn base_methods_preserve_framing_and_discovery() {
        let protocol =
            GatewayBackendProtocol::new(Arc::new(build_rpc("base-backend".into()).unwrap()));
        let response = request(&protocol, "rpc.list_methods", json!({"describe":true})).await;
        let descriptions = response["result"].as_array().unwrap();
        let names: Vec<_> = descriptions
            .iter()
            .map(|d| d["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["echo", "identify", "rpc.list_methods"]);
        for description in descriptions
            .iter()
            .filter(|d| d["name"] != "rpc.list_methods")
        {
            assert!(description["params_schema"].is_object());
            assert!(description["example_request"].is_object());
        }
        assert_eq!(
            request(&protocol, "echo", json!({"example":true})).await["result"],
            json!({"example":true})
        );
        assert_eq!(
            request(&protocol, "identify", json!({})).await["result"],
            "base-backend"
        );
        assert_eq!(
            request(&protocol, "unknown.method", json!({})).await["error"]["code"],
            -32601
        );
    }

    #[tokio::test]
    async fn notification_has_no_response() {
        let protocol =
            GatewayBackendProtocol::new(Arc::new(build_rpc("base-backend".into()).unwrap()));
        let frame = gateway_protocol::encode_request(
            &Bytes::from_static(br#"{"jsonrpc":"2.0","method":"echo","params":{}}"#),
            "base-client",
            "udp",
        )
        .unwrap()
        .into_bytes();
        let response = protocol.handle_message(&frame).await.unwrap();
        assert!(
            gateway_protocol::decode_response(response)
                .unwrap()
                .is_none()
        );
    }
}
