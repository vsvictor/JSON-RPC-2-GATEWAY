use std::{env, net::IpAddr, sync::Arc};

use anyhow::{Context, bail};
use gateway_protocol::GatewayBackendProtocol;
use json_rpc_2_runtime::MultiTransportServer;
use json_rpc_2_server::RpcServer;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
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

    let rpc = Arc::new(RpcServer::new());
    rpc.register("echo", |params| async move { Ok(params) })?;
    rpc.register("identify", move |_| {
        let instance_id = instance_id.clone();
        async move { Ok(serde_json::Value::String(instance_id)) }
    })?;

    let protocol = Arc::new(GatewayBackendProtocol::new(rpc));
    let mut server = MultiTransportServer::new(protocol);
    server
        .set_bind([bind])
        .set_tcp_port(port)
        .set_udp_port(port);
    let running = server
        .start()
        .await
        .with_context(|| format!("starting demo backend on {bind}:{port}"))?;
    println!("demo backend listening on TCP and UDP at {bind}:{port}");

    tokio::signal::ctrl_c().await?;
    running.close().await?;
    Ok(())
}
