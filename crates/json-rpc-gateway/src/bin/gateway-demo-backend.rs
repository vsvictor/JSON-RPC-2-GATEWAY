use std::{env, net::IpAddr, sync::Arc};

use anyhow::{Context, bail};
use gateway_protocol::GatewayBackendProtocol;
use json_rpc_2_runtime::MultiTransportServer;
use json_rpc_2_server::RpcServer;

mod backend_method_docs;
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

    let rpc = RpcServer::new();
    rpc.register("echo", |params| async move { Ok(params) })?;
    rpc.register("identify", move |_| {
        let instance_id = instance_id.clone();
        async move { Ok(serde_json::Value::String(instance_id)) }
    })?;
    let registry = rpc.clone();

    #[cfg(feature = "tracking")]
    let rpc = {
        let config = match env::var("TMS_TRACKING_CONFIG_DIR") {
            Ok(directory) => {
                tms_server_tracking::config::TrackingConfig::from_directory(directory)?
            }
            Err(env::VarError::NotPresent) => {
                tms_server_tracking::config::TrackingConfig::from_env()?
            }
            Err(error) => return Err(error).context("reading TMS_TRACKING_CONFIG_DIR"),
        };
        let max_batch = config.max_batch_records;
        let rpc = config
            .build_server_on(rpc)
            .await
            .context("installing tracking backend methods")?;
        backend_method_docs::install_tracking(&registry, max_batch)?;
        rpc
    };

    #[cfg(feature = "tms")]
    let rpc = {
        let config = match env::var("TMS_MAIN_CONFIG_DIR") {
            Ok(directory) => tms_server_main::config::BackendConfig::from_directory(directory)?,
            Err(env::VarError::NotPresent) => tms_server_main::config::BackendConfig::from_env()?,
            Err(error) => return Err(error).context("reading TMS_MAIN_CONFIG_DIR"),
        };
        config
            .build_server_on(rpc)
            .await
            .context("installing TMS backend methods")?
    };

    backend_method_docs::install(&registry)?;
    #[cfg(feature = "tms")]
    register_tms_aliases(&registry)?;
    let rpc = Arc::new(rpc);
    #[cfg(feature = "tms")]
    let cleanup = {
        let rpc = Arc::clone(&rpc);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(60));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                interval.tick().await;
                if let Err(error) = rpc.cleanup().await {
                    tracing::warn!(%error, "TMS session cleanup failed");
                }
            }
        })
    };
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
    #[cfg(feature = "tms")]
    cleanup.abort();
    Ok(())
}

#[cfg(feature = "tms")]
fn register_tms_aliases(rpc: &RpcServer) -> Result<(), json_rpc_2_server::RegistrationError> {
    for name in [
        "login",
        "resume",
        "session.info",
        "getShift",
        "startShift",
        "finishShift",
    ] {
        rpc.register_alias(format!("rpc.{name}"), name)?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use json_rpc_2_transport::JsonRpcProtocol;
    use serde_json::{Value, json};

    #[tokio::test]
    async fn backend_plugin_methods_preserve_gateway_framing() {
        let rpc = RpcServer::new();
        #[cfg(feature = "tms")]
        let registry = rpc.clone();
        rpc.register("echo", |params| async move { Ok(params) })
            .unwrap();

        #[cfg(feature = "tracking")]
        let rpc = {
            use tms_server_tracking::{
                TrackingServerExt,
                tracking::{TrackingRecord, TrackingRepository, TrackingService},
            };
            struct Repository;
            #[async_trait::async_trait]
            impl TrackingRepository for Repository {
                async fn insert_many(&self, _: &[TrackingRecord]) -> anyhow::Result<u64> {
                    panic!("invalid records must not reach storage")
                }
            }
            rpc.with_tracking(Arc::new(TrackingService::new(Arc::new(Repository))), 500)
                .unwrap()
        };

        #[cfg(feature = "tms")]
        let rpc = {
            use std::time::Duration;
            use tms_server_main::{
                RateLimitConfig, TmsServerExt,
                auth::AuthService,
                session::InMemorySessionRepository,
                shift::ShiftService,
                upstream::{HttpDriverApiGateway, UpstreamOptions},
            };
            let gateway = Arc::new(
                HttpDriverApiGateway::new(UpstreamOptions {
                    base_url: "http://127.0.0.1:1".into(),
                    login_url: None,
                    timeout: Duration::from_secs(1),
                    retries: 0,
                    retry_backoff: Duration::ZERO,
                })
                .unwrap(),
            );
            let ttl = Duration::from_secs(60);
            rpc.with_tms(
                Arc::new(AuthService::new(
                    Arc::new(InMemorySessionRepository::new(ttl)),
                    gateway.clone(),
                    ttl,
                )),
                Arc::new(ShiftService::new(gateway)),
                RateLimitConfig::default(),
            )
            .unwrap()
        };

        #[cfg(feature = "tms")]
        register_tms_aliases(&registry).unwrap();
        let discovery = rpc
            .handle_message(br#"{"jsonrpc":"2.0","id":0,"method":"rpc.list_methods"}"#)
            .await
            .unwrap();
        let discovery: Value = serde_json::from_slice(&discovery).unwrap();
        assert!(
            discovery["result"]
                .as_array()
                .unwrap()
                .contains(&json!("echo"))
        );
        #[cfg(feature = "tms")]
        assert!(
            discovery["result"]
                .as_array()
                .unwrap()
                .contains(&json!("rpc.login"))
        );
        #[cfg(feature = "tracking")]
        assert!(
            discovery["result"]
                .as_array()
                .unwrap()
                .contains(&json!("tracking.sendLocation"))
        );
        let protocol = GatewayBackendProtocol::new(Arc::new(rpc));
        let payload = Bytes::from(
            serde_json::to_vec(&json!({
                "jsonrpc": "2.0", "id": 1, "method": "echo", "params": {"backend": true}
            }))
            .unwrap(),
        );
        let request = gateway_protocol::encode_request(&payload, "test-client", "tcp")
            .unwrap()
            .into_bytes();
        let response = protocol.handle_message(&request).await.unwrap();
        let response = gateway_protocol::decode_response(response)
            .unwrap()
            .unwrap();
        let response: Value = serde_json::from_slice(&response).unwrap();
        assert_eq!(response["result"], json!({"backend": true}));

        for (method, enabled) in [
            ("login", cfg!(feature = "tms")),
            ("rpc.login", cfg!(feature = "tms")),
            ("resume", cfg!(feature = "tms")),
            ("rpc.resume", cfg!(feature = "tms")),
            ("session.info", cfg!(feature = "tms")),
            ("rpc.session.info", cfg!(feature = "tms")),
            ("getShift", cfg!(feature = "tms")),
            ("rpc.getShift", cfg!(feature = "tms")),
            ("startShift", cfg!(feature = "tms")),
            ("rpc.startShift", cfg!(feature = "tms")),
            ("finishShift", cfg!(feature = "tms")),
            ("rpc.finishShift", cfg!(feature = "tms")),
            ("sendLocation", cfg!(feature = "tracking")),
            ("tracking.sendLocation", cfg!(feature = "tracking")),
        ] {
            let payload = Bytes::from(
                serde_json::to_vec(&json!({
                    "jsonrpc": "2.0", "id": method, "method": method, "params": {}
                }))
                .unwrap(),
            );
            let request = gateway_protocol::encode_request(&payload, "test-client", "tcp")
                .unwrap()
                .into_bytes();
            let response = protocol.handle_message(&request).await.unwrap();
            let response = gateway_protocol::decode_response(response)
                .unwrap()
                .unwrap();
            let response: Value = serde_json::from_slice(&response).unwrap();
            assert_eq!(response["id"], method);
            assert_eq!(
                response["error"]["code"],
                if enabled { -32602 } else { -32601 },
                "{method}: {response}"
            );
        }

        let notification = Bytes::from_static(
            br#"{"jsonrpc":"2.0","method":"tracking.sendLocation","params":{}}"#,
        );
        let request = gateway_protocol::encode_request(&notification, "test-client", "udp")
            .unwrap()
            .into_bytes();
        let response = protocol.handle_message(&request).await.unwrap();
        assert!(
            gateway_protocol::decode_response(response)
                .unwrap()
                .is_none()
        );
    }
}
