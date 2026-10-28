mod components;
mod config;
mod control_plane;
mod gateway;
mod ingress;
mod logging;
mod session;
mod transport_policy;

use std::{error::Error, fs::File, io::BufReader, net::SocketAddr};

use anyhow::{Context, bail};
use config::GatewayConfig;
use gateway::Gateway;
use ingress::GatewayIngress;
use json_rpc_2_runtime::TransportKind;
use json_rpc_balancer::BackendPool;
use metrics_exporter_prometheus::PrometheusBuilder;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let started = std::time::Instant::now();
    let config = GatewayConfig::load()?;
    #[cfg(feature = "srt")]
    let srt_config = config.srt_config()?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let udplite_config = config.udplite_config()?;
    let (_log_guard, log_errors) = logging::init(&config.log_level)?;

    let metrics_address = SocketAddr::new(config.metrics_bind, config.metrics_port);
    PrometheusBuilder::new()
        .with_http_listener(metrics_address)
        .install()?;
    let _log_metrics = logging::report_dropped_logs(log_errors);
    tracing::info!(%metrics_address, "Prometheus metrics listening");

    let backends = config.load_backends()?;
    let pool = BackendPool::load(
        backends,
        std::env::var_os("GATEWAY_BACKEND_STATE_PATH")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| "/var/lib/json-rpc-gateway/backends.json".into()),
    )?;
    pool.warm_up().await?;
    let policy = transport_policy::TransportPolicy::load(
        config.transport_ports.keys().copied(),
        std::env::var_os("GATEWAY_TRANSPORT_STATE_PATH")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| "/var/lib/json-rpc-gateway/transports.json".into()),
    )?;
    let gateway = Gateway::with_policy(pool, config.dispatcher_limits, policy.clone())?;
    let control_address = SocketAddr::new(config.bind, config.control_port);
    let control_listener = TcpListener::bind(control_address)
        .await
        .with_context(|| format!("binding Control Plane at {control_address}"))?;
    let mut server = GatewayIngress::new(gateway.multiplexed_protocol());
    server.set_bind([config.bind]);
    #[cfg(any(target_os = "linux", target_os = "android"))]
    server.set_udplite_config(udplite_config);
    #[cfg(feature = "srt")]
    if let Some(srt_config) = srt_config {
        server.set_srt_config(srt_config);
    }
    for (&kind, &port) in &config.transport_ports {
        server.set_port(kind, port);

        if kind.id() == "mqtt" {
            server.set_mqtt_config(config.iot.mqtt_config(false)?);
        }

        #[cfg(feature = "websocket")]
        if kind.id() == "ws-mqtt" {
            server.set_ws_mqtt_config(config.iot.mqtt_config(true)?);
        }

        if kind.id() == "amqp" {
            server.set_amqp_config(config.iot.amqp_config(false)?);
        }

        #[cfg(feature = "websocket")]
        if kind.id() == "ws-amqp" {
            server.set_ws_amqp_config(config.iot.amqp_config(true)?);
        }
    }
    if config
        .transport_ports
        .keys()
        .any(|&kind| needs_tls_identity(kind))
    {
        let (certificates, key) = load_tls_identity(&config)?;
        server.set_tls(certificates, key);
    }
    let running = server.start().await.context("starting gateway ingress")?;
    for (kind, address) in running.listeners() {
        tracing::info!(transport = kind.id(), %address, "gateway transport listening");
    }
    let inventory = std::sync::Arc::new(
        components::ComponentInventory::new(
            started,
            running
                .listeners()
                .map(|(kind, address)| components::ListenerInfo {
                    transport: kind.id().to_owned(),
                    address,
                })
                .collect(),
            control_address,
            metrics_address,
            gateway.backends(),
        )
        .with_transport_policy(policy),
    );
    let control_task = tokio::spawn(control_plane::serve(
        control_listener,
        gateway.sessions(),
        inventory,
    ));

    tracing::info!(%control_address, "Control Plane listening");
    shutdown_signal().await?;
    control_task.abort();
    running.close().await?;
    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! {
            result = tokio::signal::ctrl_c() => result,
            _ = terminate.recv() => Ok(()),
        }
    }
    #[cfg(not(unix))]
    tokio::signal::ctrl_c().await
}

fn needs_tls_identity(kind: TransportKind) -> bool {
    matches!(
        kind.id(),
        "tls" | "quic" | "http3" | "ws-tls" | "ws-quic" | "ws-http3"
    )
}

fn load_tls_identity(
    config: &GatewayConfig,
) -> anyhow::Result<(Vec<CertificateDer<'static>>, PrivateKeyDer<'static>)> {
    let cert_path = config
        .tls_cert_path
        .as_deref()
        .context("TLS_CERT_PATH is required for TLS, QUIC, and HTTP/3 listeners")?;
    let key_path = config
        .tls_key_path
        .as_deref()
        .context("TLS_KEY_PATH is required for TLS, QUIC, and HTTP/3 listeners")?;
    let certificates = rustls_pemfile::certs(&mut BufReader::new(File::open(cert_path)?))
        .collect::<Result<Vec<_>, _>>()?;
    if certificates.is_empty() {
        bail!(
            "TLS certificate file {:?} contains no certificates",
            cert_path
        );
    }
    let key = rustls_pemfile::private_key(&mut BufReader::new(File::open(key_path)?))?
        .with_context(|| format!("TLS key file {:?} contains no private key", key_path))?;
    Ok((certificates, key))
}
