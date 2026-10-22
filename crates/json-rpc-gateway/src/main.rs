mod config;
mod control_plane;
mod gateway;
mod ingress;
mod session;

use std::{error::Error, fs::File, io::BufReader, net::SocketAddr};

use anyhow::{Context, bail};
use config::GatewayConfig;
use gateway::Gateway;
use ingress::GatewayIngress;
use json_rpc_2_runtime::{AmqpConfig, MqttConfig, TransportKind};
use json_rpc_balancer::BackendPool;
use rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let config = GatewayConfig::load()?;
    #[cfg(feature = "srt")]
    let srt_config = config.srt_config()?;
    #[cfg(any(target_os = "linux", target_os = "android"))]
    let udplite_config = config.udplite_config()?;
    let filter = EnvFilter::try_new(&config.log_level)
        .with_context(|| format!("invalid LOG_LEVEL {:?}", config.log_level))?;
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .try_init()?;

    let backends = config.load_backends()?;
    let pool = BackendPool::new(backends)?;
    pool.warm_up().await?;
    let gateway = Gateway::new(pool);
    let control_address = SocketAddr::new(config.bind, config.control_port);
    let control_listener = TcpListener::bind(control_address)
        .await
        .with_context(|| format!("binding Control Plane at {control_address}"))?;
    let mut server = GatewayIngress::new(gateway.multiplexed_protocol());
    server.set_bind([config.bind]);
    let broker_sessions = if config.transport_ports.keys().any(|kind| kind.is_broker()) {
        Some(config.broker_sessions()?)
    } else {
        None
    };
    #[cfg(any(target_os = "linux", target_os = "android"))]
    server.set_udplite_config(udplite_config);
    #[cfg(feature = "srt")]
    if let Some(srt_config) = srt_config {
        server.set_srt_config(srt_config);
    }
    for (&kind, &port) in &config.transport_ports {
        server.set_port(kind, port);

        if kind.id() == "mqtt" {
            let broker_url = config
                .mqtt_broker_url
                .as_deref()
                .context("MQTT_BROKER_URL is required when MQTT transport is enabled")?;
            let mqtt = MqttConfig {
                sessions: broker_sessions.context("broker session policy was not initialized")?,
                broker_url: broker_url.to_owned(),
                ..Default::default()
            };
            server.set_mqtt_config(mqtt);
        }

        #[cfg(feature = "websocket")]
        if kind.id() == "ws-mqtt" {
            let broker_url = config
                .mqtt_broker_url
                .as_deref()
                .context("MQTT_BROKER_URL is required when MQTT transport is enabled")?;
            let mqtt = MqttConfig {
                sessions: broker_sessions.context("broker session policy was not initialized")?,
                broker_url: broker_url.to_owned(),
                request_topic: "json-rpc-2/ws/requests".to_owned(),
                ..Default::default()
            };
            server.set_ws_mqtt_config(mqtt);
        }

        if kind.id() == "amqp" {
            let broker_url = config
                .amqp_broker_url
                .as_deref()
                .context("AMQP_BROKER_URL is required when AMQP transport is enabled")?;
            let amqp = AmqpConfig {
                sessions: broker_sessions.context("broker session policy was not initialized")?,
                broker_url: broker_url.to_owned(),
                ..Default::default()
            };
            server.set_amqp_config(amqp);
        }

        #[cfg(feature = "websocket")]
        if kind.id() == "ws-amqp" {
            let broker_url = config
                .amqp_broker_url
                .as_deref()
                .context("AMQP_BROKER_URL is required when AMQP transport is enabled")?;
            let amqp = AmqpConfig {
                sessions: broker_sessions.context("broker session policy was not initialized")?,
                broker_url: broker_url.to_owned(),
                request_queue: "json-rpc-2.ws.requests".to_owned(),
                ..Default::default()
            };
            server.set_ws_amqp_config(amqp);
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
    let control_task = tokio::spawn(control_plane::serve(control_listener, gateway.sessions()));

    tracing::info!(%control_address, "Control Plane listening");
    tokio::signal::ctrl_c().await?;
    control_task.abort();
    running.close().await?;
    Ok(())
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
