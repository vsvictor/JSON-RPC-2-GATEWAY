use std::{net::SocketAddr, sync::Arc};

use anyhow::Context;
use json_rpc_2_transport::{RpcClientTransport, H3_ALPN, QUIC_ALPN, WS_QUIC_ALPN};

use crate::BackendConfig;

pub(crate) fn supported(kind: &str) -> bool {
    match kind {
        "tcp" | "tls" | "udp" | "rtp" | "rudp" | "dccp" | "sctp" | "mqtt" | "amqp" | "coap"
        | "http1" | "quic" | "http3" | "ws-tcp" | "ws-udp" | "ws-rtp" | "ws-rudp" | "ws-dccp"
        | "ws-sctp" | "ws-mqtt" | "ws-amqp" | "ws-coap" | "ws-http1" | "ws-tls" | "ws-quic"
        | "ws-http3" => true,
        #[cfg(any(target_os = "linux", target_os = "android"))]
        "udplite" | "ws-udplite" => true,
        #[cfg(feature = "srt")]
        "srt" | "ws-srt" => true,
        _ => false,
    }
}

pub(crate) async fn connect(
    config: &BackendConfig,
    kind: &str,
    port: u16,
) -> anyhow::Result<Arc<dyn RpcClientTransport>> {
    use json_rpc_2_datagram::client::transports as datagram;
    use json_rpc_2_iot::client::transports as iot;
    use json_rpc_2_quic::client::transports as quic;
    use json_rpc_2_tcp::client::transports as tcp;
    use json_rpc_2_websockets::client::transports as ws;

    let address = tokio::net::lookup_host((config.host.as_str(), port))
        .await?
        .next()
        .context("backend host resolved to no addresses")?;
    fn broker_sessions(config: &BackendConfig) -> gateway_sessions::BrokerSessionPolicy {
        gateway_sessions::BrokerSessionPolicy {
            heartbeat_interval: std::time::Duration::from_millis(
                config.options.broker_heartbeat_interval_ms,
            ),
            session_lease: std::time::Duration::from_millis(config.options.broker_session_lease_ms),
        }
    }
    macro_rules! simple {
        ($client:ty) => {
            Arc::new(<$client>::connect(address).await?) as Arc<dyn RpcClientTransport>
        };
    }
    let transport = match kind {
        "tcp" => Arc::new(
            tcp::tcp::TcpClientTransport::connect(address)
                .await?
                .with_stream_framing(Arc::new(gateway_protocol::GatewayFraming)),
        ) as Arc<dyn RpcClientTransport>,
        "http1" => simple!(tcp::http1::Http1ClientTransport),
        "udp" => simple!(datagram::udp::UdpClientTransport),
        "rtp" => simple!(datagram::rtp::RtpClientTransport),
        "rudp" => simple!(datagram::rudp::RudpClientTransport),
        "dccp" => simple!(datagram::dccp::DccpClientTransport),
        "sctp" => simple!(datagram::sctp::SctpClientTransport),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        "udplite" => simple!(datagram::udplite::UdpLiteClientTransport),
        "coap" => simple!(iot::coap::CoapClientTransport),
        "ws-udp" => simple!(ws::udp::UdpWebSocketClientTransport),
        "ws-rtp" => simple!(ws::rtp::RtpWebSocketClientTransport),
        "ws-rudp" => simple!(ws::rudp::RudpWebSocketClientTransport),
        "ws-dccp" => simple!(ws::dccp::DccpWebSocketClientTransport),
        "ws-sctp" => simple!(ws::sctp::SctpWebSocketClientTransport),
        #[cfg(any(target_os = "linux", target_os = "android"))]
        "ws-udplite" => simple!(ws::udplite::UdpLiteWebSocketClientTransport),
        "ws-coap" => simple!(ws::coap::CoapWebSocketClientTransport),
        "ws-tcp" => {
            Arc::new(ws::tcp::TcpWebSocketClientTransport::connect(&address.to_string()).await?)
        }
        "ws-http1" => Arc::new(
            ws::http1::Http1WebSocketClientTransport::connect(&format!("ws://{address}/ws"))
                .await?,
        ),
        "tls" => {
            let pem = tls_pem(config).await?;
            Arc::new(
                tcp::tls::TlsClientTransport::connect(address, server_name(config), &pem)
                    .await?
                    .with_stream_framing(Arc::new(gateway_protocol::GatewayFraming)),
            )
        }
        "ws-tls" => {
            let pem = tls_pem(config).await?;
            let name = server_name(config);
            let authority = if name.contains(':') {
                format!("[{name}]")
            } else {
                name.to_owned()
            };
            Arc::new(
                ws::http1::Http1WebSocketClientTransport::connect_with_ca_pem_to_addr(
                    &format!("wss://{authority}:{port}/ws"),
                    address,
                    &pem,
                    Default::default(),
                )
                .await?,
            )
        }
        "quic" | "http3" | "ws-quic" | "ws-http3" => {
            let pem = tls_pem(config).await?;
            let mut tls = json_rpc_2_quic::client::ClientTlsConfig::default();
            tls.set_ca_certificate_pem(&pem)?;
            let local: SocketAddr = if address.is_ipv6() {
                "[::]:0"
            } else {
                "0.0.0.0:0"
            }
            .parse()?;
            let alpn = match kind {
                "quic" => QUIC_ALPN,
                "ws-quic" => WS_QUIC_ALPN,
                _ => H3_ALPN,
            };
            let endpoint = tls.endpoint(local, alpn)?;
            let name = server_name(config);
            match kind {
                "quic" => Arc::new(
                    quic::quic::QuicClientTransport::connect(endpoint, address, name).await?,
                ) as Arc<dyn RpcClientTransport>,
                "http3" => Arc::new(
                    quic::http3::Http3ClientTransport::connect(endpoint, address, name).await?,
                ),
                "ws-quic" => Arc::new(
                    ws::quic::QuicWebSocketClientTransport::connect(endpoint, address, name)
                        .await?,
                ),
                _ => Arc::new(
                    ws::http3::Http3WebSocketClientTransport::connect(endpoint, address, name, "/")
                        .await?,
                ),
            }
        }
        "mqtt" | "ws-mqtt" => {
            let mut broker = json_rpc_2_iot::MqttConfig::for_address(address);
            broker.sessions = broker_sessions(config);
            if let Some(variable) = &config.options.mqtt_url_env {
                broker.broker_url = std::env::var(variable)
                    .context("missing MQTT backend URL environment variable")?;
            }
            if kind == "ws-mqtt" {
                broker.request_topic = "json-rpc-2/ws/requests".to_owned();
            }
            let topic = if kind == "ws-mqtt" {
                &config.options.ws_mqtt_request_topic
            } else {
                &config.options.mqtt_request_topic
            };
            if let Some(topic) = topic {
                broker.request_topic = topic.clone();
            }
            if kind == "mqtt" {
                Arc::new(iot::mqtt::MqttClientTransport::connect_with_config(broker).await?)
                    as Arc<dyn RpcClientTransport>
            } else {
                Arc::new(ws::mqtt::MqttWebSocketClientTransport::connect_with_config(broker).await?)
            }
        }
        "amqp" | "ws-amqp" => {
            let mut broker = json_rpc_2_iot::AmqpConfig::for_address(address);
            broker.sessions = broker_sessions(config);
            if let Some(variable) = &config.options.amqp_url_env {
                broker.broker_url = std::env::var(variable)
                    .context("missing AMQP backend URL environment variable")?;
            }

            if kind == "ws-amqp" {
                broker.request_queue = "json-rpc-2.ws.requests".to_owned();
            }
            let queue = if kind == "ws-amqp" {
                &config.options.ws_amqp_request_queue
            } else {
                &config.options.amqp_request_queue
            };
            if let Some(queue) = queue {
                broker.request_queue = queue.clone();
            }
            if kind == "amqp" {
                Arc::new(iot::amqp::AmqpClientTransport::connect_with_config(broker).await?)
                    as Arc<dyn RpcClientTransport>
            } else {
                Arc::new(ws::amqp::AmqpWebSocketClientTransport::connect_with_config(broker).await?)
            }
        }
        #[cfg(feature = "srt")]
        "srt" | "ws-srt" => {
            let variable = config
                .options
                .srt_passphrase_env
                .as_deref()
                .context("SRT backend requires srt_passphrase_env")?;
            let passphrase =
                std::env::var(variable).context("missing SRT passphrase environment variable")?;
            if kind == "srt" {
                Arc::new(datagram::srt::SrtClientTransport::connect(address, passphrase).await?)
                    as Arc<dyn RpcClientTransport>
            } else {
                Arc::new(ws::srt::SrtWebSocketClientTransport::connect(address, passphrase).await?)
            }
        }
        _ => anyhow::bail!("unsupported or disabled backend transport {kind}"),
    };
    Ok(transport)
}

fn server_name(config: &BackendConfig) -> &str {
    config
        .options
        .tls_server_name
        .as_deref()
        .unwrap_or(&config.host)
}

async fn tls_pem(config: &BackendConfig) -> anyhow::Result<String> {
    let path = config
        .options
        .tls_ca_path
        .as_deref()
        .context("TLS backend requires tls_ca_path")?;
    tokio::fs::read_to_string(path)
        .await
        .context("reading backend TLS CA bundle")
}
