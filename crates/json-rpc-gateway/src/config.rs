use std::{collections::HashMap, env, fs, net::IpAddr, path::PathBuf};

use anyhow::{Context, bail};
use json_rpc_2_runtime::TransportKind;
use json_rpc_balancer::BackendConfig;
use serde::Deserialize;

#[derive(Clone, Debug)]
pub struct GatewayConfig {
    pub bind: IpAddr,
    pub control_port: u16,
    pub backend_config_path: PathBuf,
    pub log_level: String,
    pub transport_ports: HashMap<TransportKind, u16>,
    pub tls_cert_path: Option<PathBuf>,
    pub tls_key_path: Option<PathBuf>,
    pub mqtt_broker_url: Option<String>,
    pub amqp_broker_url: Option<String>,
}

#[derive(Deserialize)]
struct BackendFile {
    backends: Vec<BackendConfig>,
}

impl GatewayConfig {
    pub fn broker_sessions(&self) -> anyhow::Result<gateway_sessions::BrokerSessionPolicy> {
        let mut policy = gateway_sessions::BrokerSessionPolicy::default();
        for (name, duration) in [
            (
                "GATEWAY_BROKER_HEARTBEAT_INTERVAL_MS",
                &mut policy.heartbeat_interval,
            ),
            ("GATEWAY_BROKER_SESSION_LEASE_MS", &mut policy.session_lease),
        ] {
            match env::var(name) {
                Ok(value) => {
                    *duration = std::time::Duration::from_millis(
                        value
                            .parse()
                            .with_context(|| format!("{name} must be milliseconds"))?,
                    );
                }
                Err(env::VarError::NotPresent) => {}
                Err(error) => return Err(error).with_context(|| format!("reading {name}")),
            }
        }
        policy.validate()?;
        Ok(policy)
    }

    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub fn udplite_config(&self) -> anyhow::Result<json_rpc_2_datagram::UdpLiteConfig> {
        let mut config = json_rpc_2_datagram::UdpLiteConfig::default();
        for (name, coverage) in [
            (
                "GATEWAY_UDPLITE_SEND_CHECKSUM_COVERAGE",
                &mut config.send_checksum_coverage,
            ),
            (
                "GATEWAY_UDPLITE_RECV_CHECKSUM_COVERAGE",
                &mut config.recv_checksum_coverage,
            ),
        ] {
            match env::var(name) {
                Ok(value) => {
                    *coverage = value.parse().with_context(|| {
                        format!("{name} must be a checksum coverage in 0..=65535")
                    })?
                }
                Err(env::VarError::NotPresent) => {}
                Err(error) => return Err(error).with_context(|| format!("reading {name}")),
            }
        }
        config.validate().map_err(anyhow::Error::msg)?;
        Ok(config)
    }

    #[cfg(feature = "srt")]
    pub fn srt_config(&self) -> anyhow::Result<Option<json_rpc_2_datagram::SrtConfig>> {
        if !self
            .transport_ports
            .keys()
            .any(|kind| matches!(kind.id(), "srt" | "ws-srt"))
        {
            return Ok(None);
        }
        let config = json_rpc_2_datagram::SrtConfig {
            passphrase: required("GATEWAY_SRT_PASSPHRASE")?,
        };
        config.validate().map_err(anyhow::Error::msg)?;
        Ok(Some(config))
    }

    pub fn load() -> anyhow::Result<Self> {
        match dotenvy::dotenv() {
            Ok(_) => {}
            Err(dotenvy::Error::Io(error)) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("loading .env file"),
        }

        let gateway_port = required_u16("GATEWAY_PORT")?;
        let control_port = required_u16("CONTROL_PORT")?;
        let bind = required("GATEWAY_BIND")?
            .parse::<IpAddr>()
            .context("GATEWAY_BIND must be a valid IP address")?;
        let backend_config_path = PathBuf::from(required("BACKEND_CONFIG_PATH")?);
        let log_level = required("LOG_LEVEL")?;

        let mut transport_ports = HashMap::new();
        for &kind in TransportKind::ALL {
            let key = format!(
                "GATEWAY_{}_PORT",
                kind.id().replace('-', "_").to_ascii_uppercase()
            );
            if let Some(port) = optional_u16(&key)? {
                transport_ports.insert(kind, port);
            }
        }

        transport_ports
            .entry(TransportKind::Tcp)
            .or_insert(gateway_port);
        transport_ports
            .entry(TransportKind::Udp)
            .or_insert(gateway_port);

        validate_port_assignments(&transport_ports)?;

        Ok(Self {
            bind,
            control_port,
            backend_config_path,
            log_level,
            transport_ports,
            tls_cert_path: env::var_os("TLS_CERT_PATH").map(PathBuf::from),
            tls_key_path: env::var_os("TLS_KEY_PATH").map(PathBuf::from),
            mqtt_broker_url: env::var("MQTT_BROKER_URL").ok(),
            amqp_broker_url: env::var("AMQP_BROKER_URL").ok(),
        })
    }

    pub fn load_backends(&self) -> anyhow::Result<Vec<BackendConfig>> {
        let input = fs::read(&self.backend_config_path)
            .with_context(|| format!("reading backend config {:?}", self.backend_config_path))?;
        let parsed = serde_json::from_slice::<BackendFile>(&input)
            .map(Ok)
            .unwrap_or_else(|json_error| {
                serde_yaml::from_slice::<BackendFile>(&input).map_err(|yaml_error| {
                    anyhow::anyhow!(
                        "backend config is neither valid JSON ({json_error}) nor YAML ({yaml_error})"
                    )
                })
            })?;

        if parsed.backends.is_empty() {
            bail!("backend config must contain at least one backend");
        }

        let mut ids = std::collections::HashSet::new();
        for backend in &parsed.backends {
            if backend.id.trim().is_empty() {
                bail!("backend id must not be empty");
            }
            if !ids.insert(&backend.id) {
                bail!("duplicate backend id: {}", backend.id);
            }
            if backend.ports.is_empty() {
                bail!(
                    "backend {} must define at least one transport port",
                    backend.id
                );
            }
            if backend.pool_size == 0 {
                bail!("backend {} pool_size must be greater than zero", backend.id);
            }
        }

        Ok(parsed.backends)
    }
}

fn validate_port_assignments(ports: &HashMap<TransportKind, u16>) -> anyhow::Result<()> {
    for (&kind, &port) in ports {
        if kind.is_broker() && port == 0 {
            bail!("{} requires a nonzero external broker port", kind.id());
        }
    }
    Ok(())
}

fn required(name: &str) -> anyhow::Result<String> {
    env::var(name).with_context(|| format!("missing required environment variable {name}"))
}

fn required_u16(name: &str) -> anyhow::Result<u16> {
    required(name)?
        .parse()
        .with_context(|| format!("{name} must be a valid port number"))
}

fn optional_u16(name: &str) -> anyhow::Result<Option<u16>> {
    env::var(name)
        .ok()
        .map(|value| {
            value
                .parse()
                .with_context(|| format!("{name} must be a valid port number"))
        })
        .transpose()
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use json_rpc_2_runtime::TransportKind;

    use super::validate_port_assignments;

    #[test]
    fn allows_tcp_adapters_on_the_same_port() {
        let ports = HashMap::from([(TransportKind::Tcp, 8443), (TransportKind::Tls, 8443)]);
        assert!(validate_port_assignments(&ports).is_ok());
    }

    #[test]
    fn tcp_and_udp_can_share_the_same_numeric_port() {
        let ports = HashMap::from([(TransportKind::Tcp, 8443), (TransportKind::Udp, 8443)]);
        assert!(validate_port_assignments(&ports).is_ok());
    }

    #[test]
    fn rejects_zero_external_broker_port() {
        let ports = HashMap::from([(TransportKind::Mqtt, 0)]);
        assert!(validate_port_assignments(&ports).is_err());
    }
}
