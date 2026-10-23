use std::{env, io, time::Duration};

use gateway_sessions::BrokerSessionPolicy;

use crate::{AmqpConfig, MqttConfig};

#[derive(Clone)]
pub struct GatewayIotConfig {
    mqtt_broker_url: Option<String>,
    amqp_broker_url: Option<String>,
    sessions: BrokerSessionPolicy,
}

impl std::fmt::Debug for GatewayIotConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GatewayIotConfig")
            .field(
                "mqtt_broker_url",
                &self.mqtt_broker_url.as_ref().map(|_| "<redacted>"),
            )
            .field(
                "amqp_broker_url",
                &self.amqp_broker_url.as_ref().map(|_| "<redacted>"),
            )
            .field("sessions", &self.sessions)
            .finish()
    }
}

impl GatewayIotConfig {
    pub fn from_env(require_broker_sessions: bool) -> io::Result<Self> {
        let mut sessions = BrokerSessionPolicy::default();
        if require_broker_sessions {
            for (name, duration) in [
                (
                    "GATEWAY_BROKER_HEARTBEAT_INTERVAL_MS",
                    &mut sessions.heartbeat_interval,
                ),
                (
                    "GATEWAY_BROKER_SESSION_LEASE_MS",
                    &mut sessions.session_lease,
                ),
            ] {
                match env::var(name) {
                    Ok(value) => {
                        *duration = Duration::from_millis(value.parse().map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                format!("{name} must be milliseconds"),
                            )
                        })?);
                    }
                    Err(env::VarError::NotPresent) => {}
                    Err(error) => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidInput,
                            format!("reading {name}: {error}"),
                        ));
                    }
                }
            }
            sessions
                .validate()
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        }
        Ok(Self {
            mqtt_broker_url: optional_env("MQTT_BROKER_URL")?,
            amqp_broker_url: optional_env("AMQP_BROKER_URL")?,
            sessions,
        })
    }

    #[cfg(feature = "mqtt")]
    pub fn mqtt_config(&self, websocket_frames: bool) -> io::Result<MqttConfig> {
        let broker_url = self.mqtt_broker_url.as_deref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "MQTT_BROKER_URL is required when MQTT transport is enabled",
            )
        })?;
        Ok(MqttConfig {
            broker_url: broker_url.to_owned(),
            request_topic: if websocket_frames {
                "json-rpc-2/ws/requests".to_owned()
            } else {
                MqttConfig::default().request_topic
            },
            sessions: self.sessions,
            ..Default::default()
        })
    }

    #[cfg(feature = "amqp")]
    pub fn amqp_config(&self, websocket_frames: bool) -> io::Result<AmqpConfig> {
        let broker_url = self.amqp_broker_url.as_deref().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "AMQP_BROKER_URL is required when AMQP transport is enabled",
            )
        })?;
        Ok(AmqpConfig {
            broker_url: broker_url.to_owned(),
            request_queue: if websocket_frames {
                "json-rpc-2.ws.requests".to_owned()
            } else {
                AmqpConfig::default().request_queue
            },
            sessions: self.sessions,
            ..Default::default()
        })
    }
}

fn optional_env(name: &str) -> io::Result<Option<String>> {
    match env::var(name) {
        Ok(value) => Ok(Some(value)),
        Err(env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("reading {name}: {error}"),
        )),
    }
}
