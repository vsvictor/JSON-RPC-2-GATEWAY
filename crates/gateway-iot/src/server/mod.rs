pub mod transports;
mod websocket;

use std::{io, sync::Arc};

use json_rpc_2_transport::{JsonRpcProtocol, RunningTransport, Transport, TransportRuntime};

#[derive(Default)]
pub struct GatewayIotIngress {
    #[cfg(feature = "mqtt")]
    mqtt: Option<crate::MqttConfig>,
    #[cfg(feature = "amqp")]
    amqp: Option<crate::AmqpConfig>,
    #[cfg(feature = "mqtt")]
    ws_mqtt: Option<crate::MqttConfig>,
    #[cfg(feature = "amqp")]
    ws_amqp: Option<crate::AmqpConfig>,
}

impl GatewayIotIngress {
    #[cfg(feature = "mqtt")]
    pub fn set_mqtt_port(&mut self, port: u16) -> &mut Self {
        self.mqtt = Some(crate::MqttConfig::for_address(std::net::SocketAddr::from(
            ([127, 0, 0, 1], port),
        )));
        self
    }

    #[cfg(feature = "mqtt")]
    pub fn set_ws_mqtt_port(&mut self, port: u16) -> &mut Self {
        let mut config =
            crate::MqttConfig::for_address(std::net::SocketAddr::from(([127, 0, 0, 1], port)));
        config.request_topic = "json-rpc-2/ws/requests".into();
        self.ws_mqtt = Some(config);
        self
    }

    #[cfg(feature = "amqp")]
    pub fn set_amqp_port(&mut self, port: u16) -> &mut Self {
        self.amqp = Some(crate::AmqpConfig::for_address(std::net::SocketAddr::from(
            ([127, 0, 0, 1], port),
        )));
        self
    }

    #[cfg(feature = "amqp")]
    pub fn set_ws_amqp_port(&mut self, port: u16) -> &mut Self {
        let mut config =
            crate::AmqpConfig::for_address(std::net::SocketAddr::from(([127, 0, 0, 1], port)));
        config.request_queue = "json-rpc-2.ws.requests".into();
        self.ws_amqp = Some(config);
        self
    }

    #[cfg(feature = "mqtt")]
    pub fn set_mqtt_config(&mut self, config: crate::MqttConfig) -> &mut Self {
        self.mqtt = Some(config);
        self
    }

    #[cfg(feature = "mqtt")]
    pub fn set_ws_mqtt_config(&mut self, config: crate::MqttConfig) -> &mut Self {
        self.ws_mqtt = Some(config);
        self
    }

    #[cfg(feature = "amqp")]
    pub fn set_amqp_config(&mut self, config: crate::AmqpConfig) -> &mut Self {
        self.amqp = Some(config);
        self
    }

    #[cfg(feature = "amqp")]
    pub fn set_ws_amqp_config(&mut self, config: crate::AmqpConfig) -> &mut Self {
        self.ws_amqp = Some(config);
        self
    }

    fn validate(&self) -> io::Result<()> {
        #[cfg(feature = "mqtt")]
        if self
            .mqtt
            .as_ref()
            .zip(self.ws_mqtt.as_ref())
            .is_some_and(|(mqtt, websocket)| mqtt.request_topic == websocket.request_topic)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "MQTT raw and WebSocket ingress must use separate request topics",
            ));
        }
        #[cfg(feature = "amqp")]
        if self
            .amqp
            .as_ref()
            .zip(self.ws_amqp.as_ref())
            .is_some_and(|(amqp, websocket)| amqp.request_queue == websocket.request_queue)
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "AMQP raw and WebSocket ingress must use separate request queues",
            ));
        }
        Ok(())
    }

    pub async fn start(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        runtime: Arc<TransportRuntime>,
    ) -> io::Result<RunningGatewayIot> {
        self.validate()?;
        let mut transports = Vec::new();
        #[cfg(feature = "mqtt")]
        for (config, websocket) in [(self.mqtt, false), (self.ws_mqtt, true)] {
            if let Some(config) = config {
                let transport = if websocket {
                    transports::mqtt_websocket::MqttWebSocketTransport::connect(config)
                        .await
                        .map(|transport| transport.with_runtime(runtime.clone()))
                        .map(|transport| transport.start(protocol.clone()))
                } else {
                    transports::mqtt::MqttTransport::connect(config)
                        .await
                        .map(|transport| transport.with_runtime(runtime.clone()))
                        .map(|transport| transport.start(protocol.clone()))
                };
                match transport {
                    Ok(transport) => transports.push(transport),
                    Err(error) => return close_after_start_error(transports, error).await,
                }
            }
        }
        #[cfg(feature = "amqp")]
        for (config, websocket) in [(self.amqp, false), (self.ws_amqp, true)] {
            if let Some(config) = config {
                let transport = if websocket {
                    transports::amqp_websocket::AmqpWebSocketTransport::connect(config)
                        .await
                        .map(|transport| transport.with_runtime(runtime.clone()))
                        .map(|transport| transport.start(protocol.clone()))
                } else {
                    transports::amqp::AmqpTransport::connect(config)
                        .await
                        .map(|transport| transport.with_runtime(runtime.clone()))
                        .map(|transport| transport.start(protocol.clone()))
                };
                match transport {
                    Ok(transport) => transports.push(transport),
                    Err(error) => return close_after_start_error(transports, error).await,
                }
            }
        }
        Ok(RunningGatewayIot { transports })
    }
}

#[cfg(test)]
mod tests {
    use super::{GatewayIotIngress, io};

    #[cfg(feature = "mqtt")]
    #[test]
    fn mqtt_raw_and_websocket_routes_must_be_distinct() {
        let config = crate::MqttConfig::default();
        let mut ingress = GatewayIotIngress::default();
        ingress
            .set_mqtt_config(config.clone())
            .set_ws_mqtt_config(config);
        let error = ingress.validate().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }

    #[cfg(feature = "amqp")]
    #[test]
    fn amqp_raw_and_websocket_routes_must_be_distinct() {
        let config = crate::AmqpConfig::default();
        let mut ingress = GatewayIotIngress::default();
        ingress
            .set_amqp_config(config.clone())
            .set_ws_amqp_config(config);
        let error = ingress.validate().unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
}

async fn close_after_start_error(
    transports: Vec<RunningTransport>,
    error: io::Error,
) -> io::Result<RunningGatewayIot> {
    let running = RunningGatewayIot { transports };
    if let Err(close_error) = running.close().await {
        tracing::error!(%close_error, "failed to close partially started IoT ingress");
    }
    Err(error)
}

pub struct RunningGatewayIot {
    transports: Vec<RunningTransport>,
}

impl RunningGatewayIot {
    pub async fn close(self) -> io::Result<()> {
        let mut result = Ok(());
        for transport in self.transports {
            if let Err(error) = transport.close().await {
                tracing::error!(%error, "IoT ingress shutdown failed");
                if result.is_ok() {
                    result = Err(error);
                }
            }
        }
        result
    }
}
