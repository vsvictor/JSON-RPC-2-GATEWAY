use std::sync::Arc;

use super::mqtt::MqttTransport;
use crate::{MqttConfig, server::websocket::WebSocketJsonRpcAdapter};
use json_rpc_2_transport::{
    CancellationToken, JsonRpcProtocol, Transport, TransportFuture, TransportRuntime,
};

pub struct MqttWebSocketTransport {
    inner: MqttTransport,
}

impl MqttWebSocketTransport {
    pub fn new(config: MqttConfig) -> Self {
        Self {
            inner: MqttTransport::new(config),
        }
    }

    pub async fn connect(config: MqttConfig) -> std::io::Result<Self> {
        Ok(Self {
            inner: MqttTransport::connect(config).await?,
        })
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.inner = self.inner.with_runtime(runtime);
        self
    }
}

impl Transport for MqttWebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        self.inner.run(
            Arc::new(WebSocketJsonRpcAdapter::new(protocol, "ws-mqtt")),
            shutdown,
        )
    }
}
