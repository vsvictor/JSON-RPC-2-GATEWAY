use std::sync::Arc;

use super::amqp::AmqpTransport;
use crate::{AmqpConfig, server::websocket::WebSocketJsonRpcAdapter};
use json_rpc_2_transport::{
    CancellationToken, JsonRpcProtocol, Transport, TransportFuture, TransportRuntime,
};

pub struct AmqpWebSocketTransport {
    inner: AmqpTransport,
}

impl AmqpWebSocketTransport {
    pub fn new(config: AmqpConfig) -> Self {
        Self {
            inner: AmqpTransport::new(config),
        }
    }

    pub async fn connect(config: AmqpConfig) -> std::io::Result<Self> {
        Ok(Self {
            inner: AmqpTransport::connect(config).await?,
        })
    }

    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.inner = self.inner.with_runtime(runtime);
        self
    }
}

impl Transport for AmqpWebSocketTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        self.inner.run(
            Arc::new(WebSocketJsonRpcAdapter::new(protocol, "ws-amqp")),
            shutdown,
        )
    }
}
