//! Gateway AMQP RPC server consumer, not an AMQP broker.
pub use crate::AmqpConfig;
use crate::broker::{self, Config};
use json_rpc_2_transport::*;
use std::{io, sync::Arc};

pub struct AmqpTransport {
    config: AmqpConfig,
    runtime: Arc<TransportRuntime>,
    prepared: Option<broker::Prepared>,
}
impl AmqpTransport {
    pub fn new(config: AmqpConfig) -> Self {
        Self {
            config,
            runtime: Arc::default(),
            prepared: None,
        }
    }
    pub async fn connect(config: AmqpConfig) -> io::Result<Self> {
        let prepared = broker::prepare(&Config::Amqp(config.clone())).await?;
        Ok(Self {
            prepared: Some(prepared),
            ..Self::new(config)
        })
    }
    pub fn with_runtime(mut self, runtime: Arc<TransportRuntime>) -> Self {
        self.runtime = runtime;
        self
    }
}
impl Transport for AmqpTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(broker::serve(
            Config::Amqp(self.config),
            self.prepared,
            protocol,
            shutdown,
            self.runtime,
        ))
    }
}
pub async fn serve(
    config: AmqpConfig,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    broker::serve(Config::Amqp(config), None, protocol, shutdown, runtime).await
}
