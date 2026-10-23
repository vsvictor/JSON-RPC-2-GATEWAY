//! Gateway MQTT RPC server consumer, not an MQTT broker.
pub use crate::MqttConfig;
use crate::broker::{self, Config};
use json_rpc_2_transport::*;
use std::{io, sync::Arc};

pub struct MqttTransport {
    config: MqttConfig,
    runtime: Arc<TransportRuntime>,
    prepared: Option<broker::Prepared>,
}
impl MqttTransport {
    pub fn new(config: MqttConfig) -> Self {
        Self {
            config,
            runtime: Arc::default(),
            prepared: None,
        }
    }
    pub async fn connect(config: MqttConfig) -> io::Result<Self> {
        let prepared = broker::prepare(&Config::Mqtt(config.clone())).await?;
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
impl Transport for MqttTransport {
    fn run(
        self,
        protocol: Arc<dyn JsonRpcProtocol>,
        shutdown: CancellationToken,
    ) -> TransportFuture {
        Box::pin(broker::serve(
            Config::Mqtt(self.config),
            self.prepared,
            protocol,
            shutdown,
            self.runtime,
        ))
    }
}
pub async fn serve(
    config: MqttConfig,
    protocol: Arc<dyn JsonRpcProtocol>,
    shutdown: CancellationToken,
    runtime: Arc<TransportRuntime>,
) -> io::Result<()> {
    broker::serve(Config::Mqtt(config), None, protocol, shutdown, runtime).await
}
