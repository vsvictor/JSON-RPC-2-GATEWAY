use std::{net::SocketAddr, sync::Arc, time::Instant};

use json_rpc_balancer::{BackendPool, BackendSnapshot};
use serde::Serialize;

#[derive(Serialize)]
pub struct ListenerInfo {
    pub transport: String,
    pub address: SocketAddr,
}

pub struct ComponentInventory {
    started: Instant,
    listeners: Vec<ListenerInfo>,
    control_address: SocketAddr,
    metrics_address: SocketAddr,
    backends: Arc<BackendPool>,
    pub transport_policy: Option<Arc<crate::transport_policy::TransportPolicy>>,
}

#[derive(Serialize)]
pub struct Components {
    gateway: GatewayInfo,
    balancer: BalancerInfo,
    backends: Vec<BackendSnapshot>,
}

#[derive(Serialize)]
struct GatewayInfo {
    version: &'static str,
    pid: u32,
    uptime_seconds: u64,
    listeners: Vec<ListenerInfo>,
    control_address: SocketAddr,
    metrics_address: SocketAddr,
}

#[derive(Serialize)]
struct BalancerInfo {
    version: &'static str,
    execution: &'static str,
    backend_count: usize,
}

impl ComponentInventory {
    pub async fn list_methods(&self) -> json_rpc_balancer::MethodInventory {
        self.backends.list_methods().await
    }

    pub fn new(
        started: Instant,
        listeners: Vec<ListenerInfo>,
        control_address: SocketAddr,
        metrics_address: SocketAddr,
        backends: Arc<BackendPool>,
    ) -> Self {
        Self {
            started,
            listeners,
            control_address,
            metrics_address,
            backends,
            transport_policy: None,
        }
    }

    pub fn with_transport_policy(
        mut self,
        policy: Arc<crate::transport_policy::TransportPolicy>,
    ) -> Self {
        self.transport_policy = Some(policy);
        self
    }

    pub fn snapshot(&self) -> Components {
        let backends = self.backends.snapshot();
        Components {
            gateway: GatewayInfo {
                version: env!("CARGO_PKG_VERSION"),
                pid: std::process::id(),
                uptime_seconds: self.started.elapsed().as_secs(),
                listeners: self
                    .listeners
                    .iter()
                    .map(|listener| ListenerInfo {
                        transport: listener.transport.clone(),
                        address: listener.address,
                    })
                    .collect(),
                control_address: self.control_address,
                metrics_address: self.metrics_address,
            },
            balancer: BalancerInfo {
                version: json_rpc_balancer::VERSION,
                execution: "in_process",
                backend_count: backends.len(),
            },
            backends,
        }
    }
}
