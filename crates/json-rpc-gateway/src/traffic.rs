use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default)]
struct TrafficCounters {
    inbound_bytes: AtomicU64,
    outbound_bytes: AtomicU64,
}

impl TrafficCounters {
    fn record_inbound(&self, bytes: usize) {
        self.inbound_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn record_outbound(&self, bytes: usize) {
        self.outbound_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn inbound_bytes(&self) -> u64 {
        self.inbound_bytes.load(Ordering::Relaxed)
    }

    fn outbound_bytes(&self) -> u64 {
        self.outbound_bytes.load(Ordering::Relaxed)
    }
}

static TRAFFIC: TrafficCounters = TrafficCounters {
    inbound_bytes: AtomicU64::new(0),
    outbound_bytes: AtomicU64::new(0),
};

/// Returns cumulative inbound JSON-RPC payload bytes since the gateway started.
pub fn inbound_traffic_bytes() -> u64 {
    TRAFFIC.inbound_bytes()
}

/// Returns cumulative outbound JSON-RPC payload bytes since the gateway started.
pub fn outbound_traffic_bytes() -> u64 {
    TRAFFIC.outbound_bytes()
}

pub(crate) fn record_inbound(bytes: usize) {
    TRAFFIC.record_inbound(bytes);
}

pub(crate) fn record_outbound(bytes: usize) {
    TRAFFIC.record_outbound(bytes);
}

#[cfg(test)]
#[path = "traffic/tests.rs"]
mod tests;
