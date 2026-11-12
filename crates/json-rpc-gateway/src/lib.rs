mod traffic;

pub use traffic::{inbound_traffic_bytes, outbound_traffic_bytes};

#[doc(hidden)]
pub fn record_inbound_traffic_bytes(bytes: usize) {
    traffic::record_inbound(bytes);
}

#[doc(hidden)]
pub fn record_outbound_traffic_bytes(bytes: usize) {
    traffic::record_outbound(bytes);
}
