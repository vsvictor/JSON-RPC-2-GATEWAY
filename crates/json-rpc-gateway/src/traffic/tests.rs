use super::{
    TrafficCounters, inbound_traffic_bytes, outbound_traffic_bytes, record_inbound, record_outbound,
};

#[test]
fn counters_accumulate_inbound_and_outbound_bytes_independently() {
    let counters = TrafficCounters::default();

    counters.record_inbound(12);
    counters.record_inbound(8);
    counters.record_outbound(5);

    assert_eq!(counters.inbound_bytes(), 20);
    assert_eq!(counters.outbound_bytes(), 5);

    let inbound_before = inbound_traffic_bytes();
    let outbound_before = outbound_traffic_bytes();
    record_inbound(20);
    record_outbound(5);
    assert_eq!(inbound_traffic_bytes(), inbound_before + 20);
    assert_eq!(outbound_traffic_bytes(), outbound_before + 5);
}
