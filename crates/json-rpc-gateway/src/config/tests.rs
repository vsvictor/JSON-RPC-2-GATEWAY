use std::collections::HashMap;

use json_rpc_2_runtime::TransportKind;

use super::{parse_positive_usize, validate_port_assignments};

#[test]
fn allows_tcp_adapters_on_the_same_port() {
    let ports = HashMap::from([(TransportKind::Tcp, 8443), (TransportKind::Tls, 8443)]);
    assert!(validate_port_assignments(&ports).is_ok());
}

#[test]
fn tcp_and_udp_can_share_the_same_numeric_port() {
    let ports = HashMap::from([(TransportKind::Tcp, 8443), (TransportKind::Udp, 8443)]);
    assert!(validate_port_assignments(&ports).is_ok());
}

#[test]
fn rejects_zero_external_broker_port() {
    let ports = HashMap::from([(TransportKind::Mqtt, 0)]);
    assert!(validate_port_assignments(&ports).is_err());
}

#[test]
fn gateway_max_connections_must_be_a_positive_integer() {
    assert_eq!(
        parse_positive_usize("GATEWAY_MAX_CONNECTIONS", "4096").unwrap(),
        4096
    );
    assert!(parse_positive_usize("GATEWAY_MAX_CONNECTIONS", "0").is_err());
    assert!(parse_positive_usize("GATEWAY_MAX_CONNECTIONS", "invalid").is_err());
}
