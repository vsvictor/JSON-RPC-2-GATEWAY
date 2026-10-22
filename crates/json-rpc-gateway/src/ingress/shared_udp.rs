//! One UDP reader with bounded, owned packet lanes for technical wire profiles.

use std::{io, net::SocketAddr, sync::Arc};

use bytes::{Bytes, BytesMut};
use json_rpc_2_core::{
    decode_rtcp_control_packet, inspect_coap_message, inspect_rtp_packet, inspect_rudp_packet,
    inspect_udp_packet,
};
use json_rpc_2_transport::{CancellationToken, OwnedDatagram, log_transport_error};
use tokio::{net::UdpSocket, sync::mpsc};

use json_rpc_2_runtime::TransportKind;

pub(crate) struct Router {
    pub socket: Arc<UdpSocket>,
    pub lanes: Vec<(TransportKind, mpsc::Sender<OwnedDatagram>)>,

    pub quic: Option<quic::Ingress>,
    #[cfg(feature = "srt")]
    pub srt: Option<Arc<crate::ingress::shared_srt::SrtUdpRelay>>,
    pub udplite: bool,
}

impl Router {
    async fn dispatch(&self, packet: Bytes, peer: SocketAddr, meta: Option<quinn::udp::RecvMeta>) {
        #[cfg(feature = "srt")]
        if let Some(relay) = &self.srt
            && (relay.owns_peer(peer)
                || crate::ingress::shared_srt::SrtUdpRelay::is_handshake(&packet))
        {
            // The relay logs rejected/full admissions itself.
            let _ = relay.forward(packet, peer).await;
            return;
        }

        if let Some(ingress) = &self.quic
            && quic::is_packet(&packet)
        {
            let Some(meta) = meta else {
                log_transport_error(
                    "shared UDP",
                    peer,
                    "QUIC metadata",
                    &"missing receive metadata",
                );
                return;
            };
            if let Err(error) = ingress.sender.try_send((packet, meta)) {
                log_transport_error("shared UDP", peer, "QUIC packet queue", &error);
            }
            return;
        }
        let (profile, payload) = match inspect(&packet, self.udplite) {
            Ok(frame) => frame,
            Err(error) => {
                log_transport_error("shared UDP", peer, "packet classification", &error);
                return;
            }
        };
        let websocket = payload.starts_with(b"WSU2");
        let lane = self
            .lanes
            .iter()
            .find(|(kind, _)| {
                kind.id().strip_prefix("ws-").unwrap_or(kind.id()) == profile
                    && kind.id().starts_with("ws-") == websocket
            })
            .or_else(|| {
                // Carrier ACK/ping/pong frames have no application payload.
                payload
                    .is_empty()
                    .then(|| {
                        self.lanes.iter().find(|(kind, _)| {
                            kind.id().strip_prefix("ws-").unwrap_or(kind.id()) == profile
                        })
                    })
                    .flatten()
            });
        match lane {
            Some((_, sender)) => {
                if let Err(error) = sender.try_send((packet, peer)) {
                    log_transport_error("shared UDP", peer, "packet queue", &error);
                }
            }
            None => log_transport_error(
                "shared UDP",
                peer,
                "packet classification",
                &"disabled wire profile",
            ),
        }
    }

    pub async fn run(&self, shutdown: CancellationToken) -> io::Result<()> {
        loop {
            if let Some(ingress) = &self.quic {
                let mut buffer = vec![0; 65_535 * ingress.io.max_receive_segments()];
                let mut meta = quinn::udp::RecvMeta::default();
                let received = tokio::select! {
                    biased;
                    _ = shutdown.cancelled() => break,
                    received = std::future::poll_fn(|cx| {
                        ingress.io.poll_recv(cx, &mut [io::IoSliceMut::new(&mut buffer)], std::slice::from_mut(&mut meta))
                    }) => received?,
                };
                if received == 0 {
                    continue;
                }
                buffer.truncate(meta.len);
                // Do not pin a multi-megabyte GRO allocation per queued small
                // packet. Compaction may realloc here, before owned lane slicing.
                let buffer = Bytes::from(buffer.into_boxed_slice());
                let stride = meta.stride.max(1);
                for start in (0..buffer.len()).step_by(stride) {
                    let end = (start + stride).min(buffer.len());
                    let packet_meta = quinn::udp::RecvMeta {
                        len: end - start,
                        stride: end - start,
                        ..meta
                    };
                    self.dispatch(buffer.slice(start..end), meta.addr, Some(packet_meta))
                        .await;
                }
                continue;
            }
            let mut packet = BytesMut::zeroed(65_535);
            let (len, peer) = tokio::select! {
                biased;
                _ = shutdown.cancelled() => break,
                received = self.socket.recv_from(&mut packet) => received?,
            };
            packet.truncate(len);
            self.dispatch(packet.freeze(), peer, None).await;
        }
        Ok(())
    }
}

fn inspect(packet: &[u8], udplite: bool) -> Result<(&'static str, &[u8]), String> {
    if packet.starts_with(b"JRP2") {
        let (_, payload) = inspect_udp_packet(packet)?;
        return Ok((if udplite { "udplite" } else { "udp" }, payload));
    }
    if udplite {
        return Err("UDP-Lite requires reliable datagram framing".into());
    }
    if packet.starts_with(b"RUDP") {
        let (_, payload) = inspect_rudp_packet(packet)?;
        return Ok(("rudp", payload));
    }
    if packet.get(1) == Some(&204) && packet.first().is_some_and(|byte| byte >> 6 == 2) {
        decode_rtcp_control_packet(packet)?;
        return Ok(("rtp", &[]));
    }
    if packet.first().is_some_and(|byte| byte >> 6 == 2) {
        let (_, payload) = inspect_rtp_packet(packet)?;
        return Ok(("rtp", payload));
    }
    if packet.first().is_some_and(|byte| byte >> 6 == 1) {
        let (_, _, payload) = inspect_coap_message(packet)?;
        return Ok(("coap", payload));
    }
    Err("unrecognized datagram framing".into())
}

pub(crate) mod quic {
    use super::*;
    use quinn::{
        AsyncUdpSocket, ConnectionId, ConnectionIdGenerator, UdpPoller,
        udp::{RecvMeta, Transmit},
    };
    use std::{
        io::IoSliceMut,
        pin::Pin,
        sync::Mutex,
        task::{Context, Poll},
    };

    const CID_PREFIX: &[u8; 2] = b"QG";
    const CID_LEN: usize = 18;
    type Packet = (Bytes, RecvMeta);

    pub struct Ingress {
        pub io: Arc<dyn AsyncUdpSocket>,
        pub sender: mpsc::Sender<Packet>,
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use json_rpc_2_core::{
            CoapMessage, ReliableUdpPacket, RtpPacket, RtpPacketKind, RudpPacket,
            encode_coap_message, encode_rtp_packet, encode_rudp_packet, encode_udp_packet,
        };

        #[test]
        fn inspectors_borrow_payload_and_validate_truncated_headers() {
            let input = b"\0\xffnot-json";
            let frames = [
                (
                    "udp",
                    encode_udp_packet(&ReliableUdpPacket::request(1, input.to_vec())).unwrap(),
                ),
                (
                    "rudp",
                    encode_rudp_packet(&RudpPacket::request(1, 2, input.to_vec())).unwrap(),
                ),
                (
                    "rtp",
                    encode_rtp_packet(&RtpPacket {
                        sequence_number: 1,
                        timestamp: 2,
                        ssrc: 3,
                        kind: RtpPacketKind::Request,
                        message_id: 4,
                        payload: input.to_vec(),
                    })
                    .unwrap(),
                ),
                (
                    "coap",
                    encode_coap_message(&CoapMessage::request(1, vec![1; 8], input.to_vec()))
                        .unwrap(),
                ),
            ];
            for (profile, packet) in frames {
                let (actual, payload) = inspect(&packet, false).unwrap();
                assert_eq!(actual, profile);
                assert_eq!(payload, input);
                assert_eq!(
                    payload.as_ptr(),
                    packet[packet.len() - input.len()..].as_ptr()
                );
                for length in 0..4 {
                    assert!(inspect(&packet[..length], false).is_err());
                }
            }
        }

        #[tokio::test]
        async fn bounded_lanes_route_owned_packets_without_copying_or_blocking() {
            let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
            let peer = socket.local_addr().unwrap();
            let (raw, mut raw_rx) = mpsc::channel(1);
            let (ws, mut ws_rx) = mpsc::channel(1);
            let router = Router {
                socket,
                lanes: vec![(TransportKind::Udp, raw), (TransportKind::WebSocketUdp, ws)],

                quic: None,
                #[cfg(feature = "srt")]
                srt: None,
                udplite: false,
            };
            let raw = Bytes::from(
                encode_udp_packet(&ReliableUdpPacket::request(1, b"opaque".to_vec())).unwrap(),
            );
            let ws = Bytes::from(
                encode_udp_packet(&ReliableUdpPacket::request(2, b"WSU2framing".to_vec())).unwrap(),
            );
            for packet in [&raw, &ws, &raw] {
                router.dispatch(packet.clone(), peer, None).await;
            }
            let received = raw_rx.recv().await.unwrap().0;
            assert_eq!(received.as_ptr(), raw.as_ptr());
            let received = ws_rx.recv().await.unwrap().0;
            assert_eq!(received.as_ptr(), ws.as_ptr());
            assert!(raw_rx.try_recv().is_err());
            assert!(ws_rx.try_recv().is_err());
        }

        #[tokio::test]
        async fn quic_lane_preserves_packet_metadata_and_reserved_cids() {
            use quinn::{ConnectionIdGenerator, Runtime};
            let io = quinn::TokioRuntime
                .wrap_udp_socket(std::net::UdpSocket::bind("127.0.0.1:0").unwrap())
                .unwrap();
            let peer = io.local_addr().unwrap();
            let (ingress, lane) = quic::lane(io, 1);
            let packet = Bytes::from_static(b"\x40QGopaque");
            let dst_ip = "127.0.0.2".parse().unwrap();
            ingress
                .sender
                .try_send((
                    packet.clone(),
                    quinn::udp::RecvMeta {
                        addr: peer,
                        len: packet.len(),
                        stride: packet.len(),
                        ecn: Some(quinn::udp::EcnCodepoint::Ect0),
                        dst_ip: Some(dst_ip),
                    },
                ))
                .unwrap();
            let mut buf = [0; 64];
            let mut meta = [quinn::udp::RecvMeta::default()];
            std::future::poll_fn(|cx| {
                lane.poll_recv(cx, &mut [io::IoSliceMut::new(&mut buf)], &mut meta)
            })
            .await
            .unwrap();
            assert_eq!(&buf[..meta[0].len], packet.as_ref());
            assert_eq!(meta[0].dst_ip, Some(dst_ip));
            assert_eq!(meta[0].ecn, Some(quinn::udp::EcnCodepoint::Ect0));
            assert_eq!(meta[0].addr, peer);
            let mut generator = quic::CidGenerator;
            let first = generator.generate_cid();
            let second = generator.generate_cid();
            assert_eq!(first.len(), 18);
            assert_ne!(first, second);
            let mut header = vec![0x40];
            header.extend_from_slice(&first);
            assert!(quic::is_packet(&header));
            assert!(!quic::is_packet(b"JRP2"));
            assert!(!quic::is_packet(b"RUDP"));
            assert!(!quic::is_packet(b"\x42\x02coap"));
        }
    }

    #[derive(Debug)]
    struct Lane {
        io: Arc<dyn AsyncUdpSocket>,
        receiver: Mutex<mpsc::Receiver<Packet>>,
    }

    /// Reserved public prefix plus 128 cryptographically random bits. The
    /// prefix distinguishes short headers from reliable UDP/RUDP/CoAP.
    pub struct CidGenerator;

    impl ConnectionIdGenerator for CidGenerator {
        fn generate_cid(&mut self) -> ConnectionId {
            let mut cid = [0; CID_LEN];
            cid[..CID_PREFIX.len()].copy_from_slice(CID_PREFIX);
            rustls::crypto::ring::default_provider()
                .secure_random
                .fill(&mut cid[CID_PREFIX.len()..])
                .expect("cryptographic QUIC connection ID generation failed");
            ConnectionId::new(&cid)
        }

        fn cid_len(&self) -> usize {
            CID_LEN
        }
        fn cid_lifetime(&self) -> Option<std::time::Duration> {
            None
        }
    }

    pub fn is_packet(packet: &[u8]) -> bool {
        packet.first().is_some_and(|byte| {
            byte & 0xc0 == 0xc0
                || (byte & 0xc0 == 0x40 && packet.get(1..3) == Some(CID_PREFIX.as_slice()))
        })
    }

    pub fn lane(
        io: Arc<dyn AsyncUdpSocket>,
        capacity: usize,
    ) -> (Ingress, Arc<dyn AsyncUdpSocket>) {
        let (sender, receiver) = mpsc::channel(capacity);
        (
            Ingress {
                io: io.clone(),
                sender,
            },
            Arc::new(Lane {
                io,
                receiver: Mutex::new(receiver),
            }),
        )
    }

    impl AsyncUdpSocket for Lane {
        fn create_io_poller(self: Arc<Self>) -> Pin<Box<dyn UdpPoller>> {
            self.io.clone().create_io_poller()
        }
        fn try_send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
            self.io.try_send(transmit)
        }
        fn poll_recv(
            &self,
            cx: &mut Context<'_>,
            bufs: &mut [IoSliceMut<'_>],
            meta: &mut [RecvMeta],
        ) -> Poll<io::Result<usize>> {
            if bufs.is_empty() || meta.is_empty() {
                return Poll::Ready(Ok(0));
            }
            match self.receiver.lock().unwrap().poll_recv(cx) {
                Poll::Pending => Poll::Pending,
                Poll::Ready(None) => Poll::Ready(Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "shared QUIC ingress closed",
                ))),
                Poll::Ready(Some((packet, metadata))) => {
                    if packet.len() > bufs[0].len() {
                        return Poll::Ready(Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "QUIC receive buffer too small",
                        )));
                    }
                    bufs[0][..packet.len()].copy_from_slice(&packet);
                    meta[0] = metadata;
                    Poll::Ready(Ok(1))
                }
            }
        }
        fn local_addr(&self) -> io::Result<SocketAddr> {
            self.io.local_addr()
        }
        fn max_transmit_segments(&self) -> usize {
            self.io.max_transmit_segments()
        }
        fn may_fragment(&self) -> bool {
            self.io.may_fragment()
        }
    }
}
