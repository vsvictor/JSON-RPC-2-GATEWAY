//! Opt-in binary gateway framing. Payload bytes are never encoded or parsed here.

use std::sync::Arc;

use bytes::{BufMut, Bytes, BytesMut};

use json_rpc_2_transport::{
    JsonRpcProtocol, ProtocolFuture, ProtocolMessageFuture, TransportContext, TransportError,
    TransportMessage,
};

pub struct GatewayFraming;

impl json_rpc_2_transport::StreamFraming for GatewayFraming {
    fn boundary(&self, prefix: &[u8]) -> std::io::Result<json_rpc_2_transport::FrameBoundary> {
        use json_rpc_2_transport::FrameBoundary;
        let compared = prefix.len().min(MAGIC.len());
        if prefix[..compared] != MAGIC[..compared] {
            return Ok(FrameBoundary::Line);
        }
        if prefix.len() < HEADER_LEN {
            return Ok(FrameBoundary::Need(HEADER_LEN));
        }
        frame_length(prefix)
            .map(FrameBoundary::Frame)
            .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))
    }
}

pub const MAGIC: &[u8; 8] = b"JRPC-GW\x03";
pub const HEADER_LEN: usize = 17;
const REQUEST: u8 = 1;
const RESPONSE: u8 = 2;
const NONE: u8 = 3;
const ERROR: u8 = 4;
const PING: u8 = 5;
const PONG: u8 = 6;
pub const GATEWAY_PING: &[u8] = b"JRPC-GW\x03\x05\0\0\0\0\0\0\0\0";
pub const GATEWAY_PONG: &[u8] = b"JRPC-GW\x03\x06\0\0\0\0\0\0\0\0";
const GATEWAY_NONE: &[u8] = b"JRPC-GW\x03\x03\0\0\0\0\0\0\0\0";
const GATEWAY_ERROR: &[u8] = b"JRPC-GW\x03\x04\0\0\0\0\0\0\0\0";

/// Leaves space for the fixed header and both maximum-size metadata fields.
pub const MAX_GATEWAY_PAYLOAD_BYTES: usize = json_rpc_2_core::MAX_MESSAGE_BYTES - HEADER_LEN - 256;

pub fn is_gateway_frame(frame: &[u8]) -> bool {
    frame.starts_with(MAGIC)
}

fn invalid(message: &str) -> TransportError {
    TransportError::new(message)
}

fn valid_token(value: &[u8]) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .iter()
            .all(|byte| byte.is_ascii_alphanumeric() || *byte == b'-')
}

fn lengths(header: &[u8]) -> (usize, usize, usize) {
    (
        u16::from_be_bytes([header[9], header[10]]) as usize,
        u16::from_be_bytes([header[11], header[12]]) as usize,
        u32::from_be_bytes([header[13], header[14], header[15], header[16]]) as usize,
    )
}

/// Validate before allocating or reading the rest of a binary stream frame.
pub fn frame_length(header: &[u8]) -> Result<usize, TransportError> {
    if header.len() < HEADER_LEN || !is_gateway_frame(header) {
        return Err(invalid("invalid binary gateway header"));
    }
    let (id, transport, payload) = lengths(header);
    if id > 128 || transport > 128 || payload > MAX_GATEWAY_PAYLOAD_BYTES {
        return Err(invalid("gateway frame exceeds field or payload limit"));
    }
    match header[8] {
        REQUEST if id > 0 && transport > 0 => {}
        RESPONSE if id == 0 && transport == 0 => {}
        NONE | ERROR | PING | PONG if id == 0 && transport == 0 && payload == 0 => {}
        _ => return Err(invalid("invalid gateway frame kind or lengths")),
    }
    Ok(HEADER_LEN + id + transport + payload)
}

fn header(kind: u8, connection_id: &str, transport: &str, payload: usize) -> Bytes {
    let mut header = BytesMut::with_capacity(HEADER_LEN + connection_id.len() + transport.len());
    header.extend_from_slice(MAGIC);
    header.put_u8(kind);
    header.put_u16(connection_id.len() as u16);
    header.put_u16(transport.len() as u16);
    header.put_u32(payload as u32);
    header.extend_from_slice(connection_id.as_bytes());
    header.extend_from_slice(transport.as_bytes());
    header.freeze()
}

pub fn encode_request(
    payload: &Bytes,
    connection_id: &str,
    source_transport: &str,
) -> Result<TransportMessage, TransportError> {
    if !valid_token(connection_id.as_bytes()) || !valid_token(source_transport.as_bytes()) {
        return Err(invalid("invalid gateway metadata token"));
    }
    if payload.len() > MAX_GATEWAY_PAYLOAD_BYTES {
        return Err(invalid("gateway payload exceeds frame limit"));
    }
    Ok(TransportMessage {
        header: header(REQUEST, connection_id, source_transport, payload.len()),
        payload: payload.clone(),
        binary: true,
    })
}

fn validate_frame(frame: &[u8]) -> Result<(), TransportError> {
    if frame_length(frame)? != frame.len() {
        return Err(invalid("gateway frame length mismatch"));
    }
    Ok(())
}

/// Consume the carrier buffer and retain the response as a shared slice.
pub fn decode_response(frame: Bytes) -> Result<Option<Bytes>, TransportError> {
    validate_frame(&frame)?;
    match frame[8] {
        RESPONSE => Ok(Some(frame.slice(HEADER_LEN..))),
        NONE => Ok(None),
        ERROR => Err(invalid("backend rejected gateway frame or response size")),
        _ => Err(invalid("unexpected gateway response kind")),
    }
}

pub struct GatewayBackendProtocol {
    inner: Arc<dyn JsonRpcProtocol>,
}

impl GatewayBackendProtocol {
    pub fn new(inner: Arc<dyn JsonRpcProtocol>) -> Self {
        Self { inner }
    }

    async fn dispatch(
        &self,
        input: &[u8],
        context: Option<&TransportContext>,
    ) -> Option<TransportMessage> {
        if !is_gateway_frame(input) {
            // Reject the retired ASCII protocol instead of interpreting it as JSON.
            if input.starts_with(b"JRPC-GW/2") {
                tracing::warn!(
                    "retired gateway framing version; upgrade gateway and backend together"
                );
                return Some(TransportMessage::binary(Bytes::from_static(GATEWAY_ERROR)));
            }
            return match context {
                Some(context) => self.inner.handle_message_with_context(input, context).await,
                None => self.inner.handle_message(input).await,
            }
            .map(TransportMessage::from);
        }
        let decoded = (|| {
            validate_frame(input)?;
            if input[8] == PING {
                return Ok(None);
            }
            if input[8] != REQUEST {
                return Err(invalid("expected gateway request"));
            }
            let (id_len, transport_len, _) = lengths(input);
            let id = &input[HEADER_LEN..HEADER_LEN + id_len];
            let transport = &input[HEADER_LEN + id_len..HEADER_LEN + id_len + transport_len];
            if !valid_token(id) || !valid_token(transport) {
                return Err(invalid("invalid gateway metadata token"));
            }
            let context = TransportContext {
                connection_id: std::str::from_utf8(id)
                    .map_err(|_| invalid("invalid gateway connection ID"))?
                    .to_owned(),
                transport: std::str::from_utf8(transport)
                    .map_err(|_| invalid("invalid gateway source transport"))?
                    .to_owned(),
                push_sender: None,
            };
            Ok(Some((
                context,
                &input[HEADER_LEN + id_len + transport_len..],
            )))
        })();
        let (context, payload) = match decoded {
            Ok(None) => return Some(TransportMessage::binary(Bytes::from_static(GATEWAY_PONG))),
            Ok(Some(decoded)) => decoded,
            Err(error) => {
                tracing::warn!(%error, "invalid gateway backend frame");
                return Some(TransportMessage::binary(Bytes::from_static(GATEWAY_ERROR)));
            }
        };
        match self
            .inner
            .handle_message_with_context(payload, &context)
            .await
        {
            None => Some(TransportMessage::binary(Bytes::from_static(GATEWAY_NONE))),
            Some(payload) if payload.len() <= MAX_GATEWAY_PAYLOAD_BYTES => Some(TransportMessage {
                header: header(RESPONSE, "", "", payload.len()),
                payload,
                binary: true,
            }),
            Some(_) => {
                tracing::error!("backend response exceeds gateway frame limit");
                Some(TransportMessage::binary(Bytes::from_static(GATEWAY_ERROR)))
            }
        }
    }
}

impl JsonRpcProtocol for GatewayBackendProtocol {
    fn stream_framing(&self) -> Option<&dyn json_rpc_2_transport::StreamFraming> {
        Some(&GatewayFraming)
    }

    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.dispatch(input, None)
                .await
                .map(TransportMessage::into_bytes)
        })
    }

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        Box::pin(async move {
            self.dispatch(input, Some(context))
                .await
                .map(TransportMessage::into_bytes)
        })
    }

    fn handle_transport_message<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolMessageFuture<'a> {
        Box::pin(self.dispatch(input, Some(context)))
    }

    fn session_opened(&self, context: &TransportContext) {
        self.inner.session_opened(context);
    }

    fn session_closed(&self, context: &TransportContext) {
        self.inner.session_closed(context);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Echo;
    impl JsonRpcProtocol for Echo {
        fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
            Box::pin(async move { (!input.is_empty()).then(|| Bytes::copy_from_slice(input)) })
        }

        fn handle_message_with_context<'a>(
            &'a self,
            input: &'a [u8],
            context: &'a TransportContext,
        ) -> ProtocolFuture<'a> {
            assert_eq!(context.connection_id, "client-1");
            assert_eq!(context.transport, "ws-tcp");
            assert!(context.push_sender.is_none());
            self.handle_message(input)
        }
    }

    #[tokio::test]
    async fn preserves_binary_multiline_and_metadata() {
        let protocol = GatewayBackendProtocol::new(Arc::new(Echo));
        let payload = Bytes::from_static(b"\x00\xff\nunchanged");
        let request = encode_request(&payload, "client-1", "ws-tcp").unwrap();
        assert_eq!(request.payload.as_ptr(), payload.as_ptr());
        assert_eq!(request.len(), HEADER_LEN + 8 + 6 + payload.len());
        let response = protocol
            .handle_message(&request.into_bytes())
            .await
            .unwrap();
        let pointer = response.as_ptr();
        let decoded = decode_response(response).unwrap().unwrap();
        assert_eq!(decoded.as_ptr(), pointer.wrapping_add(HEADER_LEN));
        assert_eq!(decoded, payload);
    }

    #[tokio::test]
    async fn acknowledges_notifications_and_preserves_direct_clients() {
        let protocol = GatewayBackendProtocol::new(Arc::new(Echo));
        let request = encode_request(&Bytes::new(), "client-1", "ws-tcp")
            .unwrap()
            .into_bytes();
        let response = protocol.handle_message(&request).await.unwrap();
        assert_eq!(decode_response(response).unwrap(), None);
        assert_eq!(
            protocol.handle_message(b"direct").await.unwrap(),
            b"direct".as_slice()
        );
    }

    #[tokio::test]
    async fn response_envelope_does_not_copy_backend_payload() {
        struct Fixed(Bytes);
        impl JsonRpcProtocol for Fixed {
            fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
                Box::pin(async move { Some(self.0.clone()) })
            }
        }
        let payload = Bytes::from_static(b"backend-response");
        let protocol = GatewayBackendProtocol::new(Arc::new(Fixed(payload.clone())));
        let request = encode_request(&Bytes::new(), "client-1", "ws-tcp")
            .unwrap()
            .into_bytes();
        let context = TransportContext {
            connection_id: "physical".into(),
            transport: "tcp".into(),
            push_sender: None,
        };
        let response = protocol
            .handle_transport_message(&request, &context)
            .await
            .unwrap();
        assert_eq!(response.payload.as_ptr(), payload.as_ptr());
    }

    #[tokio::test]
    async fn rejects_malformed_lengths_kinds_and_metadata() {
        let protocol = GatewayBackendProtocol::new(Arc::new(Echo));
        let valid = encode_request(&Bytes::from_static(b"x"), "client-1", "ws-tcp")
            .unwrap()
            .into_bytes();
        let mut cases = vec![valid.slice(..valid.len() - 1), Bytes::from_static(MAGIC)];
        for (index, value) in [(8, 99), (9, 1), (13, 255), (HEADER_LEN, b'\n')] {
            let mut bytes = valid.to_vec();
            bytes[index] = value;
            cases.push(bytes.into());
        }
        for request in cases {
            let response = protocol.handle_message(&request).await.unwrap();
            assert!(decode_response(response).is_err());
        }
        for token in ["", "bad\nid", "bad id", &"a".repeat(129)] {
            assert!(encode_request(&Bytes::new(), token, "tcp").is_err());
        }
        assert!(decode_response(Bytes::from_static(GATEWAY_PONG)).is_err());
        assert!(decode_response(Bytes::from_static(GATEWAY_NONE).slice(..16)).is_err());
        assert_eq!(frame_length(GATEWAY_PING).unwrap(), HEADER_LEN);
    }

    #[test]
    fn full_size_payload_has_only_header_overhead() {
        let payload = Bytes::from(vec![0; MAX_GATEWAY_PAYLOAD_BYTES]);
        let frame = encode_request(&payload, &"a".repeat(128), &"b".repeat(128)).unwrap();
        assert_eq!(frame.len(), json_rpc_2_core::MAX_MESSAGE_BYTES);
        assert_eq!(frame.payload.as_ptr(), payload.as_ptr());
        assert!(
            encode_request(
                &Bytes::from(vec![0; MAX_GATEWAY_PAYLOAD_BYTES + 1]),
                "a",
                "tcp"
            )
            .is_err()
        );
    }
}

#[cfg(test)]
mod stream_tests;

#[cfg(test)]
mod stream_codec_tests;
