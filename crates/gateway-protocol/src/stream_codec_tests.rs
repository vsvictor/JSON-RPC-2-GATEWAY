use super::*;
use bytes::BytesMut;
use json_rpc_2_tcp::client::transports::tcp::{read_frame, write_message};
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncWrite, AsyncWriteExt};

#[tokio::test]
async fn coalesced_binary_and_line_reads_preserve_buffer_pointer() {
    let mut buffer = BytesMut::with_capacity(256);
    buffer.extend_from_slice(GATEWAY_PING);
    buffer.extend_from_slice(GATEWAY_PONG);
    buffer.extend_from_slice(b"pong\r\n");
    let pointer = buffer.as_ptr();
    let mut reader = tokio::io::empty();
    let ping = read_frame(&mut reader, &mut buffer, Some(&GatewayFraming))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(ping.as_ptr(), pointer);
    assert_eq!(ping, GATEWAY_PING);
    assert_eq!(
        read_frame(&mut reader, &mut buffer, Some(&GatewayFraming))
            .await
            .unwrap()
            .unwrap(),
        GATEWAY_PONG
    );
    assert_eq!(
        read_frame(&mut reader, &mut buffer, Some(&GatewayFraming))
            .await
            .unwrap()
            .unwrap(),
        b"pong".as_slice()
    );
    assert!(
        read_frame(&mut reader, &mut buffer, Some(&GatewayFraming))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn partial_binary_read_survives_cancellation() {
    let (mut writer, mut reader) = tokio::io::duplex(64);
    let mut buffer = BytesMut::new();
    for byte in &GATEWAY_PING[..GATEWAY_PING.len() - 1] {
        writer.write_all(&[*byte]).await.unwrap();
        tokio::select! {
            result = read_frame(&mut reader, &mut buffer, Some(&GatewayFraming)) => {
                panic!("incomplete frame was accepted: {result:?}");
            }
            _ = tokio::task::yield_now() => {}
        }
    }
    writer
        .write_all(&GATEWAY_PING[GATEWAY_PING.len() - 1..])
        .await
        .unwrap();
    assert_eq!(
        read_frame(&mut reader, &mut buffer, Some(&GatewayFraming))
            .await
            .unwrap()
            .unwrap(),
        GATEWAY_PING
    );
}

#[tokio::test]
async fn opaque_payload_is_not_treated_as_lines_or_utf8() {
    let payload = Bytes::from_static(b"\xfffirst\nsecond\r\n\0\xfe");
    let message = encode_request(&payload, "client-1", "tcp").unwrap();
    assert_eq!(message.payload.as_ptr(), payload.as_ptr());
    let mut writer = ShortWriter(Vec::new());
    write_message(&mut writer, &message, Some(&GatewayFraming))
        .await
        .unwrap();
    assert_eq!(writer.0.len(), message.len());
    assert_eq!(&writer.0[message.header.len()..], payload.as_ref());
    let mut reader = writer.0.as_slice();
    let mut buffer = BytesMut::new();
    let response = read_frame(&mut reader, &mut buffer, Some(&GatewayFraming))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(&response[message.header.len()..], payload.as_ref());
    assert!(
        read_frame(&mut reader, &mut buffer, Some(&GatewayFraming))
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn oversized_header_is_rejected_before_payload_reads() {
    let payload = Bytes::from_static(b"test");
    let message = encode_request(&payload, "client-1", "tcp").unwrap();
    let mut buffer = BytesMut::from(message.header.as_ref());
    buffer[13..17].copy_from_slice(&u32::MAX.to_be_bytes());
    let length = buffer.len();
    let error = read_frame(&mut tokio::io::empty(), &mut buffer, Some(&GatewayFraming))
        .await
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    assert_eq!(buffer.len(), length);
}

struct ShortWriter(Vec<u8>);

impl AsyncWrite for ShortWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        input: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        let count = input.len().min(3);
        self.0.extend_from_slice(&input[..count]);
        Poll::Ready(Ok(count))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        input: &[std::io::IoSlice<'_>],
    ) -> Poll<std::io::Result<usize>> {
        let mut count = 0;
        for slice in input {
            let length = slice.len().min(3 - count);
            self.0.extend_from_slice(&slice[..length]);
            count += length;
            if count == 3 {
                break;
            }
        }
        Poll::Ready(Ok(count))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn short_vectored_writes_do_not_append_binary_newlines() {
    let mut writer = ShortWriter(Vec::new());
    write_message(
        &mut writer,
        &TransportMessage::binary(Bytes::from_static(GATEWAY_PING)),
        Some(&GatewayFraming),
    )
    .await
    .unwrap();
    assert_eq!(writer.0, GATEWAY_PING);
    write_message(
        &mut writer,
        &Bytes::from_static(b"{}").into(),
        Some(&GatewayFraming),
    )
    .await
    .unwrap();
    assert_eq!(&writer.0[GATEWAY_PING.len()..], b"{}\n");
}
