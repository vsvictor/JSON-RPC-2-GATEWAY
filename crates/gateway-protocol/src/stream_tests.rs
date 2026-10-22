use super::*;
use bytes::BytesMut;
use json_rpc_2_tcp::client::transports::tcp::{read_frame, write_message};
use json_rpc_2_transport::{CONTROL_PING, CONTROL_PONG, CancellationToken};
struct Echo;
impl JsonRpcProtocol for Echo {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move { Some(Bytes::copy_from_slice(input)) })
    }
}
#[tokio::test]
async fn binary_exchanges_can_be_followed_by_legacy_control_lines() {
    for kind in ["tcp", "tls"] {
        let (mut client, server) = tokio::io::duplex(256);
        let (reader, writer) = tokio::io::split(server);
        let shutdown = CancellationToken::new();
        let task = tokio::spawn(json_rpc_2_tcp::server::transports::tcp::serve_stream(
            reader,
            writer,
            Arc::new(GatewayBackendProtocol::new(Arc::new(Echo))),
            shutdown.clone(),
            "127.0.0.1:12345".parse().unwrap(),
            kind,
        ));
        let payload = Bytes::from_static(b"\xffopaque\npayload\r\n\0");
        let request = encode_request(&payload, "client-1", kind).unwrap();
        write_message(&mut client, &request, Some(&GatewayFraming))
            .await
            .unwrap();
        let mut buffer = BytesMut::new();
        let response = read_frame(&mut client, &mut buffer, Some(&GatewayFraming))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(decode_response(response).unwrap().unwrap(), payload);
        write_message(
            &mut client,
            &Bytes::from_static(CONTROL_PING).into(),
            Some(&GatewayFraming),
        )
        .await
        .unwrap();
        assert_eq!(
            read_frame(&mut client, &mut buffer, Some(&GatewayFraming))
                .await
                .unwrap()
                .unwrap(),
            CONTROL_PONG
        );
        shutdown.cancel();
        task.await.unwrap().unwrap();
    }
}
