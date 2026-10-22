use crate::ingress::shared_association;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use json_rpc_2_core::{WebSocketMessage, decode_websocket_message, encode_websocket_message};
use json_rpc_2_transport::{JsonRpcProtocol, TransportContext, TransportSession};
use json_rpc_2_websockets::server::transports::{
    WebSocketFuture, WebSocketProtocol, protocol_adapter_with_kind,
};

#[derive(Default)]
struct WebSocketRecorder {
    opened: Mutex<Vec<TransportContext>>,
    closed: Mutex<Vec<TransportContext>>,
    messages: Mutex<Vec<TransportContext>>,
}

impl WebSocketProtocol for WebSocketRecorder {
    fn handle_message<'a>(&'a self, _: WebSocketMessage) -> WebSocketFuture<'a> {
        panic!("association context must be retained")
    }

    fn handle_message_with_context<'a>(
        &'a self,
        message: WebSocketMessage,
        context: &'a TransportContext,
    ) -> WebSocketFuture<'a> {
        Box::pin(async move {
            self.messages.lock().unwrap().push(context.clone());
            Some(message)
        })
    }

    fn session_opened(&self, context: &TransportContext) {
        self.opened.lock().unwrap().push(context.clone());
    }

    fn session_closed(&self, context: &TransportContext) {
        self.closed.lock().unwrap().push(context.clone());
    }
}

#[tokio::test]
async fn adapted_context_push_and_close_follow_the_native_association() {
    for (native, kind, push_enabled) in [("sctp", "ws-sctp", true), ("dccp", "ws-dccp", false)] {
        let ws = Arc::new(WebSocketRecorder::default());
        let protocol = shared_association::protocol_adapter(
            None,
            Some(protocol_adapter_with_kind(ws.clone(), kind)),
        );
        let mut session = TransportSession::new(native, push_enabled);
        protocol.session_opened(&session.context);
        let message = WebSocketMessage::Text("no JSON needed".into());
        let frame = encode_websocket_message(&message).unwrap();
        for _ in 0..2 {
            let response = protocol
                .handle_message_with_context(&frame, &session.context)
                .await
                .unwrap();
            assert_eq!(
                decode_websocket_message(&response).unwrap(),
                Some(message.clone())
            );
        }
        assert_eq!(ws.opened.lock().unwrap().len(), 1);
        let translated = ws.opened.lock().unwrap()[0].clone();
        assert_eq!(translated.transport, kind);
        assert_eq!(translated.connection_id, session.context.connection_id);
        assert_eq!(translated.push_sender.is_some(), push_enabled);
        for context in ws.messages.lock().unwrap().iter() {
            assert_eq!(context.transport, kind);
            assert_eq!(context.connection_id, translated.connection_id);
        }
        if let Some(sender) = &translated.push_sender {
            sender.send(Bytes::from_static(b"push")).unwrap();
            let pushed = tokio::time::timeout(Duration::from_secs(2), session.push_receiver.recv())
                .await
                .unwrap()
                .unwrap();
            let pushed = decode_websocket_message(&pushed).unwrap().unwrap();
            assert_eq!(json_rpc_2_core::websocket_message_bytes(&pushed), b"push");
        }
        let close = json_rpc_2_core::encode_websocket_close_frame().unwrap();
        assert!(
            protocol
                .handle_message_with_context(&close, &session.context)
                .await
                .is_none()
        );
        assert!(protocol.session_close_requested(&session.context));
        protocol.session_closed(&session.context);
        protocol.session_closed(&session.context);
        assert_eq!(ws.closed.lock().unwrap().len(), 1);
        assert_eq!(
            ws.closed.lock().unwrap()[0].connection_id,
            translated.connection_id
        );
        assert!(
            protocol
                .handle_message_with_context(&frame, &session.context)
                .await
                .is_none()
        );
        assert_eq!(ws.opened.lock().unwrap().len(), 1);
        if let Some(sender) = &translated.push_sender {
            tokio::time::timeout(Duration::from_secs(2), sender.closed())
                .await
                .expect("WS push relay survived association close");
        }
    }
}

#[tokio::test]
#[ignore = "requires a kernel with native SCTP support"]
async fn raw_and_ws_sctp_share_one_native_listener() {
    use json_rpc_2_datagram::{
        client::transports::sctp::SctpClientTransport, server::transports::sctp::SctpTransport,
    };
    use json_rpc_2_transport::{CancellationToken, ProtocolFuture, RpcClientTransport, Transport};
    use json_rpc_2_websockets::client::transports::sctp::SctpWebSocketClientTransport;

    #[derive(Default)]
    struct RawRecorder(Mutex<Vec<TransportContext>>);

    impl JsonRpcProtocol for RawRecorder {
        fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
            panic!("native association must carry a context")
        }

        fn handle_message_with_context<'a>(
            &'a self,
            input: &'a [u8],
            context: &'a TransportContext,
        ) -> ProtocolFuture<'a> {
            Box::pin(async move {
                self.0.lock().unwrap().push(context.clone());
                Some(Bytes::copy_from_slice(input))
            })
        }
    }

    let raw = Arc::new(RawRecorder::default());
    let ws = Arc::new(WebSocketRecorder::default());
    let protocol = shared_association::protocol_adapter(
        Some(raw.clone()),
        Some(protocol_adapter_with_kind(ws.clone(), "ws-sctp")),
    );
    let listener = SctpTransport::bind("127.0.0.1:0".parse().unwrap()).unwrap();
    let address = listener.local_addr();
    let shutdown = CancellationToken::new();
    let server = tokio::spawn(listener.run(protocol, shutdown.clone()));
    let test = async {
        let raw_client = SctpClientTransport::connect(address).await.unwrap();
        let ws_client = SctpWebSocketClientTransport::connect(address)
            .await
            .unwrap();
        raw_client.transport_ping().await.unwrap();
        ws_client.transport_ping().await.unwrap();
        // The protocol choice is not derived from the JSON payload.
        for payload in [b"not JSON".as_slice(), b"another record"] {
            for client in [&raw_client as &dyn RpcClientTransport, &ws_client] {
                assert_eq!(
                    client
                        .send_with_expectation(Bytes::copy_from_slice(payload), true)
                        .await
                        .unwrap()
                        .unwrap(),
                    payload
                );
            }
        }
        {
            let contexts = raw.0.lock().unwrap();
            assert_eq!(contexts.len(), 2);
            assert_eq!(contexts[0].transport, "sctp");
            assert!(contexts[0].push_sender.is_some());
            assert_eq!(contexts[0].connection_id, contexts[1].connection_id);
            let ws_contexts = ws.messages.lock().unwrap();
            assert_eq!(ws_contexts.len(), 2);
            assert_eq!(ws_contexts[0].transport, "ws-sctp");
            assert!(ws_contexts[0].push_sender.is_some());
            assert_eq!(ws_contexts[0].connection_id, ws_contexts[1].connection_id);
            assert_ne!(contexts[0].connection_id, ws_contexts[0].connection_id);
        }
        raw_client.close().await.unwrap();
        ws_client.close().await.unwrap();
    };
    let result = tokio::time::timeout(Duration::from_secs(10), test).await;
    shutdown.cancel();
    tokio::time::timeout(Duration::from_secs(5), server)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    result.unwrap();
    assert_eq!(ws.opened.lock().unwrap().len(), 1);
    assert_eq!(ws.closed.lock().unwrap().len(), 1);
}
