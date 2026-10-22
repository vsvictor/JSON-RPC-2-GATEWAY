//! First-record application selection for a shared native SCTP or DCCP listener.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use json_rpc_2_transport::{
    JsonRpcProtocol, ProtocolFuture, ProtocolMessageFuture, TransportContext,
};

/// Share one native association listener between raw JSON-RPC and WSU2 framing.
///
/// Pass only enabled protocols. The WebSocket argument must already be adapted
/// with `protocol_adapter_with_kind` for `ws-sctp` or `ws-dccp`. Selection is
/// sticky until the native session closes; neither JSON nor WS payloads are
/// parsed here. Native session IDs and push capabilities are retained.
pub(crate) fn protocol_adapter(
    raw: Option<Arc<dyn JsonRpcProtocol>>,
    websocket: Option<Arc<dyn JsonRpcProtocol>>,
) -> Arc<dyn JsonRpcProtocol> {
    Arc::new(SharedAssociationProtocol {
        raw,
        websocket,
        sessions: Mutex::new(HashMap::new()),
    })
}

struct SharedAssociationProtocol {
    raw: Option<Arc<dyn JsonRpcProtocol>>,
    websocket: Option<Arc<dyn JsonRpcProtocol>>,
    sessions: Mutex<HashMap<String, Association>>,
}

struct Association {
    context: TransportContext,
    selection: Selection,
}

enum Selection {
    Pending,
    Selected(Arc<dyn JsonRpcProtocol>),
    Rejected,
}

impl SharedAssociationProtocol {
    fn select(
        &self,
        input: &[u8],
        context: &TransportContext,
    ) -> Option<(Arc<dyn JsonRpcProtocol>, TransportContext)> {
        let mut sessions = self.sessions.lock().unwrap();
        // Only session_opened registers associations. In particular, an in-flight
        // future cannot recreate a session after its native guard has closed it.
        let session = sessions.get_mut(&context.connection_id)?;
        if matches!(session.selection, Selection::Pending) {
            let protocol = if input.starts_with(b"WSU2") {
                self.websocket.clone()
            } else {
                self.raw.clone()
            };
            session.selection = match protocol {
                Some(protocol) => {
                    // Serialize opening against selection and closing, including
                    // callbacks that establish an adapted WS push relay.
                    protocol.session_opened(&session.context);
                    Selection::Selected(protocol)
                }
                None => {
                    json_rpc_2_transport::log_transport_error(
                        "shared association",
                        &context.connection_id,
                        "protocol selection",
                        &"disabled wire profile",
                    );
                    Selection::Rejected
                }
            };
        }
        match &session.selection {
            Selection::Selected(protocol) => Some((protocol.clone(), session.context.clone())),
            Selection::Pending | Selection::Rejected => None,
        }
    }
}

impl JsonRpcProtocol for SharedAssociationProtocol {
    fn handle_message<'a>(&'a self, input: &'a [u8]) -> ProtocolFuture<'a> {
        Box::pin(async move {
            let protocol = if input.starts_with(b"WSU2") {
                self.websocket.as_ref()
            } else {
                self.raw.as_ref()
            }?;
            protocol.handle_message(input).await
        })
    }

    fn handle_message_with_context<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolFuture<'a> {
        Box::pin(async move {
            let (protocol, context) = self.select(input, context)?;
            protocol.handle_message_with_context(input, &context).await
        })
    }

    fn handle_transport_message<'a>(
        &'a self,
        input: &'a [u8],
        context: &'a TransportContext,
    ) -> ProtocolMessageFuture<'a> {
        Box::pin(async move {
            let (protocol, context) = self.select(input, context)?;
            protocol.handle_transport_message(input, &context).await
        })
    }

    fn handle_owned_message<'a>(
        &'a self,
        input: Bytes,
        context: &'a TransportContext,
    ) -> ProtocolMessageFuture<'a> {
        Box::pin(async move {
            let (protocol, context) = self.select(&input, context)?;
            protocol.handle_owned_message(input, &context).await
        })
    }

    fn session_opened(&self, context: &TransportContext) {
        self.sessions
            .lock()
            .unwrap()
            .entry(context.connection_id.clone())
            .or_insert_with(|| Association {
                context: context.clone(),
                selection: Selection::Pending,
            });
    }

    fn session_closed(&self, context: &TransportContext) {
        let mut sessions = self.sessions.lock().unwrap();
        if let Some(session) = sessions.remove(&context.connection_id)
            && let Selection::Selected(protocol) = session.selection
        {
            protocol.session_closed(&session.context);
        }
    }

    fn session_close_requested(&self, context: &TransportContext) -> bool {
        let sessions = self.sessions.lock().unwrap();
        match sessions.get(&context.connection_id) {
            Some(Association {
                context,
                selection: Selection::Selected(protocol),
            }) => protocol.session_close_requested(context),
            Some(Association {
                selection: Selection::Pending,
                ..
            }) => false,
            Some(Association {
                selection: Selection::Rejected,
                ..
            })
            | None => true,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use json_rpc_2_transport::TransportSession;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Default)]
    struct Recorder {
        opened: AtomicUsize,
        closed: AtomicUsize,
        messages: Mutex<Vec<(Bytes, TransportContext)>>,
    }

    impl JsonRpcProtocol for Recorder {
        fn handle_message<'a>(&'a self, _: &'a [u8]) -> ProtocolFuture<'a> {
            panic!("context must be preserved")
        }

        fn handle_message_with_context<'a>(
            &'a self,
            input: &'a [u8],
            context: &'a TransportContext,
        ) -> ProtocolFuture<'a> {
            Box::pin(async move {
                self.messages
                    .lock()
                    .unwrap()
                    .push((Bytes::copy_from_slice(input), context.clone()));
                Some(Bytes::copy_from_slice(input))
            })
        }

        fn session_opened(&self, _: &TransportContext) {
            self.opened.fetch_add(1, Ordering::SeqCst);
        }

        fn session_closed(&self, _: &TransportContext) {
            self.closed.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn first_technical_prefix_selects_once_without_json_parsing() {
        for first in [&b"not JSON"[..], &b"WSU2invalid frame"[..]] {
            let raw = Arc::new(Recorder::default());
            let ws = Arc::new(Recorder::default());
            let protocol = protocol_adapter(Some(raw.clone()), Some(ws.clone()));
            let session = TransportSession::new("sctp", true);
            protocol.session_opened(&session.context);
            protocol.session_opened(&session.context);
            let (selected, other) = if first.starts_with(b"WSU2") {
                (&ws, &raw)
            } else {
                (&raw, &ws)
            };
            assert_eq!(selected.opened.load(Ordering::SeqCst), 0);
            for input in [first, b"WSU2second", b"raw second"] {
                assert_eq!(
                    protocol
                        .handle_message_with_context(input, &session.context)
                        .await
                        .unwrap(),
                    input
                );
            }
            assert_eq!(selected.opened.load(Ordering::SeqCst), 1);
            assert_eq!(other.opened.load(Ordering::SeqCst), 0);
            for (_, context) in selected.messages.lock().unwrap().iter() {
                assert_eq!(context.connection_id, session.context.connection_id);
                assert_eq!(context.transport, "sctp");
                assert!(context.push_sender.is_some());
            }
            protocol.session_closed(&session.context);
            protocol.session_closed(&session.context);
            assert_eq!(selected.closed.load(Ordering::SeqCst), 1);
            assert_eq!(other.closed.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn disabled_kinds_are_rejected_without_opening_another_protocol() {
        for (raw_enabled, first) in [(true, &b"WSU2"[..]), (false, &b"{}"[..])] {
            let selected = Arc::new(Recorder::default());
            let protocol = if raw_enabled {
                protocol_adapter(Some(selected.clone()), None)
            } else {
                protocol_adapter(None, Some(selected.clone()))
            };
            let session = TransportSession::new("dccp", false);
            protocol.session_opened(&session.context);
            assert!(!protocol.session_close_requested(&session.context));
            assert!(
                protocol
                    .handle_message_with_context(first, &session.context)
                    .await
                    .is_none()
            );
            assert!(protocol.session_close_requested(&session.context));
            protocol.session_closed(&session.context);
            assert_eq!(selected.opened.load(Ordering::SeqCst), 0);
            assert_eq!(selected.closed.load(Ordering::SeqCst), 0);
        }
    }

    #[tokio::test]
    async fn closing_pending_or_selected_sessions_prevents_late_recreation() {
        for selected in [false, true] {
            let raw = Arc::new(Recorder::default());
            let protocol = protocol_adapter(Some(raw.clone()), None);
            let session = TransportSession::new("dccp", false);
            protocol.session_opened(&session.context);
            if selected {
                protocol
                    .handle_message_with_context(b"first", &session.context)
                    .await;
            }
            let late = protocol.handle_owned_message(Bytes::from_static(b"late"), &session.context);
            protocol.session_closed(&session.context);
            assert!(late.await.is_none());
            assert!(
                protocol
                    .handle_transport_message(b"later", &session.context)
                    .await
                    .is_none()
            );
            assert!(protocol.session_close_requested(&session.context));
            assert_eq!(raw.opened.load(Ordering::SeqCst), usize::from(selected));
            assert_eq!(raw.closed.load(Ordering::SeqCst), usize::from(selected));
            assert_eq!(raw.messages.lock().unwrap().len(), usize::from(selected));
            assert!(session.context.push_sender.is_none());
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_first_records_open_the_selected_protocol_exactly_once() {
        let raw = Arc::new(Recorder::default());
        let ws = Arc::new(Recorder::default());
        let protocol = protocol_adapter(Some(raw.clone()), Some(ws.clone()));
        let session = TransportSession::new("sctp", true);
        protocol.session_opened(&session.context);
        let barrier = Arc::new(tokio::sync::Barrier::new(16));
        let mut tasks = Vec::new();
        for index in 0..16 {
            let protocol = protocol.clone();
            let context = session.context.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                let input = if index % 2 == 0 { &b"WSU2"[..] } else { b"raw" };
                protocol.handle_message_with_context(input, &context).await
            }));
        }
        for task in tasks {
            assert!(task.await.unwrap().is_some());
        }
        assert_eq!(
            raw.opened.load(Ordering::SeqCst) + ws.opened.load(Ordering::SeqCst),
            1
        );
        let raw_count = raw.messages.lock().unwrap().len();
        let ws_count = ws.messages.lock().unwrap().len();
        assert!(matches!((raw_count, ws_count), (16, 0) | (0, 16)));
        protocol.session_closed(&session.context);
        assert_eq!(
            raw.closed.load(Ordering::SeqCst) + ws.closed.load(Ordering::SeqCst),
            1
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn concurrent_first_records_and_close_do_not_duplicate_lifecycle() {
        let raw = Arc::new(Recorder::default());
        let protocol = protocol_adapter(Some(raw.clone()), None);
        let session = TransportSession::new("sctp", true);
        protocol.session_opened(&session.context);
        let barrier = Arc::new(tokio::sync::Barrier::new(17));
        let mut tasks = Vec::new();
        for _ in 0..16 {
            let protocol = protocol.clone();
            let context = session.context.clone();
            let barrier = barrier.clone();
            tasks.push(tokio::spawn(async move {
                barrier.wait().await;
                protocol
                    .handle_message_with_context(b"first", &context)
                    .await;
            }));
        }
        barrier.wait().await;
        protocol.session_closed(&session.context);
        for task in tasks {
            task.await.unwrap();
        }
        let opened = raw.opened.load(Ordering::SeqCst);
        assert!(opened <= 1);
        assert_eq!(raw.closed.load(Ordering::SeqCst), opened);
        assert!(
            protocol
                .handle_message_with_context(b"late", &session.context)
                .await
                .is_none()
        );
        assert_eq!(raw.opened.load(Ordering::SeqCst), opened);
    }
}
