use super::*;
use json_rpc_2_transport::{JsonRpcProtocol, ProtocolFuture, TransportContext};
use std::sync::{
    Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Default)]
struct Gate {
    generation: Mutex<CancellationToken>,
    opened: tokio::sync::Notify,
    closed: tokio::sync::Notify,
    calls: AtomicUsize,
}

impl JsonRpcProtocol for Gate {
    fn handle_message<'a>(&'a self, payload: &'a [u8]) -> ProtocolFuture<'a> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        Box::pin(async move { Some(Bytes::copy_from_slice(payload)) })
    }

    fn session_opened(&self, _: &TransportContext) {
        self.opened.notify_one();
    }

    fn session_closed(&self, _: &TransportContext) {
        self.closed.notify_one();
    }

    fn session_close_requested(&self, _: &TransportContext) -> bool {
        self.generation.lock().unwrap().is_cancelled()
    }

    fn session_cancelled<'a>(
        &'a self,
        _: &'a TransportContext,
    ) -> std::pin::Pin<Box<dyn Future<Output = ()> + Send + 'a>> {
        let token = self.generation.lock().unwrap().clone();
        Box::pin(async move { token.cancelled().await })
    }
}

fn message(identity: &SessionIdentity, kind: MessageKind) -> Message {
    Message {
        payload: Bytes::from_static(b"request"),
        reply_to: Some("test-replies".into()),
        correlation_id: "test-request".into(),
        control: false,
        metadata: Metadata {
            session: Some(identity.clone()),
            kind,
        },
        #[cfg(feature = "amqp")]
        ack: None,
    }
}

#[tokio::test]
async fn broker_policy_closes_idle_sessions_and_rejects_retired_identities_after_reenable() {
    tokio::time::timeout(Duration::from_secs(5), async {
        let configs = [
            #[cfg(feature = "mqtt")]
            Config::Mqtt(MqttConfig::default()),
            #[cfg(feature = "amqp")]
            Config::Amqp(AmqpConfig::default()),
        ];
        for config in configs {
            let gate = Arc::new(Gate::default());
            let (incoming, receiver) = mpsc::channel(16);
            let (published, mut events) = mpsc::unbounded_channel();
            let stop = CancellationToken::new();
            let task = tokio::spawn(serve(
                config,
                Some(Prepared {
                    session: Session::Test(published),
                    incoming: receiver,
                }),
                gate.clone(),
                stop.clone(),
                Arc::new(json_rpc_2_transport::TransportRuntime::default()),
            ));
            let identity = SessionIdentity::generate("test-push").unwrap();
            incoming
                .send(message(&identity, MessageKind::Open))
                .await
                .unwrap();
            gate.opened.notified().await;
            incoming
                .send(message(&identity, MessageKind::Rpc))
                .await
                .unwrap();
            assert_eq!(events.recv().await.unwrap().1, b"request"[..]);
            gate.generation.lock().unwrap().cancel();
            gate.closed.notified().await;
            let (route, _, metadata) = events.recv().await.unwrap();
            assert_eq!(route, identity.push_to);
            assert_eq!(metadata.kind, MessageKind::Close);
            incoming
                .send(message(&identity, MessageKind::Rpc))
                .await
                .unwrap();
            assert_eq!(events.recv().await.unwrap().2.kind, MessageKind::Close);
            assert_eq!(gate.calls.load(Ordering::SeqCst), 1);
            *gate.generation.lock().unwrap() = CancellationToken::new();
            incoming
                .send(message(&identity, MessageKind::Open))
                .await
                .unwrap();
            incoming
                .send(message(&identity, MessageKind::Rpc))
                .await
                .unwrap();
            let fresh = SessionIdentity::generate("fresh-push").unwrap();
            incoming
                .send(message(&fresh, MessageKind::Open))
                .await
                .unwrap();
            gate.opened.notified().await;
            incoming
                .send(message(&fresh, MessageKind::Rpc))
                .await
                .unwrap();
            assert_eq!(events.recv().await.unwrap().1, b"request"[..]);
            assert_eq!(
                gate.calls.load(Ordering::SeqCst),
                2,
                "retired broker session must not resume"
            );
            stop.cancel();
            task.await.unwrap().unwrap();
        }
    })
    .await
    .expect("broker policy lifecycle timed out");
}
