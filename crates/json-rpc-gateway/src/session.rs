use std::sync::Arc;

use bytes::Bytes;
use dashmap::{DashMap, DashSet};
use json_rpc_2_runtime::TransportContext;
use tokio::sync::mpsc;

#[derive(Clone)]
struct ClientSession {
    transport: String,
    push_sender: Option<mpsc::UnboundedSender<Bytes>>,
}

#[derive(Clone)]
struct ClientRegistration {
    connection_id: String,
    groups: Vec<String>,
}

#[derive(Default)]
pub struct SessionRegistry {
    metadata: std::sync::Mutex<()>,
    sessions: DashMap<String, ClientSession>,
    clients: DashMap<String, ClientRegistration>,
    groups: DashMap<String, DashSet<String>>,
}

impl SessionRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    pub fn register_session(&self, context: &TransportContext) {
        let _metadata = self.metadata.lock().unwrap();
        tracing::info!(connection_id = %context.connection_id, transport = %context.transport,
            supports_push = context.push_sender.is_some(), "gateway session opened");
        self.sessions.insert(
            context.connection_id.clone(),
            ClientSession {
                transport: context.transport.to_owned(),
                push_sender: context.push_sender.clone(),
            },
        );
    }

    /// Stateless requests own their registration; persistent transports do not.
    pub fn register_temporary_session(&self, context: &TransportContext) -> bool {
        let _metadata = self.metadata.lock().unwrap();
        match self.sessions.entry(context.connection_id.clone()) {
            dashmap::mapref::entry::Entry::Occupied(_) => false,
            dashmap::mapref::entry::Entry::Vacant(entry) => {
                entry.insert(ClientSession {
                    transport: context.transport.clone(),
                    push_sender: context.push_sender.clone(),
                });
                true
            }
        }
    }

    pub fn register_client(
        &self,
        connection_id: &str,
        client_id: String,
        groups: Vec<String>,
    ) -> Result<(), String> {
        let _metadata = self.metadata.lock().unwrap();
        let Some(session) = self.sessions.get(connection_id) else {
            return Err("UnknownConnection".to_owned());
        };
        let transport = session.transport.clone();
        drop(session);

        if let Some(previous) = self.clients.insert(
            client_id.clone(),
            ClientRegistration {
                connection_id: connection_id.to_owned(),
                groups: groups.clone(),
            },
        ) {
            for group in previous.groups {
                self.remove_group_member(&group, &client_id);
            }
        }
        for group in groups {
            self.groups
                .entry(group)
                .or_default()
                .insert(client_id.clone());
        }
        tracing::info!(
            connection_id,
            client_id,
            transport,
            "registered gateway client"
        );
        Ok(())
    }

    pub fn push_unicast(&self, client_id: &str, payload: Bytes) -> Result<(), String> {
        let registration = self
            .clients
            .get(client_id)
            .map(|entry| entry.clone())
            .ok_or_else(|| "ClientNotFound".to_owned())?;
        self.send_to_connection(&registration.connection_id, payload)
    }

    pub fn push_multicast(&self, group_id: &str, payload: Bytes) -> Result<usize, String> {
        let members = self
            .groups
            .get(group_id)
            .map(|entry| {
                entry
                    .iter()
                    .map(|client| client.key().clone())
                    .collect::<Vec<_>>()
            })
            .ok_or_else(|| "GroupNotFound".to_owned())?;
        self.push_clients(members, payload)
    }

    pub fn push_broadcast(&self, payload: Bytes) -> Result<usize, String> {
        let connection_ids = self
            .sessions
            .iter()
            .filter(|entry| entry.push_sender.is_some())
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        let mut delivered = 0;
        for connection_id in connection_ids {
            self.send_to_connection(&connection_id, payload.clone())?;
            delivered += 1;
        }
        Ok(delivered)
    }

    fn push_clients(&self, clients: Vec<String>, payload: Bytes) -> Result<usize, String> {
        let mut delivered = 0;
        for client_id in clients {
            let registration = self
                .clients
                .get(&client_id)
                .map(|entry| entry.clone())
                .ok_or_else(|| "ClientNotFound".to_owned())?;
            self.send_to_connection(&registration.connection_id, payload.clone())?;
            delivered += 1;
        }
        Ok(delivered)
    }

    fn send_to_connection(&self, connection_id: &str, payload: Bytes) -> Result<(), String> {
        let session = self
            .sessions
            .get(connection_id)
            .map(|entry| entry.clone())
            .ok_or_else(|| "ClientNotFound".to_owned())?;
        let Some(sender) = session.push_sender else {
            return Err("PushNotSupportedForTransport".to_owned());
        };
        tracing::debug!(
            connection_id,
            transport = %session.transport,
            direction = "gateway_to_client",
            payload_bytes = payload.len(),
            payload = %String::from_utf8_lossy(&payload),
            "sending gateway push"
        );
        if sender.send(payload).is_err() {
            self.remove_session(connection_id);
            return Err("ClientDisconnected".to_owned());
        }
        Ok(())
    }

    pub fn remove_session(&self, connection_id: &str) {
        let _metadata = self.metadata.lock().unwrap();
        if self.sessions.remove(connection_id).is_some() {
            tracing::info!(connection_id, "gateway session closed");
        }
        let stale_clients = self
            .clients
            .iter()
            .filter(|entry| entry.connection_id == connection_id)
            .map(|entry| entry.key().clone())
            .collect::<Vec<_>>();
        for client_id in stale_clients {
            if let Some((_, registration)) = self.clients.remove(&client_id) {
                for group in registration.groups {
                    self.remove_group_member(&group, &client_id);
                }
            }
        }
    }

    fn remove_group_member(&self, group_id: &str, client_id: &str) {
        if let Some(members) = self.groups.get(group_id) {
            members.remove(client_id);
            let empty = members.is_empty();
            drop(members);
            if empty {
                self.groups.remove(group_id);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use json_rpc_2_runtime::TransportSession;

    use super::SessionRegistry;

    #[test]
    fn disconnect_cannot_leave_racing_client_or_group_registrations() {
        for _ in 0..32 {
            let registry = SessionRegistry::new();
            let context = TransportSession::stateless("http1");
            registry.register_session(&context);
            let barrier = std::sync::Arc::new(std::sync::Barrier::new(2));
            std::thread::scope(|scope| {
                let registry = &registry;
                let context = &context;
                let barrier = &barrier;
                scope.spawn(move || {
                    barrier.wait();
                    let result = registry.register_client(
                        &context.connection_id,
                        "user".into(),
                        vec!["group".into()],
                    );
                    assert!(result.is_ok() || result == Err("UnknownConnection".into()));
                });
                scope.spawn(move || {
                    barrier.wait();
                    registry.remove_session(&context.connection_id);
                });
            });
            assert!(registry.sessions.is_empty());
            assert!(registry.clients.is_empty());
            assert!(registry.groups.is_empty());
        }
    }

    #[test]
    fn old_disconnect_does_not_remove_new_client_binding() {
        let registry = SessionRegistry::new();
        let first = TransportSession::stateless("tcp");
        let second = TransportSession::stateless("tcp");
        registry.register_session(&first);
        registry.register_session(&second);
        registry
            .register_client(&first.connection_id, "user".into(), vec!["old".into()])
            .unwrap();
        registry
            .register_client(&second.connection_id, "user".into(), vec!["new".into()])
            .unwrap();
        registry.remove_session(&first.connection_id);
        assert_eq!(
            registry.clients.get("user").unwrap().connection_id,
            second.connection_id
        );
        assert!(!registry.groups.contains_key("old"));
        assert!(registry.groups.get("new").unwrap().contains("user"));
        registry.remove_session(&second.connection_id);
        assert!(registry.clients.is_empty());
        assert!(registry.groups.is_empty());
    }

    #[test]
    fn persistent_non_push_session_is_not_owned_by_an_individual_request() {
        let registry = SessionRegistry::new();
        let context = TransportSession::stateless("dccp");
        registry.register_session(&context);
        assert!(!registry.register_temporary_session(&context));
        registry.remove_session(&context.connection_id);
        assert!(registry.register_temporary_session(&context));
    }

    #[tokio::test]
    async fn unicast_push_uses_the_registered_session_channel() {
        let registry = SessionRegistry::new();
        let mut session = TransportSession::new("tcp", true);
        let connection_id = session.context.connection_id.clone();
        registry.register_session(&session.context);
        registry
            .register_client(
                &connection_id,
                "user-1".to_owned(),
                vec!["premium".to_owned()],
            )
            .unwrap();

        registry
            .push_unicast("user-1", Bytes::from_static(b"push"))
            .unwrap();
        assert_eq!(
            session.push_receiver.recv().await.unwrap(),
            Bytes::from_static(b"push")
        );
    }

    #[test]
    fn one_way_session_rejects_push_without_buffering() {
        let registry = SessionRegistry::new();
        let context = TransportSession::stateless("http1");
        registry.register_session(&context);
        registry
            .register_client(&context.connection_id, "user-1".to_owned(), vec![])
            .unwrap();

        assert_eq!(
            registry.push_unicast("user-1", Bytes::from_static(b"push")),
            Err("PushNotSupportedForTransport".to_owned())
        );
    }
}
