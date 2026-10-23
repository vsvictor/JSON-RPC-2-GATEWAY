//! Gateway-owned broker session identity, lifecycle metadata, and lease policy.
//!
//! Broker clients and servers can use these transport-neutral types to exchange
//! RPC, session lifecycle, and push messages without putting gateway policy in
//! a general-purpose JSON-RPC transport library.

use std::{
    collections::HashMap,
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};

use serde::{Deserialize, Serialize};

/// Heartbeat cadence and ingress-session lease selected by a gateway.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BrokerSessionPolicy {
    pub heartbeat_interval: Duration,
    pub session_lease: Duration,
}

impl Default for BrokerSessionPolicy {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(10),
            session_lease: Duration::from_secs(45),
        }
    }
}

impl BrokerSessionPolicy {
    pub fn validate(&self) -> Result<(), SessionError> {
        if self.heartbeat_interval.is_zero()
            || self.session_lease <= self.heartbeat_interval
            || self
                .session_lease
                .checked_add(self.heartbeat_interval)
                .is_none()
        {
            return Err(SessionError::InvalidPolicy);
        }
        Ok(())
    }
}

/// The broker-side routing identity associated with one logical ingress session.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct SessionIdentity {
    pub id: String,
    pub push_to: String,
}

impl SessionIdentity {
    pub fn new(id: impl Into<String>, push_to: impl Into<String>) -> Result<Self, SessionError> {
        let identity = Self {
            id: id.into(),
            push_to: push_to.into(),
        };
        identity.validate()?;
        Ok(identity)
    }

    pub fn generate(push_to: impl Into<String>) -> Result<Self, SessionError> {
        Self::new(uuid::Uuid::new_v4().to_string(), push_to)
    }

    pub fn validate(&self) -> Result<(), SessionError> {
        if uuid::Uuid::parse_str(&self.id).is_err()
            || self.push_to.is_empty()
            || self.push_to.len() > 255
            || self.push_to.chars().any(char::is_control)
        {
            return Err(SessionError::InvalidIdentity);
        }
        Ok(())
    }
}

/// Message types carried in broker metadata, serialized in snake case.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum MessageKind {
    #[default]
    Rpc,
    Open,
    Close,
    Heartbeat,
    Push,
}

/// Optional session context accompanying an RPC or lifecycle message.
#[derive(Clone, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
pub struct MessageMetadata {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session: Option<SessionIdentity>,
    #[serde(default)]
    pub kind: MessageKind,
}

/// MQTT broker envelope. AMQP carries the same metadata in message headers.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq, Serialize)]
pub struct BrokerEnvelope {
    pub payload: Vec<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,
    pub correlation_id: String,
    #[serde(default)]
    pub control: bool,
    #[serde(default)]
    pub metadata: MessageMetadata,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum SessionError {
    #[error("broker session heartbeat must be nonzero and the lease must exceed it")]
    InvalidPolicy,
    #[error("invalid broker session identity or push route")]
    InvalidIdentity,
    #[error("broker session capacity reached")]
    CapacityReached,
    #[error("broker session ID is closed or retired")]
    Retired,
    #[error("broker session ID is already bound to another push route")]
    IdentityConflict,
}

struct SessionRecord {
    identity: SessionIdentity,
    last_activity: Instant,
    in_flight: usize,
}

struct RegistryState {
    active: HashMap<String, SessionRecord>,
    retired: HashMap<String, Instant>,
}

/// Tracks broker session identity and leases independently of broker adapters.
#[derive(Clone)]
pub struct SessionRegistry {
    policy: BrokerSessionPolicy,
    max_sessions: usize,
    state: Arc<Mutex<RegistryState>>,
}

struct ManagedSession<T> {
    identity: SessionIdentity,
    value: T,
}

/// Result of applying broker metadata to the gateway-owned session runtime.
pub enum SessionDispatch {
    /// An application RPC was admitted. `identity` is absent for legacy
    /// stateless publishers; identified requests carry a lease guard.
    Rpc {
        identity: Option<SessionIdentity>,
        request: Option<SessionRequest>,
    },
    /// Lifecycle, push, malformed, expired, or otherwise rejected metadata.
    Lifecycle,
}

/// Owns broker ingress-session lifecycle state and its associated resources.
///
/// Broker adapters supply a factory for transport-specific session resources,
/// then apply the returned RPC disposition to their application handler.
pub struct SessionRuntime<T> {
    registry: SessionRegistry,
    sessions: HashMap<String, ManagedSession<T>>,
}

impl<T> SessionRuntime<T> {
    pub fn new(policy: BrokerSessionPolicy, max_sessions: usize) -> Result<Self, SessionError> {
        Ok(Self {
            registry: SessionRegistry::new(policy, max_sessions)?,
            sessions: HashMap::new(),
        })
    }

    /// Apply lifecycle metadata and admit identified RPC work through the lease registry.
    pub fn dispatch(
        &mut self,
        metadata: &MessageMetadata,
        now: Instant,
        create: impl for<'a> Fn(&'a SessionIdentity) -> T,
    ) -> SessionDispatch {
        let Some(identity) = metadata.session.as_ref() else {
            return if metadata.kind == MessageKind::Rpc {
                SessionDispatch::Rpc {
                    identity: None,
                    request: None,
                }
            } else {
                SessionDispatch::Lifecycle
            };
        };

        match metadata.kind {
            MessageKind::Open => {
                if matches!(self.open(identity, now, create), Err(SessionError::Retired)) {
                    self.remove_if_identity_matches(identity);
                }
                SessionDispatch::Lifecycle
            }
            MessageKind::Close => {
                let _ = self.close_identity(identity, now);
                SessionDispatch::Lifecycle
            }
            MessageKind::Heartbeat => {
                if !self.registry.heartbeat(identity, now) {
                    self.remove_if_identity_matches(identity);
                }
                SessionDispatch::Lifecycle
            }
            MessageKind::Rpc => match self.open(identity, now, create) {
                Ok(()) => match self.registry.begin_request(identity, now) {
                    Some(request) => SessionDispatch::Rpc {
                        identity: Some(identity.clone()),
                        request: Some(request),
                    },
                    None => {
                        self.remove_if_identity_matches(identity);
                        SessionDispatch::Lifecycle
                    }
                },
                Err(SessionError::Retired) => {
                    self.remove_if_identity_matches(identity);
                    SessionDispatch::Lifecycle
                }
                Err(_) => SessionDispatch::Lifecycle,
            },
            MessageKind::Push => SessionDispatch::Lifecycle,
        }
    }

    fn open(
        &mut self,
        identity: &SessionIdentity,
        now: Instant,
        create: impl FnOnce(&SessionIdentity) -> T,
    ) -> Result<(), SessionError> {
        match self.registry.open(identity.clone(), now) {
            Ok(()) => {
                self.sessions
                    .entry(identity.id.clone())
                    .or_insert_with(|| ManagedSession {
                        identity: identity.clone(),
                        value: create(identity),
                    });
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Look up the transport resource only when the full identity still matches.
    pub fn get(&self, identity: &SessionIdentity) -> Option<&T> {
        self.sessions
            .get(&identity.id)
            .filter(|session| session.identity == *identity)
            .map(|session| &session.value)
    }

    /// Close a session by its server-owned ID, for example after protocol close.
    pub fn close(&mut self, id: &str, now: Instant) -> Option<T> {
        let _ = self.registry.close(id, now)?;
        self.sessions.remove(id).map(|session| session.value)
    }

    /// Close only when the supplied complete identity matches the active session.
    pub fn close_identity(&mut self, identity: &SessionIdentity, now: Instant) -> Option<T> {
        let registry_closed = self.registry.close_identity(identity, now).is_some();
        let same_identity = self
            .sessions
            .get(&identity.id)
            .is_some_and(|session| session.identity == *identity);
        if !registry_closed && !same_identity {
            return None;
        }
        self.sessions
            .remove(&identity.id)
            .map(|session| session.value)
    }

    /// Expire idle sessions and return their transport resources for cleanup.
    pub fn expire(&mut self, now: Instant) -> Vec<T> {
        self.registry
            .expire(now)
            .into_iter()
            .filter_map(|identity| {
                self.sessions
                    .remove(&identity.id)
                    .filter(|session| session.identity == identity)
                    .map(|session| session.value)
            })
            .collect()
    }

    /// Retire sessions whose adapter-owned resource has failed.
    pub fn remove_failed(&mut self, now: Instant, mut failed: impl FnMut(&T) -> bool) -> Vec<T> {
        let failed_identities: Vec<_> = self
            .sessions
            .values()
            .filter(|session| failed(&session.value))
            .map(|session| session.identity.clone())
            .collect();
        failed_identities
            .into_iter()
            .filter_map(|identity| self.close_identity(&identity, now))
            .collect()
    }

    pub fn len(&self) -> usize {
        self.sessions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.sessions.is_empty()
    }

    fn remove_if_identity_matches(&mut self, identity: &SessionIdentity) {
        if self
            .sessions
            .get(&identity.id)
            .is_some_and(|session| session.identity == *identity)
        {
            self.sessions.remove(&identity.id);
        }
    }
}

impl SessionRegistry {
    pub fn new(policy: BrokerSessionPolicy, max_sessions: usize) -> Result<Self, SessionError> {
        policy.validate()?;
        if max_sessions == 0 {
            return Err(SessionError::CapacityReached);
        }
        Ok(Self {
            policy,
            max_sessions,
            state: Arc::new(Mutex::new(RegistryState {
                active: HashMap::new(),
                retired: HashMap::new(),
            })),
        })
    }

    /// Register a new session; closed IDs cannot be reused during the tombstone window.
    pub fn open(&self, identity: SessionIdentity, now: Instant) -> Result<(), SessionError> {
        identity.validate()?;
        let mut state = self.state.lock().expect("session registry poisoned");
        self.prune_retired(&mut state, now);
        if state.retired.contains_key(&identity.id) {
            return Err(SessionError::Retired);
        }
        if let Some(existing) = state.active.get_mut(&identity.id) {
            if existing.identity != identity {
                return Err(SessionError::IdentityConflict);
            }
            if existing.in_flight == 0
                && now.saturating_duration_since(existing.last_activity)
                    >= self.policy.session_lease
            {
                state.active.remove(&identity.id);
                self.retire(&mut state, identity.id, now);
                return Err(SessionError::Retired);
            }
            existing.last_activity = now;
            return Ok(());
        }
        if state.active.len() >= self.max_sessions {
            return Err(SessionError::CapacityReached);
        }
        state.active.insert(
            identity.id.clone(),
            SessionRecord {
                identity,
                last_activity: now,
                in_flight: 0,
            },
        );
        Ok(())
    }

    /// Refresh an active session only when its complete routing identity matches.
    pub fn heartbeat(&self, identity: &SessionIdentity, now: Instant) -> bool {
        let mut state = self.state.lock().expect("session registry poisoned");
        self.prune_retired(&mut state, now);
        let Some(session) = state.active.get_mut(&identity.id) else {
            return false;
        };
        if session.identity != *identity {
            return false;
        }
        if session.in_flight == 0
            && now.saturating_duration_since(session.last_activity) >= self.policy.session_lease
        {
            state.active.remove(&identity.id);
            self.retire(&mut state, identity.id.clone(), now);
            return false;
        }
        session.last_activity = now;
        true
    }

    /// Begin admitted work; its guard keeps an otherwise-idle lease alive.
    pub fn begin_request(
        &self,
        identity: &SessionIdentity,
        now: Instant,
    ) -> Option<SessionRequest> {
        let mut state = self.state.lock().expect("session registry poisoned");
        self.prune_retired(&mut state, now);
        let session = state.active.get_mut(&identity.id)?;
        if session.identity != *identity {
            return None;
        }
        if session.in_flight == 0
            && now.saturating_duration_since(session.last_activity) >= self.policy.session_lease
        {
            state.active.remove(&identity.id);
            self.retire(&mut state, identity.id.clone(), now);
            return None;
        }
        session.last_activity = now;
        session.in_flight = session.in_flight.saturating_add(1);
        Some(SessionRequest {
            registry: Arc::downgrade(&self.state),
            id: identity.id.clone(),
        })
    }

    /// Close immediately and return the identity for lifecycle notification.
    pub fn close(&self, id: &str, now: Instant) -> Option<SessionIdentity> {
        let mut state = self.state.lock().expect("session registry poisoned");
        let session = state.active.remove(id)?;
        self.retire(&mut state, id.to_owned(), now);
        Some(session.identity)
    }

    /// Close only when the supplied push route matches the registered identity.
    pub fn close_identity(
        &self,
        identity: &SessionIdentity,
        now: Instant,
    ) -> Option<SessionIdentity> {
        identity.validate().ok()?;
        let mut state = self.state.lock().expect("session registry poisoned");
        if state
            .active
            .get(&identity.id)
            .is_none_or(|session| session.identity != *identity)
        {
            return None;
        }
        let session = state.active.remove(&identity.id)?;
        self.retire(&mut state, identity.id.clone(), now);
        Some(session.identity)
    }

    /// Remove idle sessions whose lease elapsed and prune expired tombstones.
    pub fn expire(&self, now: Instant) -> Vec<SessionIdentity> {
        let mut state = self.state.lock().expect("session registry poisoned");
        let lease = self.policy.session_lease;
        let expired_ids: Vec<_> = state
            .active
            .iter()
            .filter(|(_, session)| {
                session.in_flight == 0
                    && now.saturating_duration_since(session.last_activity) >= lease
            })
            .map(|(id, _)| id.clone())
            .collect();
        let mut expired = Vec::with_capacity(expired_ids.len());
        for id in expired_ids {
            if let Some(session) = state.active.remove(&id) {
                expired.push(session.identity);
                self.retire(&mut state, id, now);
            }
        }
        self.prune_retired(&mut state, now);
        expired
    }

    fn prune_retired(&self, state: &mut RegistryState, now: Instant) {
        let Some(tombstone_lifetime) = self
            .policy
            .session_lease
            .checked_add(self.policy.heartbeat_interval)
        else {
            return;
        };
        state
            .retired
            .retain(|_, closed_at| now.saturating_duration_since(*closed_at) < tombstone_lifetime);
    }

    fn retire(&self, state: &mut RegistryState, id: String, now: Instant) {
        if !state.retired.contains_key(&id)
            && state.retired.len() >= self.max_sessions
            && let Some(oldest) = state
                .retired
                .iter()
                .min_by_key(|(_, closed_at)| **closed_at)
                .map(|(id, _)| id.clone())
        {
            state.retired.remove(&oldest);
        }
        state.retired.insert(id, now);
    }

    pub fn len(&self) -> usize {
        self.state
            .lock()
            .expect("session registry poisoned")
            .active
            .len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub struct SessionRequest {
    registry: Weak<Mutex<RegistryState>>,
    id: String,
}

impl Drop for SessionRequest {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            return;
        };
        let mut state = registry.lock().expect("session registry poisoned");
        if let Some(session) = state.active.get_mut(&self.id) {
            session.in_flight = session.in_flight.saturating_sub(1);
            session.last_activity = Instant::now();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn identity() -> SessionIdentity {
        SessionIdentity::new(
            "550e8400-e29b-41d4-a716-446655440000",
            "json-rpc-2/replies/client",
        )
        .unwrap()
    }

    #[test]
    fn metadata_keeps_the_existing_wire_names_and_defaults() {
        let metadata = MessageMetadata {
            session: Some(identity()),
            kind: MessageKind::Heartbeat,
        };
        let encoded = serde_json::to_string(&metadata).unwrap();
        assert_eq!(
            encoded,
            r#"{"session":{"id":"550e8400-e29b-41d4-a716-446655440000","push_to":"json-rpc-2/replies/client"},"kind":"heartbeat"}"#
        );
        assert_eq!(
            serde_json::from_str::<MessageMetadata>(r#"{"session":null}"#).unwrap(),
            MessageMetadata::default()
        );
    }

    #[test]
    fn policy_rejects_zero_heartbeat_and_short_lease() {
        let policy = BrokerSessionPolicy {
            heartbeat_interval: Duration::ZERO,
            ..BrokerSessionPolicy::default()
        };
        assert_eq!(policy.validate(), Err(SessionError::InvalidPolicy));

        let policy = BrokerSessionPolicy {
            heartbeat_interval: Duration::from_secs(5),
            session_lease: Duration::from_secs(5),
        };
        assert_eq!(policy.validate(), Err(SessionError::InvalidPolicy));
    }

    #[test]
    fn close_returns_identity_and_prevents_late_recreation() {
        let registry = SessionRegistry::new(BrokerSessionPolicy::default(), 1).unwrap();
        let now = Instant::now();
        let session = identity();
        registry.open(session.clone(), now).unwrap();

        let wrong_route =
            SessionIdentity::new(session.id.clone(), "json-rpc-2/replies/other").unwrap();
        assert!(registry.close_identity(&wrong_route, now).is_none());
        assert_eq!(registry.len(), 1);
        assert_eq!(
            registry.close_identity(&session, now),
            Some(session.clone())
        );
        assert!(!registry.heartbeat(&session, now));
        assert_eq!(registry.open(session, now), Err(SessionError::Retired));
    }

    #[test]
    fn registry_bounds_retired_identity_tombstones() {
        let registry = SessionRegistry::new(BrokerSessionPolicy::default(), 1).unwrap();
        let now = Instant::now();
        let first = identity();
        let second = SessionIdentity::generate("json-rpc-2/replies/second").unwrap();
        registry.open(first.clone(), now).unwrap();
        registry.close(&first.id, now).unwrap();
        registry.open(second.clone(), now).unwrap();
        registry
            .close(&second.id, now + Duration::from_secs(1))
            .unwrap();

        assert_eq!(registry.open(second, now), Err(SessionError::Retired));
        assert!(registry.open(first, now).is_ok());
    }

    #[test]
    fn registry_enforces_close_retirement_leases_and_in_flight_work() {
        let policy = BrokerSessionPolicy::default();
        let registry = SessionRegistry::new(policy, 2).unwrap();
        let start = Instant::now();
        let session = identity();
        registry.open(session.clone(), start).unwrap();

        let request = registry.begin_request(&session, start).unwrap();
        assert!(registry.expire(start + policy.session_lease).is_empty());
        drop(request);
        let expired = registry.expire(start + policy.session_lease + Duration::from_millis(1));
        assert_eq!(expired, vec![session.clone()]);
        assert_eq!(registry.len(), 0);
        assert_eq!(
            registry.open(session.clone(), start + policy.session_lease),
            Err(SessionError::Retired)
        );
        let retired_at = start + policy.session_lease + Duration::from_millis(1);
        let tombstone_expiry = retired_at + policy.session_lease + policy.heartbeat_interval;
        assert!(registry.expire(tombstone_expiry).is_empty());
        registry.open(session, tombstone_expiry).unwrap();
    }

    #[test]
    fn registry_rejects_identity_rebinding_and_honors_capacity() {
        let registry = SessionRegistry::new(BrokerSessionPolicy::default(), 1).unwrap();
        let start = Instant::now();
        let session = identity();
        registry.open(session.clone(), start).unwrap();

        let rebound = SessionIdentity::new(session.id.clone(), "json-rpc-2/replies/other").unwrap();
        assert_eq!(
            registry.open(rebound, start),
            Err(SessionError::IdentityConflict)
        );
        assert_eq!(
            registry.open(
                SessionIdentity::generate("json-rpc-2/replies/second").unwrap(),
                start
            ),
            Err(SessionError::CapacityReached)
        );
        assert!(registry.heartbeat(&session, start + Duration::from_secs(1)));
    }

    #[test]
    fn runtime_owns_metadata_dispatch_contexts_and_cleanup() {
        let policy = BrokerSessionPolicy {
            heartbeat_interval: Duration::from_secs(1),
            session_lease: Duration::from_secs(5),
        };
        let mut runtime = SessionRuntime::new(policy, 2).unwrap();
        let start = Instant::now();
        let session = identity();
        let created = std::cell::Cell::new(0);
        let create = |_: &SessionIdentity| {
            created.set(created.get() + 1);
            String::from("context")
        };

        assert!(matches!(
            runtime.dispatch(
                &MessageMetadata {
                    session: Some(session.clone()),
                    kind: MessageKind::Open,
                },
                start,
                create,
            ),
            SessionDispatch::Lifecycle
        ));
        assert!(matches!(
            runtime.dispatch(
                &MessageMetadata {
                    session: Some(session.clone()),
                    kind: MessageKind::Open,
                },
                start + Duration::from_millis(1),
                create,
            ),
            SessionDispatch::Lifecycle
        ));
        assert_eq!(created.get(), 1);
        assert_eq!(runtime.get(&session).map(String::as_str), Some("context"));

        let dispatch = runtime.dispatch(
            &MessageMetadata {
                session: Some(session.clone()),
                kind: MessageKind::Rpc,
            },
            start + Duration::from_millis(2),
            create,
        );
        let SessionDispatch::Rpc {
            identity: Some(dispatched_identity),
            request: Some(request),
        } = dispatch
        else {
            panic!("registered session RPC should be admitted");
        };
        assert_eq!(dispatched_identity, session);
        assert_eq!(runtime.expire(start + policy.session_lease).len(), 0);
        drop(request);
        assert_eq!(
            runtime.expire(Instant::now() + policy.session_lease).len(),
            1
        );
        assert!(runtime.is_empty());

        let stateless = runtime.dispatch(&MessageMetadata::default(), Instant::now(), create);
        assert!(matches!(
            stateless,
            SessionDispatch::Rpc {
                identity: None,
                request: None
            }
        ));
    }

    #[test]
    fn runtime_rejects_rebinding_and_retires_only_matching_routes() {
        let mut runtime = SessionRuntime::new(BrokerSessionPolicy::default(), 2).unwrap();
        let now = Instant::now();
        let session = identity();
        let create = |_: &SessionIdentity| String::from("context");
        runtime.dispatch(
            &MessageMetadata {
                session: Some(session.clone()),
                kind: MessageKind::Open,
            },
            now,
            create,
        );

        let wrong_route =
            SessionIdentity::new(session.id.clone(), "json-rpc-2/replies/other").unwrap();
        assert!(runtime.close_identity(&wrong_route, now).is_none());
        assert!(runtime.get(&session).is_some());

        assert!(matches!(
            runtime.dispatch(
                &MessageMetadata {
                    session: Some(wrong_route),
                    kind: MessageKind::Rpc,
                },
                now,
                create,
            ),
            SessionDispatch::Lifecycle
        ));
        assert!(runtime.get(&session).is_some());
        assert_eq!(
            runtime.close_identity(&session, now).as_deref(),
            Some("context")
        );
        assert!(runtime.is_empty());
    }

    #[test]
    fn runtime_retires_failed_adapter_resources() {
        let mut runtime = SessionRuntime::new(BrokerSessionPolicy::default(), 2).unwrap();
        let now = Instant::now();
        let session = identity();
        runtime.dispatch(
            &MessageMetadata {
                session: Some(session.clone()),
                kind: MessageKind::Open,
            },
            now,
            |_: &SessionIdentity| true,
        );
        assert_eq!(runtime.remove_failed(now, |failed| *failed), vec![true]);
        assert!(runtime.get(&session).is_none());
        assert!(matches!(
            runtime.dispatch(
                &MessageMetadata {
                    session: Some(session),
                    kind: MessageKind::Open,
                },
                now,
                |_: &SessionIdentity| true,
            ),
            SessionDispatch::Lifecycle
        ));
    }

    #[test]
    fn runtime_removes_resources_when_a_late_heartbeat_expires_the_lease() {
        let policy = BrokerSessionPolicy {
            heartbeat_interval: Duration::from_secs(1),
            session_lease: Duration::from_secs(3),
        };
        let mut runtime = SessionRuntime::new(policy, 1).unwrap();
        let start = Instant::now();
        let session = identity();
        runtime.dispatch(
            &MessageMetadata {
                session: Some(session.clone()),
                kind: MessageKind::Open,
            },
            start,
            |_: &SessionIdentity| String::from("context"),
        );

        assert!(matches!(
            runtime.dispatch(
                &MessageMetadata {
                    session: Some(session.clone()),
                    kind: MessageKind::Heartbeat,
                },
                start + policy.session_lease,
                |_: &SessionIdentity| String::from("unused"),
            ),
            SessionDispatch::Lifecycle
        ));
        assert!(runtime.is_empty());
        assert!(matches!(
            runtime.dispatch(
                &MessageMetadata {
                    session: Some(session.clone()),
                    kind: MessageKind::Rpc,
                },
                start + policy.session_lease,
                |_: &SessionIdentity| String::from("must not recreate"),
            ),
            SessionDispatch::Lifecycle
        ));
        assert!(runtime.is_empty());
    }
}
