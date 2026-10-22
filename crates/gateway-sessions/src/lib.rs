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
}
