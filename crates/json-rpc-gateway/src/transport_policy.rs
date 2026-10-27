use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use anyhow::{Context, bail};
use json_rpc_2_runtime::TransportKind;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio_util::sync::CancellationToken;

struct Entry {
    enabled: bool,
    generation: CancellationToken,
}

pub struct TransportPolicy {
    entries: RwLock<BTreeMap<String, Entry>>,
    path: Option<PathBuf>,
    updates: tokio::sync::Mutex<SavedPolicy>,
}

#[derive(Debug, Serialize)]
pub struct TransportStatus {
    pub transport: String,
    pub enabled: bool,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedPolicy {
    version: u32,
    disabled: Vec<String>,
}

impl TransportPolicy {
    pub fn new(kinds: impl IntoIterator<Item = TransportKind>) -> Arc<Self> {
        Arc::new(Self {
            entries: RwLock::new(
                kinds
                    .into_iter()
                    .map(|kind| {
                        (
                            kind.id().to_owned(),
                            Entry {
                                enabled: true,
                                generation: CancellationToken::new(),
                            },
                        )
                    })
                    .collect(),
            ),
            path: None,
            updates: tokio::sync::Mutex::new(SavedPolicy {
                version: 1,
                disabled: Vec::new(),
            }),
        })
    }

    pub fn load(
        kinds: impl IntoIterator<Item = TransportKind>,
        path: PathBuf,
    ) -> anyhow::Result<Arc<Self>> {
        let mut policy = Self::new(kinds);
        match std::fs::read(&path) {
            Ok(bytes) => {
                let saved: SavedPolicy = serde_json::from_slice(&bytes)
                    .with_context(|| format!("reading transport policy {}", path.display()))?;
                let entries = Arc::get_mut(&mut policy)
                    .unwrap()
                    .entries
                    .get_mut()
                    .unwrap();
                if saved.version != 1 {
                    bail!("unsupported transport policy version: {}", saved.version);
                }
                let mut unique = std::collections::BTreeSet::new();
                for name in &saved.disabled {
                    if !unique.insert(name) {
                        bail!("duplicate transport in saved policy: {name}");
                    }
                    if !TransportKind::ALL.iter().any(|kind| kind.id() == name) {
                        bail!("unknown transport in saved policy: {name}");
                    }
                    if let Some(entry) = entries.get_mut(name) {
                        entry.enabled = false;
                        entry.generation.cancel();
                    }
                }
                *Arc::get_mut(&mut policy).unwrap().updates.get_mut() = saved;
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error).context("reading saved transport policy"),
        }
        Arc::get_mut(&mut policy).unwrap().path = Some(path);
        Ok(policy)
    }

    pub fn enabled(&self, name: &str) -> bool {
        self.entries
            .read()
            .unwrap()
            .get(name)
            .is_some_and(|entry| entry.enabled)
    }

    pub fn generation(&self, name: &str) -> CancellationToken {
        self.entries
            .read()
            .unwrap()
            .get(name)
            .map(|entry| entry.generation.clone())
            .unwrap_or_else(|| {
                let token = CancellationToken::new();
                token.cancel();
                token
            })
    }

    pub fn snapshot(&self) -> Vec<TransportStatus> {
        self.entries
            .read()
            .unwrap()
            .iter()
            .map(|(name, entry)| TransportStatus {
                transport: name.clone(),
                enabled: entry.enabled,
            })
            .collect()
    }

    pub async fn set_enabled(&self, name: &str, enabled: bool) -> Result<(), &'static str> {
        let mut saved = self.updates.lock().await;
        {
            let entries = self.entries.read().unwrap();
            let Some(entry) = entries.get(name) else {
                return Err("TransportNotConfigured");
            };
            if entry.enabled == enabled {
                return Ok(());
            }
        }
        let mut disabled = saved.disabled.clone();
        disabled.retain(|entry| entry != name);
        if !enabled {
            disabled.push(name.to_owned());
            disabled.sort();
        }
        let Some(path) = &self.path else {
            return Err("TransportPolicyPersistenceNotConfigured");
        };
        let next = SavedPolicy {
            version: 1,
            disabled,
        };
        if let Err(error) = persist(path, &next).await {
            tracing::error!(%error, path = %path.display(), "cannot persist transport policy");
            return Err("TransportPolicyPersistenceFailed");
        }
        let mut entries = self.entries.write().unwrap();
        let entry = entries.get_mut(name).unwrap();
        if enabled {
            entry.generation = CancellationToken::new();
        } else {
            entry.generation.cancel();
        }
        entry.enabled = enabled;
        *saved = next;
        tracing::info!(transport = name, enabled, "transport policy changed");
        Ok(())
    }
}

async fn persist(path: &Path, policy: &SavedPolicy) -> anyhow::Result<()> {
    let parent = path
        .parent()
        .context("transport state path has no parent")?;
    let temp = parent.join(format!(".transport-policy-{}.tmp", uuid::Uuid::new_v4()));
    let bytes = serde_json::to_vec(policy)?;
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        tokio::fs::rename(&temp, path).await?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if result.is_err() {
        match tokio::fs::remove_file(&temp).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => tracing::warn!(%error, "cannot clean transport policy temporary file"),
        }
    }
    result
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    pub(crate) struct StateFile(pub PathBuf);

    impl StateFile {
        pub fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("gateway-policy-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&dir).unwrap();
            Self(dir.join("transports.json"))
        }
    }

    impl Drop for StateFile {
        fn drop(&mut self) {
            std::fs::remove_dir_all(self.0.parent().unwrap()).unwrap();
        }
    }

    #[tokio::test]
    async fn policy_is_durable_and_old_generations_never_reopen() {
        let file = StateFile::new();
        let policy =
            TransportPolicy::load([TransportKind::Tcp, TransportKind::Udp], file.0.clone())
                .unwrap();
        let tcp = policy.generation("tcp");
        let udp = policy.generation("udp");
        policy.set_enabled("tcp", false).await.unwrap();
        assert!(tcp.is_cancelled());
        assert!(!udp.is_cancelled());
        let restored =
            TransportPolicy::load([TransportKind::Tcp, TransportKind::Udp], file.0.clone())
                .unwrap();
        assert!(!restored.enabled("tcp"));
        assert!(restored.enabled("udp"));
        policy.set_enabled("tcp", true).await.unwrap();
        assert!(!policy.generation("tcp").is_cancelled());
        assert!(tcp.is_cancelled());
        assert_eq!(
            policy.set_enabled("quic", true).await.unwrap_err(),
            "TransportNotConfigured"
        );
        assert_eq!(
            policy.set_enabled("invalid", false).await.unwrap_err(),
            "TransportNotConfigured"
        );
    }

    #[tokio::test]
    async fn concurrent_updates_do_not_lose_each_other_or_dormant_profiles() {
        let file = StateFile::new();
        let policy =
            TransportPolicy::load([TransportKind::Tcp, TransportKind::Udp], file.0.clone())
                .unwrap();
        let (tcp, udp) = tokio::join!(
            policy.set_enabled("tcp", false),
            policy.set_enabled("udp", false)
        );
        tcp.unwrap();
        udp.unwrap();
        let policy = TransportPolicy::load([TransportKind::Udp], file.0.clone()).unwrap();
        policy.set_enabled("udp", true).await.unwrap();
        let restored =
            TransportPolicy::load([TransportKind::Tcp, TransportKind::Udp], file.0.clone())
                .unwrap();
        assert!(!restored.enabled("tcp"));
        assert!(restored.enabled("udp"));
    }

    #[tokio::test]
    async fn failed_write_never_changes_runtime_policy() {
        let file = StateFile::new();
        let policy =
            TransportPolicy::load([TransportKind::Tcp], file.0.join("missing/state.json")).unwrap();
        let token = policy.generation("tcp");
        assert_eq!(
            policy.set_enabled("tcp", false).await.unwrap_err(),
            "TransportPolicyPersistenceFailed"
        );
        assert!(policy.enabled("tcp"));
        assert!(!token.is_cancelled());
    }

    #[test]
    fn invalid_policy_is_not_silently_ignored() {
        let file = StateFile::new();
        for bytes in [
            "not JSON",
            r#"{"version":2,"disabled":[]}"#,
            r#"{"version":1,"disabled":["bogus"]}"#,
            r#"{"version":1,"disabled":["tcp","tcp"]}"#,
            r#"{"version":1,"disabled":[],"typo":true}"#,
        ] {
            std::fs::write(&file.0, bytes).unwrap();
            assert!(TransportPolicy::load([TransportKind::Tcp], file.0.clone()).is_err());
        }
    }
}
