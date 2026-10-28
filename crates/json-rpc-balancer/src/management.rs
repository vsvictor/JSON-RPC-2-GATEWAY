use std::path::{Path, PathBuf};

use anyhow::Context;
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;

use super::*;

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedBackends {
    version: u32,
    backends: Vec<SavedBackend>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct SavedBackend {
    config: BackendConfig,
    enabled: bool,
}

impl BackendPool {
    /// Once written, the state file is authoritative over the startup seed.
    pub fn load(configs: Vec<BackendConfig>, path: PathBuf) -> anyhow::Result<Self> {
        let saved = match std::fs::read(&path) {
            Ok(bytes) => {
                let saved: SavedBackends = serde_json::from_slice(&bytes)
                    .with_context(|| format!("reading backend state {}", path.display()))?;
                anyhow::ensure!(saved.version == 1, "unsupported backend state version");
                Some(saved)
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(error).context("reading backend state"),
        };
        let mut pool = Self::new(match &saved {
            Some(saved) => saved.backends.iter().map(|b| b.config.clone()).collect(),
            None => configs,
        })?;
        if let Some(saved) = saved {
            for (entry, saved) in pool.entries().iter().zip(saved.backends) {
                if !saved.enabled {
                    let mut state = entry.lifecycle.lock().unwrap();
                    state.enabled = false;
                    state.generation.admission.cancel();
                    state.generation.closed.cancel();
                }
            }
        }
        pool.state_path = Some(path);
        Ok(pool)
    }

    fn saved(&self) -> SavedBackends {
        SavedBackends {
            version: 1,
            backends: self
                .entries()
                .iter()
                .map(|b| SavedBackend {
                    config: b.config.clone(),
                    enabled: b.enabled(),
                })
                .collect(),
        }
    }

    pub async fn add_backend(&self, config: BackendConfig) -> anyhow::Result<()> {
        let _update = self.updates.lock().await;
        anyhow::ensure!(
            !self.entries().iter().any(|b| b.config.id == config.id),
            "BackendAlreadyExists"
        );
        // Reuse startup validation and connection-pool construction.
        let candidate = Self::new(vec![config.clone()]).context("InvalidBackendConfig")?;
        let path = self
            .state_path
            .as_deref()
            .context("BackendPersistenceNotConfigured")?;
        let mut saved = self.saved();
        saved.backends.push(SavedBackend {
            config,
            enabled: true,
        });
        persist(path, &saved)
            .await
            .context("BackendPersistenceFailed")?;
        let entry = candidate.entries().pop().unwrap();
        tracing::info!(backend = %entry.config.id, "backend added to balancing");
        self.backends.write().unwrap().push(entry);
        Ok(())
    }

    pub async fn set_backend_enabled(&self, id: &str, enabled: bool) -> anyhow::Result<()> {
        let _update = self.updates.lock().await;
        let entry = self
            .entries()
            .into_iter()
            .find(|b| b.config.id == id)
            .context("BackendNotFound")?;
        if entry.enabled() == enabled {
            return Ok(());
        }
        let path = self
            .state_path
            .as_deref()
            .context("BackendPersistenceNotConfigured")?;
        let mut saved = self.saved();
        saved
            .backends
            .iter_mut()
            .find(|b| b.config.id == id)
            .unwrap()
            .enabled = enabled;
        persist(path, &saved)
            .await
            .context("BackendPersistenceFailed")?;
        {
            let mut state = entry.lifecycle.lock().unwrap();
            state.enabled = enabled;
            if enabled {
                state.generation = Arc::new(BackendGeneration::default());
            } else {
                state.generation.admission.cancel();
                if state.generation.active.load(Ordering::Acquire) == 0 {
                    state.generation.closed.cancel();
                }
            }
        }
        if !enabled {
            for transport in entry.transports.values() {
                for slot in &transport.slots {
                    if let Ok(mut slot) = slot.try_lock() {
                        slot.connection.take();
                        slot.checked_at = None;
                    }
                }
            }
        }
        tracing::info!(backend = id, enabled, "backend availability changed");
        Ok(())
    }
}

async fn persist(path: &Path, saved: &SavedBackends) -> anyhow::Result<()> {
    let parent = path.parent().context("backend state path has no parent")?;
    let temp = parent.join(format!(".backend-state-{}.tmp", uuid::Uuid::new_v4()));
    let bytes = serde_json::to_vec(saved)?;
    let result = async {
        let mut file = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&temp)
            .await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        tokio::fs::rename(&temp, path).await?;
        Ok(())
    }
    .await;
    if result.is_err() {
        match tokio::fs::remove_file(&temp).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => tracing::warn!(%error, "cannot remove temporary backend state"),
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    struct StateFile(PathBuf);

    impl StateFile {
        fn new() -> Self {
            let dir = std::env::temp_dir().join(format!("backend-policy-{}", uuid::Uuid::new_v4()));
            std::fs::create_dir(&dir).unwrap();
            Self(dir.join("backends.json"))
        }
    }

    impl Drop for StateFile {
        fn drop(&mut self) {
            std::fs::remove_dir_all(self.0.parent().unwrap()).unwrap();
        }
    }

    fn config(id: &str) -> BackendConfig {
        BackendConfig {
            id: id.into(),
            host: "127.0.0.1".into(),
            ports: [("tcp".into(), 1)].into_iter().collect(),
            pool_size: 1,
            options: BackendOptions::default(),
        }
    }

    #[tokio::test]
    async fn hot_backends_persist_additions_toggles_and_concurrent_updates() {
        let file = StateFile::new();
        let pool = BackendPool::load(vec![config("a")], file.0.clone()).unwrap();
        let (add, disable) = tokio::join!(
            pool.add_backend(config("b")),
            pool.set_backend_enabled("a", false)
        );
        add.unwrap();
        disable.unwrap();
        assert!(pool
            .add_backend(config("b"))
            .await
            .unwrap_err()
            .to_string()
            .contains("AlreadyExists"));
        assert!(pool.set_backend_enabled("missing", true).await.is_err());
        let restored = BackendPool::load(vec![config("ignored-seed")], file.0.clone()).unwrap();
        assert_eq!(restored.snapshot().len(), 2);
        assert!(!restored.snapshot()[0].enabled);
        assert!(restored.snapshot()[1].enabled);
        restored.set_backend_enabled("b", false).await.unwrap();
        // All-disabled is a valid persisted administrative state.
        let restored = BackendPool::load(vec![], file.0.clone()).unwrap();
        restored.warm_up().await.unwrap();
        assert!(restored.open_session("denied", "quic").is_err());
        assert!(restored
            .forward(&Bytes::from_static(b"opaque"), "tcp", "denied")
            .await
            .is_err());
        assert!(!restored.list_methods().await.complete);
        restored.set_backend_enabled("a", true).await.unwrap();
        assert!(restored.open_session("fresh", "quic").is_ok());
    }

    #[tokio::test]
    async fn hot_backends_fail_closed_on_invalid_config_or_persistence_failure() {
        let file = StateFile::new();
        let pool = BackendPool::load(vec![config("a")], file.0.join("missing")).unwrap();
        assert!(pool
            .set_backend_enabled("a", false)
            .await
            .unwrap_err()
            .to_string()
            .contains("PersistenceFailed"));
        assert!(pool.add_backend(config("b")).await.is_err());
        assert_eq!(pool.snapshot().len(), 1);
        assert!(pool.snapshot()[0].enabled);
        for invalid in [
            {
                let mut b = config("invalid");
                b.pool_size = 0;
                b
            },
            {
                let mut b = config("invalid");
                b.ports.insert("bad-kind".into(), 4);
                b
            },
            {
                let mut b = config("invalid");
                b.ports.insert("tcp".into(), 0);
                b
            },
        ] {
            assert!(pool
                .add_backend(invalid)
                .await
                .unwrap_err()
                .to_string()
                .contains("InvalidBackendConfig"));
        }
        for bytes in [
            "not JSON",
            r#"{"version":2,"backends":[]}"#,
            r#"{"version":1,"backends":[],"typo":1}"#,
        ] {
            std::fs::write(&file.0, bytes).unwrap();
            assert!(BackendPool::load(vec![config("a")], file.0.clone()).is_err());
        }
    }

    #[tokio::test]
    async fn hot_backends_retire_old_affinity_only_after_its_own_requests_drain() {
        let file = StateFile::new();
        let pool = BackendPool::load(vec![config("a")], file.0.clone()).unwrap();
        pool.open_session("old", "quic").unwrap();
        let affinity = pool.affinity.lock().unwrap()["old"].clone();
        let entry = pool.entries()[0].clone();
        let old_request = entry.admit(Some(&affinity)).unwrap();
        pool.set_backend_enabled("a", false).await.unwrap();
        assert!(affinity.admission.is_cancelled());
        assert!(!affinity.cancelled.is_cancelled());
        assert_eq!(pool.snapshot()[0].active_requests, 1);
        assert!(entry.admit(Some(&affinity)).is_err());
        pool.set_backend_enabled("a", true).await.unwrap();
        pool.open_session("new", "ws-quic").unwrap();
        let fresh = pool.affinity.lock().unwrap()["new"].clone();
        let new_request = entry.admit(Some(&fresh)).unwrap();
        assert!(entry.admit(Some(&affinity)).is_err());
        drop(old_request);
        assert!(affinity.cancelled.is_cancelled());
        assert!(!fresh.cancelled.is_cancelled());
        assert_eq!(pool.snapshot()[0].active_requests, 1);
        drop(new_request);
        assert_eq!(pool.snapshot()[0].active_requests, 0);
    }

    #[tokio::test]
    async fn hot_backends_finish_live_inflight_request_without_replay() {
        use json_rpc_2_runtime::MultiTransportServer;
        use json_rpc_2_server::RpcServer;
        tokio::time::timeout(Duration::from_secs(5), async {
            let entered = Arc::new(tokio::sync::Notify::new());
            let release = Arc::new(tokio::sync::Notify::new());
            let calls = Arc::new(AtomicUsize::new(0));
            let rpc = Arc::new(RpcServer::new());
            let (started, done, count) = (entered.clone(), release.clone(), calls.clone());
            rpc.register("slow", move |_| {
                let (started, done, count) = (started.clone(), done.clone(), count.clone());
                async move {
                    count.fetch_add(1, Ordering::Relaxed);
                    started.notify_one();
                    done.notified().await;
                    Ok(serde_json::json!("finished"))
                }
            })
            .unwrap();
            let backend = MultiTransportServer::new(Arc::new(
                gateway_protocol::GatewayBackendProtocol::new(rpc),
            ))
            .set_bind(["127.0.0.1".parse().unwrap()])
            .set_tcp_port(0)
            .start()
            .await
            .unwrap();
            let mut cfg = config("a");
            cfg.ports
                .insert("tcp".into(), backend.listeners().next().unwrap().1.port());
            let file = StateFile::new();
            let pool = Arc::new(BackendPool::load(vec![cfg], file.0.clone()).unwrap());
            pool.warm_up().await.unwrap();
            pool.open_session("pinned", "quic").unwrap();
            let closed = pool.session_cancellation("pinned").unwrap();
            let request = Bytes::from_static(br#"{"jsonrpc":"2.0","id":1,"method":"slow"}"#);
            let worker = {
                let (pool, request) = (pool.clone(), request.clone());
                tokio::spawn(async move { pool.forward(&request, "quic", "pinned").await })
            };
            entered.notified().await;
            pool.set_backend_enabled("a", false).await.unwrap();
            assert!(!closed.is_cancelled());
            assert!(pool.forward(&request, "tcp", "new").await.is_err());
            assert!(pool.forward(&request, "quic", "pinned").await.is_err());
            release.notify_one();
            let response = worker.await.unwrap().unwrap().unwrap();
            assert_eq!(
                serde_json::from_slice::<serde_json::Value>(&response).unwrap()["result"],
                "finished"
            );
            assert!(closed.is_cancelled());
            assert_eq!(calls.load(Ordering::Relaxed), 1);
            assert_eq!(pool.snapshot()[0].active_requests, 0);
            assert_eq!(pool.snapshot()[0].transports[0].connected_idle_slots, 0);
            pool.close_session("pinned");
            drop(pool);
            backend.close().await.unwrap();
        })
        .await
        .unwrap();
    }
}
