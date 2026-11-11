use std::time::Duration;

use anyhow::Context;
use json_rpc_balancer::BackendPool;
use tokio::time::{Instant, sleep_until, timeout_at};

pub async fn wait_for_backends(pool: &BackendPool, budget: Duration) -> anyhow::Result<()> {
    let deadline = Instant::now() + budget;
    loop {
        match timeout_at(deadline, pool.warm_up()).await {
            Ok(Ok(())) => return Ok(()),
            Ok(Err(error)) => {
                tracing::warn!(error = %error, "backends not ready; retrying startup health checks");
                let retry = Instant::now() + Duration::from_secs(1);
                if retry >= deadline {
                    return Err(error).context("backend readiness deadline exceeded");
                }
                sleep_until(retry).await;
            }
            Err(error) => {
                return Err(error).context("backend readiness deadline exceeded");
            }
        }
    }
}

#[cfg(test)]
mod tests;
