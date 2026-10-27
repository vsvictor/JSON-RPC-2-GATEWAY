use std::{io, time::Duration};

use anyhow::Context;
use tracing_appender::non_blocking::{ErrorCounter, NonBlocking, NonBlockingBuilder, WorkerGuard};
use tracing_subscriber::EnvFilter;

const LOG_QUEUE_CAPACITY: usize = 1024;

pub struct LoggingGuard {
    worker: Option<WorkerGuard>,
    errors: ErrorCounter,
}

impl Drop for LoggingGuard {
    fn drop(&mut self) {
        drop(self.worker.take());
        let dropped = self.errors.dropped_lines();
        if dropped > 0 {
            eprintln!("logging: dropped {dropped} records because the log queue was full");
        }
    }
}

fn writer(output: impl io::Write + Send + 'static, capacity: usize) -> (NonBlocking, WorkerGuard) {
    NonBlockingBuilder::default()
        .buffered_lines_limit(capacity)
        .lossy(true)
        .thread_name("gateway-log-writer")
        .finish(output)
}

pub fn init(
    level: &str,
) -> Result<(LoggingGuard, ErrorCounter), Box<dyn std::error::Error + Send + Sync>> {
    let filter =
        EnvFilter::try_new(level).with_context(|| format!("invalid LOG_LEVEL {level:?}"))?;
    let (output, guard) = writer(io::stdout(), LOG_QUEUE_CAPACITY);
    let errors = output.error_counter();
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(output)
        .try_init()?;
    Ok((
        LoggingGuard {
            worker: Some(guard),
            errors: errors.clone(),
        },
        errors,
    ))
}

pub fn report_dropped_logs(errors: ErrorCounter) -> tokio::task::JoinHandle<()> {
    let dropped = metrics::counter!("gateway_log_records_dropped_total");
    dropped.increment(0);
    tokio::spawn(async move {
        let mut previous = 0;
        let mut interval = tokio::time::interval(Duration::from_secs(1));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            interval.tick().await;
            let current = errors.dropped_lines();
            dropped.increment(current.saturating_sub(previous) as u64);
            previous = current;
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex, mpsc};

    #[derive(Clone)]
    struct Buffer(Arc<Mutex<Vec<u8>>>);

    impl io::Write for Buffer {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn subscriber_keeps_filter_and_structured_fields() {
        let buffer = Buffer(Arc::default());
        let (output, guard) = writer(buffer.clone(), 8);
        let subscriber = tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::new("debug"))
            .with_ansi(false)
            .with_writer(output)
            .finish();
        tracing::subscriber::with_default(subscriber, || {
            tracing::debug!(peer = "backend-a", payload = "example", "outbound");
            tracing::trace!("filtered-out");
        });
        drop(guard);
        let bytes = buffer.0.lock().unwrap();
        let text = std::str::from_utf8(&bytes).unwrap();
        assert!(text.contains("DEBUG"));
        assert!(text.contains("outbound"));
        assert!(text.contains("backend-a"));
        assert!(text.contains("example"));
        assert!(!text.contains("filtered-out"));
    }

    #[test]
    fn guard_drains_records_on_shutdown() {
        use io::Write;

        let buffer = Buffer(Arc::default());
        let (mut output, guard) = writer(buffer.clone(), 8);
        output.write_all(b"first\n").unwrap();
        output.write_all(b"last\n").unwrap();
        drop(guard);
        assert_eq!(&*buffer.0.lock().unwrap(), b"first\nlast\n");
    }

    #[test]
    fn slow_writer_does_not_block_producers_and_counts_overflow() {
        use io::Write;

        struct SlowWriter {
            entered: mpsc::Sender<()>,
            release: mpsc::Receiver<()>,
            blocked: bool,
        }

        impl io::Write for SlowWriter {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                if !self.blocked {
                    self.blocked = true;
                    self.entered.send(()).unwrap();
                    self.release
                        .recv_timeout(Duration::from_secs(5))
                        .map_err(io::Error::other)?;
                }
                Ok(bytes.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        let (entered_tx, entered_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let (mut output, guard) = writer(
            SlowWriter {
                entered: entered_tx,
                release: release_rx,
                blocked: false,
            },
            2,
        );
        let errors = output.error_counter();
        output.write_all(b"blocked\n").unwrap();
        entered_rx.recv_timeout(Duration::from_secs(2)).unwrap();
        let (done_tx, done_rx) = mpsc::channel();
        let producer = std::thread::spawn(move || {
            for _ in 0..10 {
                output.write_all(b"overflow\n").unwrap();
            }
            done_tx.send(()).unwrap();
        });
        let completed = done_rx.recv_timeout(Duration::from_secs(2));
        release_tx.send(()).unwrap();
        producer.join().unwrap();
        completed.expect("log producers must complete while the writer remains blocked");
        assert_eq!(errors.dropped_lines(), 8);
        drop(guard);
    }
}
