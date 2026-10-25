use std::{future::Future, panic::AssertUnwindSafe, sync::Arc, time::Duration};

use anyhow::{Result, anyhow, bail};
use futures_util::{FutureExt, StreamExt, stream};
use rdkafka::{Message, message::OwnedMessage};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::{
    kafka::{batch::Batch, producer::Producer},
    metrics::{BatchOutcome, Metrics},
    service,
};

pub(crate) trait Handler: Send + Sync + 'static {
    fn process(&self, message: OwnedMessage) -> impl Future<Output = Result<()>> + Send;
}

pub(crate) enum MessageHandler {
    Service,
    Observe,
    Forward {
        producer: Producer,
        output_topic: String,
    },
}
impl Handler for MessageHandler {
    async fn process(&self, message: OwnedMessage) -> Result<()> {
        match self {
            Self::Service => {
                let summary = service::process_payload(message.payload())?;
                tracing::info!(
                    topic = message.topic(),
                    partition = message.partition(),
                    offset = message.offset(),
                    word_count = summary.word_count,
                    byte_count = summary.byte_count,
                    "Service handler processed record"
                );
                Ok(())
            }
            Self::Observe => {
                tracing::info!(
                    topic = message.topic(),
                    partition = message.partition(),
                    offset = message.offset(),
                    "Observe handler recorded metadata"
                );
                Ok(())
            }
            Self::Forward {
                producer,
                output_topic,
            } => producer.publish(output_topic, message).await,
        }
    }
}

pub(crate) struct Processor<H> {
    handler: Arc<H>,
    permits: Arc<Semaphore>,
    parallelism: usize,
    timeout: Duration,
    metrics: Metrics,
}
impl<H> Clone for Processor<H> {
    fn clone(&self) -> Self {
        Self {
            handler: self.handler.clone(),
            permits: self.permits.clone(),
            parallelism: self.parallelism,
            timeout: self.timeout,
            metrics: self.metrics.clone(),
        }
    }
}
impl<H: Handler> Processor<H> {
    pub(crate) fn new(handler: H, parallelism: usize, timeout: Duration, metrics: Metrics) -> Self {
        Self {
            handler: Arc::new(handler),
            permits: Arc::new(Semaphore::new(parallelism)),
            parallelism,
            timeout,
            metrics,
        }
    }
    pub(crate) async fn batch(&self, batch: Batch, abort: &CancellationToken) -> Result<()> {
        let topic = batch
            .messages
            .first()
            .map(|message| message.topic().to_owned())
            .ok_or_else(|| anyhow!("cannot process an empty batch"))?;
        let outcome = BatchGuard {
            metrics: self.metrics.clone(),
            topic,
            finished: false,
        };
        let cancelled = batch.assignment.cancelled;
        let mut attempts = stream::iter(batch.messages).map(|message| {
            let cancelled = &cancelled;
            async move {
                let _permit = tokio::select! {
                    biased;
                    _ = cancelled.cancelled() => bail!("assignment revoked before processing"),
                    _ = abort.cancelled() => bail!("processing drain deadline reached"),
                    permit = self.permits.acquire() => permit.map_err(|_| anyhow!("processor admission closed"))?,
                };
                let attempt = self.metrics.processing(message.topic());
                let work = AssertUnwindSafe(async { self.handler.process(message).await }).catch_unwind();
                let result = tokio::select! {
                    biased;
                    _ = cancelled.cancelled() => bail!("assignment revoked during processing"),
                    _ = abort.cancelled() => bail!("processing drain deadline reached"),
                    result = tokio::time::timeout(self.timeout, work) => result,
                };
                let result = match result {
                    Ok(Ok(result)) => result,
                    Ok(Err(_)) => { self.metrics.panic("handler"); Err(anyhow!("handler panicked")) }
                    Err(_) => Err(anyhow!("handler timed out")),
                };
                attempt.finish(result.is_ok());
                result
            }
        }).buffer_unordered(self.parallelism);
        while let Some(result) = attempts.next().await {
            if let Err(error) = result {
                outcome.finish(if cancelled.is_cancelled() || abort.is_cancelled() {
                    BatchOutcome::Discarded
                } else {
                    BatchOutcome::Failed
                });
                return Err(error);
            }
        }
        outcome.finish(BatchOutcome::Success);
        Ok(())
    }
}

struct BatchGuard {
    metrics: Metrics,
    topic: String,
    finished: bool,
}
impl BatchGuard {
    fn finish(mut self, outcome: BatchOutcome) {
        self.metrics.batch_outcome(&self.topic, outcome);
        self.finished = true;
    }
}
impl Drop for BatchGuard {
    fn drop(&mut self) {
        if !self.finished {
            self.metrics
                .batch_outcome(&self.topic, BatchOutcome::Discarded);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::kafka::batch::AssignmentToken;
    use rdkafka::message::Timestamp;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct BlockedHandler {
        active: AtomicUsize,
        peak: AtomicUsize,
        started: Semaphore,
        release: Semaphore,
    }
    impl Handler for Arc<BlockedHandler> {
        async fn process(&self, _message: OwnedMessage) -> Result<()> {
            let active = self.active.fetch_add(1, Ordering::SeqCst) + 1;
            self.peak.fetch_max(active, Ordering::SeqCst);
            self.started.add_permits(1);
            let permit = self.release.acquire().await?;
            permit.forget();
            self.active.fetch_sub(1, Ordering::SeqCst);
            Ok(())
        }
    }
    fn batch_with_payloads(payloads: &[Option<&[u8]>]) -> Batch {
        Batch {
            messages: payloads
                .iter()
                .enumerate()
                .map(|(offset, payload)| {
                    OwnedMessage::new(
                        payload.map(<[u8]>::to_vec),
                        None,
                        "input-events".into(),
                        Timestamp::NotAvailable,
                        0,
                        offset as i64,
                        None,
                    )
                })
                .collect(),
            assignment: AssignmentToken {
                epoch: 1,
                cancelled: CancellationToken::new(),
            },
        }
    }
    fn batch() -> Batch {
        batch_with_payloads(&[Some(&b"hello world"[..]); 8])
    }
    #[tokio::test]
    async fn service_handler_processes_valid_message() -> Result<()> {
        let config =
            crate::config::ServiceConfig::load(std::path::Path::new("config/example.toml"))?;
        let processor = Processor::new(
            MessageHandler::Service,
            1,
            Duration::from_secs(1),
            Metrics::new(&config),
        );
        processor
            .batch(
                batch_with_payloads(&[Some(b" hello world ")]),
                &CancellationToken::new(),
            )
            .await?;
        Ok(())
    }
    #[tokio::test]
    async fn service_handler_failure_fails_batch() -> Result<()> {
        let config =
            crate::config::ServiceConfig::load(std::path::Path::new("config/example.toml"))?;
        let metrics = Metrics::new(&config);
        let processor = Processor::new(
            MessageHandler::Service,
            1,
            Duration::from_secs(1),
            metrics.clone(),
        );
        assert!(
            processor
                .batch(
                    batch_with_payloads(&[Some(b"\xff")]),
                    &CancellationToken::new()
                )
                .await
                .is_err()
        );
        assert!(metrics.render(false)?.contains("status=\"failed\"} 1"));
        Ok(())
    }
    #[tokio::test]
    async fn concurrency_limit_is_shared_across_batches() -> Result<()> {
        let config =
            crate::config::ServiceConfig::load(std::path::Path::new("config/example.toml"))?;
        let handler = Arc::new(BlockedHandler {
            active: AtomicUsize::new(0),
            peak: AtomicUsize::new(0),
            started: Semaphore::new(0),
            release: Semaphore::new(0),
        });
        let processor = Processor::new(
            handler.clone(),
            3,
            Duration::from_secs(10),
            Metrics::new(&config),
        );
        let abort = CancellationToken::new();
        let first = processor.clone();
        let first_abort = abort.clone();
        let a = tokio::spawn(async move { first.batch(batch(), &first_abort).await });
        let b = tokio::spawn(async move { processor.batch(batch(), &abort).await });
        let admission =
            tokio::time::timeout(Duration::from_secs(2), handler.started.acquire_many(3)).await??;
        admission.forget();
        assert_eq!(handler.active.load(Ordering::SeqCst), 3);
        handler.release.add_permits(16);
        a.await??;
        b.await??;
        assert_eq!(handler.peak.load(Ordering::SeqCst), 3);
        assert_eq!(handler.active.load(Ordering::SeqCst), 0);
        Ok(())
    }
    struct BlockForever;
    impl Handler for BlockForever {
        async fn process(&self, _message: OwnedMessage) -> Result<()> {
            std::future::pending().await
        }
    }
    #[tokio::test(start_paused = true)]
    async fn handler_timeout_fails_the_batch_and_releases_attempt_gauges() -> Result<()> {
        let config =
            crate::config::ServiceConfig::load(std::path::Path::new("config/example.toml"))?;
        let metrics = Metrics::new(&config);
        let processor = Processor::new(BlockForever, 2, Duration::from_secs(1), metrics.clone());
        assert!(
            processor
                .batch(batch(), &CancellationToken::new())
                .await
                .is_err()
        );
        let rendered = metrics.render(false)?;
        assert!(rendered.contains("kafka_consumer_in_flight_messages{topic=\"input-events\"} 0"));
        assert!(rendered.contains(
            "kafka_consumer_processed_messages_total{topic=\"input-events\",status=\"failed\"} 1"
        ));
        Ok(())
    }
    struct PanickingHandler;
    impl Handler for PanickingHandler {
        async fn process(&self, _message: OwnedMessage) -> Result<()> {
            panic!("fixture panic")
        }
    }
    #[tokio::test]
    async fn handler_panic_becomes_a_failed_batch_without_unwinding_the_owner() -> Result<()> {
        let config =
            crate::config::ServiceConfig::load(std::path::Path::new("config/example.toml"))?;
        let metrics = Metrics::new(&config);
        let processor =
            Processor::new(PanickingHandler, 2, Duration::from_secs(1), metrics.clone());
        assert!(
            processor
                .batch(batch(), &CancellationToken::new())
                .await
                .is_err()
        );
        assert!(
            metrics
                .render(false)?
                .contains("service_task_panics_total{component=\"handler\"} 1")
        );
        Ok(())
    }
    #[tokio::test]
    async fn revoked_batch_never_starts_processing() -> Result<()> {
        let config =
            crate::config::ServiceConfig::load(std::path::Path::new("config/example.toml"))?;
        let metrics = Metrics::new(&config);
        let processor = Processor::new(BlockForever, 2, Duration::from_secs(1), metrics.clone());
        let revoked = batch();
        revoked.assignment.cancelled.cancel();
        assert!(
            processor
                .batch(revoked, &CancellationToken::new())
                .await
                .is_err()
        );
        let rendered = metrics.render(false)?;
        assert!(rendered.contains(
            "kafka_consumer_processed_messages_total{topic=\"input-events\",status=\"success\"} 0"
        ));
        assert!(rendered.contains(
            "kafka_consumer_processed_messages_total{topic=\"input-events\",status=\"failed\"} 0"
        ));
        Ok(())
    }
    struct FailingHandler;
    impl Handler for FailingHandler {
        async fn process(&self, _message: OwnedMessage) -> Result<()> {
            bail!("fixture failed")
        }
    }
    #[tokio::test]
    async fn failed_batch_has_no_successful_barrier() -> Result<()> {
        let config =
            crate::config::ServiceConfig::load(std::path::Path::new("config/example.toml"))?;
        let metrics = Metrics::new(&config);
        let processor = Processor::new(FailingHandler, 2, Duration::from_secs(1), metrics.clone());
        assert!(
            processor
                .batch(batch(), &CancellationToken::new())
                .await
                .is_err()
        );
        assert!(metrics.render(false)?.contains("status=\"failed\"} 1"));
        Ok(())
    }
}
