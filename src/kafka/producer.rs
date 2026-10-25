use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Result, anyhow, bail};
use rdkafka::{
    ClientConfig, Message,
    error::{KafkaError, RDKafkaErrorCode},
    message::OwnedMessage,
    producer::{FutureProducer, FutureRecord, Producer as NativeProducer},
};

use super::context::{ClientRegistration, KafkaContext};
use crate::metrics::Metrics;

#[derive(Clone)]
pub(crate) struct Producer {
    native: FutureProducer<KafkaContext>,
    context: KafkaContext,
    metrics: Metrics,
    enqueue_timeout: Duration,
    _registration: Arc<ClientRegistration>,
}
impl Producer {
    pub(crate) fn new(
        properties: BTreeMap<String, String>,
        context: KafkaContext,
        enqueue_timeout: Duration,
    ) -> Result<Self> {
        let mut config = ClientConfig::new();
        for (key, value) in properties {
            config.set(key, value);
        }
        let registration = Arc::new(ClientRegistration::new(&context));
        let native = config
            .create_with_context(context.clone())
            .map_err(|_| anyhow!("producer configuration or authentication is invalid"))?;
        Ok(Self {
            native,
            metrics: context.metrics.clone(),
            context,
            enqueue_timeout,
            _registration: registration,
        })
    }
    pub(crate) async fn publish(&self, topic: &str, mut message: OwnedMessage) -> Result<()> {
        let attempt = self.metrics.publishing(topic);
        let headers = message.detach_headers();
        let mut record: FutureRecord<'_, [u8], [u8]> = FutureRecord::to(topic);
        if let Some(key) = message.key() {
            record = record.key(key);
        }
        if let Some(payload) = message.payload() {
            record = record.payload(payload);
        }
        if let Some(headers) = headers {
            record = record.headers(headers);
        }
        if let Some(timestamp) = message.timestamp().to_millis() {
            record = record.timestamp(timestamp);
        }
        let deadline = tokio::time::Instant::now() + self.enqueue_timeout;
        let delivery = loop {
            match self.native.send_result(record) {
                Ok(delivery) => break delivery,
                Err((KafkaError::MessageProduction(RDKafkaErrorCode::QueueFull), returned)) => {
                    record = returned;
                    let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        attempt.finish(false);
                        bail!("producer enqueue timed out");
                    }
                    tokio::time::sleep(remaining.min(Duration::from_millis(10))).await;
                }
                Err(_) => {
                    attempt.finish(false);
                    bail!("producer enqueue failed");
                }
            }
        };
        let success = matches!(delivery.await, Ok(Ok(_)));
        attempt.finish(success);
        if !success {
            bail!("producer delivery failed or was not confirmed");
        }
        Ok(())
    }
    pub(crate) async fn probe(&self, timeout: Duration) -> Result<()> {
        let producer = self.native.clone();
        tokio::task::spawn_blocking(move || {
            producer
                .client()
                .fetch_metadata(None, timeout)
                .map(|_| ())
                .map_err(|_| anyhow!("producer Kafka metadata probe failed"))
        })
        .await
        .map_err(|_| anyhow!("producer probe task failed"))??;
        Ok(())
    }
    pub(crate) fn failed(&self) -> bool {
        self.context.has_failed()
    }
    pub(crate) async fn flush(&self, timeout: Duration) -> Result<()> {
        let producer = self.native.clone();
        tokio::task::spawn_blocking(move || {
            producer
                .flush(timeout)
                .map_err(|_| anyhow!("producer flush was not confirmed"))
        })
        .await
        .map_err(|_| anyhow!("producer flush task failed"))?
    }
}
