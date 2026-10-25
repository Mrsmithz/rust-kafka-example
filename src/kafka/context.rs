use std::{
    error::Error,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use rdkafka::{
    client::{ClientContext, OAuthToken},
    config::RDKafkaLogLevel,
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance},
    error::{KafkaError, RDKafkaErrorCode},
    statistics::Statistics,
    util::Timeout,
};

use super::{auth::AuthHandle, batch::Fence};
use crate::metrics::{ClientSlot, Metrics};

#[derive(Clone)]
pub(crate) struct KafkaContext {
    pub(crate) slot: ClientSlot,
    pub(crate) metrics: Metrics,
    pub(crate) auth: Option<AuthHandle>,
    pub(super) fence: Arc<Fence>,
    pub(super) paused: Arc<AtomicBool>,
    pub(crate) fatal: Arc<AtomicBool>,
}
impl KafkaContext {
    pub(crate) fn new(slot: ClientSlot, metrics: Metrics, auth: Option<AuthHandle>) -> Self {
        Self {
            slot,
            metrics,
            auth,
            fence: Arc::new(Fence::new()),
            paused: Arc::new(AtomicBool::new(false)),
            fatal: Arc::new(AtomicBool::new(false)),
        }
    }
    pub(crate) fn has_failed(&self) -> bool {
        self.fatal.load(Ordering::Acquire)
            || self.auth.as_ref().is_some_and(|auth| !auth.is_valid())
    }
}
impl ClientContext for KafkaContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(&self, _config: Option<&str>) -> Result<OAuthToken, Box<dyn Error>> {
        self.auth
            .as_ref()
            .ok_or_else(|| {
                Box::new(std::io::Error::other(
                    "OAuth authentication is not configured",
                )) as Box<dyn Error>
            })?
            .token()
    }
    fn stats(&self, statistics: Statistics) {
        if let Ok(token) = self.fence.snapshot() {
            self.metrics.stats(&self.slot, token.epoch, statistics);
        }
    }
    fn error(&self, error: KafkaError, _reason: &str) {
        if matches!(
            error.rdkafka_error_code(),
            Some(
                RDKafkaErrorCode::SaslAuthenticationFailed
                    | RDKafkaErrorCode::TopicAuthorizationFailed
                    | RDKafkaErrorCode::GroupAuthorizationFailed
                    | RDKafkaErrorCode::ClusterAuthorizationFailed
                    | RDKafkaErrorCode::Authentication
            )
        ) {
            self.fatal.store(true, Ordering::Release);
        }
    }
    fn log(&self, level: RDKafkaLogLevel, _facility: &str, _message: &str) {
        if matches!(
            level,
            RDKafkaLogLevel::Emerg
                | RDKafkaLogLevel::Alert
                | RDKafkaLogLevel::Critical
                | RDKafkaLogLevel::Error
        ) {
            tracing::debug!(
                ?level,
                "Kafka client diagnostic suppressed to protect credentials"
            );
        }
    }
}
impl ConsumerContext for KafkaContext {
    fn pre_rebalance(&self, _consumer: &BaseConsumer<Self>, _rebalance: &Rebalance<'_>) {
        match self.fence.replace([]) {
            Ok(epoch) => self.metrics.assignment(&self.slot, epoch, &[]),
            Err(_) => self.fatal.store(true, Ordering::Release),
        }
        if let ClientSlot::Consumer { topic, .. } = &self.slot {
            self.metrics.rebalance(topic);
        }
    }
    fn post_rebalance(&self, consumer: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        if matches!(rebalance, Rebalance::Error(_)) {
            self.fatal.store(true, Ordering::Release);
            return;
        }
        let assignment = match consumer.assignment() {
            Ok(assignment) => assignment,
            Err(_) => {
                self.fatal.store(true, Ordering::Release);
                return;
            }
        };
        let partitions: Vec<_> = assignment
            .elements()
            .iter()
            .map(|p| p.partition())
            .collect();
        match self.fence.replace(partitions.iter().copied()) {
            Ok(epoch) => self.metrics.assignment(&self.slot, epoch, &partitions),
            Err(_) => self.fatal.store(true, Ordering::Release),
        }
        if self.paused.load(Ordering::Acquire) && consumer.pause(&assignment).is_err() {
            self.fatal.store(true, Ordering::Release);
        }
    }
    fn main_queue_min_poll_interval(&self) -> Timeout {
        Timeout::After(Duration::from_millis(25))
    }
}

pub(crate) struct ClientRegistration {
    slot: ClientSlot,
    metrics: Metrics,
}
impl ClientRegistration {
    pub(crate) fn new(context: &KafkaContext) -> Self {
        context.metrics.client_started(&context.slot);
        Self {
            slot: context.slot.clone(),
            metrics: context.metrics.clone(),
        }
    }
}
impl Drop for ClientRegistration {
    fn drop(&mut self) {
        self.metrics.client_stopped(&self.slot);
    }
}
