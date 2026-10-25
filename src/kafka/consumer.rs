use std::{
    collections::BTreeMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use anyhow::{Result, anyhow, bail, ensure};
use rdkafka::{
    ClientConfig, Message, TopicPartitionList,
    consumer::{BaseConsumer, CommitMode, Consumer},
    message::OwnedMessage,
};
use tokio::{runtime::Handle, task::JoinHandle};
use tokio_util::sync::CancellationToken;

use super::{
    batch::{AssignmentToken, Batch, message_bytes, next_offsets},
    context::{ClientRegistration, KafkaContext},
};
use crate::{
    metrics::Metrics,
    processing::{Handler, Processor},
};

#[derive(Default)]
pub(crate) struct ProbeState {
    last_ok: Mutex<Option<Instant>>,
    drained: AtomicBool,
}
impl ProbeState {
    fn ok(&self) {
        if let Ok(mut time) = self.last_ok.lock() {
            *time = Some(Instant::now());
        }
    }
    pub(crate) fn drained(&self) -> bool {
        self.drained.load(Ordering::Acquire)
    }
    pub(crate) fn fresh(&self, age: Duration) -> bool {
        self.last_ok
            .lock()
            .is_ok_and(|time| time.is_some_and(|time| time.elapsed() <= age))
    }
}
pub(crate) struct ConsumerSettings {
    pub(crate) topic: String,
    pub(crate) properties: BTreeMap<String, String>,
    pub(crate) batch_size: usize,
    pub(crate) max_batch_bytes: usize,
    pub(crate) batch_wait: Duration,
    pub(crate) commit_timeout: Duration,
    pub(crate) probe_interval: Duration,
    pub(crate) probe_timeout: Duration,
}
#[derive(Clone)]
pub(crate) struct StopSignals {
    pub(crate) intake: CancellationToken,
    pub(crate) abort: CancellationToken,
    pub(crate) failure: CancellationToken,
    pub(crate) close: CancellationToken,
}
impl StopSignals {
    fn fail(&self) {
        self.failure.cancel();
        self.intake.cancel();
        self.abort.cancel();
    }
}
struct FailureGuard<'a> {
    signals: &'a StopSignals,
    clean: bool,
}
impl Drop for FailureGuard<'_> {
    fn drop(&mut self) {
        if !self.clean {
            self.signals.fail();
        }
    }
}
struct Work {
    offsets: TopicPartitionList,
    token: AssignmentToken,
    handle: JoinHandle<Result<()>>,
}
struct Commit {
    offsets: TopicPartitionList,
    token: AssignmentToken,
    deadline: Instant,
    metrics: Metrics,
    topic: String,
    confirmed: bool,
    handle: JoinHandle<Result<()>>,
}
impl Commit {
    fn confirm(mut self) {
        self.confirmed = true;
        self.metrics.commit(&self.topic, true);
    }
}
impl Drop for Commit {
    fn drop(&mut self) {
        if !self.confirmed {
            self.metrics.commit(&self.topic, false);
        }
    }
}
enum Pending {
    Processing(Work),
    Committing(Commit),
}
impl Pending {
    fn token(&self) -> &AssignmentToken {
        match self {
            Self::Processing(work) => &work.token,
            Self::Committing(commit) => &commit.token,
        }
    }
}

pub(crate) fn run<H: Handler>(
    settings: ConsumerSettings,
    context: KafkaContext,
    processor: Processor<H>,
    signals: StopSignals,
    progress: Arc<ProbeState>,
    runtime: Handle,
) -> Result<()> {
    let _registration = ClientRegistration::new(&context);
    let mut properties = ClientConfig::new();
    for (key, value) in &settings.properties {
        properties.set(key, value);
    }
    let consumer: BaseConsumer<KafkaContext> = properties
        .create_with_context(context.clone())
        .map_err(|_| {
            signals.fail();
            anyhow!("consumer configuration or authentication is invalid")
        })?;
    let consumer = Arc::new(consumer);
    let mut guard = FailureGuard {
        signals: &signals,
        clean: false,
    };
    consumer
        .subscribe(&[&settings.topic])
        .map_err(|_| anyhow!("Kafka subscription failed"))?;
    let mut pending = None;
    let mut result = owner_loop(
        &consumer,
        &settings,
        &context,
        &processor,
        &signals,
        &progress,
        &runtime,
        &mut pending,
    );
    if result.is_ok() {
        result = await_peer_drain(&consumer, &context, &signals, &progress);
    }
    if result.is_err() {
        signals.fail();
    }
    match pending {
        Some(Pending::Processing(work)) => {
            work.handle.abort();
            let _ = runtime.block_on(work.handle);
        }
        Some(Pending::Committing(mut commit)) => {
            let _ = runtime.block_on(&mut commit.handle);
        }
        None => {}
    }
    consumer.unsubscribe();
    guard.clean = result.is_ok();
    result
}

#[allow(clippy::too_many_arguments)]
fn owner_loop<H: Handler>(
    consumer: &Arc<BaseConsumer<KafkaContext>>,
    settings: &ConsumerSettings,
    context: &KafkaContext,
    processor: &Processor<H>,
    signals: &StopSignals,
    progress: &ProbeState,
    runtime: &Handle,
    pending: &mut Option<Pending>,
) -> Result<()> {
    let mut messages = Vec::new();
    let mut batch_bytes = 0usize;
    let mut batch_started = Instant::now();
    let mut token = context.fence.snapshot()?;
    let mut deferred: Option<(OwnedMessage, AssignmentToken)> = None;
    let mut next_probe = Instant::now();
    loop {
        ensure!(!signals.abort.is_cancelled(), "consumer drain was aborted");
        ensure!(
            !context.has_failed(),
            "Kafka authentication or authorization failed"
        );
        if matches!(pending, Some(Pending::Processing(work)) if work.handle.is_finished()) {
            let Some(Pending::Processing(mut completed)) = pending.take() else {
                unreachable!()
            };
            runtime
                .block_on(&mut completed.handle)
                .map_err(|_| anyhow!("batch processing task panicked"))?
                .map_err(|_| anyhow!("batch handler failed"))?;
            ensure!(
                context.fence.allows(&completed.token, &completed.offsets)
                    && !consumer.assignment_lost(),
                "assignment changed before commit; restart required for replay"
            );
            let commit_consumer = Arc::clone(consumer);
            let commit_offsets = completed.offsets.clone();
            let handle = runtime.spawn_blocking(move || {
                commit_consumer
                    .commit(&commit_offsets, CommitMode::Sync)
                    .map_err(|_| anyhow!("offset commit failed or was not confirmed"))
            });
            let commit = Commit {
                offsets: completed.offsets,
                token: completed.token,
                deadline: Instant::now() + settings.commit_timeout,
                metrics: context.metrics.clone(),
                topic: settings.topic.clone(),
                confirmed: false,
                handle,
            };
            *pending = Some(Pending::Committing(commit));
        }
        if let Some(Pending::Committing(commit)) = pending.as_ref() {
            ensure!(
                context.fence.allows(&commit.token, &commit.offsets) && !consumer.assignment_lost(),
                "assignment changed during commit; restart required for replay"
            );
            if commit.handle.is_finished() {
                let Some(Pending::Committing(mut commit)) = pending.take() else {
                    unreachable!()
                };
                runtime
                    .block_on(&mut commit.handle)
                    .map_err(|_| anyhow!("commit worker panicked"))??;
                commit.confirm();
            } else {
                ensure!(
                    Instant::now() < commit.deadline,
                    "offset commit confirmation deadline expired"
                );
            }
        }
        if pending.is_none()
            && messages.is_empty()
            && let Some((message, deferred_token)) = deferred.take()
        {
            ensure!(
                !deferred_token.cancelled.is_cancelled(),
                "deferred record lost its assignment; restart required for replay"
            );
            token = deferred_token;
            batch_bytes = message_bytes(&message);
            messages.push(message);
            batch_started = Instant::now();
        }
        if pending.is_none()
            && !messages.is_empty()
            && (messages.len() >= settings.batch_size
                || batch_bytes >= settings.max_batch_bytes
                || batch_started.elapsed() >= settings.batch_wait
                || deferred.is_some()
                || signals.intake.is_cancelled())
        {
            ensure!(
                !token.cancelled.is_cancelled(),
                "batch lost its assignment; restart required for replay"
            );
            set_paused(consumer, context, true)?;
            let offsets = next_offsets(&messages)?;
            let batch = Batch {
                messages: std::mem::take(&mut messages),
                assignment: token.clone(),
            };
            let processor = processor.clone();
            let abort = signals.abort.clone();
            *pending = Some(Pending::Processing(Work {
                offsets,
                token: token.clone(),
                handle: runtime.spawn(async move { processor.batch(batch, &abort).await }),
            }));
            batch_bytes = 0;
        }
        if signals.intake.is_cancelled()
            && pending.is_none()
            && messages.is_empty()
            && deferred.is_none()
        {
            return Ok(());
        }
        let paused = pending.is_some() || signals.intake.is_cancelled();
        if context.paused.load(Ordering::Acquire) != paused {
            set_paused(consumer, context, paused)?;
        }
        if Instant::now() >= next_probe {
            if consumer
                .fetch_metadata(Some(&settings.topic), settings.probe_timeout)
                .is_ok()
            {
                progress.ok();
            }
            next_probe = Instant::now() + settings.probe_interval;
        }
        match consumer.poll(Duration::from_millis(25)) {
            Some(Ok(message)) => {
                context.metrics.received(message.topic());
                ensure!(
                    !paused,
                    "record delivered while intake was paused; restart required for replay"
                );
                let bytes = message_bytes(&message);
                ensure!(
                    bytes <= settings.max_batch_bytes,
                    "record exceeds configured application byte limit"
                );
                let current = context.fence.snapshot()?;
                if !messages.is_empty() {
                    ensure!(
                        current.epoch == token.epoch && !token.cancelled.is_cancelled(),
                        "assignment changed while collecting; restart required for replay"
                    );
                }
                if messages.is_empty() {
                    token = current.clone();
                    batch_started = Instant::now();
                }
                if batch_bytes.saturating_add(bytes) > settings.max_batch_bytes {
                    ensure!(deferred.is_none(), "deferred record capacity exhausted");
                    deferred = Some((message.detach(), current));
                } else {
                    batch_bytes = batch_bytes.saturating_add(bytes);
                    messages.push(message.detach());
                }
            }
            Some(Err(_)) => bail!("Kafka consumer poll failed"),
            None => {}
        }
        if !messages.is_empty() {
            ensure!(
                !token.cancelled.is_cancelled(),
                "assignment changed while collecting; restart required for replay"
            );
        }
        if let Some(work) = pending.as_ref() {
            ensure!(
                !work.token().cancelled.is_cancelled(),
                "assignment changed during processing or commit; restart required for replay"
            );
        }
    }
}
fn await_peer_drain(
    consumer: &BaseConsumer<KafkaContext>,
    context: &KafkaContext,
    signals: &StopSignals,
    progress: &ProbeState,
) -> Result<()> {
    set_paused(consumer, context, true)?;
    progress.drained.store(true, Ordering::Release);
    while !signals.close.is_cancelled() && !signals.abort.is_cancelled() {
        ensure!(
            !context.has_failed(),
            "Kafka failed while awaiting peer drain"
        );
        ensure!(
            consumer.poll(Duration::from_millis(25)).is_none(),
            "record or error delivered while awaiting peer drain"
        );
    }
    Ok(())
}

fn set_paused(
    consumer: &BaseConsumer<KafkaContext>,
    context: &KafkaContext,
    paused: bool,
) -> Result<()> {
    context.paused.store(paused, Ordering::Release);
    let assignment = consumer
        .assignment()
        .map_err(|_| anyhow!("consumer assignment unavailable"))?;
    let result = if paused {
        consumer.pause(&assignment)
    } else {
        consumer.resume(&assignment)
    };
    result.map_err(|_| anyhow!("consumer pause or resume failed"))
}
