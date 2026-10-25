use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex, atomic::AtomicU64},
    time::Instant,
};

use prometheus_client::{
    encoding::text::encode,
    metrics::{
        counter::Counter,
        family::{Family, MetricConstructor},
        gauge::Gauge,
        histogram::Histogram,
    },
    registry::Registry,
};

use crate::config::ServiceConfig;

#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
struct NoLabels {}
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
struct TopicLabels {
    topic: String,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
struct OutcomeLabels {
    topic: String,
    status: String,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
struct ProfileLabels {
    profile: String,
    status: String,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
struct ComponentLabels {
    component: String,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
struct HttpLabels {
    method: String,
    route: String,
    status_code: String,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
struct HttpDurationLabels {
    method: String,
    route: String,
    status_code: String,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum ClientSlot {
    Consumer { topic: String, instance: usize },
    Producer,
}

#[derive(Clone)]
pub(crate) struct Metrics {
    inner: Arc<Inner>,
}
#[derive(Clone, Debug)]
struct Buckets(Arc<[f64]>);
impl MetricConstructor<Histogram> for Buckets {
    fn new_metric(&self) -> Histogram {
        Histogram::new(self.0.iter().copied())
    }
}
type Histograms<L> = Family<L, Histogram, Buckets>;

struct Inner {
    registry: Mutex<Registry>,
    received: Family<TopicLabels, Counter>,
    processed: Family<OutcomeLabels, Counter>,
    process_duration: Histograms<OutcomeLabels>,
    processing_cancelled: Family<TopicLabels, Counter>,
    processing_in_flight: Family<TopicLabels, Gauge>,
    batches: Family<OutcomeLabels, Counter>,
    rebalances: Family<TopicLabels, Counter>,
    commits: Family<OutcomeLabels, Counter>,
    lag: Family<TopicLabels, Gauge>,
    lag_available: Family<TopicLabels, Gauge>,
    consumer_age: Family<TopicLabels, Gauge<f64, AtomicU64>>,
    consumer_available: Family<TopicLabels, Gauge>,
    published: Family<OutcomeLabels, Counter>,
    publish_duration: Histograms<OutcomeLabels>,
    publish_cancelled: Family<TopicLabels, Counter>,
    publish_in_flight: Gauge,
    queue_messages: Family<NoLabels, Gauge>,
    queue_bytes: Family<NoLabels, Gauge>,
    producer_age: Family<NoLabels, Gauge<f64, AtomicU64>>,
    producer_available: Gauge,
    ready: Gauge,
    uptime: Gauge<f64, AtomicU64>,
    build_info: Family<BuildLabels, Gauge>,
    panics: Family<ComponentLabels, Counter>,
    auth: Family<ProfileLabels, Counter>,
    http_requests: Family<HttpLabels, Counter>,
    http_duration: Histograms<HttpDurationLabels>,
    topics: Vec<String>,
    expected_consumers: BTreeMap<String, usize>,
    producer_topics: BTreeSet<String>,
    profiles: Vec<String>,
    state: Mutex<NativeState>,
    started: Instant,
    stale_after: f64,
}
#[derive(Clone, Debug, Hash, PartialEq, Eq, prometheus_client::encoding::EncodeLabelSet)]
struct BuildLabels {
    name: String,
    version: String,
}
#[derive(Clone)]
struct NativeSample {
    received: Instant,
    stats: rdkafka::statistics::Statistics,
    epoch: u64,
    assignment: BTreeSet<i32>,
}
#[derive(Default)]
struct NativeState {
    clients: BTreeMap<ClientSlot, ClientState>,
}
#[derive(Default)]
struct ClientState {
    epoch: u64,
    assignment: BTreeSet<i32>,
    sample: Option<NativeSample>,
}

impl Metrics {
    pub(crate) fn new(config: &ServiceConfig) -> Self {
        let topics: Vec<String> = config
            .kafka
            .consumer
            .topics
            .iter()
            .map(|t| t.name.clone())
            .collect();
        let expected_consumers: BTreeMap<String, usize> = config
            .kafka
            .consumer
            .topics
            .iter()
            .map(|t| (t.name.clone(), t.consumers.get()))
            .collect();
        let producer_topics: BTreeSet<String> =
            config.metrics.producer_topics.iter().cloned().collect();
        let profiles: Vec<String> = config.kafka.auth_profiles.keys().cloned().collect();
        let mut registry = Registry::default();
        let received = Family::<TopicLabels, Counter>::default();
        let processed = Family::<OutcomeLabels, Counter>::default();
        let process_duration = Histograms::new_with_constructor(Buckets(
            config
                .metrics
                .processing_duration_buckets_seconds
                .clone()
                .into(),
        ));
        let processing_cancelled = Family::default();
        let processing_in_flight = Family::default();
        let batches = Family::default();
        let rebalances = Family::default();
        let commits = Family::default();
        let lag = Family::default();
        let lag_available = Family::default();
        let consumer_age = Family::default();
        let consumer_available = Family::default();
        let published = Family::default();
        let publish_duration = Histograms::new_with_constructor(Buckets(
            config
                .metrics
                .processing_duration_buckets_seconds
                .clone()
                .into(),
        ));
        let publish_cancelled = Family::default();
        let publish_in_flight = Gauge::default();
        let queue_messages = Family::<NoLabels, Gauge>::default();
        let queue_bytes = Family::<NoLabels, Gauge>::default();
        let producer_age = Family::<NoLabels, Gauge<f64, AtomicU64>>::default();
        let producer_available = Gauge::default();
        let ready = Gauge::default();
        let uptime = Gauge::<f64, AtomicU64>::default();
        let build_info = Family::default();
        let panics = Family::default();
        let auth = Family::default();
        let http_requests = Family::default();
        let http_duration = Histograms::new_with_constructor(Buckets(
            config.metrics.http_duration_buckets_seconds.clone().into(),
        ));
        registry.register(
            "kafka_consumer_received_messages",
            "Records delivered to application",
            received.clone(),
        );
        registry.register(
            "kafka_consumer_processed_messages",
            "Completed processing attempts",
            processed.clone(),
        );
        registry.register(
            "kafka_consumer_processing_duration_seconds",
            "Processing attempt duration",
            process_duration.clone(),
        );
        registry.register(
            "kafka_consumer_processing_cancelled",
            "Cancelled processing attempts",
            processing_cancelled.clone(),
        );
        registry.register(
            "kafka_consumer_in_flight_messages",
            "Active handlers",
            processing_in_flight.clone(),
        );
        registry.register(
            "kafka_consumer_batches",
            "Terminal batches",
            batches.clone(),
        );
        registry.register("kafka_consumer_commits", "Commit outcomes", commits.clone());
        registry.register(
            "kafka_consumer_rebalances",
            "Consumer rebalances",
            rebalances.clone(),
        );
        registry.register(
            "kafka_consumer_committed_lag_offsets",
            "Committed offset lag",
            lag.clone(),
        );
        registry.register(
            "kafka_consumer_lag_available",
            "Committed lag availability",
            lag_available.clone(),
        );
        registry.register(
            "kafka_consumer_statistics_age_seconds",
            "Oldest latest consumer statistics age",
            consumer_age.clone(),
        );
        registry.register(
            "kafka_consumer_statistics_available",
            "Consumer statistics availability",
            consumer_available.clone(),
        );
        registry.register(
            "kafka_producer_messages",
            "Publish outcomes",
            published.clone(),
        );
        registry.register(
            "kafka_producer_publish_duration_seconds",
            "Publish duration",
            publish_duration.clone(),
        );
        registry.register(
            "kafka_producer_publish_cancelled",
            "Cancelled publish attempts",
            publish_cancelled.clone(),
        );
        registry.register(
            "kafka_producer_in_flight_messages",
            "Application sends awaiting result",
            publish_in_flight.clone(),
        );
        registry.register(
            "kafka_producer_queue_messages",
            "Producer queue messages",
            queue_messages.clone(),
        );
        registry.register(
            "kafka_producer_queue_bytes",
            "Producer queue bytes",
            queue_bytes.clone(),
        );
        registry.register(
            "kafka_producer_statistics_age_seconds",
            "Producer sample age",
            producer_age.clone(),
        );
        registry.register(
            "kafka_producer_statistics_available",
            "Producer statistics availability",
            producer_available.clone(),
        );
        registry.register("service_ready", "Readiness state", ready.clone());
        registry.register("service_uptime_seconds", "Process uptime", uptime.clone());
        registry.register("service_build_info", "Build identity", build_info.clone());
        registry.register(
            "service_task_panics",
            "Supervised task panics",
            panics.clone(),
        );
        registry.register(
            "kafka_auth_refresh",
            "Authentication refresh outcomes",
            auth.clone(),
        );
        registry.register(
            "http_requests",
            "Completed HTTP requests",
            http_requests.clone(),
        );
        registry.register(
            "http_request_duration_seconds",
            "Completed HTTP request duration",
            http_duration.clone(),
        );
        let inner = Self {
            inner: Arc::new(Inner {
                registry: Mutex::new(registry),
                received,
                processed,
                process_duration,
                processing_cancelled,
                processing_in_flight,
                batches,
                rebalances,
                commits,
                lag,
                lag_available,
                consumer_age,
                consumer_available,
                published,
                publish_duration,
                publish_cancelled,
                publish_in_flight,
                queue_messages,
                queue_bytes,
                producer_age,
                producer_available,
                ready,
                uptime,
                build_info,
                panics,
                auth,
                http_requests,
                http_duration,
                topics,
                expected_consumers,
                producer_topics,
                profiles,
                state: Mutex::new(NativeState::default()),
                started: Instant::now(),
                stale_after: config.metrics.native_statistics_stale_after_ms.get() as f64 / 1000.0,
            }),
        };
        for topic in &inner.inner.topics {
            inner.initialize_topic(topic);
        }
        let other_topic = "__other__".to_owned();
        for topic in inner
            .inner
            .producer_topics
            .iter()
            .chain(std::iter::once(&other_topic))
        {
            for status in ["success", "failed"] {
                let labels = OutcomeLabels {
                    topic: topic.clone(),
                    status: status.into(),
                };
                drop(inner.inner.published.get_or_create(&labels));
                drop(inner.inner.publish_duration.get_or_create(&labels));
            }
            drop(inner.inner.publish_cancelled.get_or_create(&TopicLabels {
                topic: topic.clone(),
            }));
        }
        for profile in &inner.inner.profiles {
            for status in ["success", "failed"] {
                drop(inner.inner.auth.get_or_create(&ProfileLabels {
                    profile: profile.clone(),
                    status: status.into(),
                }));
            }
        }
        let build = BuildLabels {
            name: config.service.name.clone(),
            version: env!("CARGO_PKG_VERSION").into(),
        };
        inner.inner.build_info.get_or_create(&build).set(1);
        inner
    }
    fn initialize_topic(&self, topic: &str) {
        let topic_label = TopicLabels {
            topic: topic.to_owned(),
        };
        for status in ["success", "failed"] {
            let labels = OutcomeLabels {
                topic: topic.to_owned(),
                status: status.into(),
            };
            drop(self.inner.processed.get_or_create(&labels));
            drop(self.inner.process_duration.get_or_create(&labels));
        }
        for status in ["success", "failed", "discarded"] {
            drop(self.inner.batches.get_or_create(&OutcomeLabels {
                topic: topic.to_owned(),
                status: status.into(),
            }));
        }
        for status in ["success", "failed"] {
            drop(self.inner.commits.get_or_create(&OutcomeLabels {
                topic: topic.to_owned(),
                status: status.into(),
            }));
        }
        drop(self.inner.received.get_or_create(&topic_label));
        drop(self.inner.processing_cancelled.get_or_create(&topic_label));
        drop(self.inner.processing_in_flight.get_or_create(&topic_label));
        self.inner
            .consumer_available
            .get_or_create(&topic_label)
            .set(0);
        self.inner.lag_available.get_or_create(&topic_label).set(0);
    }
    fn consumer_topic(&self, topic: &str) -> String {
        if self.inner.topics.iter().any(|known| known == topic) {
            topic.to_owned()
        } else {
            "__other__".to_owned()
        }
    }
    pub(crate) fn received(&self, topic: &str) {
        self.inner
            .received
            .get_or_create(&TopicLabels {
                topic: self.consumer_topic(topic),
            })
            .inc();
    }
    pub(crate) fn processing(&self, topic: &str) -> Attempt {
        Attempt::new(self.clone(), self.consumer_topic(topic), false)
    }
    pub(crate) fn publishing(&self, topic: &str) -> Attempt {
        Attempt::new(self.clone(), self.producer_topic(topic), true)
    }
    fn producer_topic(&self, topic: &str) -> String {
        if self.inner.producer_topics.contains(topic) {
            topic.to_owned()
        } else {
            "__other__".into()
        }
    }
    pub(crate) fn batch_outcome(&self, topic: &str, outcome: BatchOutcome) {
        let status = match outcome {
            BatchOutcome::Success => "success",
            BatchOutcome::Failed => "failed",
            BatchOutcome::Discarded => "discarded",
        };
        self.inner
            .batches
            .get_or_create(&OutcomeLabels {
                topic: self.consumer_topic(topic),
                status: status.into(),
            })
            .inc();
    }
    pub(crate) fn commit(&self, topic: &str, success: bool) {
        self.inner
            .commits
            .get_or_create(&OutcomeLabels {
                topic: self.consumer_topic(topic),
                status: status(success).into(),
            })
            .inc();
    }
    pub(crate) fn rebalance(&self, topic: &str) {
        self.inner
            .rebalances
            .get_or_create(&TopicLabels {
                topic: self.consumer_topic(topic),
            })
            .inc();
    }
    #[cfg(feature = "aws-iam")]
    pub(crate) fn auth_refresh(&self, profile: &str, success: bool) {
        if self.inner.profiles.iter().any(|p| p == profile) {
            self.inner
                .auth
                .get_or_create(&ProfileLabels {
                    profile: profile.to_owned(),
                    status: status(success).into(),
                })
                .inc();
        }
    }
    pub(crate) fn panic(&self, component: &str) {
        let component = match component {
            "consumer" | "producer" | "http" | "health" | "supervisor" | "handler" => component,
            _ => "other",
        };
        self.inner
            .panics
            .get_or_create(&ComponentLabels {
                component: component.into(),
            })
            .inc();
    }
    pub(crate) fn http(&self, method: &str, route: &str, status_code: u16, seconds: f64) {
        let method = match method {
            "GET" | "POST" | "PUT" | "DELETE" | "PATCH" | "HEAD" | "OPTIONS" => method,
            _ => "OTHER",
        };
        let route = match route {
            "/health" | "/info" | "/prometheus" => route,
            _ => "__unmatched__",
        };
        let labels = HttpLabels {
            method: method.into(),
            route: route.into(),
            status_code: status_code.to_string(),
        };
        self.inner.http_requests.get_or_create(&labels).inc();
        self.inner
            .http_duration
            .get_or_create(&HttpDurationLabels {
                method: method.into(),
                route: route.into(),
                status_code: status_code.to_string(),
            })
            .observe(seconds);
    }
    pub(crate) fn render(&self, ready: bool) -> anyhow::Result<String> {
        let registry = self
            .inner
            .registry
            .lock()
            .map_err(|_| anyhow::anyhow!("metrics registry lock poisoned"))?;
        self.inner.ready.set(if ready { 1 } else { 0 });
        self.inner
            .uptime
            .set(self.inner.started.elapsed().as_secs_f64());
        self.refresh_native();
        let mut output = String::new();
        encode(&mut output, &registry)?;
        Ok(output)
    }
    pub(crate) fn client_started(&self, slot: &ClientSlot) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.clients.insert(slot.clone(), ClientState::default());
        }
    }
    pub(crate) fn client_stopped(&self, slot: &ClientSlot) {
        if let Ok(mut state) = self.inner.state.lock() {
            state.clients.remove(slot);
        }
    }
    pub(crate) fn assignment(&self, slot: &ClientSlot, epoch: u64, partitions: &[i32]) {
        if let Ok(mut state) = self.inner.state.lock() {
            let client = state.clients.entry(slot.clone()).or_default();
            client.epoch = epoch;
            client.assignment = partitions.iter().copied().collect();
            client.sample = None;
        }
    }
    pub(crate) fn stats(
        &self,
        slot: &ClientSlot,
        epoch: u64,
        statistics: rdkafka::statistics::Statistics,
    ) {
        if let Ok(mut state) = self.inner.state.lock()
            && let Some(client) = state.clients.get_mut(slot)
            && client.epoch == epoch
        {
            client.sample = Some(NativeSample {
                received: Instant::now(),
                stats: statistics,
                epoch,
                assignment: client.assignment.clone(),
            });
        }
    }
    fn refresh_native(&self) {
        let Ok(state) = self.inner.state.lock() else {
            return;
        };
        let stale = self.inner.stale_after;
        let producer = state
            .clients
            .get(&ClientSlot::Producer)
            .and_then(|c| c.sample.as_ref());
        self.inner.producer_available.set(i64::from(
            producer.is_some_and(|s| s.received.elapsed().as_secs_f64() <= stale),
        ));
        if let Some(sample) = producer {
            let age = sample.received.elapsed().as_secs_f64();
            self.inner.producer_age.get_or_create(&NoLabels {}).set(age);
            if age <= stale {
                self.inner
                    .queue_messages
                    .get_or_create(&NoLabels {})
                    .set(sample.stats.msg_cnt as i64);
                self.inner
                    .queue_bytes
                    .get_or_create(&NoLabels {})
                    .set(sample.stats.msg_size as i64);
            } else {
                self.inner.queue_messages.remove(&NoLabels {});
                self.inner.queue_bytes.remove(&NoLabels {});
            }
        } else {
            self.inner.queue_messages.remove(&NoLabels {});
            self.inner.queue_bytes.remove(&NoLabels {});
            self.inner.producer_age.remove(&NoLabels {});
        }
        for topic in &self.inner.topics {
            let labels = TopicLabels {
                topic: topic.clone(),
            };
            let slots: Vec<&ClientState> = state
                .clients
                .iter()
                .filter_map(|(slot, client)| match slot {
                    ClientSlot::Consumer { topic: t, .. } if t == topic => Some(client),
                    _ => None,
                })
                .collect();
            let all_have_samples = slots.len()
                == self
                    .inner
                    .expected_consumers
                    .get(topic)
                    .copied()
                    .unwrap_or(0)
                && !slots.is_empty()
                && slots.iter().all(|c| c.sample.is_some());
            let complete = all_have_samples
                && slots.iter().all(|c| {
                    c.sample.as_ref().is_some_and(|s| {
                        s.received.elapsed().as_secs_f64() <= stale && s.epoch == c.epoch
                    })
                });
            if all_have_samples {
                let age = slots
                    .iter()
                    .filter_map(|client| client.sample.as_ref())
                    .map(|sample| sample.received.elapsed().as_secs_f64())
                    .fold(0.0_f64, f64::max);
                self.inner.consumer_age.get_or_create(&labels).set(age);
            }
            self.inner
                .consumer_available
                .get_or_create(&labels)
                .set(i64::from(complete));
            let samples: Vec<&NativeSample> =
                slots.iter().filter_map(|c| c.sample.as_ref()).collect();
            if !complete {
                self.inner.lag_available.get_or_create(&labels).set(0);
                self.inner.lag.remove(&labels);
                if !all_have_samples {
                    self.inner.consumer_age.remove(&labels);
                }
                continue;
            }
            let mut lag_sum = 0_i64;
            let mut covered = BTreeSet::new();
            let mut age = 0.0_f64;
            let mut valid = true;
            for sample in samples {
                age = age.max(sample.received.elapsed().as_secs_f64());
                for (name, topic_stats) in &sample.stats.topics {
                    if name != topic {
                        continue;
                    }
                    for (partition, stats) in &topic_stats.partitions {
                        if sample.assignment.contains(partition) {
                            if stats.consumer_lag < 0 || !covered.insert(*partition) {
                                valid = false;
                            } else {
                                lag_sum = lag_sum.saturating_add(stats.consumer_lag);
                            }
                        }
                    }
                }
            }
            self.inner.consumer_age.get_or_create(&labels).set(age);
            let expected: BTreeSet<i32> = slots
                .iter()
                .flat_map(|c| c.assignment.iter().copied())
                .collect();
            valid &= !expected.is_empty() && expected == covered;
            self.inner
                .lag_available
                .get_or_create(&labels)
                .set(i64::from(valid));
            if valid {
                self.inner.lag.get_or_create(&labels).set(lag_sum);
            } else {
                self.inner.lag.remove(&labels);
            }
        }
    }
}
fn status(success: bool) -> &'static str {
    if success { "success" } else { "failed" }
}

pub(crate) enum BatchOutcome {
    Success,
    Failed,
    Discarded,
}

#[must_use]
pub(crate) struct Attempt {
    metrics: Metrics,
    topic: String,
    publishing: bool,
    started: Instant,
    finished: bool,
}
impl Attempt {
    fn new(metrics: Metrics, topic: String, publishing: bool) -> Self {
        if publishing {
            metrics.inner.publish_in_flight.inc();
        } else {
            metrics
                .inner
                .processing_in_flight
                .get_or_create(&TopicLabels {
                    topic: topic.clone(),
                })
                .inc();
        }
        Self {
            metrics,
            topic,
            publishing,
            started: Instant::now(),
            finished: false,
        }
    }
    pub(crate) fn finish(mut self, success: bool) {
        self.record(success);
    }
    fn record(&mut self, success: bool) {
        if self.finished {
            return;
        }
        let labels = OutcomeLabels {
            topic: self.topic.clone(),
            status: status(success).into(),
        };
        if self.publishing {
            self.metrics.inner.published.get_or_create(&labels).inc();
            self.metrics
                .inner
                .publish_duration
                .get_or_create(&labels)
                .observe(self.started.elapsed().as_secs_f64());
            self.metrics.inner.publish_in_flight.dec();
        } else {
            self.metrics.inner.processed.get_or_create(&labels).inc();
            self.metrics
                .inner
                .process_duration
                .get_or_create(&labels)
                .observe(self.started.elapsed().as_secs_f64());
            self.metrics
                .inner
                .processing_in_flight
                .get_or_create(&TopicLabels {
                    topic: self.topic.clone(),
                })
                .dec();
        }
        self.finished = true;
    }
}
impl Drop for Attempt {
    fn drop(&mut self) {
        if !self.finished {
            if self.publishing {
                self.metrics
                    .inner
                    .publish_cancelled
                    .get_or_create(&TopicLabels {
                        topic: self.topic.clone(),
                    })
                    .inc();
                self.metrics.inner.publish_in_flight.dec();
            } else {
                self.metrics
                    .inner
                    .processing_cancelled
                    .get_or_create(&TopicLabels {
                        topic: self.topic.clone(),
                    })
                    .inc();
                self.metrics
                    .inner
                    .processing_in_flight
                    .get_or_create(&TopicLabels {
                        topic: self.topic.clone(),
                    })
                    .dec();
            }
            self.finished = true;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    fn metrics() -> Metrics {
        let config = ServiceConfig::load(Path::new("config/example.toml"));
        match config {
            Ok(config) => Metrics::new(&config),
            Err(_) => panic!("example configuration should load"),
        }
    }

    #[test]
    fn cancelled_processing_counts_cancellation_without_failure_and_releases_in_flight() {
        let metrics = metrics();
        drop(metrics.processing("input-events"));
        let output = metrics.render(false);
        assert!(output.is_ok());
        if let Ok(output) = output {
            assert!(
                output.contains(
                    "kafka_consumer_processing_cancelled_total{topic=\"input-events\"} 1"
                )
            );
            assert!(!output.contains("kafka_consumer_processed_messages_total{topic=\"input-events\",status=\"failed\"} 1"));
            assert!(output.contains("kafka_consumer_in_flight_messages{topic=\"input-events\"} 0"));
        }
    }

    #[test]
    fn lag_requires_nonempty_complete_assignment_and_a_fresh_epoch() -> anyhow::Result<()> {
        use rdkafka::statistics::{Partition, Statistics, Topic};
        let metrics = metrics();
        let first = ClientSlot::Consumer {
            topic: "input-events".into(),
            instance: 0,
        };
        let second = ClientSlot::Consumer {
            topic: "input-events".into(),
            instance: 1,
        };
        for slot in [&first, &second] {
            metrics.client_started(slot);
            metrics.assignment(slot, 1, &[]);
            metrics.stats(slot, 1, Statistics::default());
        }
        let lag_available = "kafka_consumer_lag_available{topic=\"input-events\"} ";
        let lag = "kafka_consumer_committed_lag_offsets{topic=\"input-events\"} ";
        let output = metrics.render(true)?;
        assert!(output.contains(&format!("{lag_available}0")));
        assert!(!output.contains(lag));
        metrics.assignment(&first, 2, &[0]);
        let mut statistics = Statistics::default();
        let mut topic = Topic::default();
        topic.partitions.insert(
            0,
            Partition {
                consumer_lag: 7,
                ..Partition::default()
            },
        );
        statistics.topics.insert("input-events".into(), topic);
        metrics.stats(&first, 2, statistics.clone());
        let output = metrics.render(true)?;
        assert!(output.contains(&format!("{lag_available}1")));
        assert!(output.contains(&format!("{lag}7")));
        metrics.assignment(&first, 3, &[0, 1]);
        metrics.stats(&first, 2, statistics.clone());
        assert!(!metrics.render(true)?.contains(lag));
        metrics.stats(&first, 3, statistics);
        assert!(!metrics.render(true)?.contains(lag));
        metrics.client_stopped(&second);
        let output = metrics.render(true)?;
        assert!(output.contains("kafka_consumer_statistics_available{topic=\"input-events\"} 0"));
        assert!(!output.contains(lag));
        Ok(())
    }

    #[test]
    fn http_latency_labels_include_the_response_status() -> anyhow::Result<()> {
        let metrics = metrics();
        metrics.http("GET", "/health", 503, 0.01);
        let output = metrics.render(false)?;
        assert!(output.contains("http_request_duration_seconds_count{method=\"GET\",route=\"/health\",status_code=\"503\"} 1"));
        Ok(())
    }

    #[test]
    fn http_labels_normalize_unknown_method_and_path() {
        let metrics = metrics();
        metrics.http("CUSTOM", "/private/123?secret=value", 200, 0.01);
        let output = metrics.render(true);
        assert!(output.is_ok());
        if let Ok(output) = output {
            assert!(
                output.contains("method=\"OTHER\",route=\"__unmatched__\",status_code=\"200\"")
            );
            assert!(!output.contains("private/123"));
            assert!(!output.contains("secret=value"));
        }
    }
}
