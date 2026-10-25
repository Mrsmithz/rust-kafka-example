# Rust Kafka service development plan

Status. Proposed design only. No service implementation exists yet. The repository currently contains Rust development skills, not application code.

## 1. Scope and proposed defaults

Build one Rust executable with Kafka consumers, a shared Kafka producer, and an HTTP server that can also serve HTTPS directly.

- Create a configurable number of consumer instances for each topic.
- Collect records into batches and process records concurrently. Do not promise message ordering.
- Configure consumer count, batch limits, global processing concurrency, and standard Kafka client properties in TOML.
- Expose three routes, `GET /health`, `GET /info`, and `GET /prometheus`.
- Export Prometheus metrics for consumers, producers, and the service. Distinguish consumer processing success and failure by topic.
- Use at-least-once processing. Never commit unfinished work.
- Keep the producer standard. Do not add transactions, a custom buffering layer, or a producer HTTP endpoint.
- Support configurable Kafka TLS, mutual TLS, SASL PLAIN, SASL SCRAM, and AWS MSK IAM authentication for both consumers and producers.

The initial recommendation is a batch barrier per consumer. Each consumer processes one batch at a time, with concurrent handlers inside that batch. A single global semaphore limits active handlers across all topics and instances. Consumer polling continues independently of handlers.

Batch barriers reduce bookkeeping compared with pipelining several batches per consumer. A slow message delays the next batch for that consumer. Other consumers continue. Revisit pipelining only if workload measurements justify the extra completion tracking.

Proposed local defaults appear in [config/example.toml](config/example.toml). The AWS deployment example is [config/aws-msk-iam.toml](config/aws-msk-iam.toml). They are design examples until configuration loading is implemented. Numeric limits are starting values, not capacity claims.

## 2. Dependencies

Use these crates as the initial dependency shortlist. The versions below were checked against the crates.io API on 2026-10-25. They are not yet a compiled compatibility set.

| Dependency | Observed stable version | Purpose |
| --- | --- | --- |
| `tokio` | 1.54.1 | Async runtime, tasks, timers, and synchronization. |
| `rdkafka` | 0.39.0 | librdkafka-backed consumers and `FutureProducer`. |
| `axum` | 0.8.9 | HTTP routing and JSON responses. |
| `axum-server` | 0.8.0 | Optional direct HTTPS listener integration. |
| `rustls` | 0.23.46 | TLS for the HTTPS listener. |
| `serde` | 1.0.229 | Typed configuration and response serialization. |
| `config` | 0.15.27 | TOML loading into typed settings. |
| `tracing` and `tracing-subscriber` | 0.1.44 and 0.3.23 | Structured logging. |
| `tokio-util` | 0.7.20 | Cancellation coordination. |
| `thiserror` and `anyhow` | 2.0.21 and 1.0.104 | Typed internal errors and executable-level context. |
| `testcontainers` | 0.28.0 | Broker-backed integration tests. |
| `prometheus-client` | 0.25.1, checked for the metrics update | Explicit metrics registry and OpenMetrics text encoding on the existing Axum listener. |

Add `serde_json` for JSON responses. Resolve a compatible stable version during setup.

For AWS IAM, use the maintained AWS SDK for Rust credential-provider stack, including `aws-config` and compatible credential types. Evaluate `aws-msk-iam-sasl-signer` 1.0.1 behind a private signing adapter. Its API and source were checked for this plan update, but it is community-maintained, not an AWS-supported Rust signer. AWS's MSK client guide does not list a Rust signer. Release availability alone does not establish active maintenance. Before approving this dependency, review repository activity, security advisories, credential handling, and compatibility with the chosen AWS SDK versions. If it fails that gate, use a reviewed adapter built on maintained AWS SigV4 signing crates rather than accepting an unmaintained dependency. Do not implement cryptography.

Tokio, Axum, rust-rdkafka, axum-server, Rustls, and config-rs repositories were not archived and showed upstream pushes between July and October 2026. This supports the maintenance shortlist, not a claim that the future service is production-certified. axum-server is a smaller integration crate, not as widely adopted as Axum.

During setup, select a supported stable Rust toolchain, compile the dependency set, record the minimum Rust version, and commit `Cargo.lock`. Enable only needed crate features. Verify librdkafka build tooling, TLS, SASL mechanisms, and CA certificates in the final container. Rustls for HTTP does not configure Kafka TLS, which uses librdkafka's build and settings.

Sources checked include [crates.io](https://crates.io), [Tokio](https://github.com/tokio-rs/tokio), [rust-rdkafka](https://github.com/fede1024/rust-rdkafka), [Axum](https://github.com/tokio-rs/axum), [axum-server](https://github.com/programatik29/axum-server), [Rustls](https://github.com/rustls/rustls), and [config-rs](https://github.com/rust-cli/config-rs).

## 3. Configuration contract

Load one file from a `--config` argument. Resolve paths and secrets at startup. Require a restart for configuration changes in the first version.

The example defines these settings:

| Setting | Meaning |
| --- | --- |
| `service.name` | Service identity returned by `/info`. |
| `http.bind` | HTTP or HTTPS listener address. |
| `http.tls` | Optional certificate and private-key file paths for direct HTTPS. |
| `kafka.common_properties` | Standard librdkafka properties shared by consumers and producer. |
| `kafka.auth_profile` | Default named authentication profile for all Kafka clients. |
| `kafka.auth_profiles.<name>` | Typed authentication settings, including secret-file references or AWS credential-provider selection. |
| `kafka.consumer.auth_profile` | Optional consumer authentication profile override. |
| `kafka.consumer.topics[].auth_profile` | Optional per-topic authentication profile override. |
| `kafka.producer.auth_profile` | Optional producer authentication profile override. |
| `kafka.consumer.group_id` | Default consumer group, with an optional typed per-topic override. |
| `kafka.consumer.properties` | Standard consumer properties applied to every instance. |
| `kafka.consumer.topics[].consumers` | Actual Kafka consumer instances for that topic in this process. |
| `kafka.consumer.topics[].batch_size` | Maximum records in one application batch. |
| `kafka.consumer.topics[].batch_max_wait_ms` | Flush a partial batch after this time from its first record. |
| `kafka.consumer.topics[].max_batch_bytes` | Application batch byte budget, including keys, payloads, and headers. |
| `kafka.consumer.topics[].properties` | Standard consumer property overrides for this topic's instances. |
| `processing.max_concurrent_messages` | Global cap on active handlers, not a per-consumer multiplier. |
| `processing.message_timeout_ms` | Maximum time for one handler attempt after acquiring a permit. |
| `kafka.producer.properties` | Standard producer properties. |
| `kafka.producer.enqueue_timeout_ms` | Bound time spent waiting for local producer queue capacity. |
| `health.*` and `shutdown.*` | Startup, probe, freshness, and shutdown deadlines. |
| `metrics.kafka_statistics_interval_ms` | Positive interval for native Kafka statistics, applied to every consumer and producer. |
| `metrics.native_statistics_stale_after_ms` | Freshness threshold for cached native Kafka statistics. |
| `metrics.processing_duration_buckets_seconds` | Fixed finite histogram boundaries for processing and publishing duration. |
| `metrics.http_duration_buckets_seconds` | Fixed finite histogram boundaries for HTTP request duration. |
| `metrics.producer_topics` | Finite output-topic allowlist for producer metric labels. Unlisted topics map to `__other__`. |

Merge ordinary properties in this order: common properties, client properties, then topic overrides for consumers. Resolve each client's named authentication profile separately. Reject raw properties that conflict with profile-owned protocol, mechanism, credentials, or TLS verification settings. Keep group IDs as typed settings and generate unique client IDs per process and consumer. Reject conflicting raw `group.id` or `client.id` entries rather than silently choosing one.

Allow standard librdkafka consumer settings, including offset reset, fetch sizes, session and poll timeouts, and assignment strategy. Configure security protocol, SASL, and TLS through typed authentication profiles. Permit additional compatible librdkafka options without allowing them to override profile-owned authentication settings. Validate effective properties through client construction as well as typed configuration checks.

Protect the processing guarantee. Require `enable.auto.commit=false` and `enable.auto.offset.store=false`, and reject conflicting overrides at any level. Validate the effective merged properties for every instance before starting workers.

`batch_size` is an application setting. It is not a broker fetch size. Do not map it to Java consumer `max.poll.records`. Upstream librdkafka master now documents that name for share consumers only, which is not the ordinary consumer-group mode planned here. Verify options against the pinned version.

Reject zero counts, zero limits, duplicate topic definitions, missing brokers or groups, malformed addresses, invalid TLS file pairs, and unknown application keys. Never log resolved secrets or include Kafka configuration in `/info`. The local example uses plaintext Kafka. Production deployments must select an encrypted authentication profile.

### Authentication profile contract

Represent authentication as one typed variant per profile, not a collection of independent switches. Select one profile per client. Consumer selection follows topic override, consumer override, then the Kafka default. Producer selection follows producer override, then the Kafka default. This permits different identities without duplicating authentication code.

| Profile `kind` | Generated librdkafka settings | Required inputs |
| --- | --- | --- |
| `plaintext` | `security.protocol=PLAINTEXT` | None. Local development only. |
| `tls` | `security.protocol=SSL` | Optional CA file. Otherwise use the verified container trust store. No client certificate authentication. |
| `mtls` | `security.protocol=SSL` plus client certificate and key | Certificate file, private-key file, optional key-password secret file, and optional CA file. |
| `sasl_plain` | `security.protocol=SASL_SSL`, `sasl.mechanisms=PLAIN` | Username and password secret files, plus optional CA file. |
| `sasl_scram` | `security.protocol=SASL_SSL`, `sasl.mechanisms=SCRAM-SHA-256` or `SCRAM-SHA-512` | Selected mechanism, username and password secret files, plus optional CA file. |
| `aws_msk_iam` | `security.protocol=SASL_SSL`, `sasl.mechanisms=OAUTHBEARER` | AWS region, credential-provider selection, and IAM-enabled MSK bootstrap brokers. |

The `tls` variant encrypts traffic and verifies the server. It does not by itself authenticate the client. `mtls` uses a client certificate. All encrypted profiles retain CA and hostname verification. Do not allow SASL PLAIN over an unencrypted connection.

Load mounted username, password, certificate, and private-key files at startup. Do not commit credentials to TOML. Validate missing files, unknown profile references, incompatible fields, unsupported mechanisms, and conflicting raw properties before starting workers. Never silently fall back to plaintext or a different mechanism. Static secret and certificate rotation requires restart initially. AWS temporary credentials and IAM tokens must refresh during normal operation.

Keep generic external OAuth providers and Kerberos outside the initial supported set unless a deployment requires them. AWS MSK IAM's use of OAUTHBEARER is part of the initial scope.

### AWS MSK IAM integration

For Rust's librdkafka client, use IAM through `SASL_SSL` and `OAUTHBEARER`. Do not select the Java-specific `AWS_MSK_IAM` SASL mechanism or require a Java plugin. Connect to the cluster's IAM-enabled bootstrap brokers. Obtain the actual endpoints from AWS rather than changing an arbitrary broker's port.

Use the AWS default credential chain through refresh-capable providers. Support local development profiles and workload credentials, including web identity, ECS or EKS container credentials, and EC2 instance roles. Prefer workload roles in deployments. Do not copy one set of temporary credentials into a permanently cached static provider. The initial `credentials` setting accepts `default_chain`. Provider-specific settings use the standard AWS environment and shared AWS configuration, not embedded access keys.

Use a custom rdkafka `ClientContext` for both consumers and the `FutureProducer`. Enable `ENABLE_REFRESH_OAUTH_TOKEN` and implement `generate_oauth_token`. The callback is synchronous. Return a cached valid token immediately, or return a sanitized error if none is available. Never perform AWS network requests, wait on Tokio futures, or hold a long lock in that callback.

Create a bounded background refresh worker per active IAM profile. Generate the first token before constructing its Kafka clients. Cache tokens per profile, region, and credential-provider identity, never globally across different identities. Refresh ahead of expiry with jitter and bounded retry backoff. The signer candidate returns a token and absolute expiration time. Its current source uses a 900-second token lifetime, which is a dependency detail rather than a service constant. Bound usable lifetime by the credentials' validity as well. `OAuthToken.lifetime_ms` is an absolute Unix epoch timestamp in milliseconds, not a duration or seconds.

The callback exposes refreshed tokens when librdkafka requests them. Keep consumer polling and the producer's event handling active so refresh events are serviced. Prove this behavior in the integration spike. Preserve token-refresh workers through shutdown drain and stop them after the clients finish.

Provision IAM permissions for cluster connection and metadata, topic reads and writes, consumer-group operations, and any actions required by idempotent production. Scope permissions to the intended cluster, topics, and groups. Test authentication and authorization separately. A valid signed token does not grant read or write permission by itself. Redact tokens, AWS session credentials, and sensitive signer diagnostics from logs and HTTP responses.

References checked for this design include [AWS MSK IAM client configuration](https://docs.aws.amazon.com/msk/latest/developerguide/configure-clients-for-iam-access-control.html), [AWS SDK Rust credential providers](https://docs.aws.amazon.com/sdk-for-rust/latest/dg/credproviders.html), [rdkafka 0.39 ClientContext](https://docs.rs/rdkafka/0.39.0/rdkafka/client/trait.ClientContext.html), [OAuthToken](https://docs.rs/rdkafka/0.39.0/rdkafka/client/struct.OAuthToken.html), the [signer API](https://docs.rs/aws-msk-iam-sasl-signer/1.0.1/aws_msk_iam_sasl_signer/fn.generate_auth_token.html), and its [repository](https://github.com/yuhao-su/aws-msk-iam-sasl-signer-rs).

## 4. Ownership and modules

Keep one executable crate initially. The following files are proposed, not present application files:

```text
src/main.rs              Composition and process entry point.
src/config.rs            File parsing, secret resolution, and validated settings.
src/runtime.rs           Task supervision, lifecycle, and shutdown.
src/kafka/mod.rs         Private Kafka integration boundary.
src/kafka/auth.rs        Authentication profiles and shared client context/token lifecycle.
src/kafka/consumer.rs    Consumer ownership, batching, assignments, and commits.
src/kafka/producer.rs    Standard producer construction and delivery results.
src/processing.rs        Owned message types and application handler.
src/http.rs              The three routes and listener configuration.
src/health.rs            Lifecycle state and cached dependency observations.
src/metrics.rs           Registry, bounded labels, and cached Kafka statistics.
```

Use owned message envelopes with topic, partition, offset, timestamp, nullable key and payload, and headers. Preserve tombstones rather than converting absent payloads into empty bytes.

Each consumer owner exclusively controls its assignment epochs, current batch, pause state, and commit requests. Handlers return outcomes and cannot commit offsets. An assignment token identifies a particular ownership period, even when the same partition returns to the same consumer.

Represent service lifecycle as `Starting`, `Running`, `Draining`, or `Failed`. Combine lifecycle with timestamped Kafka probe results to compute readiness. HTTP reads that state and does not duplicate it.

The intended internal interfaces are sketches, not a public plugin framework:

```rust
fn load_config(path: &std::path::Path) -> Result<ServiceConfig, ConfigError>;
async fn run(config: ServiceConfig) -> Result<(), ServiceError>;
async fn handle(message: Message, producer: &Producer) -> Result<(), ProcessError>;
async fn send(&self, record: OutboundRecord) -> Result<Delivery, ProduceError>;
```

The final signature is a method on the internal `Producer` type. Handler success includes completion of every required side effect. If processing publishes a Kafka record, await its delivery result before returning success. Do not create a public extension system or generic service framework for this executable.

## 5. Consumer processing and offsets

For each topic, start the configured number of independent consumers subscribed only to that topic. Instances for a topic use the same group ID. Consumer counts apply per service replica. Kafka distributes that topic's partitions among all eligible members of the group.

Consumers beyond the available partitions remain idle and healthy. More instances do not create partitions. A single partition can still have concurrent handlers because the service does not promise processing order.

For each consumer:

1. Collect owned messages until the record limit, byte limit, or partial-batch deadline is reached.
2. Pause intake for the batch barrier and dispatch records through the global handler limit.
3. Continue servicing Kafka polling and rebalance events while handlers run or wait for permits.
4. Wait for every record in the batch to succeed.
5. Verify that assignment ownership is still valid. Commit an explicit partition-offset list containing each affected partition's highest processed offset plus one, not the client's current positions.
6. Confirm commit success before beginning that consumer's next batch. Resume intake when capacity permits.

Disable automatic offset storage and commits. The barrier ensures there is no earlier unfinished batch when advancing offsets. Offset gaps are valid, so calculate commits from delivered records, never from message counts or assumed consecutive integers.

If any handler fails, times out, or panics, fail that batch. Do not commit the batch. The initial proposed policy marks the service unhealthy and stops intake for coordinated shutdown with a nonzero exit. Shutdown must not later commit a failed batch. Restart or reassignment replays uncommitted work. A persistent poison message can therefore cause a restart loop that requires operator intervention. Do not silently skip poison messages. A retry policy or dead-letter topic requires an explicit later delivery contract.

On revocation or lost assignment, invalidate ownership before accepting further completions. Discard affected batches for commit purposes. Do not wait for asynchronous handlers inside a synchronous rebalance callback. Late handlers may finish side effects, but cannot authorize offsets in the new assignment. Be conservative and replay rather than attempting a last-moment commit after ownership is lost.

This provides at-least-once processing, not exactly-once side effects. A crash after an output send but before the input commit can duplicate output. Producer idempotence does not close that gap. Business handlers need an idempotency strategy where duplicate effects matter.

### Backpressure and poll safety

The first broker integration phase must prove the chosen rdkafka owner-loop pattern. Start with a `StreamConsumer` owner that multiplexes receives, completions, and shutdown without awaiting a whole batch inline. Move bounded blocking operations off Tokio runtime threads without introducing concurrent commit writers.

The [rdkafka 0.39 StreamConsumer documentation](https://raw.githubusercontent.com/fede1024/rust-rdkafka/v0.39.0/src/consumer/stream_consumer.rs) requires attempts to extract messages within `max.poll.interval.ms`. Background heartbeats alone are insufficient. Continue polling while partitions are paused, including when waiting for handler permits. Validate newly assigned partitions against the paused state.

Bound application batches, pending completions, processing tasks, producer queues, and native consumer queues. Do not spawn one waiting task per record without a bound. Account for all configured consumers when estimating memory, not only active handlers.

Pausing does not erase prefetched records. Reserve bounded staging capacity for records delivered around pause transitions. If the application cannot retain a record within its configured budget, fail without committing that batch so Kafka can replay it. Do not drop the record and continue committing later offsets.

Native fetch queues can overshoot their configured byte thresholds for large records. Set broker and client fetch/message limits consistently, define an oversized-record failure policy, and document the resulting memory allowance. Do not describe a batch byte limit as a hard process-memory limit.

## 6. Standard producer

Create one shared `rdkafka::producer::FutureProducer`. Pass normal producer configuration through to librdkafka. Apply its selected authentication profile and use the same authentication integration as consumers, including IAM token refresh.

Propose `acks=all` and `enable.idempotence=true` as safe initial settings. Let librdkafka manage normal batching, retries, compression, and its delivery thread. Reject settings incompatible with the selected durability contract.

Bound queue capacity and enqueue wait. Await the delivery future and expose queue-full, timeout, authorization, and broker delivery errors to callers. Successful local enqueue is not successful publication. During shutdown, keep the producer available until handlers finish, then drain deliveries within the remaining deadline.

Do not claim consume-process-produce atomicity. Kafka transactions are outside the first version.

## 7. HTTP and HTTPS contract

Return JSON with `Content-Type: application/json` from `/health` and `/info`. `/prometheus` returns the metrics encoder's Prometheus-compatible exposition format and matching content type, not JSON.

| Request | Status | Body |
| --- | --- | --- |
| `GET /health` when ready | 200 | `{"status":"UP"}` |
| `GET /health` when starting, unhealthy, or draining | 503 | `{"status":"DOWN"}` |
| `GET /info` | 200 | Service identity and non-secret build/runtime metadata. |
| `GET /prometheus` | 200 | Consumer, producer, and service metrics as OpenMetrics text with `Content-Type: application/openmetrics-text; version=1.0.0; charset=utf-8`. |

The proposed `/info` response is:

```json
{
  "name": "rust-kafka-service",
  "version": "0.1.0",
  "git_commit": null,
  "build_time": null,
  "started_at": "2026-10-25T07:00:00Z"
}
```

Derive `version` from Cargo package metadata. Supply optional commit and build time from build metadata, not runtime configuration. Derive `started_at` from process startup. Use null when optional build metadata is unavailable.

Treat `/health` as readiness. Require successful startup, live supervisors, recent consumer-owner poll activity, no fatal component error, usable authentication state, and recent successful authenticated Kafka probes for required consumer and producer clients. Keep readiness down if IAM credentials or a required cached token have expired and cannot be refreshed. A transient refresh failure can retain the last still-valid token while bounded retries run. Probe with bounded background operations and cache results. Do not make every HTTP request contact Kafka. The example proposes a five-second probe interval and a fifteen-second freshness limit. Idle topics and unassigned excess consumers do not make the service unhealthy.

Metadata probes demonstrate connectivity and authentication, not future produce authorization or handler correctness. Actual delivery failures and fatal client errors must also update health. Do not publish synthetic business records just to probe health.

Use process or TCP checks for liveness if deployment needs them. Do not restart the process solely because readiness is down during a broker outage. Do not add `/live`, `/ready`, or a `/metrics` alias. The requested scrape path is `/prometheus`.

Support plain HTTP for local or trusted ingress deployments. Support direct HTTPS with Rustls and configured PEM certificate/key files. Reject invalid TLS configuration rather than falling back to HTTP. Exercise a real TLS handshake in tests. Certificate rotation can require restart in the first version.

### Prometheus metric contract

Use one explicit `prometheus-client` registry for the process. Share metric handles with consumer owners, processing supervision, the producer wrapper, HTTP middleware, and the health supervisor. Register metrics once at startup, not per scrape or per client restart. Encode the registry through `prometheus_client::encoding::text::encode` in `GET /prometheus`. Set the OpenMetrics content type explicitly and preserve the encoder's format terminator. Do not start a second exporter listener.

Expose the following application metrics. Names are the intended exported names, including the `_total` counter suffix. Avoid adding the suffix twice when registering counters.

| Metric | Type | Labels | Meaning |
| --- | --- | --- | --- |
| `kafka_consumer_received_messages_total` | Counter | `topic` | Records delivered to the application, including redeliveries. |
| `kafka_consumer_processed_messages_total` | Counter | `topic`, `status` | Completed processing attempts. `status` is exactly `success` or `failed`. |
| `kafka_consumer_processing_duration_seconds` | Histogram | `topic`, `status` | Handler attempt duration after acquiring the global permit. Includes required output delivery waits. |
| `kafka_consumer_processing_cancelled_total` | Counter | `topic` | Started attempts cancelled before a terminal processing result was observed. |
| `kafka_consumer_in_flight_messages` | Gauge | `topic` | Currently active handlers. The sum cannot exceed the global concurrency limit. |
| `kafka_consumer_batches_total` | Counter | `topic`, `status` | Terminal batch outcomes, with `success`, `failed`, or `discarded`. Discarded includes assignment loss and cancellation. |
| `kafka_consumer_commits_total` | Counter | `topic`, `status` | Confirmed successful commit requests or requests that failed or returned an uncertain result. Status is `success` or `failed`. |
| `kafka_consumer_committed_lag_offsets` | Gauge | `topic` | Sum of committed-offset lag for currently owned local partitions, when native statistics provide valid values. |
| `kafka_consumer_lag_available` | Gauge | `topic` | One only when the lag aggregate has fresh, valid coverage of the locally owned partitions. Otherwise zero. |
| `kafka_consumer_statistics_age_seconds` | Gauge | `topic` | Age of the oldest latest statistics sample across the topic's configured local consumer slots. Omitted until all slots have live clients with samples. |
| `kafka_consumer_statistics_available` | Gauge | `topic` | One only when every configured local consumer slot has a live client with fresh statistics. Otherwise zero. |
| `kafka_producer_messages_total` | Counter | `topic`, `status` | Publish attempts with confirmed delivery success or a terminal publish error. Status is `success` or `failed`. |
| `kafka_producer_publish_duration_seconds` | Histogram | `topic`, `status` | Time from the application send call to its final result, including enqueue wait. |
| `kafka_producer_publish_cancelled_total` | Counter | `topic` | Started publish attempts cancelled before observing delivery success or failure. |
| `kafka_producer_in_flight_messages` | Gauge | None | Application sends awaiting their final result. |
| `kafka_producer_queue_messages` | Gauge | None | Current producer queue depth from the latest valid native statistics sample. |
| `kafka_producer_queue_bytes` | Gauge | None | Current producer queue bytes from the latest valid native statistics sample. |
| `kafka_producer_statistics_age_seconds` | Gauge | None | Age of the latest producer statistics sample, omitted until the first sample. |
| `kafka_producer_statistics_available` | Gauge | None | One only when a fresh producer statistics sample exists. Otherwise zero. |
| `service_ready` | Gauge | None | One only when the same readiness calculation used by `/health` is up. |
| `service_uptime_seconds` | Gauge | None | Elapsed monotonic time since process startup. |
| `service_build_info` | Gauge | `name`, `version` | Constant one with service identity and Cargo package version. |
| `service_task_panics_total` | Counter | `component` | Panics observed by supervision. Component values come from a fixed enum. |
| `kafka_auth_refresh_total` | Counter | `profile`, `status` | IAM refresh outcomes, with `success` or `failed`, for configured active profiles. |
| `http_requests_total` | Counter | `method`, `route`, `status_code` | Completed HTTP requests, including scrapes. |
| `http_request_duration_seconds` | Histogram | `method`, `route`, `status_code` | Duration of completed HTTP requests. |

Processing success means the handler and required side effects succeeded. It does not mean its batch's offsets were committed. An error, timeout, or panic observed for a started handler records one failed attempt. Cancellation records the separate cancellation counter, not an invented success or failure. Records that never start have no processing outcome. The coordinator records one terminal outcome per started attempt, even when cancellation races completion. Decrement in-flight gauges on every exit path.

One failed record makes its batch fail, but does not relabel other successful records as failed. Replayed records create new processing attempts and can increment counters again. These are process-local counters, not durable business totals. Prometheus `rate` handles process restarts. Commit failures do not retroactively change processing success. A commit error or timeout with an unknown broker outcome uses `status="failed"`, meaning the service did not confirm success. It does not prove that the broker rejected the commit.

A producer success requires delivery acknowledgment, not local enqueue. Queue rejection and delivery timeout record failed publish attempts. A timeout can leave delivery uncertain and must not be described as proven loss. Count once per application send call, not per librdkafka retry. Cancellation does not prove that a record was not delivered. Keep producer publish failure separate from the input record's processing failure, even when one causes the other.

For example, a deployment can expose these completed processing attempts:

```text
kafka_consumer_processed_messages_total{topic="input-events",status="success"} 120
kafka_consumer_processed_messages_total{topic="input-events",status="failed"} 3
kafka_consumer_processed_messages_total{topic="audit-events",status="success"} 80
kafka_consumer_processed_messages_total{topic="audit-events",status="failed"} 1
```

A monitoring query is `sum by (topic, status) (rate(kafka_consumer_processed_messages_total[5m]))`.

### Native statistics, labels, and scrape safety

Apply `metrics.kafka_statistics_interval_ms` as librdkafka `statistics.interval.ms` to every client. Reject conflicting raw values and a zero interval. The native callback is otherwise disabled by default. Extend the same `ClientContext` that handles IAM tokens with `stats` handling, rather than replacing either callback. Update bounded shared snapshots or use a nonblocking, bounded handoff. Do not wait for HTTP scrapes or perform network operations in the synchronous callback.

Validate lag semantics against the pinned librdkafka version. Current upstream statistics define `consumer_lag` against the committed offset, and `consumer_lag_stored` against the stored offset. Do not substitute fetched-position or stored-offset lag for committed lag. Lag measures offset distance, not an exact remaining-message count, since offsets can have gaps. Aggregate only currently owned partitions and discard revoked-assignment snapshots. Check each sample's assignment generation and partition coverage against current ownership before publishing an aggregate. Missing partitions make lag unavailable until fresh samples cover the current assignment. Never convert unavailable or negative sentinel values to zero. Omit a lag sample without complete fresh coverage, and set its availability gauge to zero. No assignment is not evidence of a measured zero backlog.

Preserve native sample timestamps. Suppress stale queue and lag values after `metrics.native_statistics_stale_after_ms` while retaining age metrics. Expose an availability state for samples that have never arrived. Invalidate snapshots when a client exits or restarts. A missing configured consumer slot makes that topic's statistics and lag unavailable. No live consumers means availability zero, not a vacuously fresh aggregate. Reset a replacement producer's snapshot and availability until its first fresh sample. Do not reset application counters when clients restart. Do not add native cumulative totals to application counters on every statistics callback. That would double-count the same activity. Application counters are updated at their own lifecycle events.

Consumer topic labels come only from configured topics and are pre-registered for both processing statuses. Producer labels use the configured finite `metrics.producer_topics` allowlist. All other output topics use the reserved `__other__` label. Reject the reserved name in the allowlist. Do not drop publication or create unlimited label families when a new output topic appears.

Use fixed enums for status, component, and HTTP method labels. Normalize unknown methods to `OTHER`. HTTP route labels are exactly `/health`, `/info`, `/prometheus`, or `__unmatched__`, never the raw requested path or query. Do not label by partition, record offset, key, payload, exception message, credential identity, or generated client ID. The deployment's Prometheus target labels identify replicas. Configured names and package versions bound build and authentication labels. The authentication `profile` label is exactly a configured profile name. It never contains a credential-provider identity, role session name, token cache key, or resolved principal.

Require positive, strictly increasing, finite duration buckets. The encoder adds the positive-infinity bucket. Require a native-statistics freshness window larger than its sample interval. Estimate the series count from configured topics, status enums, and histogram buckets before deployment.

Keep `/prometheus` available while readiness is down, including startup, draining, and Kafka outages while the listener is alive. A scrape reads local registry state and cached statistics only. It cannot contact Kafka or AWS, trigger a token refresh, or increment business counters. HTTP middleware records the scrape request after its response completes, so that request appears on a subsequent scrape. Bound concurrent scrapes and avoid holding registry locks across awaits. Never cache an old response and label it as fresh telemetry.

Dependency and API references checked for this addition include [prometheus-client](https://docs.rs/prometheus-client/0.25.1/prometheus_client/), [its upstream repository](https://github.com/prometheus/client_rust), [OpenMetrics encoding](https://docs.rs/prometheus-client/0.25.1/prometheus_client/encoding/text/fn.encode.html), [ClientContext statistics](https://docs.rs/rdkafka/0.39.0/rdkafka/client/trait.ClientContext.html), and [librdkafka statistics definitions](https://github.com/confluentinc/librdkafka/blob/master/STATISTICS.md).

## 8. Startup, shutdown, and operations

Start structured logging and validate all configuration before admitting work. Bind the listener early enough to return `DOWN` during startup. Establish Kafka connectivity and start consumer owners before reporting `UP`. Bound startup with a deadline.

Supervise all long-lived tasks. Treat unexpected owner-task exits and panics as service failures. Report topic, partition, offset, batch identity, processing duration, commit errors, and rebalance events without payloads or credentials by default.

On SIGTERM or SIGINT, mark health down and stop intake. Keep authentication refresh available while handlers, commits, and producer deliveries drain. Drain handlers within the shared shutdown deadline. Commit only fully successful, still-owned batches, then flush producer deliveries and close the listener. Leave unfinished work uncommitted. Forceful termination can leave uncertain external effects and therefore cause replay.

Use a multi-stage container build, a non-root runtime user, and the CA certificates and native libraries required by the selected librdkafka build. Export operational metrics through `/prometheus` and retain structured logs for diagnosis. Restrict the scrape route to trusted monitoring networks through ingress or network policy. Kafka authentication does not protect HTTP routes. Do not expose credentials, tokens, message contents, or raw error text as metric labels.

## 9. Delivery phases and acceptance gates

Implement each phase only after its checks pass.

1. Establish the crate, toolchain, dependency features, and CI. Run formatting, Clippy, unit tests, and a release/container build. Verify TLS, PLAIN, SCRAM, and OAUTHBEARER support in the built Kafka client artifact. Resolve and compile the AWS credential and IAM signer dependency set.
2. Implement typed TOML configuration and the single-consumer Kafka ownership spike. Prove polling continues during a blocked handler and pause transitions never lose fetched records. Reject unsafe effective offset settings before work starts.
3. Implement batches and globally bounded processing with per-topic receive, processing outcome, duration, batch, and in-flight metrics. Test full batches, timed partial batches, byte limits, tombstones, and limits across multiple topics and consumers. Use barriers in tests to prove actual concurrency and the global ceiling without relying on timing alone. Verify each started attempt records exactly one success, failure, or separate cancellation outcome.
4. Implement manual commits and assignment fencing. Verify out-of-order completion, numerical offset gaps, batch failure, crash before commit, ambiguous commit, and revoke/reassign to the same partition. No unfinished or stale work may advance commits.
5. Integrate the standard producer and business handler boundary with publish outcome, duration, cancellation, and native queue metrics. Test delivery acknowledgments, queue pressure, broker rejection, and output-before-input-commit replay. Confirm successful local enqueue alone never increments successful publication. Keep a demonstration handler separate from unspecified production business logic.
6. Implement the three routes, shared metrics registry, and HTTP/HTTPS listeners. Verify exact healthy JSON, readiness transitions, info metadata, unknown-path 404 responses, idle consumers, and a direct TLS handshake. Scrape `/prometheus` with a Prometheus/OpenMetrics parser. Verify its content type, finite labels, histogram buckets, distinct topic outcomes, and stale native-statistics handling. Confirm repeated scrapes do not change Kafka counters or call Kafka or AWS. A down readiness state must not prevent metrics scrapes.
7. Integrate supervision, shutdown, and security. Test SIGTERM with active work, shutdown deadline expiry, broker outages, authorization failures, secret redaction, and recovery of readiness after transient outages. Cover each authentication profile and profile inheritance. Test TLS trust and hostname failures, bad credentials, and conflicting raw properties. Verify IAM refresh failures and credential expiry without blocking consumer polling. Prove both consume and acknowledged produce operations against an IAM-enabled AWS MSK cluster through at least one token refresh and temporary credential rotation. A local Kafka container cannot establish AWS IAM compatibility.
8. Run end-to-end checks against a pinned Kafka container with multiple partitions and consumer instances. Verify observed processing outcomes, confirmed commits, producer deliveries, replays, and metrics agree. Run a real Prometheus scrape and a per-topic success/failure-rate query. Add CI integration coverage and an operator runbook with metric semantics and freshness guidance. Record workload-specific capacity only after testing representative payload sizes and handler latency.

Use unit tests for batch state and safe-commit rules, route tests for HTTP contracts, and real Kafka integration tests for broker behavior. Add deterministic offset-gap fixtures and confirm real broker behavior separately. Mocks cannot establish rebalances or committed-offset behavior.

The throughput checkpoint before implementation is explicit. Consumer count controls ownership, batch size controls collection and barrier granularity, and the global semaphore controls active work. The proposed 32 handlers is a limit, not measured throughput. Confirm throughput and memory requirements before production sizing.

## 10. Synthesis and remaining decisions

The design exploration compared four independent candidates. Candidates 1, 2, and 4 chose pipelined per-partition completion ledgers. Candidate 3 chose batch barriers with continued polling. All used the configured same runner model, so the exploration had reduced model diversity. The independent cross-judge used a different configured model.

The base is candidate 3. The independent cross-judge agreed and scored it 21 out of 25 across requirement coverage, offset safety, resource/lifecycle bounds, maintainability, and testability. Each pipelined candidate scored 17 out of 25. These are design judgments, not runtime measurements. The batch barrier meets the concurrency requirement without a completion ledger for several overlapping batches. Adopt global rather than per-consumer handler limits from candidates 2 and 4. Adopt explicit delivered-offset gap handling and assignment-period tokens from the pipelined candidates. Adopt candidate 2's service-wide fail-stop policy, with no implicit retry or dead-letter behavior. Reject the argument that a batch barrier must block polling, since the consumer owner continues polling separately.

The design applies Model the Domain by making batch ownership, assignment epochs, and authentication profile variants explicit. Boundary Discipline validates authentication references and rejects conflicting raw settings before clients start. Laziness Protocol keeps a single crate and omits pipelining until measurements justify it. Prove It Works requires broker-backed acceptance gates and treats dependency release checks as evidence of maintenance, not evidence that the service already works.

Before implementing business processing, decide these application-specific contracts:

- What input schema and business operation does the handler implement?
- Which output topics, if any, does processing publish to?
- What protects external side effects against duplicate input delivery?
- Is the proposed fail-stop poison-message policy acceptable, or is a specified retry/dead-letter policy required?
- What payload sizes, throughput, memory limits, and shutdown budget must production support?

Kafka authentication support is required for both consumers and producers, including AWS MSK IAM. Confirm target MSK regions, deployment roles, certificate distribution, and required broker authentication modes before deployment. Keep Kafka authentication separate from HTTP listener TLS.

The metrics requirement expands the original two-route scope to three routes. Consumer outcomes describe processing attempts, not unique records or committed offsets. Counter resets on process restart and repeated processing under at-least-once delivery are expected.

Direct HTTPS support and readiness semantics are included as proposed defaults. Deployment can use HTTP behind a TLS-terminating ingress without changing the route contract.

Plan verification completed. Python's TOML parser accepted both configuration examples, and static coverage checks found each requested capability in this document. Authentication profile references and manual-offset settings were consistent. Re-run the authentication example checks with `python3 .audit/check-auth-plan.py`. This checks design examples, not an implemented configuration schema or real authentication. No service compilation, broker test, or HTTP test has run because there is no application implementation yet.

No implementation reconciliation is needed yet. Start development with phase 1 after this plan is accepted.
