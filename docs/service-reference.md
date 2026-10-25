# Service reference

The executable uses Tokio, rust-rdkafka, Axum, rustls, prometheus-client, and the AWS Rust SDK. `Cargo.lock` records the resolved versions. `Cargo.toml` forbids application `unsafe` code and denies Clippy `unwrap_used` and `expect_used`.

## Configuration

`config/example.toml` describes local plaintext development. `config/aws-msk-iam.toml` describes AWS IAM configuration. Unknown application fields, zero capacities, incompatible authentication overrides, and unreadable referenced files are rejected.

| Table | Fields and purpose |
| --- | --- |
| `service` | `name` identifies the service. Package metadata supplies the version. |
| `http` | `bind` sets the listener address. Optional `tls` has `enabled`, `cert_file`, and `key_file`. |
| `health` | `startup_timeout_ms`, `kafka_probe_interval_ms`, `kafka_probe_timeout_ms`, and `kafka_stale_after_ms` control startup and cached broker readiness. |
| `processing` | `max_concurrent_messages` is the service-wide handler limit. `message_timeout_ms` bounds one handler attempt. |
| `metrics` | `kafka_statistics_interval_ms`, `native_statistics_stale_after_ms`, processing and HTTP histogram bucket arrays, and `producer_topics` control metrics. |
| `shutdown` | `grace_period_ms` bounds shutdown before the watchdog exits with code 2. |
| `kafka` | `auth_profile`, `auth_profiles`, and `common_properties` provide shared client settings. |
| `kafka.consumer` | `group_id`, `auth_profile`, `properties`, `topics`, and `commit_timeout_ms` define consumer defaults. The commit timeout defaults to 10000 ms and bounds application confirmation, not the native call. |
| `kafka.consumer.topics` | Each entry has `name`, `consumers`, `batch_size`, `batch_max_wait_ms`, `max_batch_bytes`, and optional `group_id`, `auth_profile`, and `properties`. |
| `kafka.producer` | `auth_profile`, `properties`, and `enqueue_timeout_ms` configure the shared producer. |

Kafka properties are string maps. Common properties are applied first, followed by consumer or producer properties, then topic properties for consumers. Topic group and authentication selections override their consumer defaults. Authentication-owned properties and generated client IDs cannot be replaced by raw settings.

The service disables both `enable.auto.commit` and `enable.auto.offset.store`. A raw `statistics.interval.ms` must match the application statistics interval. Native client construction rejects unsupported or invalid native properties. `--check-config` does not construct clients.

## Kafka authentication profiles

| `kind` | Profile fields |
| --- | --- |
| `plaintext` | No secrets. Intended for local development. |
| `tls` | Optional `ca_file`. Server certificate and hostname verification remain enabled. |
| `mtls` | `cert_file`, `key_file`, optional `ca_file`, and optional `key_password_file`. |
| `sasl_plain` | `username_file`, `password_file`, and optional `ca_file`. Uses TLS. |
| `sasl_scram` | `mechanism` is `SCRAM-SHA-256` or `SCRAM-SHA-512`. Also has `username_file`, `password_file`, and optional `ca_file`. Uses TLS. |
| `aws_msk_iam` | `region` and `credentials = "default_chain"`. Uses TLS and OAUTHBEARER with the default trust store. Requires the `aws-iam` build feature. |

Consumers, topics, and the producer can select different profiles. File secrets are read at startup and trailing line endings are removed. The implementation does not hot-reload file credentials or the HTTP certificate.

IAM token signing uses the AWS SDK credential chain and SigV4 query signing for `kafka-cluster`. A refresh task resolves credentials again and updates a token cache. The native OAuth callback reads that cache and performs no network access. Unexpected refresh-task exit or expired credentials makes readiness fail. Real AWS authentication and rotation acceptance remain unverified.

## Service handler

The `--handler service` choice accepts payloads as UTF-8 text, trims surrounding Unicode whitespace, and rejects missing, invalid, or whitespace-only payloads. It reports the trimmed UTF-8 byte length and whitespace-separated word count. Errors omit payload content. The handler logs these counts with Kafka metadata and does not persist or publish the summary. Rejected messages fail the batch and can replay after restart.

## Processing and delivery semantics

Each consumer has one polling owner and at most one active batch. Polling continues while its assigned partitions are paused for processing or commit confirmation. A batch ends at the configured record count, elapsed wait, or byte limit.

Handlers share one global semaphore. They can finish in any order. The service does not preserve message ordering or provide exactly-once effects.

A batch is eligible for a commit only after every delivered record succeeds. Explicit offsets contain one past the highest delivered offset for each partition. A separate blocking worker performs one synchronous commit while the owner keeps polling. Producer success requires a delivery acknowledgment, not just queue admission.

Assignment callbacks cancel previous ownership tokens, including reassignment of the same partition. Results carrying an invalid token are discarded. Strict atomic native generation fencing across a commit race is not established. The related acceptance gap is recorded in [open issues](open-issues.md).

Failures, panics, timeouts, and uncertain commits stop intake and trigger bounded cleanup. Publish and commit uncertainty can produce duplicates. Business side effects must tolerate replay. Cancelling a producer future does not retract a record already accepted by the native producer.

Application batch limits do not bound all process memory. Native consumer queues, producer queues, record headers, and client overhead also consume memory. The concurrency limit does not imply a throughput target.

## HTTP responses

| Route | Response |
| --- | --- |
| `GET /health` | HTTP 200 and `{"status":"UP"}` when running with fresh broker probes and valid authentication. Otherwise HTTP 503 and `{"status":"DOWN"}`. No broker operation runs in the request handler. |
| `GET /info` | Service `name`, application `version`, `started_at`, optional `git_commit`, and optional `build_time`. |
| `GET /prometheus` | OpenMetrics text with its content type and `# EOF`. A concurrent scrape can return HTTP 503. |

`GIT_COMMIT` and `BUILD_TIME` are optional compile-time environment variables. Without them, their `/info` fields are null. The HTTP middleware records completed responses. The current scrape's request appears in a later scrape.

## Prometheus metrics

Consumer and producer counters use bounded `topic` labels. Terminal processing and publish counters use `status="success"` or `status="failed"`. Cancellation has separate counters and does not count as success or failure.

| Metrics | Meaning |
| --- | --- |
| `kafka_consumer_received_messages_total` | Records delivered into application batches. |
| `kafka_consumer_processed_messages_total` | Finished handler attempts by topic and status. Success does not mean the offset was committed. |
| `kafka_consumer_processing_duration_seconds` | Handler attempt duration by topic and terminal status. |
| `kafka_consumer_processing_cancelled_total`, `kafka_consumer_in_flight_messages` | Cancelled attempts and currently active handlers. |
| `kafka_consumer_batches_total`, `kafka_consumer_commits_total`, `kafka_consumer_rebalances_total` | Batch, commit-confirmation, and assignment outcomes. A failed commit confirmation includes uncertainty. |
| `kafka_consumer_committed_lag_offsets`, `kafka_consumer_lag_available` | Committed-offset distance and whether complete current assignment coverage is available. These are offset distances, not message counts. |
| `kafka_consumer_statistics_age_seconds`, `kafka_consumer_statistics_available` | Oldest latest sample age among live consumers, and complete fresh sample availability. Age measures callback receipt. |
| `kafka_producer_messages_total`, `kafka_producer_publish_duration_seconds` | Delivery outcomes and duration, including queue admission and acknowledgment wait. |
| `kafka_producer_publish_cancelled_total`, `kafka_producer_in_flight_messages` | Cancelled application sends and sends awaiting results. |
| `kafka_producer_queue_messages`, `kafka_producer_queue_bytes` | Cached native producer queue samples. |
| `kafka_producer_statistics_age_seconds`, `kafka_producer_statistics_available` | Cached producer sample age and availability. |
| `service_ready`, `service_uptime_seconds`, `service_build_info` | Cached readiness, uptime, and service identity. |
| `service_task_panics_total`, `kafka_auth_refresh_total` | Supervised panics and authentication refresh outcomes. Authentication profile names are fixed configuration values. |
| `http_requests_total`, `http_request_duration_seconds` | Completed requests by bounded method, matched route, and response status code. |

Lag is omitted if samples are stale, any owned partition lacks a valid lag, or the assignment is empty. Missing samples do not become zero lag. Topic, partition coverage, and assignment-epoch checks run before aggregation. A queued statistics sample's generation time across a rebalance still needs an integration test.

Message keys, payloads, offsets, exception strings, raw URL paths, and client IDs are not metric labels. Unknown HTTP routes use `__unmatched__`. Producer topics outside `metrics.producer_topics` use `__other__`.
