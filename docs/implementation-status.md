# Implementation status

This repository now contains a runnable service baseline. It is not certified production-ready. The approved plan remains in `DEVELOPMENT_PLAN.md`; the implementation does not claim that every acceptance check in that plan has passed.

## Local verification

The following checks passed during implementation.

- Thirty-eight unit, property, and asynchronous behavior tests passed with the default AWS feature enabled. Thirty-seven tests passed without default features.
- Clippy passed with warnings denied, both with all features and without default features.
- Local Kafka checks passed against Redpanda `v25.2.9` in an isolated container.
- The broker checks covered timed partial batches, concurrent forwarding, producer acknowledgments, broker-confirmed committed offsets, and restart from those offsets.
- The service handler processed valid text, logged numeric summaries without payload content, and failed on whitespace-only input without advancing committed offsets.
- An oversized record caused failure without advancing the group's committed offsets.
- HTTP endpoints returned the expected bodies and status codes. Unknown paths did not become metric labels.
- Live HTTP and HTTPS tests passed. HTTPS rejected an untrusted certificate and a hostname mismatch.
- Normal multi-consumer shutdown drained successfully. A paused broker with a ten-second metadata timeout and a one-second grace period triggered exit code 2 within the test's three-second tolerance.
- The initial baseline Dockerfile built through Podman. That image runs as UID 65532 and uses the locked Rust 1.98 toolchain build. The later service-handler addition was verified as a local executable, not rebuilt into that image.
- The application and service-handler comment audits found no required changes. No comments were deleted or restored, and no constraint remained open.
- An independent review of the service-layer addition found no confirmed correctness or regression issue.
- Two independent follow-up reviews of the initial baseline found no remaining P1 or P2 issue in their assigned code. One covered Kafka ownership, processing, and shutdown. The other covered configuration, authentication, metrics, and HTTP.

These checks can be repeated with `bash scripts/check.sh` and `python3 scripts/broker-smoke.py`. The broker script requires Docker or Podman. CI configuration includes these commands and an image build, but no remote CI run is claimed here.

## Remaining acceptance checks

The following work is not established by the local checks.

- Real AWS MSK IAM authentication, connection refresh after token expiry, workload-identity rotation, denied permissions, and credential-provider failures.
- Kafka broker integration for TLS, mutual TLS, SASL PLAIN, and both SCRAM mechanisms. The current native feature test proves feature availability, not successful authentication.
- Strict native generation fencing during a commit submitted concurrently with revocation or reassignment. See [KAFKA-001](open-issues.md#kafka-001-native-commit-generation-fencing).
- Broker-backed rebalance during delayed processing, coordinator failure during commit, producer delivery rejection, and reconnect after broker restart.
- A long-running Prometheus scrape and query test. Encoding and bounded-label behavior have local tests, but a real Prometheus server has not been used.
- Statistics callback ordering across rebalance and same-partition reassignment. Callback receipt time is not proof of sample generation time.
- Dependency vulnerability audit, sustained resource testing, memory under worst-case record sizes, and performance measurements on the intended deployment platform.
- The application's business-specific handler and its replay behavior.

## Design changes from the plan

The polling client is `BaseConsumer`, not `StreamConsumer`. Each owner uses a dedicated thread and continues polling while processing or commit work is pending.

The pinned native client did not dispatch the manual asynchronous commit callback needed for application confirmation in the local broker test. The implementation instead performs one synchronous commit on a separate blocking worker. An application deadline treats a missing result as uncertain. The shutdown watchdog bounds process termination if a native call or destructor does not return.

Drained owners remain subscribed until all peer owners have finished processing and commit confirmation. Leaving the group earlier caused peers to lose their assignment during shutdown in the local test.

The executable requires an explicitly selected handler. The service handler validates UTF-8 text and logs byte and word counts without a durable side effect. Observe and forward remain separate explicit choices. There is no silently enabled no-op business handler.
