# Run the Rust Kafka service

This implementation provides unordered batch consumers, a producer, HTTP or HTTPS endpoints, and Prometheus metrics. See [implementation status](docs/implementation-status.md) before using it in production.

## Build and check configuration

1. Install the Rust toolchain in `rust-toolchain.toml`.
2. Install a C and C++ compiler, Make, Perl, and CMake. The pinned build vendors librdkafka, OpenSSL, and zlib.
3. Build the executable.

```sh
cargo build --locked
```

4. Set `bootstrap.servers`, topic names, authentication profiles, and consumer limits in a copy of `config/example.toml`.
5. Validate the configuration and referenced files.

```sh
cargo run --locked -- --config config/example.toml --check-config
```

The validation command does not contact Kafka or validate every native Kafka property. File paths resolve relative to the configuration file.

## Run with explicit processing behavior

The executable requires an explicit handler. The `service` handler validates text payloads and reports their trimmed byte and word counts.

The `observe` handler logs record metadata, then allows commits. It does not retain the payload or perform business processing. Use it only when acknowledging those records is intentional.

```sh
cargo run --locked -- --config config/example.toml --handler observe
```

To validate and count consumed text payloads, select `service`.

```sh
cargo run --locked -- --config config/example.toml --handler service
```

The service handler rejects tombstones, invalid UTF-8, and payloads that contain only whitespace. It logs counts and Kafka metadata, but does not store or publish results. A rejected message fails its batch, so records in that batch can replay after restart.

To forward consumed records to another topic, select `forward`. Create the destination topic first.

```sh
cargo run --locked -- --config config/example.toml \
  --handler forward --output-topic output-events
```

Add known output topics to `metrics.producer_topics` to get individual producer topic labels. Unlisted topics share `__other__`.

## Check HTTP endpoints

Run these commands against a running service.

```sh
curl -i http://localhost:8080/health
curl http://localhost:8080/info
curl http://localhost:8080/prometheus
```

A ready service returns HTTP 200 and `{"status":"UP"}` from `/health`. Startup, stale broker probes, failures, and drain return HTTP 503 and `{"status":"DOWN"}`.

To enable HTTPS, configure `http.tls` with `enabled = true`, a PEM certificate, and a PEM private key. Use a trusted certificate or pass its CA certificate to your client. Do not disable certificate verification.

Restrict these endpoints to your operational network. The service does not implement HTTP authentication.

## Select Kafka authentication

Use the named profiles in [the configuration reference](docs/service-reference.md). Keep secret contents in mounted files rather than TOML.

For AWS MSK IAM, edit `config/aws-msk-iam.toml` and provide an AWS workload identity through the SDK credential chain.

```sh
cargo run --locked -- --config config/aws-msk-iam.toml --handler observe
```

The default build includes AWS IAM. To omit it, build with `--no-default-features` and select a different authentication profile.

## Run checks

Run formatting, Clippy, unit tests, the build without AWS, and the release build.

```sh
bash scripts/check.sh
```

With Docker or Podman installed, run the broker checks. The script creates and removes its own Redpanda container and uses temporary files and ports.

```sh
cargo build --locked
python3 scripts/broker-smoke.py --engine podman
```

Use `--engine docker` for Docker. See [verification results and remaining checks](docs/implementation-status.md).

## Build and run the container

Build the non-root image.

```sh
docker build -t rust-kafka-service:local .
```

Mount a configuration file and its secret files. Set `bootstrap.servers` to an address reachable from the container.

```sh
docker run --rm -p 8080:8080 \
  --mount type=bind,src="$PWD/config",dst=/etc/service,readonly \
  rust-kafka-service:local \
  --config /etc/service/example.toml --handler observe
```

The image runs as UID 65532. Grant that UID read access to mounted files. The image intentionally does not select a handler for you.

Send SIGTERM for graceful shutdown. The service stops intake, drains completed work, and waits for commit confirmation. If native operations exceed the grace period, the watchdog exits with code 2. Records with uncertain outcomes can replay.
