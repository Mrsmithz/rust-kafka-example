FROM docker.io/library/rust:1.98.0-bookworm AS build
RUN apt-get update && apt-get install -y --no-install-recommends cmake perl make g++ && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY src ./src
RUN cargo build --release --locked

FROM docker.io/library/debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/rust-kafka-service /usr/local/bin/rust-kafka-service
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/rust-kafka-service"]
CMD ["--config", "/etc/service/config.toml"]
