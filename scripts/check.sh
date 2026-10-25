#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
cargo fmt --check
cargo clippy --all-targets --all-features --locked -- -D warnings
cargo test --all-features --locked
cargo clippy --all-targets --no-default-features --locked -- -D warnings
cargo test --no-default-features --locked
cargo check --no-default-features --locked
cargo build --release --locked
