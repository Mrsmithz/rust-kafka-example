#!/usr/bin/env python3
"""Exercise the executable against an isolated single-node Redpanda broker."""
import argparse
import json
import os
from pathlib import Path
import re
import signal
import socket
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid

ROOT = Path(__file__).resolve().parents[1]


def run(*args, **kwargs):
    return subprocess.run(args, check=True, capture_output=True, text=True, timeout=90, **kwargs).stdout


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def eventually(check, seconds=45):
    deadline = time.monotonic() + seconds
    last = None
    while time.monotonic() < deadline:
        try:
            value = check()
            if value:
                return value
        except (OSError, subprocess.SubprocessError, AssertionError) as error:
            last = error
        time.sleep(0.1)
    raise AssertionError(f"condition was not met before deadline: {last}")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--engine", default=os.environ.get("CONTAINER_ENGINE", "podman"))
    parser.add_argument("--binary", type=Path, default=ROOT / "target/debug/rust-kafka-service")
    args = parser.parse_args()
    binary = args.binary.resolve()
    assert binary.is_file(), "build the executable first with cargo build --locked"
    name = f"rust-kafka-smoke-{uuid.uuid4().hex[:10]}"
    kafka_port, http_port = free_port(), free_port()
    endpoint = f"http://127.0.0.1:{http_port}"
    process = None

    def rpk(*command, **kwargs):
        return run(args.engine, "exec", "-i", name, "rpk", "-X", f"brokers=127.0.0.1:{kafka_port}", *command, **kwargs)

    def get(path):
        with urllib.request.urlopen(endpoint + path, timeout=2) as response:
            return response.read().decode()

    def metric(name, **labels):
        for line in get("/prometheus").splitlines():
            if line.startswith(name + "{") and all(f'{key}="{value}"' in line for key, value in labels.items()):
                return float(line.rsplit(" ", 1)[1])
        return None

    def committed(group, expected):
        output = rpk("group", "describe", "-c", group)
        for line in output.splitlines():
            parts = line.split()
            if len(parts) > 2 and parts[0] == "input-events" and parts[1] == "0":
                return parts[2] == str(expected)
        return False

    def stop(expected=0):
        nonlocal process
        assert process is not None
        process.send_signal(signal.SIGTERM)
        code = process.wait(timeout=15)
        process = None
        assert code == expected, f"unexpected service exit code {code}"

    run(args.engine, "run", "--rm", "-d", "--name", name, "-p", f"127.0.0.1:{kafka_port}:{kafka_port}",
        "docker.io/redpandadata/redpanda:v25.2.9", "redpanda", "start", "--overprovisioned", "--smp", "1", "--memory", "512M", "--reserve-memory", "0M", "--node-id", "0", "--check=false", "--kafka-addr", f"PLAINTEXT://0.0.0.0:{kafka_port}", "--advertise-kafka-addr", f"PLAINTEXT://127.0.0.1:{kafka_port}")
    try:
        eventually(lambda: rpk("cluster", "info"))
        rpk("topic", "create", "input-events", "-p", "2")
        rpk("topic", "create", "output-events", "-p", "2")
        with tempfile.TemporaryDirectory(prefix="rust-kafka-smoke-") as temporary:
            temporary = Path(temporary)
            config = (ROOT / "config/example.toml").read_text().replace("localhost:9092", f"127.0.0.1:{kafka_port}").replace("0.0.0.0:8080", f"127.0.0.1:{http_port}")
            config = config.replace("kafka_statistics_interval_ms = 10000", "kafka_statistics_interval_ms = 200").replace("native_statistics_stale_after_ms = 30000", "native_statistics_stale_after_ms = 3000")
            config = config.replace('group_id = "rust-kafka-service-v1"', 'group_id = "smoke-forward"')
            config = config.replace("producer_topics = []", 'producer_topics = ["output-events"]')
            config_path = temporary / "config.toml"
            config_path.write_text(config)
            with (temporary / "service.log").open("w+") as log:
                def start(handler="forward"):
                    nonlocal process
                    command = [str(binary), "--config", str(config_path), "--handler", handler]
                    if handler == "forward":
                        command += ["--output-topic", "output-events"]
                    process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT)
                    eventually(lambda: json.loads(get("/health"))["status"] == "UP")
                    info = json.loads(get("/info"))
                    assert info["name"] == "rust-kafka-service" and info["version"]
                    assert "bootstrap" not in json.dumps(info)

                try:
                    start()
                    rpk("topic", "produce", "input-events", "-p", "0", input="one\ntwo\nthree\n")
                    eventually(lambda: metric("kafka_consumer_processed_messages_total", topic="input-events", status="success") == 3)
                    eventually(lambda: metric("kafka_producer_messages_total", topic="output-events", status="success") == 3)
                    eventually(lambda: committed("smoke-forward", 3))
                    output = rpk("topic", "consume", "output-events", "-n", "3", "-o", "start", "--format", "%v\n")
                    assert sorted(output.splitlines()) == ["one", "three", "two"], output
                    scrape = get("/prometheus")
                    assert "_total_total" not in scrape
                    assert 'service_ready 1' in scrape
                    try:
                        get("/untrusted/record-id?secret=value")
                    except urllib.error.HTTPError as error:
                        assert error.code == 404
                    scrape = get("/prometheus")
                    assert "record-id" not in scrape and "secret=value" not in scrape
                    assert 'route="__unmatched__"' in scrape
                    assert 'route="/prometheus"' in scrape
                    stop()
                    print("PASS timed partial batch, unordered forwarding, delivery acknowledgment, confirmed broker offsets, HTTP, bounded labels, scrape metrics, graceful stop")
                    start()
                    assert metric("kafka_consumer_processed_messages_total", topic="input-events", status="success") == 0
                    rpk("topic", "produce", "input-events", "-p", "0", input="four\n")
                    eventually(lambda: metric("kafka_consumer_processed_messages_total", topic="input-events", status="success") == 1)
                    eventually(lambda: committed("smoke-forward", 4))
                    stop()
                    print("PASS restart resumes at committed offsets without replaying confirmed records")
                    service_config = config.replace('group_id = "smoke-forward"', 'group_id = "smoke-service"')
                    config_path.write_text(service_config)
                    start("service")
                    eventually(lambda: metric("kafka_consumer_processed_messages_total", topic="input-events", status="success") == 4)
                    rpk("topic", "produce", "input-events", "-p", "0", input="  SensitiveFixture   alpha  \n  Rust service  \n")
                    eventually(lambda: metric("kafka_consumer_processed_messages_total", topic="input-events", status="success") == 6)
                    eventually(lambda: committed("smoke-service", 6))
                    stop()
                    events = [json.loads(line).get("fields", {}) for line in (temporary / "service.log").read_text().splitlines() if line.startswith("{")]
                    assert sum(event.get("word_count") == 2 for event in events) == 2, events
                    assert "SensitiveFixture" not in (temporary / "service.log").read_text(), "service must not log payloads"
                    rpk("topic", "produce", "input-events", "-p", "0", input="   \n")
                    process = subprocess.Popen([str(binary), "--config", str(config_path), "--handler", "service"], stdout=log, stderr=subprocess.STDOUT)
                    code = process.wait(timeout=20)
                    process = None
                    assert code != 0, "invalid service payload must fail the batch"
                    assert committed("smoke-service", 6), "failed service batch advanced committed offsets"
                    print("PASS service validation, word counts, payload-safe logs and no commit on invalid input")
                    failure_config = re.sub(r"max_batch_bytes = \d+", "max_batch_bytes = 1", config.replace('group_id = "smoke-forward"', 'group_id = "smoke-oversize"'))
                    assert failure_config != config
                    config_path.write_text(failure_config)
                    process = subprocess.Popen([str(binary), "--config", str(config_path), "--handler", "observe"], stdout=log, stderr=subprocess.STDOUT)
                    code = process.wait(timeout=20)
                    assert code != 0, "oversized record must fail the service"
                    process = None
                    group = rpk("group", "describe", "-c", "smoke-oversize")
                    for line in group.splitlines():
                        parts = line.split()
                        if len(parts) > 2 and parts[0] == "input-events":
                            assert parts[2] == "-", "oversized unprocessed records advanced commits"
                    print("PASS oversized record fails closed without advancing committed offsets")
                    outage_config = config.replace('group_id = "smoke-forward"', 'group_id = "smoke-shutdown"')
                    outage_config = outage_config.replace('"auto.offset.reset" = "earliest"', '"auto.offset.reset" = "latest"')
                    outage_config = outage_config.replace("kafka_probe_interval_ms = 5000", "kafka_probe_interval_ms = 100")
                    outage_config = outage_config.replace("kafka_probe_timeout_ms = 2000", "kafka_probe_timeout_ms = 10000")
                    outage_config = outage_config.replace("grace_period_ms = 60000", "grace_period_ms = 1000")
                    config_path.write_text(outage_config)
                    start("observe")
                    run(args.engine, "pause", name)
                    try:
                        time.sleep(0.3)
                        began = time.monotonic()
                        stop(expected=2)
                        elapsed = time.monotonic() - began
                        assert elapsed < 3, f"shutdown exceeded watchdog tolerance: {elapsed} seconds"
                    finally:
                        run(args.engine, "unpause", name)
                    print("PASS shutdown deadline interrupts blocked broker probes with nonzero exit")
                except Exception:
                    log.flush()
                    print((temporary / "service.log").read_text())
                    raise
    finally:
        if process is not None and process.poll() is None:
            process.send_signal(signal.SIGTERM)
            try:
                process.wait(timeout=15)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        run(args.engine, "rm", "-f", name)


if __name__ == "__main__":
    main()
