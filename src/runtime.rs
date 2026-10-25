use std::{path::PathBuf, sync::Arc, time::Duration};

use anyhow::{Result, anyhow, bail, ensure};
use clap::{Parser, ValueEnum};
use tokio::{
    task::JoinSet,
    time::{Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;

use crate::{
    config::ServiceConfig,
    health::Health,
    http,
    kafka::{
        auth::AuthManager,
        consumer::{self, ConsumerSettings, ProbeState},
        context::KafkaContext,
        producer::Producer,
    },
    metrics::{ClientSlot, Metrics},
    processing::{MessageHandler, Processor},
};

#[derive(Parser)]
#[command(
    version,
    about = "Configurable unordered at-least-once Kafka batch service"
)]
pub(crate) struct Cli {
    #[arg(long, default_value = "config/example.toml")]
    config: PathBuf,
    #[arg(
        long,
        help = "Validate application configuration and referenced files without contacting Kafka"
    )]
    check_config: bool,
    #[arg(
        long,
        value_enum,
        required_unless_present = "check_config",
        help = "Message processing behavior"
    )]
    handler: Option<HandlerChoice>,
    #[arg(long, help = "Destination topic required for the forward handler")]
    output_topic: Option<String>,
}
#[derive(Clone, Copy, ValueEnum)]
enum HandlerChoice {
    Service,
    Observe,
    Forward,
}

pub(crate) async fn run(cli: Cli) -> Result<()> {
    let config = ServiceConfig::load(&cli.config)?;
    if cli.check_config {
        println!(
            "Application configuration is valid. Kafka connectivity and native properties were not checked."
        );
        return Ok(());
    }
    let handler_choice = cli
        .handler
        .ok_or_else(|| anyhow!("select an explicit handler"))?;
    if matches!(handler_choice, HandlerChoice::Forward) {
        ensure!(
            cli.output_topic
                .as_ref()
                .is_some_and(|topic| !topic.trim().is_empty()),
            "--output-topic is required for --handler forward"
        );
    }
    let stale_after = Duration::from_millis(config.health.kafka_stale_after_ms.get());
    let grace = Duration::from_millis(config.shutdown.grace_period_ms.get());
    let health = Health::new(stale_after);
    let metrics = Metrics::new(&config);
    let intake_stop = CancellationToken::new();
    let abort = CancellationToken::new();
    let failure = CancellationToken::new();
    let close = CancellationToken::new();
    let http_stop = CancellationToken::new();
    let auth_stop = CancellationToken::new();
    let router = http::router(health.clone(), metrics.clone(), config.service.name.clone());
    let http_config = config.http.clone();
    let server_stop = http_stop.clone();
    let mut server =
        tokio::spawn(async move { http::serve(&http_config, router, server_stop).await });
    let startup_deadline =
        Instant::now() + Duration::from_millis(config.health.startup_timeout_ms.get());
    let signal = shutdown_signal();
    tokio::pin!(signal);
    let initialized = tokio::select! {
        result = tokio::time::timeout_at(startup_deadline, AuthManager::start(&config, metrics.clone(), auth_stop.clone())) => result,
        result = &mut signal => {
            let watchdog = ShutdownWatchdog::arm(grace);
            health.draining();
            auth_stop.cancel();
            http_stop.cancel();
            let stopped = server.await;
            watchdog.disarm();
            result?;
            stopped??;
            return Ok(());
        }
        _ = &mut server => return Err(anyhow!("HTTP server failed during startup")),
    };
    let auth = match initialized {
        Ok(Ok(auth)) => auth,
        result => {
            health.fail();
            http_stop.cancel();
            let _ = tokio::time::timeout(grace, server).await;
            return match result {
                Ok(Err(error)) => Err(error),
                _ => Err(anyhow!("authentication initialization timed out")),
            };
        }
    };
    let mut producer_properties = config.producer_properties();
    producer_properties.extend(auth.properties(config.producer_profile())?);
    let producer_context = KafkaContext::new(
        ClientSlot::Producer,
        metrics.clone(),
        auth.handle(config.producer_profile()),
    );
    let producer = Producer::new(
        producer_properties,
        producer_context,
        Duration::from_millis(config.kafka.producer.enqueue_timeout_ms.get()),
    )?;
    let handler = match handler_choice {
        HandlerChoice::Service => MessageHandler::Service,
        HandlerChoice::Observe => MessageHandler::Observe,
        HandlerChoice::Forward => MessageHandler::Forward {
            producer: producer.clone(),
            output_topic: cli
                .output_topic
                .ok_or_else(|| anyhow!("output topic is required"))?,
        },
    };
    let processor = Processor::new(
        handler,
        config.processing.max_concurrent_messages.get(),
        Duration::from_millis(config.processing.message_timeout_ms.get()),
        metrics.clone(),
    );
    let mut owners = JoinSet::new();
    let mut probes = Vec::new();
    for topic in &config.kafka.consumer.topics {
        for instance in 0..topic.consumers.get() {
            let profile = config.consumer_profile(topic);
            let mut properties = config.consumer_properties(topic, instance);
            properties.extend(auth.properties(profile)?);
            let settings = ConsumerSettings {
                topic: topic.name.clone(),
                properties,
                batch_size: topic.batch_size.get(),
                max_batch_bytes: topic.max_batch_bytes.get(),
                batch_wait: Duration::from_millis(topic.batch_max_wait_ms.get()),
                commit_timeout: Duration::from_millis(
                    config.kafka.consumer.commit_timeout_ms.get(),
                ),
                probe_interval: Duration::from_millis(config.health.kafka_probe_interval_ms.get()),
                probe_timeout: Duration::from_millis(config.health.kafka_probe_timeout_ms.get()),
            };
            let context = KafkaContext::new(
                ClientSlot::Consumer {
                    topic: topic.name.clone(),
                    instance,
                },
                metrics.clone(),
                auth.handle(profile),
            );
            let progress = Arc::new(ProbeState::default());
            probes.push(progress.clone());
            let processor = processor.clone();
            let signals = consumer::StopSignals {
                intake: intake_stop.clone(),
                abort: abort.clone(),
                failure: failure.clone(),
                close: close.clone(),
            };
            let runtime = tokio::runtime::Handle::current();
            owners.spawn(async move {
                let (send, receive) = tokio::sync::oneshot::channel();
                let owner = std::thread::Builder::new()
                    .name("kafka-owner".to_owned())
                    .spawn(move || {
                        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                            consumer::run(settings, context, processor, signals, progress, runtime)
                        }))
                        .unwrap_or_else(|_| Err(anyhow!("Kafka owner thread panicked")));
                        let _ = send.send(result);
                    })
                    .map_err(|_| anyhow!("Kafka owner thread could not be started"))?;
                let result = receive
                    .await
                    .map_err(|_| anyhow!("Kafka owner result channel closed"))?;
                tokio::task::spawn_blocking(move || owner.join())
                    .await
                    .map_err(|_| anyhow!("owner join task panicked"))?
                    .map_err(|_| anyhow!("owner thread panicked"))?;
                result
            });
        }
    }
    tracing::info!(
        consumers = probes.len(),
        "Kafka owners started; readiness is initially DOWN"
    );
    let mut tick = tokio::time::interval(Duration::from_millis(100));
    tick.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut next_probe = Instant::now();
    let mut producer_ok: Option<Instant> = None;
    let mut producer_probes = JoinSet::new();
    let mut started = false;
    let mut server_finished = false;
    let mut failed = false;
    loop {
        tokio::select! {
            result = &mut signal => {
                if result.is_err() { failed = true; }
                break;
            }
            _ = &mut server => { server_finished = true; failed = true; tracing::error!("HTTP server exited unexpectedly"); break; }
            _ = failure.cancelled() => { failed = true; tracing::error!("Kafka owner failed; stopping before native cleanup completes"); break; }
            result = producer_probes.join_next(), if !producer_probes.is_empty() => {
                match result {
                    Some(Ok(Ok(()))) => producer_ok = Some(Instant::now()),
                    Some(Err(_)) => { failed = true; metrics.panic("producer"); break; }
                    _ => {}
                }
            }
            result = owners.join_next() => {
                if matches!(result, Some(Err(_))) { metrics.panic("consumer"); }
                failed = true;
                tracing::error!("Kafka owner exited unexpectedly; restart required for replay");
                break;
            }
            _ = tick.tick() => {
                if !auth.healthy() || producer.failed() { failed = true; tracing::error!("Authentication is no longer healthy"); break; }
                if producer_probes.is_empty() && Instant::now() >= next_probe {
                    let probe_producer = producer.clone();
                    let probe_timeout = Duration::from_millis(config.health.kafka_probe_timeout_ms.get());
                    producer_probes.spawn(async move { probe_producer.probe(probe_timeout).await });
                    next_probe = Instant::now() + Duration::from_millis(config.health.kafka_probe_interval_ms.get());
                }
                let brokers_ready = producer_ok.is_some_and(|time| time.elapsed() <= stale_after) && probes.iter().all(|probe| probe.fresh(stale_after));
                if brokers_ready { health.broker_ok(); if !started { health.running(); started = true; tracing::info!("Service readiness is UP"); } }
                else { health.broker_unavailable(); }
                if !started && Instant::now() >= startup_deadline { failed = true; tracing::error!("Kafka startup deadline expired"); break; }
            }
        }
    }
    if failed {
        health.fail();
        abort.cancel();
    } else {
        health.draining();
    }
    intake_stop.cancel();
    let watchdog = ShutdownWatchdog::arm(grace);
    let deadline = Instant::now() + grace;
    let reserve = Duration::from_secs(2).min(grace / 4);
    let drain_deadline = deadline - reserve;
    while !owners.is_empty() {
        if abort.is_cancelled() || probes.iter().all(|probe| probe.drained()) {
            close.cancel();
        }
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_millis(20)), if !close.is_cancelled() => {}
            result = owners.join_next() => {
                if !matches!(result, Some(Ok(Ok(())))) {
                    if let Some(Ok(Err(error))) = result { tracing::error!(error = %error, "Kafka owner failed during drain"); }
                    failed = true; health.fail(); abort.cancel();
                }
            }
            _ = tokio::time::sleep_until(drain_deadline), if !abort.is_cancelled() => { failed = true; health.fail(); abort.cancel(); close.cancel(); }
        }
    }
    drop(processor);
    while let Some(result) = producer_probes.join_next().await {
        if result.is_err() {
            failed = true;
            health.fail();
        }
    }
    if producer
        .flush(deadline.saturating_duration_since(Instant::now()))
        .await
        .is_err()
    {
        failed = true;
        health.fail();
    }
    auth.shutdown().await;
    drop(producer);
    http_stop.cancel();
    if !server_finished && !matches!(server.await, Ok(Ok(()))) {
        failed = true;
    }
    watchdog.disarm();
    if failed {
        bail!("service stopped after a failure; unconfirmed records may replay");
    }
    tracing::info!("Service drained and stopped");
    Ok(())
}

async fn shutdown_signal() -> std::io::Result<()> {
    #[cfg(unix)]
    {
        let mut terminate =
            tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
        tokio::select! { result = tokio::signal::ctrl_c() => result, _ = terminate.recv() => Ok(()) }
    }
    #[cfg(not(unix))]
    {
        tokio::signal::ctrl_c().await
    }
}
struct ShutdownWatchdog {
    done: std::sync::mpsc::Sender<()>,
    thread: std::thread::JoinHandle<()>,
}
impl ShutdownWatchdog {
    fn arm(grace: Duration) -> Self {
        let (done, receive) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            if receive.recv_timeout(grace).is_err() {
                eprintln!("shutdown deadline exceeded; forcing nonzero exit");
                std::process::exit(2);
            }
        });
        Self { done, thread }
    }
    fn disarm(self) {
        let _ = self.done.send(());
        let _ = self.thread.join();
    }
}
