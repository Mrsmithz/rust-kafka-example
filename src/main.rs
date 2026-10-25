mod config;
mod health;
mod http;
mod kafka;
mod metrics;
mod processing;
mod runtime;
mod service;

use clap::Parser;

#[tokio::main]
async fn main() -> std::process::ExitCode {
    let _ = rustls::crypto::ring::default_provider().install_default();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,rdkafka=off".into()),
        )
        .json()
        .init();
    std::panic::set_hook(Box::new(|info| {
        if let Some(location) = info.location() {
            tracing::error!(
                file = location.file(),
                line = location.line(),
                "Task panicked; panic payload suppressed"
            );
        }
    }));
    match runtime::run(runtime::Cli::parse()).await {
        Ok(()) => std::process::ExitCode::SUCCESS,
        Err(error) => {
            tracing::error!(error = %error, "Service failed");
            std::process::ExitCode::FAILURE
        }
    }
}
