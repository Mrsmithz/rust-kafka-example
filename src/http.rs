use std::{net::SocketAddr, sync::Arc};

use axum::{
    Json, Router,
    body::Body,
    extract::{MatchedPath, Request, State},
    http::{HeaderValue, Method, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::Serialize;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

use crate::{config::HttpConfig, health::Health, metrics::Metrics};

const PROMETHEUS_CONTENT_TYPE: &str = "application/openmetrics-text; version=1.0.0; charset=utf-8";
const VERSION: &str = env!("CARGO_PKG_VERSION");

#[derive(Clone)]
struct HttpState {
    health: Health,
    metrics: Metrics,
    service_name: Arc<str>,
    started_at: Arc<str>,
    scrape_slots: Arc<Semaphore>,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
struct InfoResponse {
    name: String,
    version: &'static str,
    git_commit: Option<&'static str>,
    build_time: Option<&'static str>,
    started_at: String,
}

pub(crate) fn router(health: Health, metrics: Metrics, service_name: String) -> Router {
    let state = HttpState {
        health,
        metrics,
        service_name: Arc::from(service_name),
        started_at: Arc::from(started_at()),
        scrape_slots: Arc::new(Semaphore::new(1)),
    };

    Router::new()
        .route("/health", get(health_handler))
        .route("/info", get(info_handler))
        .route("/prometheus", get(prometheus_handler))
        .fallback(not_found)
        .with_state(state.clone())
        .layer(middleware::from_fn_with_state(state, record_http))
}

async fn health_handler(State(state): State<HttpState>) -> Response {
    let ready = state.health.ready();
    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };
    (
        status,
        Json(HealthResponse {
            status: if ready { "UP" } else { "DOWN" },
        }),
    )
        .into_response()
}

async fn info_handler(State(state): State<HttpState>) -> Json<InfoResponse> {
    Json(InfoResponse {
        name: state.service_name.to_string(),
        version: VERSION,
        git_commit: option_env!("GIT_COMMIT"),
        build_time: option_env!("BUILD_TIME"),
        started_at: state.started_at.to_string(),
    })
}

async fn prometheus_handler(State(state): State<HttpState>) -> Response {
    let Ok(_permit) = state.scrape_slots.clone().try_acquire_owned() else {
        return StatusCode::SERVICE_UNAVAILABLE.into_response();
    };
    match state.metrics.render(state.health.ready()) {
        Ok(output) => {
            let mut response = Response::new(Body::from(output));
            *response.status_mut() = StatusCode::OK;
            response.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static(PROMETHEUS_CONTENT_TYPE),
            );
            response
        }
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR.into_response(),
    }
}

async fn not_found() -> impl IntoResponse {
    StatusCode::NOT_FOUND
}

async fn record_http(State(state): State<HttpState>, request: Request, next: Next) -> Response {
    let started = tokio::time::Instant::now();
    let method = method_label(request.method());
    let route = match request
        .extensions()
        .get::<MatchedPath>()
        .map(MatchedPath::as_str)
    {
        Some("/health") => "/health",
        Some("/info") => "/info",
        Some("/prometheus") => "/prometheus",
        _ => "__unmatched__",
    };
    let response = next.run(request).await;
    state.metrics.http(
        method,
        route,
        response.status().as_u16(),
        started.elapsed().as_secs_f64(),
    );
    response
}

fn method_label(method: &Method) -> &'static str {
    match method.as_str() {
        "GET" => "GET",
        "POST" => "POST",
        "PUT" => "PUT",
        "DELETE" => "DELETE",
        "PATCH" => "PATCH",
        "HEAD" => "HEAD",
        "OPTIONS" => "OPTIONS",
        _ => "OTHER",
    }
}

fn started_at() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

pub(crate) async fn serve(
    config: &HttpConfig,
    router: Router,
    shutdown: CancellationToken,
) -> anyhow::Result<()> {
    let address: SocketAddr = config.bind;
    let handle = axum_server::Handle::new();

    let mut server = if let Some(tls) = config.tls.as_ref().filter(|tls| tls.enabled) {
        let tls_config =
            axum_server::tls_rustls::RustlsConfig::from_pem_file(&tls.cert_file, &tls.key_file)
                .await
                .map_err(|_| anyhow::anyhow!("failed to load HTTP TLS identity"))?;
        let server_handle = handle.clone();
        Box::pin(async move {
            axum_server::bind_rustls(address, tls_config)
                .handle(server_handle)
                .serve(router.into_make_service())
                .await
        })
            as std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>>
    } else {
        let server_handle = handle.clone();
        Box::pin(async move {
            axum_server::bind(address)
                .handle(server_handle)
                .serve(router.into_make_service())
                .await
        })
            as std::pin::Pin<Box<dyn std::future::Future<Output = std::io::Result<()>> + Send>>
    };

    tokio::select! {
        result = &mut server => result.map_err(|error| anyhow::anyhow!("HTTP server failed: {error}")),
        _ = shutdown.cancelled() => {
            handle.graceful_shutdown(None);
            server.await.map_err(|error| anyhow::anyhow!("HTTP server shutdown failed: {error}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    fn test_metrics() -> anyhow::Result<Metrics> {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config/example.toml");
        let config = crate::config::ServiceConfig::load(&path)
            .map_err(|_| anyhow::anyhow!("test configuration could not be loaded"))?;
        Ok(Metrics::new(&config))
    }

    #[tokio::test]
    async fn live_http_and_https_validate_trust_hostname_and_graceful_stop() -> anyhow::Result<()> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let identity = rcgen::generate_simple_self_signed(vec!["localhost".to_owned()])?;
        let files = tempfile::tempdir()?;
        let certificate = files.path().join("server.crt");
        let key = files.path().join("server.key");
        std::fs::write(&certificate, identity.cert.pem())?;
        std::fs::write(&key, identity.signing_key.serialize_pem())?;
        for secure in [false, true] {
            let reservation = std::net::TcpListener::bind("127.0.0.1:0")?;
            let address = reservation.local_addr()?;
            drop(reservation);
            let config = HttpConfig {
                bind: address,
                tls: secure.then(|| crate::config::HttpTlsConfig {
                    enabled: true,
                    cert_file: certificate.clone(),
                    key_file: key.clone(),
                }),
            };
            let health = Health::new(std::time::Duration::from_secs(60));
            health.broker_ok();
            health.running();
            let app = router(health, test_metrics()?, "listener-test".to_owned());
            let stop = CancellationToken::new();
            let server_stop = stop.clone();
            let server = tokio::spawn(async move { serve(&config, app, server_stop).await });
            let client = reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(2))
                .add_root_certificate(reqwest::Certificate::from_pem(
                    identity.cert.pem().as_bytes(),
                )?)
                .build()?;
            let scheme = if secure { "https" } else { "http" };
            let url = format!("{scheme}://localhost:{}/health", address.port());
            let response = tokio::time::timeout(std::time::Duration::from_secs(5), async {
                loop {
                    if let Ok(response) = client.get(&url).send().await {
                        break response;
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await?;
            assert_eq!(response.status(), StatusCode::OK);
            assert_eq!(
                response.json::<serde_json::Value>().await?,
                serde_json::json!({"status":"UP"})
            );
            if secure {
                assert!(
                    reqwest::Client::builder()
                        .timeout(std::time::Duration::from_secs(2))
                        .build()?
                        .get(&url)
                        .send()
                        .await
                        .is_err()
                );
                let wrong_host = format!("https://127.0.0.1:{}/health", address.port());
                assert!(client.get(wrong_host).send().await.is_err());
            }
            stop.cancel();
            tokio::time::timeout(std::time::Duration::from_secs(5), server).await???;
        }
        Ok(())
    }

    #[tokio::test]
    async fn endpoints_return_contract_status_and_safe_unknown_route_metrics() -> anyhow::Result<()>
    {
        let health = Health::new(std::time::Duration::from_secs(10));
        let app = router(health.clone(), test_metrics()?, "test-service".to_owned());

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/health")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), usize::MAX).await?,
            r#"{"status":"DOWN"}"#
        );

        health.broker_ok();
        health.running();
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/health")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            axum::body::to_bytes(response.into_body(), usize::MAX).await?,
            r#"{"status":"UP"}"#
        );

        let response = app
            .clone()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/info")
                    .body(Body::empty())?,
            )
            .await?;
        let info: serde_json::Value =
            serde_json::from_slice(&axum::body::to_bytes(response.into_body(), usize::MAX).await?)?;
        assert_eq!(info["name"], "test-service");
        assert_eq!(info["version"], VERSION);
        assert!(info.get("config").is_none());

        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/untrusted/secret?token=x")
                    .body(Body::empty())?,
            )
            .await?;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        Ok(())
    }
}
