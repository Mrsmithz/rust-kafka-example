use std::{
    collections::{BTreeMap, BTreeSet},
    error::Error,
    path::Path,
    sync::{Arc, RwLock},
    time::Instant,
};

use anyhow::anyhow;
#[cfg(any(test, feature = "aws-iam"))]
use std::time::Duration;
#[cfg(feature = "aws-iam")]
use std::time::SystemTime;
use tokio::task::JoinHandle;
#[cfg(feature = "aws-iam")]
use tokio::time::timeout;
use tokio_util::sync::CancellationToken;

use crate::{
    config::{AuthProfile, ScramMechanism, ServiceConfig},
    metrics::Metrics,
};

#[cfg(feature = "aws-iam")]
const REFRESH_TIMEOUT: Duration = Duration::from_secs(10);
#[cfg(feature = "aws-iam")]
const REFRESH_RETRY: Duration = Duration::from_secs(5);
#[cfg(feature = "aws-iam")]
const REFRESH_AHEAD: Duration = Duration::from_secs(60);
#[cfg(feature = "aws-iam")]
const MAX_SIGNED_TOKEN_AGE: Duration = Duration::from_secs(900);
#[cfg(feature = "aws-iam")]
const CREDENTIAL_EXPIRY_MARGIN: Duration = Duration::from_secs(60);
const PRINCIPAL: &str = "rust-kafka-service";

#[derive(Clone)]
pub(crate) struct AuthHandle {
    cache: Arc<RwLock<Option<CachedToken>>>,
}

struct CachedToken {
    token: String,
    expires_at: Instant,
    lifetime_ms: i64,
}

impl AuthHandle {
    pub(crate) fn token(&self) -> Result<rdkafka::client::OAuthToken, Box<dyn Error>> {
        let cache = self
            .cache
            .read()
            .map_err(|_| boxed_error("authentication token cache unavailable"))?;
        let cached = cache
            .as_ref()
            .filter(|cached| cached.expires_at > Instant::now())
            .ok_or_else(|| boxed_error("authentication token unavailable or expired"))?;
        Ok(rdkafka::client::OAuthToken {
            token: cached.token.clone(),
            principal_name: PRINCIPAL.to_owned(),
            lifetime_ms: cached.lifetime_ms,
        })
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.cache
            .read()
            .map(|cache| {
                cache
                    .as_ref()
                    .is_some_and(|cached| cached.expires_at > Instant::now())
            })
            .unwrap_or(false)
    }
}

fn boxed_error(message: &'static str) -> Box<dyn Error> {
    Box::new(std::io::Error::other(message))
}

pub(crate) struct AuthManager {
    properties: BTreeMap<String, BTreeMap<String, String>>,
    handles: BTreeMap<String, AuthHandle>,
    refresh_tasks: Vec<JoinHandle<()>>,
    refresh_cancel: CancellationToken,
}

impl AuthManager {
    pub(crate) async fn start(
        config: &ServiceConfig,
        metrics: Metrics,
        cancel: CancellationToken,
    ) -> anyhow::Result<Self> {
        let active = active_profiles(config);
        let mut properties = BTreeMap::new();
        #[cfg(feature = "aws-iam")]
        let mut handles = BTreeMap::new();
        #[cfg(not(feature = "aws-iam"))]
        let handles = BTreeMap::new();
        #[cfg(not(feature = "aws-iam"))]
        let _ = &metrics;
        #[cfg(feature = "aws-iam")]
        let mut refreshers = Vec::new();
        let refresh_cancel = cancel;

        for name in active {
            let profile = config
                .kafka
                .auth_profiles
                .get(&name)
                .ok_or_else(|| anyhow!("configured authentication profile is unavailable"))?;
            match profile {
                AuthProfile::Plaintext => {
                    properties.insert(name, plaintext_properties());
                }
                AuthProfile::Tls { ca_file } => {
                    properties.insert(name, tls_properties(ca_file.as_deref())?);
                }
                AuthProfile::Mtls {
                    cert_file,
                    key_file,
                    key_password_file,
                    ca_file,
                } => {
                    let mut props = tls_properties(ca_file.as_deref())?;
                    props.insert("ssl.certificate.location".into(), path_value(cert_file)?);
                    props.insert("ssl.key.location".into(), path_value(key_file)?);
                    if let Some(path) = key_password_file {
                        props.insert("ssl.key.password".into(), read_secret(path)?);
                    }
                    properties.insert(name, props);
                }
                AuthProfile::SaslPlain {
                    username_file,
                    password_file,
                    ca_file,
                } => {
                    let mut props = tls_properties(ca_file.as_deref())?;
                    props.extend(sasl_credentials("PLAIN", username_file, password_file)?);
                    properties.insert(name, props);
                }
                AuthProfile::SaslScram {
                    mechanism,
                    username_file,
                    password_file,
                    ca_file,
                } => {
                    let mechanism = match mechanism {
                        ScramMechanism::Sha256 => "SCRAM-SHA-256",
                        ScramMechanism::Sha512 => "SCRAM-SHA-512",
                    };
                    let mut props = tls_properties(ca_file.as_deref())?;
                    props.extend(sasl_credentials(mechanism, username_file, password_file)?);
                    properties.insert(name, props);
                }
                AuthProfile::AwsMskIam { region, .. } => {
                    #[cfg(feature = "aws-iam")]
                    {
                        let initialized = initialize_iam(region).await;
                        if initialized.is_err() {
                            metrics.auth_refresh(&name, false);
                        }
                        let (provider, cache) = initialized?;
                        let handle = AuthHandle {
                            cache: Arc::clone(&cache),
                        };
                        handles.insert(name.clone(), handle);
                        properties.insert(name.clone(), iam_properties());
                        metrics.auth_refresh(&name, true);
                        refreshers.push((name, region.clone(), provider, cache));
                    }
                    #[cfg(not(feature = "aws-iam"))]
                    {
                        let _ = region;
                        return Err(anyhow!(
                            "AWS MSK IAM authentication is unavailable in this build"
                        ));
                    }
                }
            }
        }

        #[cfg(feature = "aws-iam")]
        let mut tasks = Vec::with_capacity(refreshers.len());
        #[cfg(feature = "aws-iam")]
        for (name, region, provider, cache) in refreshers {
            let task_cancel = refresh_cancel.child_token();
            let task_metrics = metrics.clone();
            tasks.push(tokio::spawn(async move {
                refresh_loop(name, region, provider, cache, task_metrics, task_cancel).await;
            }));
        }

        Ok(Self {
            properties,
            handles,
            refresh_cancel,
            refresh_tasks: {
                #[cfg(feature = "aws-iam")]
                {
                    tasks
                }
                #[cfg(not(feature = "aws-iam"))]
                {
                    Vec::new()
                }
            },
        })
    }

    pub(crate) fn properties(&self, profile: &str) -> anyhow::Result<BTreeMap<String, String>> {
        self.properties
            .get(profile)
            .cloned()
            .ok_or_else(|| anyhow!("authentication profile is not active"))
    }

    pub(crate) fn handle(&self, profile: &str) -> Option<AuthHandle> {
        self.handles.get(profile).cloned()
    }

    pub(crate) fn healthy(&self) -> bool {
        self.handles.values().all(AuthHandle::is_valid)
            && self.refresh_tasks.iter().all(|task| !task.is_finished())
    }

    pub(crate) async fn shutdown(self) {
        self.refresh_cancel.cancel();
        for task in self.refresh_tasks {
            let _ = task.await;
        }
    }
}

fn active_profiles(config: &ServiceConfig) -> BTreeSet<String> {
    let mut active = BTreeSet::new();
    active.insert(config.producer_profile().to_owned());
    for topic in &config.kafka.consumer.topics {
        active.insert(config.consumer_profile(topic).to_owned());
    }
    active
}

fn plaintext_properties() -> BTreeMap<String, String> {
    BTreeMap::from([("security.protocol".into(), "plaintext".into())])
}

fn tls_properties(ca_file: Option<&Path>) -> anyhow::Result<BTreeMap<String, String>> {
    let mut props = BTreeMap::from([
        ("security.protocol".into(), "ssl".into()),
        ("enable.ssl.certificate.verification".into(), "true".into()),
        (
            "ssl.endpoint.identification.algorithm".into(),
            "https".into(),
        ),
    ]);
    if let Some(path) = ca_file {
        props.insert("ssl.ca.location".into(), path_value(path)?);
    }
    Ok(props)
}

fn path_value(path: &Path) -> anyhow::Result<String> {
    path.to_str()
        .map(ToOwned::to_owned)
        .ok_or_else(|| anyhow!("authentication file path is not valid Unicode"))
}

fn read_secret(path: &Path) -> anyhow::Result<String> {
    let value = std::fs::read_to_string(path)
        .map_err(|_| anyhow!("authentication credential file could not be read"))?;
    let value = value.trim_end_matches(['\r', '\n']);
    if value.trim().is_empty() {
        return Err(anyhow!("authentication credential file is empty"));
    }
    Ok(value.to_owned())
}

fn sasl_credentials(
    mechanism: &str,
    username_file: &Path,
    password_file: &Path,
) -> anyhow::Result<BTreeMap<String, String>> {
    Ok(BTreeMap::from([
        ("security.protocol".into(), "sasl_ssl".into()),
        ("sasl.mechanism".into(), mechanism.into()),
        ("sasl.username".into(), read_secret(username_file)?),
        ("sasl.password".into(), read_secret(password_file)?),
    ]))
}

#[cfg(any(test, feature = "aws-iam"))]
fn iam_properties() -> BTreeMap<String, String> {
    BTreeMap::from([
        ("security.protocol".into(), "sasl_ssl".into()),
        ("sasl.mechanism".into(), "OAUTHBEARER".into()),
    ])
}

#[cfg(feature = "aws-iam")]
async fn initialize_iam(
    region: &str,
) -> anyhow::Result<(
    aws_credential_types::provider::SharedCredentialsProvider,
    Arc<RwLock<Option<CachedToken>>>,
)> {
    let config = timeout(
        REFRESH_TIMEOUT,
        aws_config::defaults(aws_config::BehaviorVersion::latest())
            .region(aws_types::region::Region::new(region.to_owned()))
            .load(),
    )
    .await
    .map_err(|_| anyhow!("AWS credential provider initialization timed out"))?;
    let provider = config
        .credentials_provider()
        .ok_or_else(|| anyhow!("AWS default credential provider is unavailable"))?;
    let cache = Arc::new(RwLock::new(None));
    let token = timeout(REFRESH_TIMEOUT, sign_iam_token(&provider, region))
        .await
        .map_err(|_| anyhow!("AWS MSK IAM token initialization timed out"))??;
    set_cache(&cache, token)?;
    Ok((provider, cache))
}

#[cfg(feature = "aws-iam")]
async fn refresh_loop(
    name: String,
    region: String,
    provider: aws_credential_types::provider::SharedCredentialsProvider,
    cache: Arc<RwLock<Option<CachedToken>>>,
    metrics: Metrics,
    cancel: CancellationToken,
) {
    loop {
        let delay = cache
            .read()
            .ok()
            .and_then(|value| {
                value
                    .as_ref()
                    .map(|token| token.expires_at.saturating_duration_since(Instant::now()))
            })
            .map(|remaining| remaining.saturating_sub(REFRESH_AHEAD))
            .unwrap_or(Duration::ZERO);
        tokio::select! {
            _ = cancel.cancelled() => return,
            _ = tokio::time::sleep(delay) => {}
        }

        let mut refreshed = false;
        for attempt in 0..3 {
            let result = timeout(REFRESH_TIMEOUT, sign_iam_token(&provider, &region)).await;
            if let Ok(Ok(token)) = result
                && set_cache(&cache, token).is_ok()
            {
                refreshed = true;
                metrics.auth_refresh(&name, true);
                break;
            }
            if attempt < 2 {
                tokio::select! {
                    _ = cancel.cancelled() => return,
                    _ = tokio::time::sleep(REFRESH_RETRY) => {}
                }
            }
        }
        if !refreshed {
            metrics.auth_refresh(&name, false);
        }
    }
}

#[cfg(feature = "aws-iam")]
fn set_cache(cache: &Arc<RwLock<Option<CachedToken>>>, token: SignedToken) -> anyhow::Result<()> {
    let mut guard = cache
        .write()
        .map_err(|_| anyhow!("authentication token cache unavailable"))?;
    *guard = Some(CachedToken {
        token: token.encoded,
        expires_at: token.expires_at,
        lifetime_ms: token.lifetime_ms,
    });
    Ok(())
}

#[cfg(feature = "aws-iam")]
struct SignedToken {
    encoded: String,
    expires_at: Instant,
    lifetime_ms: i64,
}

#[cfg(feature = "aws-iam")]
async fn sign_iam_token(
    provider: &aws_credential_types::provider::SharedCredentialsProvider,
    region: &str,
) -> anyhow::Result<SignedToken> {
    use aws_credential_types::provider::ProvideCredentials;

    let credentials = timeout(REFRESH_TIMEOUT, provider.provide_credentials())
        .await
        .map_err(|_| anyhow!("AWS credential refresh timed out"))?
        .map_err(|_| anyhow!("AWS credentials are unavailable"))?;
    sign_credentials(&credentials, region, SystemTime::now())
}

#[cfg(feature = "aws-iam")]
fn sign_credentials(
    credentials: &aws_credential_types::Credentials,
    region: &str,
    now: SystemTime,
) -> anyhow::Result<SignedToken> {
    use aws_sigv4::http_request::{
        SignableBody, SignableRequest, SignatureLocation, SigningParams, SigningSettings, sign,
    };
    use aws_sigv4::sign::v4;
    use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
    use std::time::UNIX_EPOCH;

    let monotonic_now = Instant::now();
    let mut lifetime = MAX_SIGNED_TOKEN_AGE;
    if let Some(expiry) = credentials.expiry() {
        let until_expiry = expiry
            .duration_since(now)
            .map_err(|_| anyhow!("AWS credentials have expired"))?;
        lifetime = lifetime.min(until_expiry.saturating_sub(CREDENTIAL_EXPIRY_MARGIN));
    }
    if lifetime.is_zero() {
        return Err(anyhow!("AWS credentials expire too soon to sign a token"));
    }

    let expires_seconds = lifetime.as_secs().min(MAX_SIGNED_TOKEN_AGE.as_secs());
    anyhow::ensure!(
        expires_seconds > 0,
        "AWS credentials expire too soon to sign a token"
    );
    let lifetime = Duration::from_secs(expires_seconds);
    let query = "Action=kafka-cluster%3AConnect&User-Agent=rust-kafka-service";
    let uri = format!("https://kafka.{region}.amazonaws.com/?{query}");
    let mut request = axum::http::Request::builder()
        .method("GET")
        .uri(&uri)
        .body(())
        .map_err(|_| anyhow!("AWS MSK signing request could not be built"))?;
    let signable = SignableRequest::new(
        "GET",
        request.uri().to_string(),
        request
            .headers()
            .iter()
            .filter_map(|(name, value)| value.to_str().ok().map(|value| (name.as_str(), value))),
        SignableBody::empty(),
    )
    .map_err(|_| anyhow!("AWS MSK signing request is invalid"))?;
    let identity = credentials.clone().into();
    let mut settings = SigningSettings::default();
    settings.signature_location = SignatureLocation::QueryParams;
    settings.expires_in = Some(lifetime);
    let params = v4::SigningParams::builder()
        .identity(&identity)
        .region(region)
        .name("kafka-cluster")
        .time(now)
        .settings(settings)
        .build()
        .map_err(|_| anyhow!("AWS MSK signing parameters are invalid"))?;
    let (instructions, _) = sign(signable, &SigningParams::from(params))
        .map_err(|_| anyhow!("AWS MSK token signing failed"))?
        .into_parts();
    instructions.apply_to_request_http1x(&mut request);

    let expires_at_system = now
        .checked_add(lifetime)
        .ok_or_else(|| anyhow!("AWS MSK token expiration is out of range"))?;
    let lifetime_ms = expires_at_system
        .duration_since(UNIX_EPOCH)
        .map_err(|_| anyhow!("system clock is before Unix epoch"))?
        .as_millis()
        .try_into()
        .map_err(|_| anyhow!("AWS MSK token expiration is out of range"))?;
    let encoded = URL_SAFE_NO_PAD.encode(request.uri().to_string().as_bytes());
    let expires_at = monotonic_now
        .checked_add(lifetime)
        .ok_or_else(|| anyhow!("AWS MSK token expiration is out of range"))?;
    Ok(SignedToken {
        encoded,
        expires_at,
        lifetime_ms,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[cfg(feature = "aws-iam")]
    use std::time::UNIX_EPOCH;

    #[test]
    fn refresh_properties_use_verified_tls_and_fixed_auth_modes() {
        assert_eq!(
            tls_properties(None)
                .ok()
                .and_then(|props| props.get("enable.ssl.certificate.verification").cloned()),
            Some("true".to_owned())
        );
        assert_eq!(
            iam_properties().get("sasl.mechanism"),
            Some(&"OAUTHBEARER".to_owned())
        );
        assert_eq!(
            plaintext_properties().get("security.protocol"),
            Some(&"plaintext".to_owned())
        );
    }

    #[tokio::test]
    async fn unexpected_refresh_worker_exit_makes_authentication_unhealthy() -> anyhow::Result<()> {
        let worker = tokio::spawn(async {});
        tokio::time::timeout(Duration::from_secs(2), async {
            while !worker.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        let manager = AuthManager {
            properties: BTreeMap::new(),
            handles: BTreeMap::new(),
            refresh_cancel: CancellationToken::new(),
            refresh_tasks: vec![worker],
        };
        assert!(!manager.healthy());
        manager.shutdown().await;
        Ok(())
    }

    #[test]
    fn cached_handle_returns_only_unexpired_token() {
        let handle = AuthHandle {
            cache: Arc::new(RwLock::new(Some(CachedToken {
                token: "fixture-token".into(),
                expires_at: Instant::now() + Duration::from_secs(30),
                lifetime_ms: 1234,
            }))),
        };
        let token = handle.token();
        assert!(token.is_ok());
        assert_eq!(
            token.map(|token| token.token).ok().as_deref(),
            Some("fixture-token")
        );

        if let Ok(mut cache) = handle.cache.write() {
            *cache = Some(CachedToken {
                token: "expired-fixture".into(),
                expires_at: Instant::now() - Duration::from_secs(1),
                lifetime_ms: 1234,
            });
        }
        assert!(!handle.is_valid());
        assert!(handle.token().is_err());
    }

    #[cfg(feature = "aws-iam")]
    #[test]
    fn signed_token_shape_and_lifetime_are_fixture_deterministic() {
        use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};

        let now = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let credentials = aws_credential_types::Credentials::new(
            "access-key-fixture",
            "secret-fixture",
            Some("session-fixture".into()),
            Some(now + Duration::from_secs(120)),
            "fixture-provider",
        );
        let signed = sign_credentials(&credentials, "us-east-1", now);
        assert!(signed.is_ok());
        let Ok(signed) = signed else {
            return;
        };
        assert_eq!(signed.lifetime_ms, 1_700_000_060_000);
        let decoded = URL_SAFE_NO_PAD.decode(signed.encoded);
        assert!(decoded.is_ok());
        let Ok(decoded) = decoded else {
            return;
        };
        let uri = String::from_utf8(decoded);
        assert!(uri.is_ok());
        let Ok(uri) = uri else {
            return;
        };
        assert!(uri.starts_with("https://kafka.us-east-1.amazonaws.com/?"));
        assert!(uri.contains("Action=kafka-cluster%3AConnect"));
        assert!(uri.contains("User-Agent=rust-kafka-service"));
        assert!(uri.contains("X-Amz-Expires=60"));
        assert!(uri.contains("X-Amz-Signature="));
    }
}
