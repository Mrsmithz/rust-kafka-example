use std::{
    collections::BTreeMap,
    net::SocketAddr,
    num::{NonZeroU64, NonZeroUsize},
    path::{Path, PathBuf},
};

use serde::Deserialize;
use thiserror::Error;

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = include_str!("../config/example.toml");

    #[test]
    fn example_config_parses_and_inherits_profiles() -> Result<(), ConfigError> {
        let config = ServiceConfig::parse(BASE)?;
        let topic = &config.kafka.consumer.topics[0];
        assert_eq!(config.consumer_profile(topic), "local");
        assert_eq!(config.producer_profile(), "local");
        let properties = config.consumer_properties(topic, 0);
        assert_eq!(
            properties.get("bootstrap.servers").map(String::as_str),
            Some("localhost:9092")
        );
        assert_eq!(
            properties.get("enable.auto.commit").map(String::as_str),
            Some("false")
        );
        Ok(())
    }

    #[test]
    fn syntax_errors_do_not_echo_input() {
        let secret = "super-secret-marker";
        let error = match ServiceConfig::parse(&format!("[service\nname='{secret}'")) {
            Err(error) => error,
            Ok(_) => panic!("invalid syntax accepted"),
        };
        assert!(!error.to_string().contains(secret));
    }

    #[test]
    fn rejects_unknown_application_fields() {
        assert!(
            ServiceConfig::parse(&BASE.replace(
                "name = \"rust-kafka-service\"",
                "name = \"rust-kafka-service\"\nunknown = 1"
            ))
            .is_err()
        );
    }

    #[test]
    fn rejects_zero_consumer_count() {
        assert!(ServiceConfig::parse(&BASE.replace("consumers = 2", "consumers = 0")).is_err());
    }

    #[test]
    fn rejects_unsafe_properties_even_when_shadowed() {
        let input = BASE.replace(
            "[kafka.consumer.properties]",
            "[kafka.consumer.properties]\n\"enable.auto.commit\" = \"true\"",
        );
        assert!(ServiceConfig::parse(&input).is_err());
    }

    #[test]
    fn rejects_unavailable_named_profile() {
        assert!(
            ServiceConfig::parse(
                &BASE.replace("auth_profile = \"local\"", "auth_profile = \"missing\"")
            )
            .is_err()
        );
    }

    #[test]
    fn generated_consumer_client_ids_are_unique_and_safe_properties_are_set()
    -> Result<(), ConfigError> {
        let config = ServiceConfig::parse(BASE)?;
        let topic = &config.kafka.consumer.topics[0];
        let a = config.consumer_properties(topic, 0);
        let b = config.consumer_properties(topic, 1);
        assert_ne!(a.get("client.id"), b.get("client.id"));
        assert_eq!(
            a.get("statistics.interval.ms").map(String::as_str),
            Some("10000")
        );
        Ok(())
    }

    #[test]
    fn rejects_conflicting_statistics_interval_at_common_level() {
        let input = BASE.replace(
            "[kafka.common_properties]",
            "[kafka.common_properties]\n\"statistics.interval.ms\" = \"5\"",
        );
        assert!(ServiceConfig::parse(&input).is_err());
    }
}

#[derive(Debug, Error)]
pub(crate) enum ConfigError {
    #[error("configuration file could not be read")]
    Read,
    #[error("configuration is invalid")]
    Invalid,
    #[error("configuration contains an unknown field or malformed value")]
    Parse,
    #[error("a configured file is missing or unreadable")]
    File,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ServiceConfig {
    pub(crate) service: ServiceSettings,
    pub(crate) http: HttpConfig,
    pub(crate) health: HealthConfig,
    pub(crate) processing: ProcessingConfig,
    pub(crate) metrics: MetricsConfig,
    pub(crate) shutdown: ShutdownConfig,
    pub(crate) kafka: KafkaConfig,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ServiceSettings {
    pub(crate) name: String,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpConfig {
    pub(crate) bind: SocketAddr,
    #[serde(default)]
    pub(crate) tls: Option<HttpTlsConfig>,
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HttpTlsConfig {
    pub(crate) enabled: bool,
    pub(crate) cert_file: PathBuf,
    pub(crate) key_file: PathBuf,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HealthConfig {
    pub(crate) startup_timeout_ms: NonZeroU64,
    pub(crate) kafka_probe_interval_ms: NonZeroU64,
    pub(crate) kafka_probe_timeout_ms: NonZeroU64,
    pub(crate) kafka_stale_after_ms: NonZeroU64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProcessingConfig {
    pub(crate) max_concurrent_messages: NonZeroUsize,
    pub(crate) message_timeout_ms: NonZeroU64,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct MetricsConfig {
    pub(crate) kafka_statistics_interval_ms: NonZeroU64,
    pub(crate) native_statistics_stale_after_ms: NonZeroU64,
    pub(crate) processing_duration_buckets_seconds: Vec<f64>,
    pub(crate) http_duration_buckets_seconds: Vec<f64>,
    #[serde(default)]
    pub(crate) producer_topics: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ShutdownConfig {
    pub(crate) grace_period_ms: NonZeroU64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct KafkaConfig {
    #[serde(default)]
    pub(crate) auth_profile: Option<String>,
    #[serde(default)]
    pub(crate) auth_profiles: BTreeMap<String, AuthProfile>,
    #[serde(default)]
    pub(crate) common_properties: BTreeMap<String, String>,
    pub(crate) consumer: ConsumerConfig,
    pub(crate) producer: ProducerConfig,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ConsumerConfig {
    #[serde(default = "default_commit_timeout")]
    pub(crate) commit_timeout_ms: NonZeroU64,
    #[serde(default)]
    pub(crate) group_id: Option<String>,
    #[serde(default)]
    pub(crate) auth_profile: Option<String>,
    #[serde(default)]
    pub(crate) properties: BTreeMap<String, String>,
    pub(crate) topics: Vec<TopicConfig>,
}
fn default_commit_timeout() -> NonZeroU64 {
    NonZeroU64::new(10_000).unwrap_or(NonZeroU64::MIN)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct TopicConfig {
    pub(crate) name: String,
    pub(crate) consumers: NonZeroUsize,
    pub(crate) batch_size: NonZeroUsize,
    pub(crate) batch_max_wait_ms: NonZeroU64,
    pub(crate) max_batch_bytes: NonZeroUsize,
    #[serde(default)]
    pub(crate) group_id: Option<String>,
    #[serde(default)]
    pub(crate) auth_profile: Option<String>,
    #[serde(default)]
    pub(crate) properties: BTreeMap<String, String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct ProducerConfig {
    #[serde(default)]
    pub(crate) auth_profile: Option<String>,
    #[serde(default)]
    pub(crate) properties: BTreeMap<String, String>,
    pub(crate) enqueue_timeout_ms: NonZeroU64,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum AuthProfile {
    Plaintext,
    Tls {
        #[serde(default)]
        ca_file: Option<PathBuf>,
    },
    Mtls {
        cert_file: PathBuf,
        key_file: PathBuf,
        #[serde(default)]
        key_password_file: Option<PathBuf>,
        #[serde(default)]
        ca_file: Option<PathBuf>,
    },
    SaslPlain {
        username_file: PathBuf,
        password_file: PathBuf,
        #[serde(default)]
        ca_file: Option<PathBuf>,
    },
    SaslScram {
        mechanism: ScramMechanism,
        username_file: PathBuf,
        password_file: PathBuf,
        #[serde(default)]
        ca_file: Option<PathBuf>,
    },
    AwsMskIam {
        region: String,
        #[serde(default = "default_credentials")]
        credentials: String,
    },
}
#[derive(Deserialize, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ScramMechanism {
    #[serde(rename = "SCRAM-SHA-256")]
    Sha256,
    #[serde(rename = "SCRAM-SHA-512")]
    Sha512,
}
fn default_credentials() -> String {
    "default_chain".to_owned()
}

impl ServiceConfig {
    pub(crate) fn load(path: &Path) -> Result<Self, ConfigError> {
        let input = std::fs::read_to_string(path).map_err(|_| ConfigError::Read)?;
        let mut config = Self::parse(&input)?;
        config.resolve_paths(path.parent().unwrap_or_else(|| Path::new(".")));
        config.validate(true)?;
        Ok(config)
    }

    fn parse(input: &str) -> Result<Self, ConfigError> {
        let source = config::File::from_str(input, config::FileFormat::Toml);
        let config: Self = config::Config::builder()
            .add_source(source)
            .build()
            .map_err(|_| ConfigError::Parse)?
            .try_deserialize()
            .map_err(|_| ConfigError::Parse)?;
        config.validate(false)?;
        Ok(config)
    }

    fn validate(&self, check_files: bool) -> Result<(), ConfigError> {
        let invalid = || ConfigError::Invalid;
        if self.service.name.trim().is_empty() {
            return Err(invalid());
        }
        if let Some(tls) = &self.http.tls {
            if tls.cert_file.as_os_str().is_empty() || tls.key_file.as_os_str().is_empty() {
                return Err(invalid());
            }
            if check_files
                && tls.enabled
                && (std::fs::File::open(&tls.cert_file).is_err()
                    || std::fs::File::open(&tls.key_file).is_err())
            {
                return Err(ConfigError::File);
            }
        }
        for buckets in [
            &self.metrics.processing_duration_buckets_seconds,
            &self.metrics.http_duration_buckets_seconds,
        ] {
            if buckets.is_empty()
                || buckets.iter().any(|n| !n.is_finite() || *n <= 0.0)
                || buckets.windows(2).any(|w| w[0] >= w[1])
            {
                return Err(invalid());
            }
        }
        if self.metrics.native_statistics_stale_after_ms
            <= self.metrics.kafka_statistics_interval_ms
            || self
                .metrics
                .producer_topics
                .iter()
                .any(|t| t == "__other__")
        {
            return Err(invalid());
        }
        if self.kafka.consumer.topics.is_empty() {
            return Err(invalid());
        }
        let mut names = std::collections::BTreeSet::new();
        if self
            .kafka
            .consumer
            .topics
            .iter()
            .any(|t| !names.insert(&t.name) || t.name.is_empty())
        {
            return Err(invalid());
        }
        let mut all = vec![
            &self.kafka.common_properties,
            &self.kafka.consumer.properties,
            &self.kafka.producer.properties,
        ];
        for topic in &self.kafka.consumer.topics {
            all.push(&topic.properties);
        }
        for map in all {
            for (key, value) in map {
                if is_auth_property(key)
                    || matches!(key.as_str(), "group.id" | "client.id" | "max.poll.records")
                {
                    return Err(invalid());
                }
                if matches!(
                    key.as_str(),
                    "enable.auto.commit" | "enable.auto.offset.store"
                ) && value != "false"
                {
                    return Err(invalid());
                }
                if key == "statistics.interval.ms"
                    && value.parse::<u64>().ok()
                        != Some(self.metrics.kafka_statistics_interval_ms.get())
                {
                    return Err(invalid());
                }
            }
        }
        for topic in &self.kafka.consumer.topics {
            if topic
                .group_id
                .as_ref()
                .or(self.kafka.consumer.group_id.as_ref())
                .is_none_or(|s| s.is_empty())
            {
                return Err(invalid());
            }
            let props = self.merged_consumer(topic);
            if !props.contains_key("bootstrap.servers") {
                return Err(invalid());
            }
        }
        let producer = self.merged_producer();
        if !producer.contains_key("bootstrap.servers") {
            return Err(invalid());
        }
        let acks = producer.get("acks").map(String::as_str).unwrap_or("all");
        let idem = producer
            .get("enable.idempotence")
            .map(String::as_str)
            .unwrap_or("true");
        if !matches!(acks, "all" | "-1") || idem != "true" {
            return Err(invalid());
        }
        for profile in self.kafka.auth_profiles.values() {
            validate_profile(profile, check_files)?;
        }
        for name in [
            self.kafka.auth_profile.as_ref(),
            self.kafka.consumer.auth_profile.as_ref(),
            self.kafka.producer.auth_profile.as_ref(),
        ]
        .into_iter()
        .flatten()
        .chain(
            self.kafka
                .consumer
                .topics
                .iter()
                .filter_map(|t| t.auth_profile.as_ref()),
        ) {
            if !self.kafka.auth_profiles.contains_key(name) {
                return Err(invalid());
            }
        }
        if self.kafka.consumer.topics.iter().any(|t| {
            !self
                .kafka
                .auth_profiles
                .contains_key(self.consumer_profile(t))
        }) || !self
            .kafka
            .auth_profiles
            .contains_key(self.producer_profile())
        {
            return Err(invalid());
        }
        Ok(())
    }

    fn merged_consumer(&self, topic: &TopicConfig) -> BTreeMap<String, String> {
        let mut result = self.kafka.common_properties.clone();
        result.extend(self.kafka.consumer.properties.clone());
        result.extend(topic.properties.clone());
        result
    }
    fn merged_producer(&self) -> BTreeMap<String, String> {
        let mut result = self.kafka.common_properties.clone();
        result.extend(self.kafka.producer.properties.clone());
        result
    }
    pub(crate) fn consumer_profile<'a>(&'a self, topic: &'a TopicConfig) -> &'a str {
        topic
            .auth_profile
            .as_deref()
            .or(self.kafka.consumer.auth_profile.as_deref())
            .or(self.kafka.auth_profile.as_deref())
            .unwrap_or("")
    }
    pub(crate) fn producer_profile(&self) -> &str {
        self.kafka
            .producer
            .auth_profile
            .as_deref()
            .or(self.kafka.auth_profile.as_deref())
            .unwrap_or("")
    }
    pub(crate) fn consumer_properties(
        &self,
        topic: &TopicConfig,
        instance: usize,
    ) -> BTreeMap<String, String> {
        let mut result = self.merged_consumer(topic);
        result.insert(
            "group.id".into(),
            topic
                .group_id
                .as_ref()
                .or(self.kafka.consumer.group_id.as_ref())
                .cloned()
                .unwrap_or_default(),
        );
        result.insert("enable.auto.commit".into(), "false".into());
        result.insert("enable.auto.offset.store".into(), "false".into());
        result.insert(
            "client.id".into(),
            format!(
                "{}-{}-{}-{}",
                self.service.name,
                std::process::id(),
                topic_index(&self.kafka.consumer.topics, topic),
                instance
            ),
        );
        result.insert(
            "statistics.interval.ms".into(),
            self.metrics.kafka_statistics_interval_ms.to_string(),
        );
        result
    }
    pub(crate) fn producer_properties(&self) -> BTreeMap<String, String> {
        let mut result = self.merged_producer();
        result.entry("acks".into()).or_insert_with(|| "all".into());
        result
            .entry("enable.idempotence".into())
            .or_insert_with(|| "true".into());
        result
            .entry("queue.buffering.max.messages".into())
            .or_insert_with(|| "10000".into());
        result
            .entry("queue.buffering.max.kbytes".into())
            .or_insert_with(|| "16384".into());
        result.insert(
            "client.id".into(),
            format!("{}-{}-producer", self.service.name, std::process::id()),
        );
        result.insert(
            "statistics.interval.ms".into(),
            self.metrics.kafka_statistics_interval_ms.to_string(),
        );
        result
    }
    fn resolve_paths(&mut self, base: &Path) {
        let resolve = |p: &mut PathBuf| {
            if p.is_relative() {
                *p = base.join(&*p);
            }
        };
        if let Some(tls) = &mut self.http.tls {
            resolve(&mut tls.cert_file);
            resolve(&mut tls.key_file);
        }
        for profile in self.kafka.auth_profiles.values_mut() {
            match profile {
                AuthProfile::Plaintext | AuthProfile::AwsMskIam { .. } => (),
                AuthProfile::Tls { ca_file } => opt_path(ca_file, &resolve),
                AuthProfile::Mtls {
                    cert_file,
                    key_file,
                    key_password_file,
                    ca_file,
                } => {
                    resolve(cert_file);
                    resolve(key_file);
                    opt_path(key_password_file, &resolve);
                    opt_path(ca_file, &resolve);
                }
                AuthProfile::SaslPlain {
                    username_file,
                    password_file,
                    ca_file,
                }
                | AuthProfile::SaslScram {
                    username_file,
                    password_file,
                    ca_file,
                    ..
                } => {
                    resolve(username_file);
                    resolve(password_file);
                    opt_path(ca_file, &resolve);
                }
            }
        }
    }
}
fn opt_path<F: Fn(&mut PathBuf)>(p: &mut Option<PathBuf>, f: &F) {
    if let Some(path) = p {
        f(path);
    }
}
fn topic_index(topics: &[TopicConfig], topic: &TopicConfig) -> usize {
    topics
        .iter()
        .position(|t| std::ptr::eq(t, topic))
        .unwrap_or(0)
}
fn is_auth_property(key: &str) -> bool {
    key == "security.protocol"
        || key.starts_with("sasl.")
        || key.starts_with("ssl.")
        || key == "enable.ssl.certificate.verification"
}
fn validate_profile(profile: &AuthProfile, check_files: bool) -> Result<(), ConfigError> {
    let mut paths: Vec<&PathBuf> = Vec::new();
    match profile {
        AuthProfile::Plaintext => (),
        AuthProfile::Tls { ca_file } => paths.extend(ca_file.iter()),
        AuthProfile::Mtls {
            cert_file,
            key_file,
            key_password_file,
            ca_file,
        } => {
            paths.extend([cert_file, key_file]);
            paths.extend(key_password_file.iter());
            paths.extend(ca_file.iter());
        }
        AuthProfile::SaslPlain {
            username_file,
            password_file,
            ca_file,
        }
        | AuthProfile::SaslScram {
            username_file,
            password_file,
            ca_file,
            ..
        } => {
            paths.extend([username_file, password_file]);
            paths.extend(ca_file.iter());
        }
        AuthProfile::AwsMskIam {
            region,
            credentials,
        } if region.trim().is_empty() || credentials != "default_chain" => {
            return Err(ConfigError::Invalid);
        }
        AuthProfile::AwsMskIam { .. } => (),
    }
    if check_files && paths.into_iter().any(|p| std::fs::File::open(p).is_err()) {
        return Err(ConfigError::File);
    }
    Ok(())
}
