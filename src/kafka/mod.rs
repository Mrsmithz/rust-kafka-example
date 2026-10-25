pub(crate) mod auth;
pub(crate) mod batch;
pub(crate) mod consumer;
pub(crate) mod context;
pub(crate) mod producer;

#[cfg(test)]
mod tests {
    use rdkafka::ClientConfig;

    #[test]
    fn native_artifact_supports_required_security_mechanisms()
    -> Result<(), rdkafka::error::KafkaError> {
        let mut config = ClientConfig::new();
        config.set(
            "builtin.features",
            "ssl,sasl_plain,sasl_scram,sasl_oauthbearer",
        );
        config.create_native_config()?;
        Ok(())
    }
}
