use color_eyre::Result;
use rdkafka::ClientConfig;

use hopsworks_core::feature_store::storage_connector::FeatureStoreKafkaConnector;

#[tracing::instrument]
pub fn setup_kafka_configuration(
    kafka_connector: FeatureStoreKafkaConnector,
    cert_dir: &str,
) -> Result<ClientConfig> {
    let bootstrap_servers =
        std::env::var("HOPSWORKS_KAFKA_BROKERS").unwrap_or(kafka_connector.bootstrap_servers().to_string());

    // Experiment with different configurations
    let queue_buffering_max_ms = std::env::var("HOPSWORKS_KAFKA_PRODUCER_QUEUE_BUFFERING_MAX_MS").unwrap_or("5".to_string()); // Equivalent to linger.ms
    let batch_num_messages = std::env::var("HOPSWORKS_KAFKA_PRODUCER_BATCH_NUM_MESSAGES").unwrap_or("10000".to_string());
    let queue_buffering_max_messages = std::env::var("HOPSWORKS_KAFKA_PRODUCER_QUEUE_BUFFERING_MAX_MESSAGES").unwrap_or("100000".to_string());
    let queue_buffering_max_kbytes = std::env::var("HOPSWORKS_KAFKA_PRODUCER_QUEUE_BUFFERING_MAX_KBYTES").unwrap_or("4000000".to_string());
    let log_debug_kafka = std::env::var("HOPOSWORKS_KAFKA_PRODUCER_LOG_DEBUG");

    let mut config = ClientConfig::new();
    config
        .set("bootstrap.servers", bootstrap_servers)
        // Hopsworks specific, jks truststore not supported by rdkafka, get cert key from Hopsworks client
        .set("security.protocol", "SSL")
        .set("ssl.endpoint.identification.algorithm", "none")
        .set("ssl.ca.location", format!("{cert_dir}/ca_chain.pem"))
        .set(
            "ssl.certificate.location",
            format!("{cert_dir}/client_cert.pem"),
        )
        .set("ssl.key.location", format!("{cert_dir}/client_key.pem"))
        // End of Hopsworks specific configuration
        .set("message.timeout.ms", "300000")
        .set("queue.buffering.max.ms", queue_buffering_max_ms.as_str())
        .set("batch.num.messages", batch_num_messages.as_str())
        .set("queue.buffering.max.messages", queue_buffering_max_messages.as_str())
        .set("queue.buffering.max.kbytes", queue_buffering_max_kbytes.as_str());

    if let Ok(debug_kafka) = log_debug_kafka {
        if !debug_kafka.is_empty() {
            config.set("debug", debug_kafka.as_str());
            config.set_log_level(rdkafka::config::RDKafkaLogLevel::Debug);
        }
    }
    tracing::info!("Setting up Hopsworks Kafka producer");
    tracing::debug!("Kafka producer config: {:#?}", config);

    Ok(config)
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_setup_future_producer() {
        // Arrange
        let kafka_connector = FeatureStoreKafkaConnector::new_test();
        let cert_dir = "test_cert_dir";

        // Act
        let result = setup_kafka_configuration(kafka_connector, cert_dir);

        // Assert
        assert!(result.is_ok());
    }
}