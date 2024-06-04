

use color_eyre::Result;
use rdkafka::producer::FutureProducer;
use rdkafka::ClientConfig;

use hopsworks_core::feature_store::storage_connector::FeatureStoreKafkaConnector;

pub async fn setup_future_producer(
    kafka_connector: FeatureStoreKafkaConnector,
    cert_dir: &str,
) -> Result<FutureProducer> {
    let bootstrap_servers =
        std::env::var("HOPSWORKS_KAFKA_BROKERS").unwrap_or(kafka_connector.bootstrap_servers().to_string());
    Ok(ClientConfig::new()
        .set("bootstrap.servers", bootstrap_servers)
        .set("message.timeout.ms", "300000")
        .set("security.protocol", "SSL")
        .set("ssl.endpoint.identification.algorithm", "none")
        .set("ssl.ca.location", format!("{cert_dir}/ca_chain.pem"))
        .set(
            "ssl.certificate.location",
            format!("{cert_dir}/client_cert.pem"),
        )
        .set("ssl.key.location", format!("{cert_dir}/client_key.pem"))
        // jks truststore not supported by rdkafka, get cert key from Hopsworks client
        // .set("debug", "all")
        .create()
        .expect("Error setting up kafka producer"))
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
        let result = setup_future_producer(kafka_connector, cert_dir).await;

        // Assert
        assert!(result.is_ok());
    }
}