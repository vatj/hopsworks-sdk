use serde::{Deserialize, Serialize};

pub mod kafka_connector;
pub mod online_connector;

pub use kafka_connector::FeatureStoreKafkaConnector;
pub use online_connector::FeatureStoreOnlineConnector;

#[derive(Serialize, Deserialize, Debug, Clone)]
pub enum StorageConnector {
    Kafka(FeatureStoreKafkaConnector),
    Online(FeatureStoreOnlineConnector),
    Jdbc(FeatureStoreJdbcConnector),
    HopsFs(FeatureStoreHopsFsConnector),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FeatureStoreJdbcConnector {}

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FeatureStoreHopsFsConnector {}
