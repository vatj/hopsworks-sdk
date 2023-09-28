use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct FeatureStoreKafkaConnectorDTO {
    #[serde(rename = "type")]
    _type: String,
    pub bootstrap_servers: String,
    security_protocol: String,
    ssl_endpoint_identification_algorithm: String,
    options: String,
    external_kafka: bool,
    id: i32,
    description: String,
    name: String,
    featurestore_id: i32,
    storage_connector_type: String,
}