use serde::{Deserialize, Serialize};

use crate::cluster_api::feature_store::storage_connector::{
    FeatureStoreJdbcConnectorDTO, FeatureStoreKafkaConnectorDTO,
};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct FeatureStoreKafkaConnector {
    pub(crate) feature_store_id: i32,
    pub(crate) external_kafka: bool,
    pub(crate) bootstrap_servers: String,
    pub(crate) security_protocol: String,
    pub(crate) ssl_endpoint_identification_algorithm: String,
    pub(crate) options: Vec<String>,
    pub(crate) storage_connector_type: String,
    pub(crate) id: i32,
    pub(crate) description: String,
    pub(crate) name: String,
}

impl FeatureStoreKafkaConnector {
    pub fn feature_store_id(&self) -> i32 {
        self.feature_store_id
    }

    pub fn external_kafka(&self) -> bool {
        self.external_kafka
    }

    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    pub fn security_protocol(&self) -> &str {
        &self.security_protocol
    }

    pub fn ssl_endpoint_identification_algorithm(&self) -> &str {
        &self.ssl_endpoint_identification_algorithm
    }
}

impl FeatureStoreKafkaConnector {
    pub fn new_test() -> Self {
        Self {
            feature_store_id: 1,
            external_kafka: true,
            bootstrap_servers: "localhost:9092".to_string(),
            security_protocol: "SASL_SSL".to_string(),
            ssl_endpoint_identification_algorithm: "https".to_string(),
            options: vec![],
            storage_connector_type: "kafka".to_string(),
            id: 1,
            description: "description".to_string(),
            name: "name".to_string(),
        }
    }
}

impl From<FeatureStoreKafkaConnectorDTO> for FeatureStoreKafkaConnector {
    fn from(dto: FeatureStoreKafkaConnectorDTO) -> Self {
        Self {
            feature_store_id: dto.feature_store_id,
            external_kafka: dto.external_kafka,
            bootstrap_servers: dto.bootstrap_servers,
            security_protocol: dto.security_protocol,
            ssl_endpoint_identification_algorithm: dto.ssl_endpoint_identification_algorithm,
            options: dto.options,
            storage_connector_type: dto.storage_connector_type,
            id: dto.id,
            description: dto.description,
            name: dto.name,
        }
    }
}

pub struct FeatureStoreOnlineConnector {
    connection_string: String,
    arguments: Vec<std::collections::HashMap<String, String>>,
}

impl FeatureStoreOnlineConnector {
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    pub fn arguments(&self) -> &[std::collections::HashMap<String, String>] {
        &self.arguments
    }
}

impl From<FeatureStoreJdbcConnectorDTO> for FeatureStoreOnlineConnector {
    fn from(dto: FeatureStoreJdbcConnectorDTO) -> Self {
        Self {
            connection_string: dto.connection_string,
            arguments: dto.arguments,
        }
    }
}

pub struct FeatureStoreJdbcConnector {}
