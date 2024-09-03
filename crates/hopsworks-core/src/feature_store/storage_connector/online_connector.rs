use serde::{Deserialize, Serialize};

use crate::cluster_api::feature_store::storage_connector::FeatureStoreJdbcConnectorDTO;

#[derive(Serialize, Deserialize, Debug, Clone)]
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
