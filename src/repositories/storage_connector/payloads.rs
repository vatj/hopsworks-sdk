use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::repositories::query::payloads::QueryFilterOrLogicArrowFlightPayload;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct FeatureGroupConnectorArrowFlightPayload {
    #[serde(rename = "type")]
    pub connector_type: String,
    pub options: Option<HashMap<String, String>>,
    pub query: Option<String>,
    pub alias: Option<String>,
    pub filters: Option<Vec<QueryFilterOrLogicArrowFlightPayload>>,
}

impl FeatureGroupConnectorArrowFlightPayload {
    pub fn new_hudi_connector() -> Self {
        Self {
            connector_type: "hudi".to_string(),
            options: None,
            query: None,
            alias: None,
            filters: None,
        }
    }
}
