use std::collections::HashMap;

use serde::{Serialize, Deserialize};

use crate::repositories::query::payloads::FilterArrowFlightPayload;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ExternalFeatureGroupConnectorArrowFlightPayload {
    #[serde(rename = "type")]
    pub connector_type: String,
    pub options: Option<HashMap<String, String>>,
    pub query: Option<String>,
    pub alias: Option<String>,
    pub filters: Option<Vec<FilterArrowFlightPayload>>,
}