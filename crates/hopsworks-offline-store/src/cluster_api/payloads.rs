use serde::{Deserialize, Serialize};
use std::collections::HashMap;

use hopsworks_core::feature_store::query::enums::{QueryFilterCondition, QueryLogicType};

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

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryArrowFlightPayload {
    pub query_string: String,
    pub connectors: Option<HashMap<String, FeatureGroupConnectorArrowFlightPayload>>,
    pub filters: Option<Vec<QueryFilterOrLogicArrowFlightPayload>>,
    pub features: HashMap<String, Vec<String>>, // key is feature_group_name, value is vec of feature names
}

impl QueryArrowFlightPayload {
    pub fn new(
        duckdb_query_string: String,
        serialized_feature_names: HashMap<String, Vec<String>>,
        serialized_connectors: Option<HashMap<String, FeatureGroupConnectorArrowFlightPayload>>,
        serialized_filters: Option<Vec<QueryFilterOrLogicArrowFlightPayload>>,
    ) -> Self {
        Self {
            query_string: duckdb_query_string,
            connectors: serialized_connectors,
            filters: serialized_filters,
            features: serialized_feature_names,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryFilterArrowFlightPayload {
    #[serde(rename = "type")]
    pub filter_type: String,
    pub condition: QueryFilterCondition,
    pub value: serde_json::Value,
    pub feature: String,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct QueryLogicArrowFlightPayload {
    #[serde(rename = "type")]
    pub filter_type: String,
    pub logic_type: QueryLogicType,
    pub left_filter: Option<Box<QueryFilterOrLogicArrowFlightPayload>>,
    pub right_filter: Option<Box<QueryFilterOrLogicArrowFlightPayload>>,
}

impl QueryFilterArrowFlightPayload {
    pub fn new(condition: QueryFilterCondition, value: serde_json::Value, feature: String) -> Self {
        Self {
            filter_type: "filter".to_string(),
            condition,
            value,
            feature,
        }
    }
}

impl QueryLogicArrowFlightPayload {
    pub fn new(
        logic_type: QueryLogicType,
        left_filter: Option<Box<QueryFilterOrLogicArrowFlightPayload>>,
        right_filter: Option<Box<QueryFilterOrLogicArrowFlightPayload>>,
    ) -> Self {
        Self {
            filter_type: "logic".to_string(),
            logic_type,
            left_filter,
            right_filter,
        }
    }
}

pub enum QueryFilterOrLogicArrowFlightPayload {
    Filter(QueryFilterArrowFlightPayload),
    Logic(QueryLogicArrowFlightPayload),
}

impl Serialize for QueryFilterOrLogicArrowFlightPayload {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        match *self {
            QueryFilterOrLogicArrowFlightPayload::Filter(ref filter) => {
                filter.serialize(serializer)
            }
            QueryFilterOrLogicArrowFlightPayload::Logic(ref logic) => logic.serialize(serializer),
        }
    }
}

impl<'de> Deserialize<'de> for QueryFilterOrLogicArrowFlightPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let value = serde_json::Value::deserialize(deserializer)?;
        if value.is_object() {
            let filter = serde_json::from_value(value.clone());
            if let Ok(filter) = filter {
                return Ok(QueryFilterOrLogicArrowFlightPayload::Filter(filter));
            }
            let logic = serde_json::from_value(value.clone());
            if let Ok(logic) = logic {
                return Ok(QueryFilterOrLogicArrowFlightPayload::Logic(logic));
            }
            Err(serde::de::Error::custom(
                "expected a JSON object for QueryFilterOrLogicArrowFlightPayload",
            ))
        } else {
            Err(serde::de::Error::custom(
                "expected a JSON object for QueryFilterOrLogicArrowFlightPayload",
            ))
        }
    }
}

impl Clone for QueryFilterOrLogicArrowFlightPayload {
    fn clone(&self) -> Self {
        match *self {
            QueryFilterOrLogicArrowFlightPayload::Filter(ref filter) => {
                QueryFilterOrLogicArrowFlightPayload::Filter(filter.clone())
            }
            QueryFilterOrLogicArrowFlightPayload::Logic(ref logic) => {
                QueryFilterOrLogicArrowFlightPayload::Logic(logic.clone())
            }
        }
    }
}

impl std::fmt::Debug for QueryFilterOrLogicArrowFlightPayload {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            QueryFilterOrLogicArrowFlightPayload::Filter(ref filter) => filter.fmt(f),
            QueryFilterOrLogicArrowFlightPayload::Logic(ref logic) => logic.fmt(f),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct TrainingDatasetArrowFlightPayload {
    pub fs_name: String,
    pub fv_name: String,
    pub fv_version: i32,
    pub tds_version: i32,
    pub tds_query: String,
}

impl TrainingDatasetArrowFlightPayload {
    pub fn new(
        fs_name: String,
        fv_name: String,
        fv_version: i32,
        tds_version: i32,
        tds_query: String,
    ) -> Self {
        Self {
            fs_name,
            fv_name,
            fv_version,
            tds_version,
            tds_query,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct RegisterArrowFlightClientCertificatePayload {
    tstore: String,
    kstore: String,
    cert_key: String,
}

impl RegisterArrowFlightClientCertificatePayload {
    pub fn new(tstore: String, kstore: String, cert_key: String) -> Self {
        Self {
            tstore,
            kstore,
            cert_key,
        }
    }
}
